package hashring

import (
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"hash"
	"hash/fnv"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"
)

var (
	LocateKeyFailure = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "prom_agg_gateway",
		Subsystem: "hashing",
		Name:      "locate_key_failure",
		Help:      "Error finding the appropriate member to send the metrics",
	})

	MemberAddFailure = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "prom_agg_gateway",
		Subsystem: "hashing",
		Name:      "member_add_failure",
		Help:      "Error adding a new member",
	})

	MemberDeleteFailure = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "prom_agg_gateway",
		Subsystem: "hashing",
		Name:      "member_delete_failure",
		Help:      "Error deleting a member",
	})

	JQHashFunctionCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "prom_agg_gateway",
		Subsystem: "client",
		Name:      "hash_fn",
		Help:      "Http error rate for the failure to push metrics to global gateway",
	}, []string{"hash", "server"})
)

func init() {
	prometheus.MustRegister(
		LocateKeyFailure,
		MemberAddFailure,
		MemberDeleteFailure,
		JQHashFunctionCount,
	)
}

type Member string

func (m Member) String() string {
	return string(m)
}

// memberList type implementing Sort Interface
type memberList []uint32

// Len returns the size of memberList
func (idx memberList) Len() int {
	return len(idx)
}

// Swap swaps the ith with jth
func (idx memberList) Swap(i, j int) {
	idx[i], idx[j] = idx[j], idx[i]
}

// Less returns true if ith <= jth else false
func (idx memberList) Less(i, j int) bool {
	return idx[i] <= idx[j]
}

// HashRing to hold the members and indexes
type HashRing struct {
	members      map[uint32]Member // map to member hash -> member
	idx          memberList        // sorted hash list for the list of members.
	replicaCount int               // replicas to be inserted
	hash         hash.Hash32
	mu           sync.RWMutex // to protect above fields
}

// New returns a Hash ring with provided virtual member count and hash
// If hash is nil, fvn32a is used instead
func New(replicaCount int, hash hash.Hash32) *HashRing {
	if hash == nil {
		hash = fnv.New32a()
	}

	return &HashRing{
		members:      make(map[uint32]Member),
		replicaCount: replicaCount,
		hash:         hash,
	}
}

// hasher returns uint32 hash
func hasher(hash hash.Hash32, key []byte) (uint32, error) {
	hash.Reset()
	_, err := hash.Write(key)
	if err != nil {
		return 0, err
	}

	return hash.Sum32(), nil
}

func (hr *HashRing) AddMembers(member []Member) {
	hr.mu.Lock()
	defer hr.mu.Unlock()

	for _, member := range member {
		hr.add(member)
	}
	sort.Sort(hr.idx)
}

// AddMember adds a member to Hash ring
func (hr *HashRing) add(member Member) {
	for i := 0; i < hr.replicaCount; i++ {
		key := fmt.Sprintf("%s:%d", member.String(), i)
		hkey, err := hasher(hr.hash, []byte(key))
		if err != nil {
			MemberAddFailure.Inc()
			log.Printf("failed to add member: %v", err)
			return
		}
		hr.idx = append(hr.idx, hkey)
		hr.members[hkey] = member
	}
}

// Error safe wrapper for Locate function
func (hr *HashRing) LocateKey(key string) string {
	member, err := hr.Locate(key)
	if err != nil {
		LocateKeyFailure.Inc()
		log.Printf("LocateKey failed: %v", err)
		return ""
	}
	return member.String()
}

// Locate returns the member for a given key
// 1. Get the hash of the given key
// 2. Run binary search on memberList and find the next closest smallest number of the hash
// 3. If the next small index not found, consider first element and return the member (complete the ring)
// 4. If next big index found, return the respective member

func (hr *HashRing) Locate(key string) (member Member, err error) {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	if len(hr.idx) < 1 {
		return member, fmt.Errorf("no available members")
	}

	hkey, err := hasher(hr.hash, []byte(key))

	if err != nil {
		return member, fmt.Errorf("failed to fetch member: %v", err)
	}

	pos := sort.Search(len(hr.idx), func(i int) bool { return hr.idx[i] >= hkey })
	if pos == len(hr.idx) {
		pos = 0
	}
	endpointMember := hr.members[hr.idx[pos]]
	if strings.Contains(key, "execute_duration_seconds_bucket") {
		JQHashFunctionCount.With(prometheus.Labels{"hash": strconv.FormatUint(uint64(hkey), 10), "server": endpointMember.String()}).Inc()
	}
	return endpointMember, nil
}

func (hr *HashRing) GetMembers() []Member {
	var members []Member
	hr.mu.RLock()
	defer hr.mu.RUnlock()
	for _, mem := range hr.members {
		members = append(members, mem)
	}
	return members
}

func (hr *HashRing) GetRing() map[uint32]Member {
	hr.mu.RLock()
	defer hr.mu.RUnlock()
	ring := make(map[uint32]Member)
	for k, v := range hr.members {
		ring[k] = v
	}
	return ring
}

func MemberChecksum(hash hash.Hash32, memberList []Member) (uint32, error) {
	return hasher(hash, []byte(fmt.Sprintf("%v", memberList)))
}
