# hashring

Implements consistent hashing that can be used when the number of server nodes can increase or decrease (like in memcached).

# How to use

```go
import (
	"fmt"
	"hash/fnv"
	"hashring"
)

func callHashRing() {
	
	replicaCount:= 2
	ring := hashring.New(replicaCount, fnv.New32a())
	ring.AddMember("10.10.10.1")
	members := []hashring.Member{"10.10.10.2", "10.10.10.3", "10.10.10.4"}
	ring.AddMembers(members)

	key := "abc"

	node, _ := ring.Locate(key)

	fmt.Println(node)
}

```

Remove a member from the ring

```go
ring.RemoveMember("10.10.10.1")
```