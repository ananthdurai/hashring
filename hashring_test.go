package hashring

import (
	"reflect"
	"sort"
	"testing"
)

func TestNodeIdx_Sort(t *testing.T) {
	tests := []struct {
		v memberList
		r memberList
	}{
		{
			v: memberList{5, 1, 3, 9, 10, 0, 78},
			r: memberList{0, 1, 3, 5, 9, 10, 78},
		},

		{
			v: memberList{9, 3, 5, 4, 2, 3, 6},
			r: memberList{2, 3, 3, 4, 5, 6, 9},
		},
	}

	for _, c := range tests {
		sort.Sort(c.v)
		if !reflect.DeepEqual(c.v, c.r) {
			t.Fatalf("expected %v but got %v\n", c.r, c.v)
		}
	}
}

func TestHashRing_AddNode(t *testing.T) {
	tests := []struct {
		virtualNodeCount int
		nodes            []Member
		requiredCount    int
	}{
		{
			virtualNodeCount: 1,
			nodes:            []Member{"abc_node"},
			requiredCount:    1,
		},

		{
			virtualNodeCount: 3,
			nodes:            []Member{"abc_node"},
			requiredCount:    3,
		},

		{
			virtualNodeCount: 4,
			nodes:            []Member{"abc_1", "abc_2"},
			requiredCount:    8,
		},

		{
			virtualNodeCount: 1,
			nodes:            []Member{"abc_1", "abc_2", "abc_3"},
			requiredCount:    3,
		},
	}

	testContains := func(hr *HashRing, nodes []Member) bool {
		result := true
		for _, node := range nodes {
			count := 0
			for _, v := range hr.members {
				if v == node {
					count++
				}
			}

			result = result && (count == hr.replicaCount)
		}

		return result
	}

	for _, c := range tests {
		hr := New(c.virtualNodeCount, nil)
		hr.AddMembers(c.nodes)

		if !(testContains(hr, c.nodes) && len(hr.idx) == c.virtualNodeCount*len(c.nodes)) {
			t.Fatalf("node count didn't match: %+v\n", hr)
		}
	}
}

func TestHashRing_Get(t *testing.T) {
	tests := []struct {
		replicas int
		nodes    []Member
		keys     []string
	}{
		{
			replicas: 2,
			nodes:    []Member{"10.10.10.1", "10.10.10.2", "10.10.10.3"},
			keys:     []string{"abc", "cde", "some random key", "found it", "some random key", "abc"},
		},

		{
			replicas: 4,
			nodes:    []Member{"10.10.10.1", "10.10.10.2"},
			keys:     []string{"abc", "cde", "some random key", "found it", "cde", "found it"},
		},

		{
			replicas: 1,
			nodes:    []Member{"10.10.10.1", "10.10.10.2", "10.10.10.3", "10.10.10.4"},
			keys:     []string{"abc", "cde", "some random key", "found it", "abc", "cde"},
		},
	}

	testContains := func(list []Member, item Member) bool {
		for _, l := range list {
			if l == item {
				return true
			}
		}

		return false
	}

	for _, c := range tests {
		hr := New(c.replicas, nil)
		hr.AddMembers(c.nodes)
		for _, k := range c.keys {
			n, err := hr.Locate(k)
			if err != nil {
				t.Fatalf("unexpected error: %v\n", err)
			}

			if !testContains(c.nodes, n) {
				t.Fatalf("unexpected node found: %s\n", n)
			}
		}
	}

}
