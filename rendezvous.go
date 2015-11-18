package rendezvous

import (
	"bytes"
	"crypto/md5"
	"sort"
	"sync"
)

// Table implements a rendezvous hash table.
type Table struct {
	nodes   []string
	bufPool sync.Pool
}

// New creates a new rendezvous hash table with the given nodes.
func New(nodes []string) *Table {
	localNodes := make([]string, len(nodes))
	copy(localNodes, nodes)

	return &Table{
		nodes: localNodes,
		bufPool: sync.Pool{New: func() interface{} {
			return &bytes.Buffer{}
		}},
	}
}

// Get returns the node with the highest score for the given key. If this Table
// has no nodes, an empty string is returned. Get is an O(n) operation, where n
// is the number of nodes in this table.
func (t *Table) Get(key string) string {
	var maxScore uint32
	var maxNode string
	var score uint32

	for _, node := range t.nodes {
		score = t.score(node, key)
		if score > maxScore {
			maxScore = score
			maxNode = node
		}
	}

	return maxNode
}

// GetN returns n number of nodes for the given key, ordered by descending
// score. If n is -1, all nodes are returned.
func (t *Table) GetN(n int, key string) (nodes []string) {
	if n == 0 || len(t.nodes) == 0 {
		return []string{}
	}

	if n < 0 || n > len(t.nodes) {
		n = len(t.nodes)
	}

	ns := make(nodeScores, len(t.nodes))
	for i, node := range t.nodes {
		ns[i] = nodeScore{node, t.score(node, key)}
	}
	sort.Sort(sort.Reverse(ns))

	nodes = make([]string, n)
	for i := 0; i < n; i++ {
		nodes[i] = string(ns[i].node)
	}
	return nodes
}

func (t *Table) score(node, key string) (val uint32) {
	buf := t.bufPool.Get().(*bytes.Buffer)

	buf.WriteString(node)
	buf.WriteString(key)
	for _, b := range md5.Sum(buf.Bytes()) {
		val = val + uint32(b)
	}

	buf.Reset()
	t.bufPool.Put(buf)

	return val
}

type nodeScore struct {
	node  string
	score uint32
}

type nodeScores []nodeScore

func (n nodeScores) Len() int           { return len(n) }
func (n nodeScores) Swap(i, j int)      { n[i], n[j] = n[j], n[i] }
func (n nodeScores) Less(i, j int) bool { return n[i].score < n[j].score }
