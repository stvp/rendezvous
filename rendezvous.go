package rendezvous

import (
	"bytes"
	"crypto/md5"
	"sort"
	"sync"
)

type nodeScore struct {
	node  string
	score uint32
}

type nodeScores []nodeScore

func (n nodeScores) Len() int           { return len(n) }
func (n nodeScores) Swap(i, j int)      { n[i], n[j] = n[j], n[i] }
func (n nodeScores) Less(i, j int) bool { return n[i].score < n[j].score }

type Hash struct {
	nodes   []string
	bufPool sync.Pool
}

// New creates a new Hash with the given keys.
func New(nodes ...string) *Hash {
	return &Hash{
		nodes: nodes,
		bufPool: sync.Pool{New: func() interface{} {
			return &bytes.Buffer{}
		}},
	}
}

// Get returns the node with the highest score for the given key. If this Hash
// has no nodes, an empty string is returned.
func (h *Hash) Get(key string) string {
	var maxScore uint32
	var maxNode string
	var score uint32

	for _, node := range h.nodes {
		score = h.hash(node, key)
		if score > maxScore {
			maxScore = score
			maxNode = node
		}
	}

	return maxNode
}

// GetN returns n nodes for the given key, ordered by descending score.
func (h *Hash) GetN(n int, key string) (nodes []string) {
	if len(h.nodes) == 0 || n == 0 {
		return []string{}
	}

	if n > len(h.nodes) {
		n = len(h.nodes)
	}

	ns := make(nodeScores, len(h.nodes))
	for i, node := range h.nodes {
		ns[i] = nodeScore{node, h.hash(node, key)}
	}
	sort.Sort(sort.Reverse(ns))

	nodes = make([]string, n)
	for i := 0; i < n; i++ {
		nodes[i] = string(ns[i].node)
	}
	return nodes
}

func (h *Hash) hash(node, key string) (val uint32) {
	buf := h.bufPool.Get().(*bytes.Buffer)
	buf.WriteString(node)
	buf.WriteString(key)
	for _, b := range md5.Sum(buf.Bytes()) {
		val = val + uint32(b)
	}
	buf.Reset()
	h.bufPool.Put(buf)
	return val
}
