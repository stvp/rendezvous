package rendezvous

import (
	"bytes"
	"crypto/md5"
	"sort"
)

type nodeScore struct {
	node  string
	score uint32
}

type byScore []nodeScore

func (s byScore) Len() int           { return len(s) }
func (s byScore) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s byScore) Less(i, j int) bool { return s[i].score < s[j].score }

type Hash struct {
	nodes []nodeScore
	buf   bytes.Buffer
}

// New creates a new Hash with the given keys (optional).
func New(nodes ...string) *Hash {
	hash := &Hash{}
	hash.Add(nodes...)
	return hash
}

// Add takes any number of nodes and adds them to this Hash.
func (h *Hash) Add(nodes ...string) {
	for _, node := range nodes {
		h.nodes = append(h.nodes, nodeScore{node, 0})
	}
}

// Get returns the node with the highest score for the given key. If this Hash
// has no nodes, an empty string is returned.
func (h *Hash) Get(key string) string {
	var maxScore uint32
	var maxNode string
	var score uint32

	for _, node := range h.nodes {
		score = h.hash(node.node, key)
		if score > maxScore {
			maxScore = score
			maxNode = node.node
		}
	}

	return maxNode
}

// GetN returns n nodes for the given key, ordered by descending score.
func (h *Hash) GetN(n int, key string) []string {
	if len(h.nodes) == 0 || n == 0 {
		return []string{}
	}

	if n > len(h.nodes) {
		n = len(h.nodes)
	}

	for i := 0; i < len(h.nodes); i++ {
		h.nodes[i].score = h.hash(h.nodes[i].node, key)
	}
	sort.Sort(sort.Reverse(byScore(h.nodes)))

	nodes := make([]string, n)
	for i := 0; i < n; i++ {
		nodes[i] = string(h.nodes[i].node)
	}
	return nodes
}

func (h *Hash) hash(node, key string) (val uint32) {
	h.buf.Reset()
	h.buf.WriteString(node)
	h.buf.WriteString(key)
	for _, b := range md5.Sum(h.buf.Bytes()) {
		val = val + uint32(b)
	}
	return val
}
