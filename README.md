Rendezvous
==========

`rendezvous` is a Go implementation of [rendezvous hashing][wikipedia] (also
known as highest random weight hashing). It uses md5 to compute key weights to
distribute keys as evenly as possible, even with short keys or keys sharing
common prefixes / suffixes.

[API documentation][api]

[wikipedia]: http://en.wikipedia.org/wiki/Rendezvous_hashing
[api]: http://godoc.org/github.com/stvp/rendezvous
