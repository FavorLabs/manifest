// Copyright 2020 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mantaray

import (
	"bytes"
	"context"
	"errors"
	"fmt"
)

const (
	PathSeparator = '/' // path separator
)

var (
	ZeroObfuscationKey []byte
)

func init() {
	ZeroObfuscationKey = make([]byte, 32)
}

// Error used when lookup path does not match
var (
	ErrNotFound         = errors.New("not found")
	ErrEmptyPath        = errors.New("empty path")
	ErrInvalidFile      = errors.New("invalid file")
	ErrMetadataTooLarge = errors.New("metadata too large")
	ErrForbiddenAction  = errors.New("forbidden action")
)

// Node represents a mantaray Node
type Node struct {
	nodeType       uint8
	refBytesSize   int
	index          int64
	obfuscationKey []byte
	ref            []byte // reference to uninstantiated Node persisted serialised
	entry          []byte
	metadata       map[string]string
	forks          map[byte]*fork
}

type fork struct {
	prefix []byte // the non-branching part of the subpath
	*Node         // in memory structure that represents the Node
}

const (
	nodeTypeValue             = uint8(2)
	nodeTypeEdge              = uint8(4)
	nodeTypeWithPathSeparator = uint8(8)
	nodeTypeWithMetadata      = uint8(16)
	nodeTypeEmptyDirectory    = uint8(32)

	nodeTypeMask = uint8(255)
)

func nodeTypeIsWithMetadataType(nodeType uint8) bool {
	return nodeType&nodeTypeWithMetadata == nodeTypeWithMetadata
}

// NewNodeRef is the exported Node constructor used to represent manifests by reference
func NewNodeRef(ref []byte) *Node {
	return &Node{ref: ref}
}

// New is the constructor for in-memory Node structure
func New() *Node {
	return &Node{forks: make(map[byte]*fork)}
}

func notFound(path []byte) error {
	return fmt.Errorf("entry on '%s' ('%x'): %w", path, path, ErrNotFound)
}

// IsValueType returns true if the node contains entry.
func (n *Node) IsValueType() bool {
	return n.nodeType&nodeTypeValue == nodeTypeValue
}

// IsEdgeType returns true if the node forks into other nodes.
func (n *Node) IsEdgeType() bool {
	return n.nodeType&nodeTypeEdge == nodeTypeEdge
}

// IsWithPathSeparatorType returns true if the node path contains separator character.
func (n *Node) IsWithPathSeparatorType() bool {
	return n.nodeType&nodeTypeWithPathSeparator == nodeTypeWithPathSeparator
}

// IsWithMetadataType returns true if the node contains metadata.
func (n *Node) IsWithMetadataType() bool {
	return n.nodeType&nodeTypeWithMetadata == nodeTypeWithMetadata
}

func (n *Node) IsEmptyDirectory() bool {
	return n.nodeType&nodeTypeEmptyDirectory == nodeTypeEmptyDirectory
}

func (n *Node) makeValue() {
	n.nodeType = n.nodeType | nodeTypeValue
}

func (n *Node) makeEdge() {
	n.nodeType = n.nodeType | nodeTypeEdge
}

func (n *Node) makeWithPathSeparator() {
	n.nodeType = n.nodeType | nodeTypeWithPathSeparator
}

func (n *Node) makeWithMetadata() {
	n.nodeType = n.nodeType | nodeTypeWithMetadata
}

func (n *Node) makeEmptyDirectory() {
	n.nodeType = n.nodeType | nodeTypeEmptyDirectory
}

// nolint,unused
func (n *Node) makeNotValue() {
	n.nodeType = (nodeTypeMask ^ nodeTypeValue) & n.nodeType
}

// nolint,unused
func (n *Node) makeNotEdge() {
	n.nodeType = (nodeTypeMask ^ nodeTypeEdge) & n.nodeType
}

func (n *Node) makeNotWithPathSeparator() {
	n.nodeType = (nodeTypeMask ^ nodeTypeWithPathSeparator) & n.nodeType
}

// nolint,unused
func (n *Node) makeNotWithMetadata() {
	n.nodeType = (nodeTypeMask ^ nodeTypeWithMetadata) & n.nodeType
}

func (n *Node) makeNotEmptyDirectory() {
	n.nodeType = (nodeTypeMask ^ nodeTypeEmptyDirectory) & n.nodeType
}

func (n *Node) SetObfuscationKey(obfuscationKey []byte) {
	bytes := make([]byte, 32)
	copy(bytes, obfuscationKey)
	n.obfuscationKey = bytes
}

// Reference returns the address of the mantaray node if saved.
func (n *Node) Reference() []byte {
	return n.ref
}

// Entry returns the value stored on the specific path.
func (n *Node) Entry() []byte {
	return n.entry
}

// Metadata returns the metadata stored on the specific path.
func (n *Node) Metadata() map[string]string {
	return n.metadata
}

func (n *Node) Index() int64 {
	return n.index
}

func (n *Node) Prefix() [][]byte {
	prefixes := make([][]byte, 0, len(n.forks))
	for _, f := range n.forks {
		prefixes = append(prefixes, f.prefix)
	}
	return prefixes
}

// LookupNode finds the node for a path or returns error if not found
func (n *Node) LookupNode(ctx context.Context, path []byte, l Loader) (*Node, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	if n.forks == nil {
		if err := n.load(ctx, l); err != nil {
			return nil, err
		}
	}
	if len(path) == 0 {
		return n, nil
	}
	f := n.forks[path[0]]
	if f == nil {
		return nil, notFound(path)
	}
	c := common(f.prefix, path)
	if len(c) == len(f.prefix) {
		f.Node.index = n.index
		node, err := f.Node.LookupNode(ctx, path[len(c):], l)
		if err != nil {
			return node, err
		}
		n.index = node.index
		return node, err
	}
	return nil, notFound(path)
}

// Lookup finds the entry for a path or returns error if not found
func (n *Node) Lookup(ctx context.Context, path []byte, l Loader) ([]byte, error) {
	node, err := n.LookupNode(ctx, path, l)
	if err != nil {
		return nil, err
	}
	if !node.IsValueType() && len(path) > 0 {
		return nil, notFound(path)
	}
	return node.entry, nil
}

// Add adds an entry to the path
func (n *Node) Add(ctx context.Context, path, entry []byte, metadata map[string]string, ls LoadSaver) error {
	nn := New()
	nn.entry = entry

	if bytes.Equal(nn.entry, zero32) {
		if path[len(path)-1] != PathSeparator {
			return ErrInvalidFile
		}
		nn.makeEmptyDirectory()
	} else {
		nn.makeValue()
	}

	if len(metadata) > 0 {
		nn.metadata = metadata
		nn.makeWithMetadata()
	}

	return n.addNode(ctx, path, nn, ls)
}

func (n *Node) updateIsWithPathSeparator(path []byte) {
	if bytes.IndexRune(path, PathSeparator) > 0 {
		n.makeWithPathSeparator()
	} else {
		n.makeNotWithPathSeparator()
	}
}

// Remove removes a path from the node
func (n *Node) Remove(ctx context.Context, path []byte, ls LoadSaver) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if len(path) == 0 {
		return ErrEmptyPath
	}
	if n.forks == nil {
		if err := n.load(ctx, ls); err != nil {
			return err
		}
	}
	f := n.forks[path[0]]
	if f == nil {
		return ErrNotFound
	}
	if len(f.prefix) <= len(path) {
		if !bytes.HasPrefix(path, f.prefix) {
			return ErrNotFound
		}
		rest := path[len(f.prefix):]
		if len(rest) == 0 {
			if f.IsValueType() {
				f.makeNotValue()
			}
			// first slash not recognized as path type
			if f.prefix[0] == PathSeparator || f.IsWithPathSeparatorType() {
				f.forks = make(map[byte]*fork, 0)
				f.prefix = f.prefix[:bytes.LastIndexByte(f.prefix, PathSeparator)+1]
				copy(f.entry, zero32)
				f.makeEmptyDirectory()
				f.updateIsWithPathSeparator(f.prefix)
			}
			if !f.IsWithPathSeparatorType() && len(f.forks) == 0 {
				delete(n.forks, path[0])
			}
			// clear ref
			n.reborn()
			return nil
		}
		err := f.Node.Remove(ctx, rest, ls)
		if err != nil {
			return err
		}
	} else {
		// must match directory leading
		if path[len(path)-1] != PathSeparator && !bytes.HasPrefix(f.prefix, path) {
			return ErrNotFound
		}
		f.prefix = f.prefix[:len(path)]
		if len(f.prefix) == 0 {
			delete(n.forks, path[0])
		} else {
			if f.IsValueType() {
				f.makeNotValue()
				copy(f.entry, zero32)
			}
			f.makeEmptyDirectory()
			f.Node.reborn()
		}
		// clear ref
		n.reborn()
		return nil
	}
	if len(f.forks) == 1 {
		var ff *fork
		for _, fork := range f.forks {
			ff = fork
		}
		// merge fork
		delete(n.forks, f.prefix[0])
		err := n.addNode(ctx, append(f.prefix, ff.prefix...), ff.Node, ls)
		if err != nil {
			return err
		}
	}
	// clear parent ref recursively
	n.reborn()
	return nil
}

func common(a, b []byte) (c []byte) {
	for i := 0; i < len(a) && i < len(b) && a[i] == b[i]; i++ {
		c = append(c, a[i])
	}
	return c
}

// HasPrefix tests whether the node contains prefix path.
func (n *Node) HasPrefix(ctx context.Context, path []byte, l Loader) (bool, error) {
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	default:
	}
	if n.forks == nil {
		if err := n.load(ctx, l); err != nil {
			return false, err
		}
	}
	if len(path) == 0 {
		return true, nil
	}
	f := n.forks[path[0]]
	if f == nil {
		return false, nil
	}
	c := common(f.prefix, path)
	if len(c) == len(f.prefix) {
		return f.Node.HasPrefix(ctx, path[len(c):], l)
	}
	if bytes.HasPrefix(f.prefix, path) {
		return true, nil
	}
	return false, nil
}

func (n *Node) clone(other *Node) {
	n.entry = other.entry
	n.nodeType |= other.nodeType
	if other.refBytesSize != 0 {
		n.refBytesSize = other.refBytesSize
	}
	if len(other.metadata) > 0 {
		n.metadata = other.metadata
	}
	// TODO merge
	if len(other.forks) > 0 {
		n.forks = other.forks
	}
	if len(other.obfuscationKey) > 0 {
		n.SetObfuscationKey(other.obfuscationKey)
	}
}

func (n *Node) reborn() {
	n.ref = nil
}

func (n *Node) addNode(ctx context.Context, path []byte, node *Node, ls LoadSaver) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if err := node.load(ctx, ls); err != nil {
		return err
	}
	if n.refBytesSize == 0 {
		if len(node.entry) > 256 {
			return fmt.Errorf("node entry size > 256: %d", len(node.entry))
		}
		// empty entry for directories
		if len(node.entry) > 0 {
			n.refBytesSize = len(node.entry)
		}
	} else if len(node.entry) > 0 && n.refBytesSize != len(node.entry) {
		return fmt.Errorf("invalid entry size: %d, expected: %d", len(node.entry), n.refBytesSize)
	}

	if len(path) == 0 {
		n.clone(node)
		n.reborn()
		return nil
	}
	if n.forks == nil {
		if err := n.load(ctx, ls); err != nil {
			return err
		}
		n.ref = nil
	}
	f := n.forks[path[0]]
	if f == nil {
		// check for prefix size limit
		if len(path) > nodePrefixMaxSize {
			prefix := path[:nodePrefixMaxSize]
			rest := path[nodePrefixMaxSize:]
			nn := New()
			if len(n.obfuscationKey) > 0 {
				nn.SetObfuscationKey(n.obfuscationKey)
			}
			nn.refBytesSize = n.refBytesSize
			err := nn.addNode(ctx, rest, node, ls)
			if err != nil {
				return err
			}
			nn.updateIsWithPathSeparator(prefix)
			n.forks[path[0]] = &fork{prefix, nn}
			n.reborn()
			n.makeEdge()
			return nil
		}
		node.ref = nil
		if node.refBytesSize == 0 {
			node.refBytesSize = n.refBytesSize
		}
		if len(node.obfuscationKey) == 0 && len(n.obfuscationKey) > 0 {
			node.SetObfuscationKey(n.obfuscationKey)
		}
		node.updateIsWithPathSeparator(path)
		n.forks[path[0]] = &fork{path, node}
		n.reborn()
		n.makeEdge()
		return nil
	}
	c := common(f.prefix, path)
	rest := f.prefix[len(c):]
	nn := f.Node
	if len(rest) > 0 {
		// move current common prefix node
		nn = New()
		if len(n.obfuscationKey) > 0 {
			nn.SetObfuscationKey(n.obfuscationKey)
		}
		nn.refBytesSize = n.refBytesSize
		f.Node.updateIsWithPathSeparator(rest)
		nn.forks[rest[0]] = &fork{rest, f.Node}
		nn.makeEdge()
	}
	// NOTE: special case on edge split
	nn.updateIsWithPathSeparator(path)
	// add new for shared prefix
	if nn.IsEmptyDirectory() {
		nn.makeNotEmptyDirectory()
		nn.clone(node)
		n.forks[path[0]] = &fork{path, nn}
	} else {
		err := nn.addNode(ctx, path[len(c):], node, ls)
		if err != nil {
			return err
		}
		n.forks[path[0]] = &fork{c, nn}
	}
	n.reborn()
	n.makeEdge()
	return nil
}

func (n *Node) Copy(ctx context.Context, target *Node, path, newPath []byte, create bool, ls LoadSaver) error {
	return n.move(ctx, target, path, newPath, create, true, ls)
}

func (n *Node) Move(ctx context.Context, target *Node, path, newPath []byte, create bool, ls LoadSaver) error {
	return n.move(ctx, target, path, newPath, create, false, ls)
}

func (n *Node) move(ctx context.Context, target *Node, path, newPath []byte, create, keepOrigin bool, ls LoadSaver) error {
	if len(path) == 0 {
		return ErrEmptyPath
	}

	sourceDir := path[len(path)-1] == PathSeparator
	targetDir := newPath[len(newPath)-1] == PathSeparator

	if sourceDir && !targetDir {
		return ErrForbiddenAction
	}

	if target == n && bytes.HasPrefix(newPath, path) {
		return ErrForbiddenAction
	}

	source, sourcePrefix, err := n.lookupClosest(ctx, path, ls)
	if err != nil {
		return err
	}

	sourcePath := sourcePrefix
	if !sourceDir {
		if !source.IsValueType() {
			return ErrNotFound
		}
		sourcePath = path[bytes.LastIndexByte(path, PathSeparator)+1:]
		// clone value node
		nn := New()
		nn.makeValue()
		nn.entry = source.entry
		if len(source.obfuscationKey) > 0 {
			nn.SetObfuscationKey(source.obfuscationKey)
		}
		nn.ref = source.ref
		nn.refBytesSize = source.refBytesSize
		nn.metadata = source.metadata
		source = nn
	}

	if !create {
		_, _, err = target.lookupClosest(ctx, newPath, ls)
		if err != nil {
			return err
		}
	}

	targetPath := newPath
	if targetDir {
		targetPath = append(newPath, sourcePath...)
	}

	if len(source.forks) == 0 {
		err = target.addNode(ctx, targetPath, source, ls)
		if err != nil {
			return err
		}
	} else {
		for _, node := range source.forks {
			addPath := make([]byte, len(targetPath))
			copy(addPath, targetPath)
			addPath = append(addPath, node.prefix...)
			err = target.addNode(ctx, addPath, node.Node, ls)
			if err != nil {
				return err
			}
		}
	}

	if !keepOrigin {
		if sourceDir {
			err = n.Remove(ctx, append(path, sourcePrefix...), ls)
			if err != nil {
				return err
			}
		} else {
			err = n.Remove(ctx, path, ls)
			if err != nil {
				return err
			}
		}
	}

	return nil
}
