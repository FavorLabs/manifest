// Copyright 2020 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mantaray

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
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
	ErrMetadataTooLarge = errors.New("metadata too large")
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
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if n.refBytesSize == 0 {
		if len(entry) > 256 {
			return fmt.Errorf("node entry size > 256: %d", len(entry))
		}
		// empty entry for directories
		if len(entry) > 0 {
			n.refBytesSize = len(entry)
		}
	} else if len(entry) > 0 && n.refBytesSize != len(entry) {
		return fmt.Errorf("invalid entry size: %d, expected: %d", len(entry), n.refBytesSize)
	}

	if len(path) == 0 {
		n.entry = entry
		n.makeValue()
		if len(metadata) > 0 {
			n.metadata = metadata
			n.makeWithMetadata()
		}
		n.ref = nil
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
		nn := New()
		if len(n.obfuscationKey) > 0 {
			nn.SetObfuscationKey(n.obfuscationKey)
		}
		nn.refBytesSize = n.refBytesSize
		// check for prefix size limit
		if len(path) > nodePrefixMaxSize {
			prefix := path[:nodePrefixMaxSize]
			rest := path[nodePrefixMaxSize:]
			err := nn.Add(ctx, rest, entry, metadata, ls)
			if err != nil {
				return err
			}
			nn.updateIsWithPathSeparator(prefix)
			n.forks[path[0]] = &fork{prefix, nn}
			n.makeEdge()
			return nil
		}
		nn.entry = entry
		if len(metadata) > 0 {
			nn.metadata = metadata
			nn.makeWithMetadata()
		}
		nn.makeValue()
		nn.updateIsWithPathSeparator(path)
		n.forks[path[0]] = &fork{path, nn}
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
		// if common path is full path new node is value type
		if len(path) == len(c) {
			nn.makeValue()
		}
	}
	// NOTE: special case on edge split
	nn.updateIsWithPathSeparator(path)
	// add new for shared prefix
	err := nn.Add(ctx, path[len(c):], entry, metadata, ls)
	if err != nil {
		return err
	}
	n.forks[path[0]] = &fork{c, nn}
	n.makeEdge()
	return nil
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
	prefixIndex := bytes.Index(path, f.prefix)
	if prefixIndex != 0 {
		return ErrNotFound
	}
	rest := path[len(f.prefix):]
	if len(rest) == 0 {
		// full path matched
		delete(n.forks, path[0])
		return nil
	}
	return f.Node.Remove(ctx, rest, ls)
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

func (n *Node) addNode(ctx context.Context, path []byte, node *Node, ls LoadSaver) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
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
		n.entry = node.entry
		n.nodeType = node.nodeType
		if len(node.metadata) > 0 {
			n.metadata = node.metadata
		}
		n.ref = nil
		n.forks = node.forks
		if len(node.obfuscationKey) > 0 {
			n.SetObfuscationKey(node.obfuscationKey)
		}
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
			n.makeEdge()
			return nil
		}
		n.forks[path[0]] = &fork{path, node}
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
		// if common path is full path new node is value type
		if len(path) == len(c) {
			nn.makeValue()
		}
	}
	// NOTE: special case on edge split
	nn.updateIsWithPathSeparator(path)
	// add new for shared prefix
	err := nn.addNode(ctx, path[len(c):], node, ls)
	if err != nil {
		return err
	}
	n.forks[path[0]] = &fork{c, nn}
	n.makeEdge()
	return nil
}

func (n *Node) Copy(ctx context.Context, path, newPath []byte, ls LoadSaver) error {
	return n.move(ctx, path, newPath, true, ls)
}

func (n *Node) Move(ctx context.Context, path, newPath []byte, ls LoadSaver) error {
	return n.move(ctx, path, newPath, false, ls)
}

func (n *Node) move(ctx context.Context, path, newPath []byte, keepOrigin bool, ls LoadSaver) error {
	if len(path) == 0 {
		return ErrEmptyPath
	}

	sourceDir := path[len(path)-1] == os.PathSeparator
	targetDir := newPath[len(newPath)-1] == os.PathSeparator

	if sourceDir && !targetDir {
		return ErrInvalidInput
	}

	if bytes.HasPrefix(newPath, path) {
		return ErrInvalidInput
	}

	source, sourcePrefix, err := n.matchPath(ctx, path, ls)
	if err != nil {
		return err
	}

	target, _, err := n.matchPath(ctx, newPath, ls)
	if err != nil {
		return err
	}

	target.ref = nil

	sourcePath := sourcePrefix
	if !sourceDir {
		lastSlash := bytes.LastIndexByte(path, os.PathSeparator)
		sourcePath = path[lastSlash+1:]
	}

	targetPath := append(newPath, sourcePath...)

	if len(source.forks) == 0 {
		err = n.addNode(ctx, targetPath, source, ls)
		if err != nil {
			return err
		}
	} else {
		for _, node := range source.forks {
			addPath := make([]byte, len(targetPath))
			copy(addPath, targetPath)
			addPath = append(addPath, node.prefix...)
			err = n.addNode(ctx, addPath, node.Node, ls)
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

	return target.Save(ctx, ls)
}

func (n *Node) matchPath(ctx context.Context, path []byte, l Loader) (*Node, []byte, error) {
	select {
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	default:
	}
	if n.forks == nil {
		if err := n.load(ctx, l); err != nil {
			return nil, nil, err
		}
	}
	if len(path) == 0 {
		return n, nil, nil
	}
	f := n.forks[path[0]]
	if f == nil {
		return nil, nil, ErrNotFound
	}
	if len(f.prefix) < len(path) {
		c := common(f.prefix, path)
		if len(c) == len(f.prefix) {
			return f.Node.matchPath(ctx, path[len(c):], l)
		}

		return nil, nil, ErrNotFound
	} else {
		if path[len(path)-1] == os.PathSeparator {
			if !bytes.HasPrefix(f.prefix, path) {
				return nil, nil, ErrNotFound
			}
		} else {
			if !bytes.Equal(f.prefix, path) && !f.IsValueType() {
				return nil, nil, ErrNotFound
			}
		}

		return f.Node, f.prefix[len(path):], nil
	}
}
