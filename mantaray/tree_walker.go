package mantaray

import (
	"bytes"
	"container/list"
	"context"
	"errors"
	"math"
	"sort"
)

type WalkLevelFunc func(nodeType int, path, prefix, hash []byte, metadata map[string]string) error

const (
	File = iota
	Directory

	MaxLevel = math.MaxUint32
)

var (
	errMaxHeight      = errors.New("reach maximum directory level")
	errAlreadyEntered = errors.New("already entered the directory")
)

func copyPath(src []byte) []byte {
	p := make([]byte, len(src), len(src))
	copy(p, src)
	return p
}

func walkDeepFirst(ctx context.Context, l Loader, n *Node, path, prefix []byte, walker WalkLevelFunc) error {
	if len(n.forks) == 0 {
		if err := n.load(ctx, l); err != nil {
			return err
		}
	}

	nextPath := make([]byte, len(path), len(path)+len(prefix))
	copy(nextPath, path)

	for i := 0; i < len(prefix); i++ {
		nextPath = append(nextPath, prefix[i])
		if prefix[i] == byte(PathSeparator) {
			if err := walker(Directory, copyPath(nextPath), []byte{}, n.Reference(), n.Metadata()); err != nil {
				return err
			}
		}
	}

	if n.IsValueType() {
		if len(nextPath) != 0 && nextPath[len(nextPath)-1] != byte(PathSeparator) {
			afterSlash := bytes.LastIndexByte(nextPath, byte(PathSeparator))
			var curPath, curFile []byte
			if afterSlash != -1 {
				curPath = nextPath[:afterSlash+1]
				curFile = nextPath[afterSlash+1:]
			} else {
				curPath = []byte{}
				curFile = nextPath
			}
			if err := walker(File, copyPath(curPath), copyPath(curFile), n.Entry(), n.Metadata()); err != nil {
				return err
			}
		}
	}

	bytesOrder := make([]byte, 0)

	for b := range n.forks {
		bytesOrder = append(bytesOrder, b)
	}

	sort.Slice(bytesOrder, func(i, j int) bool {
		return bytesOrder[i] <= bytesOrder[j]
	})

	for _, b := range bytesOrder {
		v := n.forks[b]
		v.index = n.Index()
		err := walkDeepFirst(ctx, l, v.Node, nextPath, v.prefix, walker)
		if err != nil {
			return err
		}
		n.index = v.index

	}

	return nil
}

func (n *Node) lookupClosest(ctx context.Context, path []byte, l Loader) (*Node, []byte, error) {
	select {
	case <-ctx.Done():
		return nil, path, ctx.Err()
	default:
	}
	if n.forks == nil {
		if err := n.load(ctx, l); err != nil {
			return nil, path, err
		}
	}
	if len(path) == 0 {
		return n, path, nil
	}
	f := n.forks[path[0]]
	if f == nil {
		return n, path, nil
	}
	c := common(f.prefix, path)
	if len(c) == len(f.prefix) {
		return f.Node.lookupClosest(ctx, path[len(c):], l)
	}
	return f.Node, f.prefix[len(c):], errAlreadyEntered
}

type nodeTag struct {
	*Node
	level  uint
	path   []byte
	prefix []byte
}

func walkBreathFirst(ctx context.Context, l Loader, n *Node, path []byte, level uint, walker WalkLevelFunc) error {
	p, remain, err := n.lookupClosest(ctx, path, l)
	root := &nodeTag{
		Node:   p,
		path:   []byte{},
		prefix: []byte{},
	}

	if len(remain) == 0 {
		root.level++
	}

	if err != nil {
		if !errors.Is(err, errAlreadyEntered) {
			return err
		}
		slashIndex := bytes.IndexByte(remain, byte(PathSeparator))
		if slashIndex == -1 {
			root.level++
			root.prefix = remain
		} else {
			if bytes.HasSuffix(path, remain[slashIndex+1:]) {
				root.level++
			}
			root.prefix = remain[slashIndex+1:]
		}
	}

	q := list.New()
	q.PushBack(root)

pop:
	for q.Len() > 0 {
		e := q.Front()
		t := e.Value.(*nodeTag)

		q.Remove(e)

		if t.forks == nil {
			if err := t.load(ctx, l); err != nil {
				return err
			}
		}

		nextPath := make([]byte, len(t.path), len(t.path)+len(t.prefix))
		copy(nextPath, t.path)

		for i := 0; i < len(t.prefix); i++ {
			nextPath = append(nextPath, t.prefix[i])
			if t.prefix[i] == byte(PathSeparator) {
				t.level++
				if err := walker(Directory, copyPath(nextPath), []byte{}, t.Reference(), t.Metadata()); err != nil {
					return err
				}
			}
			if t.level > level {
				continue pop
			}
		}

		if t.IsValueType() {
			if len(nextPath) != 0 && nextPath[len(nextPath)-1] != byte(PathSeparator) {
				afterSlash := bytes.LastIndexByte(nextPath, byte(PathSeparator))
				var curPath, curFile []byte
				if afterSlash != -1 {
					curPath = nextPath[:afterSlash+1]
					curFile = nextPath[afterSlash+1:]
				} else {
					curPath = []byte{}
					curFile = nextPath
				}
				if err := walker(File, copyPath(curPath), copyPath(curFile), t.Entry(), t.Metadata()); err != nil {
					return err
				}
			}
		}

		bytesOrder := make([]byte, 0)

		for b := range t.forks {
			bytesOrder = append(bytesOrder, b)
		}

		sort.Slice(bytesOrder, func(i, j int) bool {
			return bytesOrder[i] <= bytesOrder[j]
		})

		for _, b := range bytesOrder {
			v := t.forks[b]
			q.PushBack(&nodeTag{
				Node:   v.Node,
				path:   nextPath,
				level:  t.level,
				prefix: v.prefix,
			})
		}
	}

	return nil
}

func (n *Node) WalkLevel(ctx context.Context, root []byte, l Loader, level uint, walker WalkLevelFunc) error {
	if len(root) == 0 && level == MaxLevel {
		return walkDeepFirst(ctx, l, n, root, []byte{}, walker)
	}

	return walkBreathFirst(ctx, l, n, root, level, walker)
}
