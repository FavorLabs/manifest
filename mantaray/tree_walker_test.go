package mantaray

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"testing"
)

func TestWalkLevelAtExpandAll(t *testing.T) {
	for _, tc := range []struct {
		name     string
		toAdd    [][]byte
		expected [][]byte
	}{
		{
			name: "simple",
			toAdd: [][]byte{
				[]byte("empty"),
			},
			expected: [][]byte{
				[]byte("empty"),
			},
		},
		{
			name: "simple-dir",
			toAdd: [][]byte{
				[]byte("index.html"),
				[]byte("img/test/"),
				[]byte("img/test/oho.png"),
				[]byte("img/test/old/test.png"),
				[]byte("img/test/olds/person.jpg"),
				[]byte("img/test/ow/secret/.empty"),
				[]byte("robots.txt"),
				[]byte("robot/baidu.com"),
				[]byte("robot/google/robots.txt"),
				[]byte("robot/baidu/robots.txt"),
				[]byte("src/logo.gif"),
				[]byte("src/default/check.jpg"),
				[]byte("src/defaults/1/apple.png"),
				[]byte("src/defaults/1/apple.png.bak"),
			},
			expected: [][]byte{
				[]byte("index.html"),
				[]byte("img/"),
				[]byte("img/test/"),
				[]byte("img/test/oho.png"),
				[]byte("img/test/old/"),
				[]byte("img/test/old/test.png"),
				[]byte("img/test/olds/"),
				[]byte("img/test/olds/person.jpg"),
				[]byte("img/test/ow/"),
				[]byte("img/test/ow/secret/"),
				[]byte("img/test/ow/secret/.empty"),
				[]byte("robots.txt"),
				[]byte("robot/"),
				[]byte("robot/baidu.com"),
				[]byte("robot/google/"),
				[]byte("robot/google/robots.txt"),
				[]byte("robot/baidu/"),
				[]byte("robot/baidu/robots.txt"),
				[]byte("src/"),
				[]byte("src/logo.gif"),
				[]byte("src/default/"),
				[]byte("src/default/check.jpg"),
				[]byte("src/defaults/"),
				[]byte("src/defaults/1/"),
				[]byte("src/defaults/1/apple.png"),
				[]byte("src/defaults/1/apple.png.bak"),
			},
		},
		{
			name: "no-direct-dir",
			toAdd: [][]byte{
				[]byte("dir/"),
				[]byte("dir/aufs/"),
				[]byte("dir/aufs/app"),
				[]byte("dir/aufs.old/"),
				[]byte("dir/aufs.old/app"),
				[]byte("dir/aux"),
				[]byte("dir/video.tar"),
				[]byte("dir/video/"),
				[]byte("dir/video/file"),
			},
			expected: [][]byte{
				[]byte("dir/"),
				[]byte("dir/aufs/"),
				[]byte("dir/aufs/app"),
				[]byte("dir/aufs.old/"),
				[]byte("dir/aufs.old/app"),
				[]byte("dir/aux"),
				[]byte("dir/video.tar"),
				[]byte("dir/video/"),
				[]byte("dir/video/file"),
			},
		},
		{
			name: "complex-dir",
			toAdd: [][]byte{
				[]byte("dir1/"),
				[]byte("dir1/di/"),
				[]byte("dir1/dx.txt"),
				[]byte("dir1/dx1.txt"),
				[]byte("dir1/di/a/"),
				[]byte("dir1/di/a/b/"),
				[]byte("dir1/di/a/b/x/"),
				[]byte("dir1/di/a/b/x.txt"),
				[]byte("dir1/di/a/b/x/cv.txt"),
				[]byte("dir1/di/a/c/"),
				[]byte("dir1/di/a/caaa.txt"),
				[]byte("dir1/di/a/c/aaa.txt"),
				[]byte("dir1/di/ab/"),
				[]byte("dir1/di/ab/c.txt"),
				[]byte("dir2/"),
				[]byte("dir2/abc/"),
				[]byte("dir2/abcde/"),
				[]byte("dir2/abc/de/"),
				[]byte("dir2/abc/de.txt"),
				[]byte("dir2/abc/de1.txt"),
				[]byte("dir2/abc/de/1.txt"),
				[]byte("dir2/abc/de/mm/"),
				[]byte("dir2/abc/de/mm/n.txt"),
				[]byte("dir2/abcde/1.txt"),
				[]byte("dir3/"),
				[]byte("dir3/1.txt"),
				[]byte("dir3/12.txt"),
				[]byte("dir3/2.txt"),
				[]byte("dir3/222.txt"),
			},
			expected: [][]byte{
				[]byte("dir1/"),
				[]byte("dir1/di/"),
				[]byte("dir1/dx.txt"),
				[]byte("dir1/dx1.txt"),
				[]byte("dir1/di/a/"),
				[]byte("dir1/di/a/b/"),
				[]byte("dir1/di/a/b/x/"),
				[]byte("dir1/di/a/b/x.txt"),
				[]byte("dir1/di/a/b/x/cv.txt"),
				[]byte("dir1/di/a/c/"),
				[]byte("dir1/di/a/caaa.txt"),
				[]byte("dir1/di/a/c/aaa.txt"),
				[]byte("dir1/di/ab/"),
				[]byte("dir1/di/ab/c.txt"),
				[]byte("dir2/"),
				[]byte("dir2/abc/"),
				[]byte("dir2/abcde/"),
				[]byte("dir2/abc/de/"),
				[]byte("dir2/abc/de.txt"),
				[]byte("dir2/abc/de1.txt"),
				[]byte("dir2/abc/de/1.txt"),
				[]byte("dir2/abc/de/mm/"),
				[]byte("dir2/abc/de/mm/n.txt"),
				[]byte("dir2/abcde/1.txt"),
				[]byte("dir3/"),
				[]byte("dir3/1.txt"),
				[]byte("dir3/12.txt"),
				[]byte("dir3/2.txt"),
				[]byte("dir3/222.txt"),
			},
		},
	} {
		ctx := context.Background()
		t.Run(tc.name, func(t *testing.T) {
			n := New()

			for i := 0; i < len(tc.toAdd); i++ {
				c := tc.toAdd[i]
				e := append(make([]byte, 32-len(c)), c...)
				err := n.Add(ctx, c, e, nil, nil)
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
			}

			walkedCount := 0

			walker := func(nodeType int, path, prefix, hash []byte, metadata map[string]string) error {
				walkedCount++

				pathFound := false

				fullPath := make([]byte, len(path))
				copy(fullPath, path)

				if nodeType == File {
					fullPath = append(fullPath, prefix...)
				}

				for i := 0; i < len(tc.expected); i++ {
					c := tc.expected[i]
					if bytes.Equal(fullPath, c) {
						pathFound = true
						break
					}
				}

				if !pathFound {
					return fmt.Errorf("walkFn returned unknown path: %s", fullPath)
				}

				return nil
			}
			// Expect no errors.
			err := n.WalkLevel(ctx, []byte{}, nil, math.MaxUint64, walker)
			if err != nil {
				t.Fatalf("no error expected, found: %s", err)
			}

			if len(tc.expected) != walkedCount {
				t.Errorf("expected %d nodes, got %d", len(tc.expected), walkedCount)
			}

		})
	}
}

func TestWalkLevelAtCollapse(t *testing.T) {
	for _, tc := range []struct {
		name     string
		level    int
		root     []byte
		toAdd    [][]byte
		expected [][]byte
	}{
		{
			name:  "simple",
			level: 1,
			root:  []byte{},
			toAdd: [][]byte{
				[]byte("dir1/"),
				[]byte("dir2/"),
				[]byte("dir3/"),
			},
			expected: [][]byte{
				[]byte("dir1/"),
				[]byte("dir2/"),
				[]byte("dir3/"),
			},
		},
		{
			name:  "dive-into-dir",
			level: 2,
			root:  []byte{},
			toAdd: [][]byte{
				[]byte("dir1/"),
				[]byte("dir2/"),
				[]byte("dir3/"),
				[]byte("dir1/di/file"),
				[]byte("dir1/dx.txt"),
			},
			expected: [][]byte{
				[]byte("dir1/"),
				[]byte("dir2/"),
				[]byte("dir3/"),
				[]byte("dir1/di/"),
				[]byte("dir1/dx.txt"),
			},
		},
		{
			name:  "file-and-dir-share-prefix",
			level: 1,
			root:  []byte("dir2/"),
			toAdd: [][]byte{
				[]byte("dir1/"),
				[]byte("dir1/di/file"),
				[]byte("dir1/dx.txt"),
				[]byte("dir2/direct.old"),
				[]byte("dir2/direct/file"),
			},
			expected: [][]byte{
				[]byte("direct.old"),
				[]byte("direct/"),
			},
		},
		{
			name:  "dir-with-same-prefix",
			level: 1,
			root:  []byte{},
			toAdd: [][]byte{
				[]byte("abc/file"),
				[]byte("abcde/file"),
				[]byte("abx/file"),
				[]byte("a/file"),
				[]byte("b/file"),
				[]byte("bc/file"),
			},
			expected: [][]byte{
				[]byte("abc/"),
				[]byte("abcde/"),
				[]byte("abx/"),
				[]byte("a/"),
				[]byte("b/"),
				[]byte("bc/"),
			},
		},
		{
			name:  "no-direct-dir",
			level: 1,
			root:  []byte("dir/"),
			toAdd: [][]byte{
				[]byte("dir/"),
				[]byte("dir/aufs/app"),
				[]byte("dir/aufs.old/app"),
				[]byte("dir/aux"),
				[]byte("dir/video.tar"),
				[]byte("dir/video/file"),
			},
			expected: [][]byte{
				[]byte("aufs/"),
				[]byte("aufs.old/"),
				[]byte("aux"),
				[]byte("video.tar"),
				[]byte("video/"),
			},
		},
		{
			name:  "deep-dir-share-prefix",
			level: 3,
			root:  []byte("dir/"),
			toAdd: [][]byte{
				[]byte("dir/a/"),
				[]byte("dir/a/b/"),
				[]byte("dir/a/b/c/"),
				[]byte("dir/a/b/c/d/"),
				[]byte("dir/a/b/c/d/e/"),
				[]byte("dir/a/b/c/d/e/file"),
				[]byte("dir/a/b/cde/"),
				[]byte("dir/a/b/cde/file"),
				[]byte("dir/abc/"),
				[]byte("dir/abc/de/"),
				[]byte("dir/abc/de/file"),
				[]byte("dir/abcde/"),
				[]byte("dir/abcde/file"),
			},
			expected: [][]byte{
				[]byte("a/"),
				[]byte("a/b/"),
				[]byte("a/b/c/"),
				[]byte("a/b/cde/"),
				[]byte("abc/"),
				[]byte("abc/de/"),
				[]byte("abc/de/file"),
				[]byte("abcde/"),
				[]byte("abcde/file"),
			},
		},
	} {
		ctx := context.Background()
		t.Run(tc.name, func(t *testing.T) {
			n := New()

			for i := 0; i < len(tc.toAdd); i++ {
				c := tc.toAdd[i]
				e := append(make([]byte, 32-len(c)), c...)
				err := n.Add(ctx, c, e, nil, nil)
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
			}

			walkedCount := 0

			walker := func(nodeType int, path, prefix, hash []byte, metadata map[string]string) error {
				walkedCount++

				pathFound := false

				fullPath := make([]byte, len(path))
				copy(fullPath, path)

				if nodeType == File {
					fullPath = append(fullPath, prefix...)
				}

				for i := 0; i < len(tc.expected); i++ {
					c := tc.expected[i]
					if bytes.Equal(fullPath, c) {
						pathFound = true
						break
					}
				}

				if !pathFound {
					return fmt.Errorf("walkFn returned unknown path: %s", fullPath)
				}

				return nil
			}
			// Expect no errors.
			err := n.WalkLevel(ctx, tc.root, nil, uint(tc.level), walker)
			if err != nil {
				t.Fatalf("no error expected, found: %s", err)
			}

			if len(tc.expected) != walkedCount {
				t.Errorf("expected %d nodes, got %d", len(tc.expected), walkedCount)
			}
		})
	}
}
