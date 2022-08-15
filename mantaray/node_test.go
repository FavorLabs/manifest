// Copyright 2020 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mantaray_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"

	"github.com/FavorLabs/manifest/mantaray"
)

func TestNilPath(t *testing.T) {
	ctx := context.Background()
	n := mantaray.New()
	_, err := n.Lookup(ctx, nil, nil)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestAddAndLookup(t *testing.T) {
	ctx := context.Background()
	n := mantaray.New()
	testCases := [][]byte{
		[]byte("aaaaaa"),
		[]byte("aaaaab"),
		[]byte("abbbb"),
		[]byte("abbba"),
		[]byte("bbbbba"),
		[]byte("bbbaaa"),
		[]byte("bbbaab"),
		[]byte("aa"),
		[]byte("b"),
	}
	for i := 0; i < len(testCases); i++ {
		c := testCases[i]
		e := append(make([]byte, 32-len(c)), c...)
		err := n.Add(ctx, c, e, nil, nil)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		for j := 0; j < i; j++ {
			d := testCases[j]
			m, err := n.Lookup(ctx, d, nil)
			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
			de := append(make([]byte, 32-len(d)), d...)
			if !bytes.Equal(m, de) {
				t.Fatalf("expected value %x, got %x", d, m)
			}
		}
	}
}

func TestAddAndLookupNode(t *testing.T) {
	for _, tc := range []struct {
		name  string
		toAdd [][]byte
	}{
		{
			name: "a",
			toAdd: [][]byte{
				[]byte("aaaaaa"),
				[]byte("aaaaab"),
				[]byte("abbbb"),
				[]byte("abbba"),
				[]byte("bbbbba"),
				[]byte("bbbaaa"),
				[]byte("bbbaab"),
				[]byte("aa"),
				[]byte("b"),
			},
		},
		{
			name: "simple",
			toAdd: [][]byte{
				[]byte("/"),
				[]byte("index.html"),
				[]byte("img/1.png"),
				[]byte("img/2.png"),
				[]byte("robots.txt"),
			},
		},
		{
			// mantaray.nodePrefixMaxSize number of '.'
			name: "nested-value-node-is-recognized",
			toAdd: [][]byte{
				[]byte("..............................@"),
				[]byte(".............................."),
			},
		},
		{
			name: "nested-prefix-is-not-collapsed",
			toAdd: [][]byte{
				[]byte("index.html"),
				[]byte("img/1.png"),
				[]byte("img/2/test1.png"),
				[]byte("img/2/test2.png"),
				[]byte("robots.txt"),
			},
		},
		{
			name: "conflicting-path",
			toAdd: [][]byte{
				[]byte("app.js.map"),
				[]byte("app.js"),
			},
		},
		{
			name: "spa-website",
			toAdd: [][]byte{
				[]byte("css/"),
				[]byte("css/app.css"),
				[]byte("favicon.ico"),
				[]byte("img/"),
				[]byte("img/logo.png"),
				[]byte("index.html"),
				[]byte("js/"),
				[]byte("js/chunk-vendors.js.map"),
				[]byte("js/chunk-vendors.js"),
				[]byte("js/app.js.map"),
				[]byte("js/app.js"),
			},
		},
	} {
		ctx := context.Background()
		t.Run(tc.name, func(t *testing.T) {
			n := mantaray.New()

			for i := 0; i < len(tc.toAdd); i++ {
				c := tc.toAdd[i]
				e := append(make([]byte, 32-len(c)), c...)
				err := n.Add(ctx, c, e, nil, nil)
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
				for j := 0; j < i+1; j++ {
					d := tc.toAdd[j]
					node, err := n.LookupNode(ctx, d, nil)
					if err != nil {
						t.Fatalf("expected no error, got %v", err)
					}
					if !node.IsValueType() {
						t.Fatalf("expected value type, got %v", strconv.FormatInt(int64(node.NodeType()), 2))
					}
					de := append(make([]byte, 32-len(d)), d...)
					if !bytes.Equal(node.Entry(), de) {
						t.Fatalf("expected value %x, got %x", d, node.Entry())
					}
				}
			}
		})
		t.Run(tc.name+"/with load save", func(t *testing.T) {
			n := mantaray.New()

			for i := 0; i < len(tc.toAdd); i++ {
				c := tc.toAdd[i]
				e := append(make([]byte, 32-len(c)), c...)
				err := n.Add(ctx, c, e, nil, nil)
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
			}
			ls := newMockLoadSaver()
			err := n.Save(ctx, ls)
			if err != nil {
				t.Fatal(err)
			}

			n2 := mantaray.NewNodeRef(n.Reference())

			for j := 0; j < len(tc.toAdd); j++ {
				d := tc.toAdd[j]
				node, err := n2.LookupNode(ctx, d, ls)
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
				if !node.IsValueType() {
					t.Fatalf("expected value type, got %v", strconv.FormatInt(int64(node.NodeType()), 2))
				}
				de := append(make([]byte, 32-len(d)), d...)
				if !bytes.Equal(node.Entry(), de) {
					t.Fatalf("expected value %x, got %x", d, node.Entry())
				}
			}
		})
	}
}

func TestRemove(t *testing.T) {
	for _, tc := range []struct {
		name     string
		toAdd    []mantaray.NodeEntry
		toRemove [][]byte
	}{
		{
			name: "simple",
			toAdd: []mantaray.NodeEntry{
				{
					Path: []byte("/"),
					Metadata: map[string]string{
						"index-document": "index.html",
					},
				},
				{
					Path: []byte("index.html"),
				},
				{
					Path: []byte("img/1.png"),
				},
				{
					Path: []byte("img/2.png"),
				},
				{
					Path: []byte("robots.txt"),
				},
			},
			toRemove: [][]byte{
				[]byte("img/2.png"),
			},
		},
		{
			name: "nested-prefix-is-not-collapsed",
			toAdd: []mantaray.NodeEntry{
				{
					Path: []byte("index.html"),
				},
				{
					Path: []byte("img/1.png"),
				},
				{
					Path: []byte("img/2/test1.png"),
				},
				{
					Path: []byte("img/2/test2.png"),
				},
				{
					Path: []byte("robots.txt"),
				},
			},
			toRemove: [][]byte{
				[]byte("img/2/test1.png"),
			},
		},
		{
			name: "common-prefix",
			toAdd: []mantaray.NodeEntry{
				{
					Path: []byte("apple.png"),
				},
				{
					Path: []byte("apple.png.bak"),
				},
			},
			toRemove: [][]byte{
				[]byte("apple.png"),
			},
		},
		{
			name: "collapsed-prefix",
			toAdd: []mantaray.NodeEntry{
				{
					Path: []byte("img/1.png"),
				},
				{
					Path: []byte("robots.txt"),
				},
			},
			toRemove: [][]byte{
				[]byte("img/"),
			},
		},
		{
			name: "long-prefix-merge",
			toAdd: []mantaray.NodeEntry{
				{
					Path:  []byte("abc/abcdefghijklmnopqrstuvwxyz/uvwxyz/.hidden"),
					Entry: []byte("0000000000000000000000000000000"),
				},
			},
			toRemove: [][]byte{
				[]byte("abc/abcdefghijklmnopqrstuvwxyz/uvwxyz/.hidden"),
			},
		},
	} {
		ctx := context.Background()
		t.Run(tc.name, func(t *testing.T) {
			n := mantaray.New()

			for i := 0; i < len(tc.toAdd); i++ {
				c := tc.toAdd[i].Path
				e := tc.toAdd[i].Entry
				if len(e) == 0 {
					e = append(make([]byte, 32-len(c)), c...)
				}
				m := tc.toAdd[i].Metadata
				err := n.Add(ctx, c, e, m, nil)
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
				for j := 0; j < i; j++ {
					d := tc.toAdd[j].Path
					m, err := n.Lookup(ctx, d, nil)
					if err != nil {
						t.Fatalf("expected no error, got %v", err)
					}
					de := append(make([]byte, 32-len(d)), d...)
					if !bytes.Equal(m, de) {
						t.Fatalf("expected value %x, got %x", d, m)
					}
				}
			}

			for i := 0; i < len(tc.toRemove); i++ {
				c := tc.toRemove[i]
				err := n.Remove(ctx, c, nil)
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
				_, err = n.Lookup(ctx, c, nil)
				if !errors.Is(err, mantaray.ErrNotFound) {
					t.Fatalf("expected not found error, got %v", err)
				}
			}

		})
	}
}

func TestHasPrefix(t *testing.T) {
	for _, tc := range []struct {
		name        string
		toAdd       [][]byte
		testPrefix  [][]byte
		shouldExist []bool
	}{
		{
			name: "simple",
			toAdd: [][]byte{
				[]byte("index.html"),
				[]byte("img/1.png"),
				[]byte("img/2.png"),
				[]byte("robots.txt"),
			},
			testPrefix: [][]byte{
				[]byte("img/"),
				[]byte("images/"),
			},
			shouldExist: []bool{
				true,
				false,
			},
		},
		{
			name: "nested-single",
			toAdd: [][]byte{
				[]byte("some-path/file.ext"),
			},
			testPrefix: [][]byte{
				[]byte("some-path/"),
				[]byte("some-path/file"),
				[]byte("some-other-path/"),
			},
			shouldExist: []bool{
				true,
				true,
				false,
			},
		},
	} {
		ctx := context.Background()
		t.Run(tc.name, func(t *testing.T) {
			n := mantaray.New()

			for i := 0; i < len(tc.toAdd); i++ {
				c := tc.toAdd[i]
				e := append(make([]byte, 32-len(c)), c...)
				err := n.Add(ctx, c, e, nil, nil)
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
			}

			for i := 0; i < len(tc.testPrefix); i++ {
				testPrefix := tc.testPrefix[i]
				shouldExist := tc.shouldExist[i]

				exists, err := n.HasPrefix(ctx, testPrefix, nil)
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}

				if shouldExist != exists {
					t.Errorf("expected prefix path %s to be %t, was %t", testPrefix, shouldExist, exists)
				}
			}

		})
	}
}

func TestMove(t *testing.T) {
	for _, tc := range []struct {
		toAdd    [][]byte
		target   [][]byte
		expected [][]byte
		unwanted [][]byte
	}{
		{
			toAdd: [][]byte{
				[]byte("index.html"),
				[]byte("img/test/oho.png"),
				[]byte("img/test/old/test.png"),
				[]byte("img/test/olds/person.jpg"),
				[]byte("img/test/ow/secret/.empty"),
				[]byte("src/logo.gif"),
				[]byte("src/default/check.jpg"),
				[]byte("src/defaults/1/apple.png"),
				[]byte("src/defaults/1/apple.png.bak"),
			},
			target: [][]byte{
				[]byte("img/"),
				[]byte("src/"),
			},
			expected: [][]byte{
				[]byte("index.html"),
				[]byte("src/test/oho.png"),
				[]byte("src/test/old/test.png"),
				[]byte("src/test/olds/person.jpg"),
				[]byte("src/logo.gif"),
				[]byte("src/default/check.jpg"),
				[]byte("src/defaults/1/apple.png"),
				[]byte("src/defaults/1/apple.png.bak"),
			},
			unwanted: [][]byte{
				[]byte("img/test/oho.png"),
				[]byte("img/test/old/test.png"),
				[]byte("img/test/olds/person.jpg"),
			},
		},
		{
			toAdd: [][]byte{
				[]byte("robots.txt"),
				[]byte("robots.tx1"),
				[]byte("robot/baidu.com"),
				[]byte("robot/google/robots.txt"),
				[]byte("robot/baidu/robots.txt"),
				[]byte("src/logo.gif"),
				[]byte("src/default/check.jpg"),
				[]byte("src/defaults/1/apple.png"),
				[]byte("src/defaults/1/apple.png.bak"),
			},
			target: [][]byte{
				[]byte("robots.txt"),
				[]byte("src/defaults/"),
			},
			expected: [][]byte{
				[]byte("robots.tx1"),
				[]byte("src/defaults/robots.txt"),
				[]byte("robot/baidu.com"),
				[]byte("robot/google/robots.txt"),
				[]byte("robot/baidu/robots.txt"),
				[]byte("src/logo.gif"),
				[]byte("src/default/check.jpg"),
				[]byte("src/defaults/1/apple.png"),
				[]byte("src/defaults/1/apple.png.bak"),
			},
			unwanted: [][]byte{
				[]byte("robots.txt"),
			},
		},
		{
			toAdd: [][]byte{
				[]byte("img/apple.png"),
				[]byte("img/apple/1x/1x.png"),
				[]byte("img/apple/2x/1x.png"),
				[]byte("src/logo.gif"),
				[]byte("src/default/check.jpg"),
				[]byte("src/defaults/1/apple.png"),
				[]byte("src/defaults/1/apple.png.bak"),
			},
			target: [][]byte{
				[]byte("img/apple/"),
				[]byte("src/defaults/"),
			},
			expected: [][]byte{
				[]byte("src/defaults/1x/1x.png"),
				[]byte("src/defaults/2x/1x.png"),
				[]byte("img/apple.png"),
				[]byte("src/logo.gif"),
				[]byte("src/default/check.jpg"),
				[]byte("src/defaults/1/apple.png"),
				[]byte("src/defaults/1/apple.png.bak"),
			},
			unwanted: [][]byte{
				[]byte("img/apple/1x/1x.png"),
				[]byte("img/apple/2x/1x.png"),
			},
		},
		{
			toAdd: [][]byte{
				[]byte("dir/aufs/app_new"),
				[]byte("dir/aufs.old/app"),
				[]byte("dir/aux"),
				[]byte("dir/video.tar"),
				[]byte("dir/video/"),
				[]byte("dir/video/file"),
			},
			target: [][]byte{
				[]byte("dir/aufs/"),
				[]byte("dir/aufs.old/"),
			},
			expected: [][]byte{
				[]byte("dir/aufs.old/app_new"),
				[]byte("dir/aufs.old/app"),
				[]byte("dir/aux"),
				[]byte("dir/video.tar"),
				[]byte("dir/video/file"),
			},
			unwanted: [][]byte{
				[]byte("dir/aufs/app_new"),
			},
		},
		{
			toAdd: [][]byte{
				[]byte("dir1/dx.txt"),
				[]byte("dir1/dx1.txt"),
				[]byte("dir1/di/a/b/x.txt"),
				[]byte("dir1/di/a/b/x/cv.txt"),
				[]byte("dir1/di/a/caaa.txt"),
				[]byte("dir1/di/a/c/aaa.txt"),
				[]byte("dir1/di/ab/c.txt"),
				[]byte("dir2/abc/de.txt"),
				[]byte("dir2/abc/de1.txt"),
				[]byte("dir2/abc/de/1.txt"),
				[]byte("dir2/abc/de/mm/n.txt"),
				[]byte("dir2/abcde/1.txt"),
				[]byte("dir3/1.txt"),
				[]byte("dir3/12.txt"),
				[]byte("dir3/2.txt"),
				[]byte("dir3/222.txt"),
			},
			target: [][]byte{
				[]byte("dir1/di/a/"),
				[]byte("dir2/abc/de/"),
			},
			expected: [][]byte{
				[]byte("dir2/abc/de/b/x.txt"),
				[]byte("dir2/abc/de/b/x/cv.txt"),
				[]byte("dir2/abc/de/caaa.txt"),
				[]byte("dir2/abc/de/c/aaa.txt"),
			},
			unwanted: [][]byte{
				[]byte("dir1/di/a/b/x.txt"),
				[]byte("dir1/di/a/b/x/cv.txt"),
				[]byte("dir1/di/a/caaa.txt"),
				[]byte("dir1/di/a/c/aaa.txt"),
			},
		},
		{
			toAdd: [][]byte{
				[]byte("a/aaaaa/aa.mp4"),
				[]byte("a/aa/aaa/aa.mp4"),
			},
			target: [][]byte{
				[]byte("a/aa/aaa/aa.mp4"),
				[]byte("a/aaaaa/"),
			},
			expected: [][]byte{
				[]byte("a/aaaaa/aa.mp4"),
			},
			unwanted: [][]byte{
				[]byte("a/aa/aaa/aa.mp4"),
				[]byte("a/aaaaa/a/aa/aaa/aa.mp4"),
			},
		},
		{
			toAdd: [][]byte{
				[]byte("test/a/a/a.png"),
			},
			target: [][]byte{
				[]byte("test/a/a/a.png"),
				[]byte("test/a/a/b.png"),
			},
			expected: [][]byte{
				[]byte("test/a/a/b.png"),
			},
			unwanted: [][]byte{
				[]byte("test/a/a/a.png"),
			},
		},
		{
			toAdd: [][]byte{
				[]byte("aaaaaa"),
				[]byte("aaaaab"),
				[]byte("abbbb"),
				[]byte("abbba"),
				[]byte("bbbbba"),
				[]byte("bbbaaa"),
				[]byte("bbbaab"),
				[]byte("aa"),
				[]byte("b"),
			},
			target: [][]byte{
				[]byte("aa"),
				[]byte("abbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
			},
			expected: [][]byte{
				[]byte("abbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
			},
			unwanted: [][]byte{
				[]byte("aa"),
			},
		},
	} {
		ctx := context.Background()
		t.Run(fmt.Sprintf("move-{%s}-to-{%s}", tc.target[0], tc.target[1]), func(t *testing.T) {
			n := mantaray.New()
			ls := newMockLoadSaver()

			for i := 0; i < len(tc.toAdd); i++ {
				c := tc.toAdd[i]
				e := append(make([]byte, 32-len(c)), c...)
				err := n.Add(ctx, c, e, nil, nil)
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
			}

			source := tc.target[0]
			target := tc.target[1]

			err := n.Move(ctx, n, source, target, true, ls)
			if err != nil {
				t.Fatal(err)
			}

			for _, expect := range tc.expected {
				_, err := n.LookupNode(ctx, expect, ls)
				if err != nil {
					t.Fatalf("find path %s failed: %v", expect, err)
				}
			}

			for _, unwant := range tc.unwanted {
				_, err := n.Lookup(ctx, unwant, ls)
				if !errors.Is(err, mantaray.ErrNotFound) {
					t.Fatalf("path %s should be removed, but find it or got error %v", unwant, err)
				}
			}
		})
	}
}
