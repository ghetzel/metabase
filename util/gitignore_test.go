package util

import (
	"path"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type singleMatch struct {
	path   string
	isDir  bool
	expect bool
}

func TestGitIgnoreMatch(t *testing.T) {
	// Implementing tests from this guy:
	// https://github.com/svent/gitignore-test
	// In Pubic Domain, via tweet 2016-4-3
	// @sven_t: Feel free to keep the test cases, I would consider the test repo public domain anyway.

	gi, _ := NewGitIgnore(strings.NewReader(`
*.[oa]
*.html
*.min.js


!foo*.html
foo-excl.html

vmlinux*

\!important!.txt

log/*.log
!/log/foo.log

**/logdir/log
**/foodir/bar
exclude/**

!findthis*

**/hide/**
subdir/subdir2/

/rootsubdir/

dirpattern/

README.md

# arch/foo/kernel/.gitignore
!arch/foo/kernel/vmlinux*

# htmldoc/.gitignore
!htmldoc/*.html

# git-sample-3/.gitignore
git-sample-3/*
!git-sample-3/foo
git-sample-3/foo/*
!git-sample-3/foo/bar
`))

	matches := []singleMatch{
		singleMatch{"!important!.txt", false, true},
		singleMatch{"arch", true, false},
		singleMatch{"arch/foo", true, false},
		singleMatch{"arch/foo/kernel", true, false},
		singleMatch{"arch/foo/kernel/vmlinux.lds.S", false, false},
		singleMatch{"arch/foo/vmlinux.lds.S", false, true},
		singleMatch{"bar", true, false},
		singleMatch{"bar/testfile", false, false},
		singleMatch{"dirpattern", false, false},
		singleMatch{"Documentation", true, false},
		singleMatch{"Documentation/foo-excl.html", false, true},
		singleMatch{"Documentation/foo.html", false, false},
		singleMatch{"Documentation/gitignore.html", false, true},
		singleMatch{"Documentation/test.a.html", false, true},
		singleMatch{"exclude", true, true},
		singleMatch{"exclude/dir1", true, true},
		singleMatch{"exclude/dir1/dir2", true, true},
		singleMatch{"exclude/dir1/dir2/dir3", true, true},
		singleMatch{"exclude/dir1/dir2/dir3/testfile", false, true},
		singleMatch{"file.o", false, true},
		singleMatch{"foodir", true, false},
		singleMatch{"foodir/bar", true, true},
		singleMatch{"foodir/bar/testfile", false, true},
		singleMatch{"git-sample-3", true, false},
		singleMatch{"git-sample-3/foo", true, false},
		singleMatch{"git-sample-3/foo", true, false},
		singleMatch{"git-sample-3/foo/bar", true, false},
		singleMatch{"git-sample-3/foo/test", true, true},
		singleMatch{"git-sample-3/foo/test", true, true},
		singleMatch{"git-sample-3/test", true, true},
		singleMatch{"htmldoc", true, false},
		singleMatch{"htmldoc/docs.html", false, false},
		singleMatch{"htmldoc/jslib.min.js", false, true},
		singleMatch{"lib.a", false, true},
		singleMatch{"log", true, false},
		singleMatch{"log/foo.log", false, false},
		singleMatch{"log/test.log", false, true},
		singleMatch{"rootsubdir", true, true},
		singleMatch{"rootsubdir/foo", false, true},
		singleMatch{"src", true, false},
		singleMatch{"src/findthis.o", false, false},
		singleMatch{"src/internal.o", false, true},
		singleMatch{"subdir", true, false},
		singleMatch{"subdir/hide", true, true},
		singleMatch{"subdir/hide/foo", false, true},
		singleMatch{"subdir/logdir", true, false},
		singleMatch{"subdir/logdir/log", true, true},
		singleMatch{"subdir/logdir/log/findthis.log", false, true},
		singleMatch{"subdir/logdir/log/foo.log", false, true},
		singleMatch{"subdir/logdir/log/test.log", false, true},
		singleMatch{"subdir/rootsubdir", true, false},
		singleMatch{"subdir/rootsubdir/foo", false, false},
		singleMatch{"subdir/subdir2", true, true},
		singleMatch{"subdir/subdir2/bar", false, true},
		singleMatch{"README.md", false, true},
	}

	for _, match := range matches {
		if result := matchTree(gi, match); result != match.expect {
			t.Errorf("Match should return %t, got %t on %v", match.expect, result, match)
		}
	}
}

// Matches up the entire tree, returning if any item matches
// This matches typical behavior where a tree is traversed, and the branch is skipped if the item matches
func matchTree(gi *GitIgnore, match singleMatch) bool {
	matchPath := match.path
	isDir := match.isDir

	for matchPath != "." {
		if gi.ShouldKeep(matchPath, isDir) {
			return true
		}
		matchPath = path.Dir(matchPath)
		isDir = true
	}
	return false
}

type assert struct {
	patterns []string
	file     file
	expect   bool
}

type file struct {
	path  string
	isDir bool
}

func TestGitIgnoreMatch2(t *testing.T) {
	/*
		// Source: github.com/monochromegane/go-gitignore/gitignore_test.go
		The MIT License (MIT)

		Copyright (c) [2015] [go-gitignore]
		Modified to use backup gitignore Vincent Khougaz [2016]

		Permission is hereby granted, free of charge, to any person obtaining a copy
		of this software and associated documentation files (the "Software"), to deal
		in the Software without restriction, including without limitation the rights
		to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
		copies of the Software, and to permit persons to whom the Software is
		furnished to do so, subject to the following conditions:

		The above copyright notice and this permission notice shall be included in all
		copies or substantial portions of the Software.

		THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
		IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
		FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
		AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
		LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
		OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
		SOFTWARE.
	*/
	asserts := []assert{
		assert{[]string{"a.txt"}, file{"a.txt", false}, true},
		assert{[]string{"*.txt"}, file{"a.txt", false}, true},
		assert{[]string{"dir/a.txt"}, file{"dir/a.txt", false}, true},
		assert{[]string{"dir/*.txt"}, file{"dir/a.txt", false}, true},
		assert{[]string{"**/dir2/a.txt"}, file{"dir1/dir2/a.txt", false}, true},
		assert{[]string{"**/dir3/a.txt"}, file{"dir1/dir2/dir3/a.txt", false}, true},
		assert{[]string{"a.txt"}, file{"dir/a.txt", false}, true},
		assert{[]string{"*.txt"}, file{"dir/a.txt", false}, true},
		assert{[]string{"a.txt"}, file{"dir1/dir2/a.txt", false}, true},
		// Duplicate
		//assert{[]string{"dir2/a.txt"}, file{"dir1/dir2/a.txt", false}, true},
		assert{[]string{"dir"}, file{"dir", true}, true},
		assert{[]string{"dir/"}, file{"dir", true}, true},
		assert{[]string{"dir/"}, file{"dir", false}, false},
		assert{[]string{"dir1/dir2/"}, file{"dir1/dir2", true}, true},
		assert{[]string{"/a.txt"}, file{"a.txt", false}, true},
		assert{[]string{"/dir/a.txt"}, file{"dir/a.txt", false}, true},
		assert{[]string{"/dir1/a.txt"}, file{"dir/dir1/a.txt", false}, false},
		assert{[]string{"/a.txt"}, file{"dir/a.txt", false}, false},
		assert{[]string{"a.txt", "b.txt"}, file{"dir/b.txt", false}, true},
		assert{[]string{"*.txt", "!b.txt"}, file{"dir/b.txt", false}, false},
		assert{[]string{"dir/*.txt", "!dir/b.txt"}, file{"dir/b.txt", false}, false},
		assert{[]string{"dir/*.txt", "!/b.txt"}, file{"dir/b.txt", false}, true},
	}

	for _, assert := range asserts {
		gi, _ := NewGitIgnoreLines(assert.patterns)
		result := gi.ShouldKeep(assert.file.path, assert.file.isDir)
		if result != assert.expect {
			t.Errorf("Match should return %t, got %t on %v", assert.expect, result, assert)
		}
	}
}

func TestGitIgnoreMatch3(t *testing.T) {
	assert := require.New(t)

	gi, err := NewGitIgnore(strings.NewReader(`
*
!/.config
!/.config/htop
!/.config/htop/**
!/.config/openbox
!/.config/openbox/**
/.config/openbox/*.local.*
`))

	assert.NoError(err)

	matches := []singleMatch{
		singleMatch{"/.bashrc", false, false},
		singleMatch{"/Desktop", true, false},
		singleMatch{"/Desktop/file.txt", false, false},
		singleMatch{"/.config", true, true},
		singleMatch{"/.config/google-chrome", true, false},
		singleMatch{"/.config/google-chrome/buncha-files.log", false, false},
		singleMatch{"/.config/openbox", true, true},
		singleMatch{"/.config/openbox/include", true, true},
		singleMatch{"/.config/openbox/include/keys.xml", false, true},
		singleMatch{"/.config/openbox/include/common", true, true},
		singleMatch{"/.config/openbox/include/common/mouse.xml", false, true},
		singleMatch{"/.config/openbox/autostart.sh", false, true},
		singleMatch{"/.config/openbox/autostart.local.sh", false, false},
		singleMatch{"/.config/openbox/rc.xml", false, true},
	}

	for _, match := range matches {
		assert.Equal(match.expect, gi.ShouldKeep(match.path, match.isDir), match.path)
	}
}
