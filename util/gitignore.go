package util

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"path"
	"strings"

	"github.com/gobwas/glob"
)

type GitIgnore struct {
	patterns         []pattern
	defaultIgnoreAll bool
}

type pattern interface {
	Match(path string, isDir bool) bool
	Inverted() bool
}

type globPattern struct {
	invert       bool
	dirOnly      bool
	leadingSlash bool
	orig         string
	glob         glob.Glob
	globSuffix   bool
	globPrefix   bool
	depth        int
}

func (self *globPattern) String() string {
	// return self.orig
	return fmt.Sprintf("%+v", self.glob)
}

func (self *globPattern) Inverted() bool {
	return self.invert
}

func (self *globPattern) Match(matchPath string, isDir bool) bool {
	if self.dirOnly && !isDir {
		return false
	}

	if self.leadingSlash {
		matchPath = "/" + strings.TrimPrefix(matchPath, `/`)
	}

	if isDir {
		if self.globSuffix {
			matchPath = matchPath + "/"
		}
		if self.globPrefix && !self.leadingSlash {
			matchPath = "/" + matchPath
		}
	}

	if self.depth == 0 {
		matchPath = path.Base(matchPath)
	} else {
		pathDepth := strings.Count(matchPath, "/")
		if pathDepth < self.depth {
			return false
		}
	}

	return self.glob.Match(matchPath)
}

func trimTrailingSpace(in []byte) []byte {
	// TODO: handle escaped last spaces
	return bytes.Trim(in, " ")
}

func NewGitIgnoreLines(lines []string) (*GitIgnore, error) {
	return NewGitIgnore(bytes.NewBufferString(strings.Join(lines, "\n")))
}

func NewGitIgnore(reader io.Reader) (*GitIgnore, error) {
	scanner := bufio.NewScanner(reader)
	gi := &GitIgnore{}

	globPrefix := []byte{'*', '*', '/'}
	globSuffix := []byte{'/', '*', '*'}

	for scanner.Scan() {
		line := scanner.Bytes()
		line = trimTrailingSpace(line)

		if len(line) == 0 {
			continue
		}

		// TODO: Escape \# to #
		if line[0] == '#' {
			continue
		}

		gp := &globPattern{
			orig: string(line),
		}

		if string(line) == `*` {
			gi.defaultIgnoreAll = true
		}

		// TODO: Escape \! to !
		if line[0] == '!' {
			// Strip first char
			line = line[1:]
			gp.invert = true
		}

		if line[0] == '/' {
			gp.leadingSlash = true
		}

		if line[len(line)-1] == '/' {
			// Strip trailing slash
			line = line[0 : len(line)-1]
			gp.dirOnly = true
		}

		for i := 0; i < len(line); i++ {
			if line[i] == '/' {
				gp.depth += 1
			}
		}

		if bytes.HasSuffix(line, globSuffix) {
			gp.globSuffix = true
		}
		if bytes.HasPrefix(line, globPrefix) {
			gp.globPrefix = true
		}

		gp.glob = glob.MustCompile(string(line), os.PathSeparator)
		gi.patterns = append(gi.patterns, gp)

	}

	return gi, nil
}

func (self *GitIgnore) ShouldKeep(path string, isDir bool) bool {
	for i := len(self.patterns) - 1; i >= 0; i-- {
		gp := self.patterns[i]

		// if we've matched...
		if gp.Match(path, isDir) {
			// apply match inversion
			match := !gp.Inverted()

			// invert again if we encountered an "ignore all" rule
			if self.defaultIgnoreAll {
				match = !match
			}

			// return the result
			return match
		}
	}

	return false
}
