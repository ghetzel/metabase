package metadata

import (
	"regexp"
	"strings"

	"github.com/ghetzel/go-stockutil/stringutil"
)

var RegexpPatterns []*regexp.Regexp

type RegexLoader struct {
	Loader
}

func (self *RegexLoader) CanHandle(name string) Loader {
	for _, pattern := range RegexpPatterns {
		if pattern.MatchString(name) {
			return self
		}
	}

	return nil
}

func (self *RegexLoader) LoadMetadata(name string) (map[string]interface{}, error) {
	metadata := map[string]interface{}{}

	for _, pattern := range RegexpPatterns {
		if match := pattern.FindStringSubmatch(name); len(match) > 0 {
			for i, fieldName := range pattern.SubexpNames() {
				if i > 0 && match[i] != `` {
					metadata[strings.Replace(fieldName, `__`, `.`, -1)] = stringutil.Autotype(match[i])
				}
			}
		}
	}

	return metadata, nil
}
