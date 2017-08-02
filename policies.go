package metabase

import (
	"github.com/ghetzel/go-stockutil/stringutil"
)

type SyncPolicy struct {
	ID     string   `json:"id"`
	Fields []string `json:"fields"`
}

func (self *SyncPolicy) Compare(field string, value interface{}, other interface{}) bool {
	// TODO: provide some kind of comparator other than ==
	if eq, err := stringutil.RelaxedEqual(value, other); err == nil && eq {
		return true
	}

	return false
}

var ChecksumPolicy = SyncPolicy{
	Fields: []string{`checksum`},
}
