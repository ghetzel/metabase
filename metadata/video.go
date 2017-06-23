package metadata

import (
	"fmt"
)

type VideoLoader struct {
	Loader
}

func (self VideoLoader) CanHandle(_ string) bool {
	return false
}

func (self VideoLoader) LoadMetadata(name string) (map[string]interface{}, error) {
	return nil, fmt.Errorf("%T: NI", self)
}
