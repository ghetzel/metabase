package metadata

import (
	"fmt"
)

type ImageLoader struct {
	Loader
}

func (self ImageLoader) CanHandle(_ string) bool {
	return false
}

func (self ImageLoader) LoadMetadata(name string) (map[string]interface{}, error) {
	return nil, fmt.Errorf("%T: NI", self)
}
