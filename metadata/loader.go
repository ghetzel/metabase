package metadata

import (
	"sort"
)

type Loader interface {
	CanHandle(string) Loader
	LoadMetadata(string) (map[string]interface{}, error)
}

type LoaderGroup struct {
	Pass    int
	Loaders []Loader
}

type LoaderSet []LoaderGroup

func (self LoaderSet) Passes() (passes []int) {
	for _, group := range self {
		if group.Pass > 0 {
			passes = append(passes, group.Pass)
		}
	}

	sort.Ints(passes)
	return
}

func GetLoaders() LoaderSet {
	SetupMimeTypes()

	return LoaderSet{
		{
			Pass: 1,
			Loaders: []Loader{
				&FileLoader{},
				&RegexLoader{},
				&MediaLoader{},
				&YTDLLoader{},
			},
		}, {
			Pass: 2,
			Loaders: []Loader{
				&AudioLoader{},
			},
		},
	}
}

func GetLoadersForFile(name string, pass int) []Loader {
	loaders := make([]Loader, 0)

	for _, group := range GetLoaders() {
		if pass <= 0 || group.Pass == pass {
			for _, loader := range group.Loaders {
				if instance := loader.CanHandle(name); instance != nil {
					loaders = append(loaders, instance)
				}
			}
		}
	}

	return loaders
}
