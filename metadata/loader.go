package metadata

import (
	"sort"
)

type Loader interface {
	CanHandle(string) Loader
	LoadMetadata(string) (map[string]interface{}, error)
}

type LoaderGroup struct {
	Pass     int
	Checksum bool
	Finalize bool
	Loaders  []Loader
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

func GetChecksumPass() int {
	for _, loader := range GetLoaders() {
		if loader.Checksum {
			return loader.Pass
		}
	}

	return -1
}

func GetLoaders() LoaderSet {
	initMime.Do(func() {
		SetupMimeTypes()
	})

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
			Pass:     2,
			Checksum: true,
			Finalize: true,
			Loaders: []Loader{
				&AudioLoader{},
				&VideoLoader{},
			},
		},
	}
}

func GetLoaderGroupForPass(pass int) *LoaderGroup {
	for _, group := range GetLoaders() {
		if group.Pass == pass {
			return &group
		}
	}

	return nil
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

func IsFinalizePass(pass int) bool {
	for _, group := range GetLoaders() {
		if group.Pass == pass {
			if group.Finalize {
				return true
			}
		}
	}

	return false
}
