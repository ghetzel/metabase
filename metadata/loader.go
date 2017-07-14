package metadata

type Loader interface {
	CanHandle(string) Loader
	LoadMetadata(string) (map[string]interface{}, error)
}

func GetLoaders() []Loader {
	return []Loader{
		&FileLoader{},
		&RegexLoader{},
		&MediaLoader{},
		&AudioLoader{},
		&YTDLLoader{},
	}
}

func GetLoadersForFile(name string) []Loader {
	loaders := make([]Loader, 0)

	for _, loader := range GetLoaders() {
		if instance := loader.CanHandle(name); instance != nil {
			loaders = append(loaders, instance)
		}
	}

	return loaders
}
