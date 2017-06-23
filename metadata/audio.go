package metadata

import (
	"time"

	"github.com/wtolson/go-taglib"
)

type AudioLoader struct {
	Loader
	metadata *taglib.File
	data     map[string]interface{}
}

func (self AudioLoader) CanHandle(name string) Loader {
	if f, err := taglib.Read(name); err == nil {
		return &AudioLoader{
			metadata: f,
		}
	}

	return nil
}

func (self AudioLoader) LoadMetadata(name string) (map[string]interface{}, error) {
	if self.metadata != nil {
		defer self.metadata.Close()

		self.data = map[string]interface{}{
			`media`: map[string]interface{}{
				`artist`:     self.metadata.Artist(),
				`album`:      self.metadata.Album(),
				`genre`:      self.metadata.Genre(),
				`title`:      self.metadata.Title(),
				`track`:      self.metadata.Track(),
				`year`:       self.metadata.Year(),
				`comment`:    self.metadata.Comment(),
				`duration`:   (self.metadata.Length() / time.Millisecond),
				`bitrate`:    self.metadata.Bitrate(),
				`channels`:   self.metadata.Channels(),
				`samplerate`: self.metadata.Samplerate(),
			},
		}

		self.metadata = nil
	}

	return self.data, nil
}
