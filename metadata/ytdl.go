package metadata

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"strings"

	"github.com/ghetzel/go-stockutil/maputil"
	"github.com/ghetzel/go-stockutil/stringutil"
)

var DefaultExcludeFields = []string{
	`_filename`,
	`description`,
	`dislike_count`,
	`formats`,
	`http_headers`,
	`like_count`,
	`thumbnails`,
	`url`,
	`view_count`,
}

type YTDLLoader struct {
	Loader
	ExcludeFields []string
	infofile      string
}

func (self *YTDLLoader) CanHandle(name string) Loader {
	if infofile := self.getInfoFilePath(name); infofile != `` {
		if _, err := os.Stat(infofile); err == nil {
			return &YTDLLoader{
				infofile: infofile,
			}
		}
	}

	return nil
}

func (self *YTDLLoader) LoadMetadata(name string) (map[string]interface{}, error) {
	if self.ExcludeFields == nil {
		self.ExcludeFields = DefaultExcludeFields
	}

	if self.infofile != `` {
		return self.parseInfoFile(self.infofile)
	}

	return nil, nil
}

func (self *YTDLLoader) getInfoFilePath(name string) string {
	dir, base := path.Split(name)
	ext := path.Ext(base)

	if ext != `.json` {
		return path.Join(dir, strings.TrimSuffix(base, ext)+`.info.json`)
	}

	return ``
}

func (self *YTDLLoader) parseInfoFile(name string) (map[string]interface{}, error) {
	if file, err := os.Open(name); err == nil {
		rv := make(map[string]interface{})

		if err := json.NewDecoder(file).Decode(&rv); err == nil {
			var duration interface{}

			if dSecI := maputil.DeepGet(rv, []string{`duration`}, nil); dSecI != nil {
				if dSec, err := stringutil.ConvertToInteger(dSecI); err == nil {
					duration = dSec * int64(1000)
				}
			}

			output := map[string]interface{}{
				`media`: map[string]interface{}{
					`type`:        `web`,
					`title`:       maputil.DeepGet(rv, []string{`title`}, nil),
					`description`: maputil.DeepGet(rv, []string{`description`}, nil),
					`duration`:    duration,
					`aired`: func() interface{} {
						if v := maputil.DeepGet(rv, []string{`upload_date`}, nil); v != nil {
							if vS := fmt.Sprintf("%v", v); len(vS) == 8 {
								return vS[0:4] + `-` + vS[4:6] + `-` + vS[6:8]
							}
						}

						return nil
					}(),
					`rating`:    maputil.DeepGet(rv, []string{`average_rating`}, nil),
					`thumbnail`: maputil.DeepGet(rv, []string{`thumbnail`}, nil),
				},
			}

			for _, field := range self.ExcludeFields {
				maputil.DeepSet(rv, strings.Split(field, `.`), nil)
			}

			if v, err := maputil.Compact(rv); err == nil {
				rv = v
			}

			output[`ytdl`] = rv

			return output, nil
		} else {
			return nil, fmt.Errorf("Unrecognized ytdl info file format at %q: %v", name, err)
		}
	} else {
		return nil, err
	}
}
