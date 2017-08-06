package metadata

import (
	"encoding/json"
	"os/exec"
	"strings"

	"github.com/ghetzel/go-stockutil/maputil"
	"github.com/ghetzel/go-stockutil/sliceutil"
	"github.com/ghetzel/go-stockutil/stringutil"
	"github.com/ghetzel/go-stockutil/typeutil"
)

var FFProbeCommandName = `ffprobe`
var FFProbeCommandArguments = []string{
	`-v`, `quiet`,
	`-show_format`,
	`-print_format`, `json`,
	`-show_streams`,
	`{source}`,
}

var FFProbeOmitFields = []string{
	`format.filename`,
}

type VideoLoader struct {
	Loader
}

func (self *VideoLoader) CanHandle(filename string) Loader {
	if GetGeneralFileType(filename) == `video` {
		return &VideoLoader{}
	}

	return nil
}

func (self *VideoLoader) LoadMetadata(name string) (map[string]interface{}, error) {
	if info, err := self.probeVideoInfo(name); err == nil {
		var duration interface{}

		if dSecI := maputil.DeepGet(info, []string{`format`, `duration`}, nil); dSecI != nil {
			if dSecF, err := stringutil.ConvertToFloat(dSecI); err == nil {
				duration = int64(dSecF * float64(1000))
			}
		}

		return map[string]interface{}{
			`media`: map[string]interface{}{
				`type`:     `video`,
				`duration`: duration,
			},
			`video`: info,
		}, nil
	} else {
		return nil, err
	}
}

// Loads video metadata from the source file
func (self *VideoLoader) probeVideoInfo(filename string) (map[string]interface{}, error) {
	rv := make(map[string]interface{})

	args := make([]string, len(FFProbeCommandArguments))
	copy(args, FFProbeCommandArguments)

	for i, _ := range args {
		args[i] = strings.Replace(
			args[i], `{source}`, filename, -1,
		)
	}

	probe := exec.Command(FFProbeCommandName, args...)
	probe.Env = []string{
		`AV_LOG_FORCE_NOCOLOR=1`,
	}

	if data, err := probe.Output(); err == nil {
		var metadata map[string]interface{}

		if err := json.Unmarshal(data, &metadata); err == nil {
			// recursively walk through all metadata values, autotyping and dropping empty ones
			if err := maputil.Walk(metadata, func(value interface{}, path []string, isLeaf bool) error {
				if isLeaf {
					if !sliceutil.Contains(FFProbeOmitFields, strings.Join(path, `.`)) {
						if !typeutil.IsEmpty(value) {
							maputil.DeepSet(rv, path, stringutil.Autotype(value))
						}
					}
				}

				return nil
			}); err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	} else {
		return nil, err
	}

	return rv, nil
}
