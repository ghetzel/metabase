package metadata

import (
	"mime"
	"os"
	"path/filepath"
	"strings"
)

var FileModeFlags = map[string]os.FileMode{
	`symlink`:   os.ModeSymlink,
	`device`:    os.ModeDevice,
	`pipe`:      os.ModeNamedPipe,
	`socket`:    os.ModeSocket,
	`character`: os.ModeCharDevice,
}

type FileLoader struct {
	Loader
}

func (self *FileLoader) CanHandle(_ string) Loader {
	return self
}

func (self *FileLoader) LoadMetadata(name string) (map[string]interface{}, error) {
	if stat, err := os.Stat(name); err == nil {
		mode := stat.Mode()
		perms := map[string]interface{}{
			`mode`:      mode.Perm(),
			`regular`:   mode.IsRegular(),
			`string`:    mode.String(),
			`directory`: mode.IsDir(),
		}

		for lbl, flag := range FileModeFlags {
			if (mode & flag) == flag {
				perms[lbl] = true
			}
		}

		metadata := map[string]interface{}{
			`name`:        stat.Name(),
			`permissions`: perms,
			`modified_at`: stat.ModTime(),
		}

		if !mode.IsDir() {
			mimetype := make(map[string]interface{})

			if mediaType, mimeParams, err := mime.ParseMediaType(mime.TypeByExtension(filepath.Ext(stat.Name()))); err == nil {
				for k, v := range mimeParams {
					mimetype[k] = v
				}

				mimetype[`type`] = mediaType
			}

			metadata[`mime`] = mimetype
			metadata[`size`] = stat.Size()

			if strings.HasPrefix(stat.Name(), `.`) {
				metadata[`hidden`] = true
			} else {
				metadata[`extension`] = strings.TrimPrefix(filepath.Ext(stat.Name()), `.`)
			}
		}

		return map[string]interface{}{
			`file`: metadata,
		}, nil
	} else {
		return nil, err
	}
}
