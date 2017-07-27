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

func GetGeneralFileType(filename string) string {
	if mediaType, _, err := mime.ParseMediaType(mime.TypeByExtension(filepath.Ext(filename))); err == nil {
		var major, minor string

		if parts := strings.SplitN(mediaType, `/`, 2); len(parts) == 2 {
			major = parts[0]
			minor = parts[1]
		}

		switch major {
		case `audio`:
			return `audio`
		case `video`:
			return `video`
		case `image`:
			return `image`
		}

		switch minor {
		case `ecmascript`, `html`, `javascript`, `scriptlet`, `vrml`, `x-c++hdr`, `x-c++src`, `x-chdr`, `x-csrc`,
			`x-dsrc`, `x-java`, `x-moc`, `x-pascal`, `x-perl`, `x-python`, `x-ruby`, `x-sh`, `x-sql`, `x-tcl`,
			`x-tex-pk`, `x-tex`, `x-vrml`:
			return `code`
		}
	}

	return `file`
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
			`hidden`:    strings.HasPrefix(stat.Name(), `.`),
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

				if parts := strings.SplitN(mediaType, `/`, 2); len(parts) == 2 {
					mimetype[`major`] = parts[0]
					mimetype[`minor`] = parts[1]
				}
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
