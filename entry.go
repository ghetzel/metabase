package metabase

import (
	"bufio"
	"crypto/sha1"
	"encoding/base32"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"os"
	"path"
	"strings"
	"time"

	"github.com/ghetzel/go-stockutil/maputil"
	"github.com/ghetzel/go-stockutil/sliceutil"
	"github.com/ghetzel/go-stockutil/stringutil"
	"github.com/ghetzel/go-stockutil/typeutil"
	"github.com/ghetzel/metabase/metadata"
	"github.com/spaolacci/murmur3"
)

const FileFingerprintSize = 16777216

var MetadataEncoding = base32.NewEncoding(`abcdefghijklmnopqrstuvwxyz234567`)
var MaxChildEntries = 10000

type Entry struct {
	ID              string                 `json:"id"`
	RelativePath    string                 `json:"name"`
	Parent          string                 `json:"parent,omitempty"`
	Checksum        string                 `json:"checksum,omitempty"`
	Size            int64                  `json:"size,omitempty"`
	RootGroup       string                 `json:"root_group"`
	IsGroup         bool                   `json:"group"`
	ChildCount      int                    `json:"children"`
	DescendantCount int                    `json:"descendants"`
	LastModifiedAt  int64                  `json:"last_modified_at,omitempty"`
	Metadata        map[string]interface{} `json:"metadata"`
	InitialPath     string                 `json:"-"`
	info            os.FileInfo
	metadataLoaded  bool
}

type WalkFunc func(path string, file *Entry, err error) error // {}

func NewEntry(rootGroup string, root string, name string) *Entry {
	normFileName := NormalizeFileName(root, name)

	return &Entry{
		ID:           FileIdFromName(rootGroup, normFileName),
		RootGroup:    rootGroup,
		RelativePath: normFileName,
		Metadata:     make(map[string]interface{}),
		InitialPath:  name,
	}
}

func (self *Entry) Info() os.FileInfo {
	return self.info
}

func (self *Entry) LastModifiedTime() time.Time {
	if self.LastModifiedAt == 0 {
		return time.Time{}
	} else {
		return time.Unix(0, self.LastModifiedAt)
	}
}

func (self *Entry) LoadAllMetadata() error {
	if err := self.LoadMetadata(0); err != nil {
		return err
	}

	return nil
}

func (self *Entry) LoadMetadata(pass int) error {
	if stat, err := os.Stat(self.InitialPath); err == nil {
		self.info = stat
	} else {
		return err
	}

	for _, loader := range metadata.GetLoadersForFile(self.InitialPath, pass) {
		if data, err := loader.LoadMetadata(self.InitialPath); err == nil {
			// unwrap dot-separated keys into a deeply nested map for iteration
			if diffused, err := maputil.DiffuseMap(data, `.`); err == nil {
				// recursively walk through all nested keys of the map, testing that leaf values
				// are not empty before committing them to Metadata
				if err := maputil.Walk(diffused, func(value interface{}, path []string, isLeaf bool) error {
					if isLeaf {
						if !typeutil.IsEmpty(value) {
							maputil.DeepSet(self.Metadata, path, value)
						}
					}

					return nil
				}); err != nil {
					log.Warningf("%T on %q: %v", loader, self.InitialPath, err)
				}
			} else {
				return err
			}
		} else {
			log.Warningf("%T on %q: %v", loader, self.InitialPath, err)
		}
	}

	self.metadataLoaded = true

	return nil
}

func (self *Entry) String() string {
	if data, err := json.MarshalIndent(self, ``, `  `); err == nil {
		return string(data[:])
	} else {
		return err.Error()
	}
}

func (self *Entry) Children(filterString ...string) ([]*Entry, error) {
	filterString = append(filterString, fmt.Sprintf("parent=%s", self.ID))

	if f, err := ParseFilter(sliceutil.CompactString(filterString)); err == nil {
		f.Limit = MaxChildEntries
		f.Sort = []string{`-directory`, `name`}

		files := make([]*Entry, 0)

		if err := Metadata.Find(f, &files); err == nil {
			// enforce a strict path hierarchy for parent-child relationships
			for _, file := range files {
				if !strings.HasPrefix(file.RelativePath, self.RelativePath+`/`) {
					return nil, fmt.Errorf("child entry falls outside of parent path")
				}
			}

			return files, nil
		} else {
			return nil, err
		}
	} else {
		return nil, err
	}
}

func (self *Entry) GenerateChecksum(forceRecalculate bool) (string, error) {
	if self.IsGroup {
		return ``, fmt.Errorf("Cannot generate checksum on directory")
	}

	if !forceRecalculate {
		if ckFile, err := os.Open(fmt.Sprintf("%s.sha1", self.InitialPath)); err == nil {
			scanner := bufio.NewScanner(ckFile)

			for scanner.Scan() {
				if scanner.Err() == nil {
					parts := strings.SplitN(scanner.Text(), ` `, 3)

					// looks for all the world like a SHA-1 sum....
					if len(parts) == 3 && stringutil.IsHexadecimal(parts[0], 40) {
						if path.Base(parts[2]) == path.Base(self.InitialPath) {
							return parts[0], nil
						}
					}
				}
			}
		}
	}

	if fsFile, err := os.Open(self.InitialPath); err == nil {
		hash := sha1.New()

		if _, err := io.Copy(hash, fsFile); err != nil {
			return ``, err
		}

		result := hash.Sum(nil)
		return hex.EncodeToString([]byte(result[:])), nil
	} else {
		return ``, err
	}
}

func (self *Entry) GetAbsolutePath() (string, error) {
	if rootDirectory, ok := rootGroupToPath[self.RootGroup]; ok {
		return path.Join(rootDirectory, self.RelativePath), nil
	} else {
		return ``, fmt.Errorf("Unknown path for root group %q", self.RootGroup)
	}
}

func (self *Entry) GetHumanSize(format string) string {
	if format == `` {
		format = "%f"
	}

	if human, err := stringutil.ToByteString(self.Size, format); err == nil {
		return human
	}

	return fmt.Sprintf("%v", self.Size)
}

func (self *Entry) Get(key string, fallback ...interface{}) interface{} {
	if len(fallback) == 0 {
		fallback = append(fallback, nil)
	}

	return maputil.DeepGet(self.Metadata, strings.Split(key, `.`), fallback[0])
}

func (self *Entry) GetManifest(fields []string, filterString string) (*Manifest, error) {
	manifest := NewManifest(self.RelativePath)

	if err := self.Walk(func(path string, entry *Entry, err error) error {
		if err == nil {
			var itemType ManifestItemType

			if entry.IsGroup {
				return nil
			} else {
				itemType = FileItem
			}

			fieldValues := make([]ManifestValue, len(fields))

			for i, field := range fields {
				switch field {
				case `name`:
					fieldValues[i] = entry.RelativePath
				case `root_group`:
					fieldValues[i] = entry.RootGroup
				case `parent`:
					fieldValues[i] = entry.Parent
				case `checksum`:
					fieldValues[i] = entry.Checksum
				default:
					fieldValues[i] = entry.Get(field)
				}

				manifest.Fields = append(manifest.Fields, field)
			}

			manifest.Add(ManifestItem{
				ID:           entry.ID,
				Type:         itemType,
				RelativePath: path,
				Values:       fieldValues,
			})

			return nil
		} else {
			return err
		}
	}, filterString); err != nil {
		return nil, err
	}

	return manifest, nil
}

func (self *Entry) normalizeLoaderName(loader metadata.Loader) string {
	name := fmt.Sprintf("%T", loader)
	name = strings.TrimPrefix(name, `metadata.`)
	name = strings.TrimSuffix(name, `Loader`)

	return stringutil.Underscore(name)
}

func (self *Entry) Walk(walkFn WalkFunc, filterStrings ...string) error {
	if self.IsGroup {
		if err := walkFn(self.RelativePath, self, nil); err == nil {
			if children, err := self.Children(filterStrings...); err == nil {
				for _, child := range children {
					if err := child.Walk(walkFn, filterStrings...); err != nil {
						return err
					}
				}

				return nil
			} else {
				return err
			}
		} else {
			return err
		}
	} else {
		return walkFn(self.RelativePath, self, nil)
	}
}

func FileIdFromName(rootGroup string, name string) string {
	uid := fmt.Sprintf("%s:%s", rootGroup, name)
	hash64 := murmur3.Sum64([]byte(uid[:]))
	return strings.TrimRight(
		MetadataEncoding.EncodeToString(big.NewInt(int64(hash64)).Bytes()),
		`=`,
	)
}

func NormalizeFileName(root string, name string) string {
	prefix := strings.TrimSuffix(root, `/`)
	name = strings.TrimPrefix(name, prefix)
	name = `/` + strings.TrimPrefix(name, `/`)

	return name
}
