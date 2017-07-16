package metabase

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/ghetzel/go-stockutil/stringutil"
	"github.com/ghetzel/go-stockutil/typeutil"
)

type ManifestItemType string

const (
	FileItem      ManifestItemType = `file`
	DirectoryItem                  = `directory`
)

type ManifestValue interface{}

var DefaultManifestFields = []string{`id`, `relative_path`, `type`}

type ManifestItem struct {
	ID           string
	Type         ManifestItemType
	Label        string
	RelativePath string
	Values       []ManifestValue
}

func (self *ManifestItem) NeedsUpdate(manifest *Manifest, policy *SyncPolicy) (bool, error) {
	var absPath string
	var stat os.FileInfo

	// log.Debugf("Compare: %v to %v", manifest.Fields, self.Values)

	// perform the local filesystem check.  If the file does not exist, then we don't have it
	if localAbsPath, err := filepath.Abs(
		filepath.Join(manifest.BaseDirectory, self.RelativePath),
	); err == nil {
		absPath = localAbsPath

		if fileinfo, err := os.Stat(localAbsPath); os.IsNotExist(err) {
			log.Debugf("Need %s because a local copy does not exist", self.ID)
			return true, nil
		} else if err != nil {
			return false, err
		} else {
			stat = fileinfo
		}
	} else {
		return false, err
	}

	if stat.IsDir() {
		return false, nil
	}

	// perform field checks
	file := NewEntry(self.Label, manifest.BaseDirectory, absPath)

	for i, value := range self.Values {
		if i < len(manifest.Fields) {
			fieldName := manifest.Fields[i]
			// log.Debugf("Item %s: check field %q", self.ID, fieldName)

			switch fieldName {
			case `checksum`:
				if sum, err := file.GenerateChecksum(true); err == nil {
					if fmt.Sprintf("%v", value) != sum {
						log.Debugf("Need %s because field 'checksum' differs from local copy", self.ID)
						return true, nil
					}
				} else {
					return false, err
				}

			default:
				// lazy load file metadata
				if !file.metadataLoaded {
					if err := file.LoadAllMetadata(); err != nil {
						return false, err
					}
				}

				// perform metadata comparison
				if !policy.Compare(fieldName, file.Get(fieldName), value) {
					log.Debugf("Need %s because field '%s' differs from local copy", self.ID, fieldName)
					return true, nil
				}
			}
		} else {
			return false, fmt.Errorf(
				"Manifest item %s contains fewer fields than the given policy requires",
				self.ID,
			)
		}
	}

	// log.Debugf("Item %s: up-to-date at %s", self.ID, absPath)
	return false, nil
}

type Manifest struct {
	BaseDirectory string
	Items         []ManifestItem
	Fields        []string
}

func NewManifest(baseDirectory string, fields ...string) *Manifest {
	return &Manifest{
		BaseDirectory: baseDirectory,
		Items:         make([]ManifestItem, 0),
		Fields:        fields,
	}
}

func (self *Manifest) LoadTSV(r io.Reader) error {
	scanner := bufio.NewScanner(r)
	self.Fields = nil
	allFields := make([]string, 0)

ScanLine:
	for scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return err
		}

		line := scanner.Text()
		values := strings.Split(line, "\t")

		// parse the header into fields
		if self.Fields == nil {
			if len(values) >= len(DefaultManifestFields) {
				allFields = values
				self.Fields = values[len(DefaultManifestFields):]
			} else {
				return fmt.Errorf(
					"Not enough columns in header (expected: >= %d, got: %d)",
					len(DefaultManifestFields),
					len(values),
				)
			}
		} else {
			expected := len(values)
			actual := len(allFields)

			if expected != actual {
				return fmt.Errorf(
					"Column count does not match given schema (got %d values for %d fields)",
					expected,
					actual,
				)
			}

			item := ManifestItem{}

			for i, value := range values {
				fieldName := allFields[i]

				switch fieldName {
				case `id`:
					item.ID = value
				case `type`:
					switch value {
					case `directory`:
						item.Type = DirectoryItem
					case `file`:
						item.Type = FileItem
					default:
						return fmt.Errorf("Unrecognized type %q", value)
					}
				case `label`:
					item.Label = value
				case `relative_path`:
					item.RelativePath = value
				default:
					if typeutil.IsEmpty(value) {
						log.Warningf("Invalid manifest TSV: field '%s' is empty", fieldName)
						continue ScanLine
					}

					item.Values = append(item.Values, stringutil.Autotype(value))
				}
			}

			self.Add(item)
		}
	}

	return nil
}

func (self *Manifest) Add(items ...ManifestItem) {
	self.Items = append(self.Items, items...)
}

func (self *Manifest) GetUpdateManifest(policy SyncPolicy) (*Manifest, error) {
	diff := NewManifest(self.BaseDirectory)
	copy(diff.Fields, self.Fields)

	for _, item := range self.Items {
		if update, err := item.NeedsUpdate(self, &policy); err == nil {
			if update {
				diff.Add(item)
			}
		} else {
			return nil, err
		}
	}

	if l := len(diff.Items); l == 0 {
		log.Debugf("Local directory %v is up-to-date", self.BaseDirectory)
	} else {
		log.Debugf("Want %d items from remote sources", l)
	}

	return diff, nil
}
