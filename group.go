package metabase

import (
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/ghetzel/go-stockutil/pathutil"
	"github.com/ghetzel/go-stockutil/sliceutil"
	"github.com/ghetzel/metabase/metadata"
	"github.com/ghetzel/metabase/util"
	"github.com/ghetzel/mobius"
)

type Group struct {
	ID                   string                 `json:"id"`
	Path                 string                 `json:"path"`
	Parent               string                 `json:"parent"`
	RootPath             string                 `json:"-"`
	FilePattern          string                 `json:"file_pattern,omitempty"`
	NoRecurseDirectories bool                   `json:"no_recurse"`
	FollowSymlinks       bool                   `json:"follow_symlinks"`
	FileMinimumSize      int                    `json:"min_file_size,omitempty"`
	DeepScan             bool                   `json:"deep_scan"`
	SkipChecksum         bool                   `json:"skip_checksum"`
	CurrentPass          int                    `json:"-"`
	PassesDone           int                    `json:"-"`
	TargetSubgroups      []string               `json:"-"`
	FileCount            int                    `json:"file_count"`
	ModifiedFileCount    int                    `json:"modified_file_count"`
	Properties           map[string]interface{} `json:"properties,omitempty"`
	compiledIgnoreList   *util.GitIgnore
	parentGroup          *Group
	db                   *DB
}

var SkipEntry = errors.New("skip entry")
var RootGroupName = `root`
var MaxTimeBetweenDeepScans = time.Duration(0)

type WalkEntryFunc func(entry *Entry, isNew bool) error // {}
type PopulateGroupFunc func(group *Group) error         // {}
var parentPathCache sync.Map

var PopulateGroup = func(group *Group) error {
	if group.ID == `` && group.Path != `` {
		group.ID = path.Base(group.Path)
	}

	if group.RootPath == `` {
		group.RootPath = group.Path
	}

	group.RootPath = strings.TrimSuffix(group.RootPath, `/`)

	if group.Parent == `` {
		group.Parent = RootGroupName
	}

	return nil
}

func (self *Group) Initialize() error {
	if self.Path == `` {
		return fmt.Errorf("Group path must be specified.")
	} else {
		if p, err := pathutil.ExpandUser(self.Path); err == nil {
			self.Path = p
		} else {
			return err
		}
	}

	PopulateGroup(self)

	return nil
}

func (self *Group) ContainsPath(absPath string) bool {
	relPath := strings.TrimPrefix(absPath, self.RootPath)

	// perform a simple "does this path start with this directory's path" check before symlink deref
	if !strings.HasPrefix(path.Clean(absPath), path.Clean(self.Path)) {
		return false
	}

	if fileStat, err := os.Stat(absPath); err == nil {
		if realstat, err := self.resolveRealStat(absPath, fileStat); err == nil {
			fileStat = realstat

			self.populateIgnoreList()

			// if an ignore list is in effect for this directory, verify our file isn't in it
			if self.compiledIgnoreList != nil {
				if !self.compiledIgnoreList.ShouldKeep(relPath, fileStat.Mode().IsDir()) {
					return false
				}
			}

			// if we just got though all that, we belong here
			return true
		} else {
			return false
		}
	} else {
		return false
	}
}

func (self *Group) GetLatestModifyTime() (time.Time, error) {
	if f, err := ParseFilter(map[string]interface{}{
		`root_group`: self.ID,
	}); err == nil {
		if epochNs, err := Metadata.Maximum(`last_modified_at`, f); err == nil {
			return time.Unix(0, int64(epochNs)), nil
		} else {
			return time.Time{}, err
		}
	} else {
		return time.Time{}, err
	}
}

func (self *Group) GetParentFromPath(relPath string) (string, error) {
	parentName := strings.TrimPrefix(path.Dir(relPath), self.Path)

	if parentName == `/` {
		parentName = RootGroupName
	}

	parentsCacheKey := fmt.Sprintf("%v__%v", self.ID, parentName)

	if v, ok := parentPathCache.Load(parentsCacheKey); ok {
		if vStr, ok := v.(string); ok {
			return vStr, nil
		}
	}

	if f, err := ParseFilter(map[string]interface{}{
		`root_group`: self.ID,
		`name`:       fmt.Sprintf("is:%v", parentName),
	}); err == nil {
		var results []*Entry

		if err := Metadata.Find(f, &results); err == nil {
			if l := len(results); l == 1 {
				parentPath := fmt.Sprintf("%v", results[0].ID)
				parentPathCache.Store(parentsCacheKey, parentPath)

				return parentPath, nil
			} else {
				return ``, fmt.Errorf("Failed to get parent ID: expected 1 result, got: %d", l)
			}
		} else {
			return ``, fmt.Errorf("Failed to get parent ID: %v", err)
		}
	} else {
		return ``, fmt.Errorf("Failed to get parent ID: %v", err)
	}
}

func (self *Group) populateIgnoreList() error {
	// file pattern matching
	if self.FilePattern != `` {
		if self.compiledIgnoreList == nil {
			if ig, err := util.NewGitIgnoreLines(strings.Split(self.FilePattern, "\n")); err == nil {
				self.compiledIgnoreList = ig
			} else {
				return err
			}
		}
	}

	return nil
}

func (self *Group) Scan(subgroups []string) error {
	self.TargetSubgroups = subgroups

	if err := self.populateIgnoreList(); err != nil {
		return err
	}

	// reset file count for each pass
	self.FileCount = 0
	self.ModifiedFileCount = 0

	if fileStats, err := ioutil.ReadDir(self.Path); err == nil {
		for _, file := range fileStats {
			if err := self.ScanPath(path.Join(self.Path, file.Name())); err == SkipEntry {
				continue
			} else if err != nil {
				return err
			}
		}
	} else {
		return err
	}

	return nil
}

func (self *Group) GetAncestors() []string {
	if self.parentGroup == nil {
		return []string{self.ID}
	} else {
		return append([]string{self.Parent}, self.parentGroup.GetAncestors()...)
	}
}

func (self *Group) ScanPath(absPath string) error {
	parentsCleanup := make([]string, 0)
	parentsCleanupForced := make([]string, 0)

	defer func() {
		self.cleanupMissingEntriesUnderParents(parentsCleanup, false)
		self.cleanupMissingEntriesUnderParents(parentsCleanupForced, true)
	}()

	if fileStat, err := os.Stat(absPath); err == nil {
		if realstat, err := self.resolveRealStat(absPath, fileStat); err == nil {
			fileStat = realstat
		} else {
			log.Warningf("PASS %d: [%s] %v", self.CurrentPass, self.ID, err)
			return SkipEntry
		}

		relPath := strings.TrimPrefix(absPath, self.RootPath)
		var parent string

		if p, err := self.GetParentFromPath(relPath); err == nil {
			parent = p
		} else {
			parent = self.Parent
		}

		dirEntry := NewEntry(self.ID, self.RootPath, absPath)

		if !self.ContainsPath(absPath) {
			log.Debugf("PASS %d: [%s] Ignoring entry %s", self.CurrentPass, self.ID, relPath)
			parentsCleanupForced = append(parentsCleanupForced, dirEntry.ID)
			self.cleanupMissingEntries(map[string]interface{}{`id`: dirEntry.ID}, true)
			self.cleanupMissingEntries(map[string]interface{}{`id`: self.ID}, true)
			return SkipEntry
		}

		// recursive directory handling
		if fileStat.IsDir() {
			if !self.NoRecurseDirectories {
				subdirectory := new(Group)

				if !self.DeepScan {
					if self.PassesDone > 0 {
						if self.hasNotChanged(dirEntry.ID) {
							return SkipEntry
						}
					}
				}

				if err := PopulateGroup(subdirectory); err == nil {
					subdirectory.compiledIgnoreList = self.compiledIgnoreList
					subdirectory.CurrentPass = self.CurrentPass
					subdirectory.db = self.db
					subdirectory.DeepScan = self.DeepScan
					subdirectory.FileMinimumSize = self.FileMinimumSize
					subdirectory.FilePattern = self.FilePattern
					subdirectory.FollowSymlinks = self.FollowSymlinks
					subdirectory.ID = self.ID
					subdirectory.NoRecurseDirectories = self.NoRecurseDirectories
					subdirectory.Parent = dirEntry.ID
					subdirectory.parentGroup = self
					subdirectory.PassesDone = self.PassesDone
					subdirectory.Path = absPath
					subdirectory.RootPath = self.RootPath

					if err := subdirectory.Initialize(); err == nil {
						if len(self.TargetSubgroups) > 0 {
							if !sliceutil.ContainsString(self.TargetSubgroups, subdirectory.Parent) {
								// log.Debugf("PASS %d: Skipping group %s [%s]", self.CurrentPass, subdirectory.Path, subdirectory.Parent)
								return nil
							}
						}

						log.Debugf("PASS %d: [%s] %16s: Scanning subdirectory %s", self.CurrentPass, self.ID, subdirectory.Parent, relPath)

						if err := subdirectory.Scan(self.TargetSubgroups); err == nil {
							self.FileCount = subdirectory.FileCount
							self.ModifiedFileCount += subdirectory.ModifiedFileCount
						} else {
							return err
						}
					} else {
						return err
					}

					if self.FileCount == 0 {
						// cleanup entries for whom we are the parent
						if f, err := ParseFilter(map[string]interface{}{
							`parent`: subdirectory.Parent,
						}); err == nil {
							if values, err := Metadata.ListWithFilter([]string{`id`}, f); err == nil {
								if ids, ok := values[`id`]; ok {
									Metadata.Delete(ids...)
								}
							} else {
								log.Errorf("PASS %d: [%s] Failed to cleanup entries under %s: %v", self.CurrentPass, self.ID, subdirectory.Parent, err)
							}
						} else {
							log.Errorf("PASS %d: [%s] Failed to cleanup entries under %s: %v", self.CurrentPass, self.ID, subdirectory.Parent, err)
						}

						if Metadata.Exists(dirEntry.ID) {
							Metadata.Delete(dirEntry.ID)
						}
					} else {
						if _, err := self.scanEntry(absPath, parent, true); err == nil {
							// cleanup entries for whom we are the parent
							parentsCleanup = append(parentsCleanup, subdirectory.Parent)
						} else {
							return err
						}
					}
				} else {
					return fmt.Errorf("Failed to populate new group: %v", err)
				}
			}
		} else {
			// if we've specified a minimum file size, and this file is less than that,
			// then skip it
			if self.FileMinimumSize > 0 && fileStat.Size() < int64(self.FileMinimumSize) {
				return SkipEntry
			}

			// scan the entry as a sharable asset
			if _, err := self.scanEntry(absPath, parent, false); err == nil {
				self.FileCount += 1
			} else {
				return err
			}
		}
	} else {
		return err
	}

	return nil
}

func (self *Group) WalkModifiedSince(lastModifiedAt time.Time, entryFn WalkEntryFunc) error {

	return filepath.Walk(self.Path, func(name string, info os.FileInfo, err error) error {
		if err == nil {
			if self.ContainsPath(name) {
				if !info.Mode().IsDir() {
					if info.ModTime().Add(-1 * time.Second).After(lastModifiedAt) {
						if entry := NewEntry(self.ID, self.Path, name); Metadata.Exists(entry.ID) {
							if err := Metadata.Get(entry.ID, entry); err == nil {
								return entryFn(entry, false)
							} else {
								log.Warningf("Failed to retrieve entry %v: %v", entry.ID, err)
							}
						} else {
							return entryFn(entry, true)
						}
					}
				}
			} else {
				// if this is a directory not contained in the current
				if info.Mode().IsDir() {
					return filepath.SkipDir
				}
			}
		}

		return nil
	})
}

func (self *Group) RefreshStats() error {
	if f, err := ParseFilter(`all`); err == nil {
		f.Limit = -1
		f.Fields = []string{`directory`, `size`}
		f.Sort = []string{`-directory`, `size`}

		// file stats
		if filesFilter, err := f.NewFromMap(map[string]interface{}{
			`bool:directory`: `false`,
		}); err == nil {
			if v, err := Metadata.Sum(`size`, filesFilter); err == nil {
				mobius.Gauge(`metabase.db.total_bytes`, float64(v), map[string]interface{}{
					`root_group`: self.ID,
				})
			} else {
				return err
			}

			if v, err := Metadata.Count(filesFilter); err == nil {
				mobius.Gauge(`metabase.db.file_count`, float64(v), map[string]interface{}{
					`root_group`: self.ID,
				})
			} else {
				return err
			}
		} else {
			return err
		}

		// directory stats
		if dirFilter, err := f.NewFromMap(map[string]interface{}{
			`bool:directory`: `true`,
		}); err == nil {
			if v, err := Metadata.Count(dirFilter); err == nil {
				mobius.Gauge(`metabase.db.directory_count`, float64(v), map[string]interface{}{
					`root_group`: self.ID,
				})
			} else {
				return err
			}
		} else {
			return err
		}

		return nil
	} else {
		return err
	}
}

func (self *Group) hasNotChanged(id string) bool {
	// entry is considered "unchanged" if
	//   1. we're not doing a multi-pass scan
	//   2. we're on the first pass
	//   3. this ID is NOT in the set of changed IDs found in prior passes
	if self.CurrentPass == 0 {
		return true
	} else if self.PassesDone == 0 {
		return true
	} else if _, ok := changedEntries.Load(id); ok {
		return true
	}

	return false
}

func (self *Group) scanEntry(name string, parent string, isDir bool) (*Entry, error) {
	if self.db == nil {
		return nil, fmt.Errorf("Database instance is required to scan a group")
	}

	defer mobius.NewTiming().Send(`metabase.db.entry.scan_time_ms`, map[string]interface{}{
		`root_group`: self.ID,
		`directory`:  isDir,
	})

	mobius.Increment(`metabase.db.entry.num_scanned`, map[string]interface{}{
		`root_group`: self.ID,
		`directory`:  isDir,
	})

	// get entry implementation
	entry := NewEntry(self.ID, self.RootPath, name)

	// skip the entry if it's in the global exclusions list (case sensitive exact match)
	if sliceutil.ContainsString(Instance.GlobalExclusions, path.Base(name)) {
		return entry, nil
	}

	// if we're on a subsequent pass, but this entry was not modified, skip it
	if self.CurrentPass > 1 {
		if _, ok := changedEntries.Load(entry.ID); !ok {
			return entry, nil
		}
	}

	if stat, err := os.Stat(name); err == nil {
		entry.Size = stat.Size()
		entry.LastModifiedAt = stat.ModTime().UnixNano()

		// Deep scan: only proceed with loading metadata and updating the record if
		//   - The entry is new, or...
		//   - The entry exists but has been modified since we last saw it
		//
		var existingFile Entry

		if err := Metadata.Get(entry.ID, &existingFile); err == nil {
			if !self.DeepScan && existingFile.LastDeepScannedAt > 0 {
				// trigger a deep scan if the data is considered stale
				if MaxTimeBetweenDeepScans == 0 || time.Since(time.Unix(0, entry.LastDeepScannedAt)) < MaxTimeBetweenDeepScans {
					absModTimeDiff := math.Abs(float64(entry.LastModifiedAt) - float64(existingFile.LastModifiedAt))

					if absModTimeDiff < 1e9 && self.hasNotChanged(entry.ID) {
						return &existingFile, nil
					}
				}
			}

			entry.Metadata = existingFile.Metadata
		}
	} else if os.IsNotExist(err) {
		return nil, err
	}

	// Deep Scan only from here on...
	// --------------------------------------------------------------------------------------------
	log.Noticef("PASS %d: [%s] %16s: Scanning entry %v (%s)", self.CurrentPass, self.ID, parent, entry.ID, name)

	self.ModifiedFileCount += 1

	changedEntries.Store(entry.ID, true)

	for _, id := range self.GetAncestors() {
		changedEntries.Store(id, true)
	}

	entry.Parent = parent
	entry.RootGroup = self.ID
	entry.IsGroup = isDir
	entry.LastDeepScannedAt = time.Now().UnixNano()

	if isDir {
		entry.ChildCount = self.FileCount
		entry.Type = `directory`
	} else {
		entry.Type = metadata.GetGeneralFileType(name)
	}

	tm := mobius.NewTiming()

	// load entry metadata
	if err := entry.LoadMetadata(self.CurrentPass); err != nil {
		return nil, err
	}

	tm.Send(`metabase.db.entry.metadata_load_time_ms`, map[string]interface{}{
		`root_group`: self.ID,
		`directory`:  isDir,
	})

	tm = mobius.NewTiming()

	if !entry.IsGroup {
		if !self.SkipChecksum && !self.db.SkipChecksum {
			if self.CurrentPass == 0 || self.CurrentPass == metadata.GetChecksumPass() {
				// calculate checksum for entry
				if sum, err := entry.GenerateChecksum(false); err == nil {
					entry.Checksum = sum

					tm.Send(`metabase.db.entry.checksum_time_ms`, map[string]interface{}{
						`root_group`: self.ID,
						`directory`:  isDir,
					})

					tm = mobius.NewTiming()

				} else {
					return nil, err
				}
			}
		}

		if self.CurrentPass <= 1 {
			mobius.Gauge(`metabase.db.entry.bytes_scanned`, float64(entry.Size), map[string]interface{}{
				`root_group`: self.ID,
				`directory`:  isDir,
			})
		}
	}

	// persist the entry record
	if err := Metadata.CreateOrUpdate(entry.ID, entry); err != nil {
		return nil, err
	}

	tm.Send(`metabase.db.entry.persist_time_ms`, map[string]interface{}{
		`root_group`: self.ID,
		`directory`:  isDir,
	})

	return entry, nil
}

func (self *Group) resolveRealStat(absPath string, fileStat os.FileInfo) (os.FileInfo, error) {
	// if we're following symlinks, dereference it first to make sure we can.
	if pathutil.IsSymlink(fileStat.Mode()) {
		if self.FollowSymlinks {
			// verify the symlink is readabled, expanded, and ready to scan
			if realpath, err := os.Readlink(absPath); err == nil {
				if realAbsPath, err := filepath.Abs(path.Join(self.Path, realpath)); err == nil {
					if realstat, err := os.Stat(realAbsPath); err == nil {
						log.Debugf("[%s] Following symbolic link %s -> %s", self.ID, absPath, realAbsPath)
						return realstat, nil
					} else {
						return fileStat, fmt.Errorf("[%s] Error reading target of symbolic link %s: %v", self.ID, realAbsPath, err)
					}
				} else {
					return fileStat, fmt.Errorf("[%s] Error following symbolic link %s: %v", self.ID, realpath, err)
				}
			} else {
				return fileStat, fmt.Errorf("[%s] Error reading symbolic link %s: %v", self.ID, fileStat.Name(), err)
			}
		} else {
			return fileStat, fmt.Errorf("[%s] Skipping symbolic link %s", self.ID, absPath)
		}
	}

	return fileStat, nil
}

func reportEntryDeletionStats(parentRootGroup string, entry *Entry) {
	mobius.Gauge(`metabase.db.entry.bytes_removed`, float64(entry.Size), map[string]interface{}{
		`root_group`: parentRootGroup,
	})

	mobius.Increment(`metabase.db.entry.num_removed`, map[string]interface{}{
		`root_group`: parentRootGroup,
	})
}

func (self *Group) cleanupMissingEntries(query interface{}, force bool) error {
	var entries []Entry

	if f, err := ParseFilter(query); err == nil {
		if err := Metadata.Find(f, &entries); err == nil {
			entriesToDelete := make([]interface{}, 0)

			for _, entry := range entries {
				if force {
					entriesToDelete = append(entriesToDelete, entry.ID)
					reportEntryDeletionStats(self.ID, &entry)
					continue
				}

				if self.compiledIgnoreList != nil {
					if !self.compiledIgnoreList.ShouldKeep(entry.RelativePath, entry.IsGroup) {
						entriesToDelete = append(entriesToDelete, entry.ID)
						reportEntryDeletionStats(self.ID, &entry)
						continue
					}
				}

				if absPath, err := entry.GetAbsolutePath(); err == nil {
					if _, err := os.Stat(absPath); os.IsNotExist(err) {
						entriesToDelete = append(entriesToDelete, entry.ID)
						reportEntryDeletionStats(self.ID, &entry)
					}
				} else {
					log.Warningf("[%s] Failed to cleanup missing entry %s (%s)", self.ID, entry.ID, entry.RelativePath)
				}
			}

			if l := len(entriesToDelete); l > 0 {
				if err := self.cleanup(entriesToDelete...); err == nil {
					log.Debugf("[%s] Cleaned up %d missing entries", self.ID, l)
				} else {
					log.Warningf("[%s] Failed to cleanup missing entries: %v", self.ID, err)
				}
			}

			return nil
		} else {
			return err
		}
	} else {
		return err
	}
}

func (self *Group) cleanupMissingEntriesUnderParents(parents []string, force bool) error {
	if len(parents) == 0 {
		return nil
	}

	// cleanup entries for whom we are the parent
	return self.cleanupMissingEntries(map[string]interface{}{
		`parent`: parents,
	}, force)
}

func (self *Group) cleanup(entries ...interface{}) error {
	if err := Metadata.Delete(entries...); err == nil {
		return nil
	} else {
		return err
	}
}
