package metabase

import (
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/ghetzel/byteflood/util"
	"github.com/ghetzel/go-stockutil/pathutil"
	"github.com/ghetzel/go-stockutil/stringutil"
	"github.com/ghetzel/metabase/metadata"
	"github.com/ghetzel/metabase/stats"
	"github.com/ghetzel/pivot"
	"github.com/ghetzel/pivot/backends"
	"github.com/ghetzel/pivot/dal"
	"github.com/ghetzel/pivot/filter"
	"github.com/ghetzel/pivot/mapper"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger(`metabase`)

var DefaultGlobalExclusions = []string{
	`._.DS_Store`,
	`._.Trashes`,
	`.DS_Store`,
	`.Spotlight-V100`,
	`.Trashes`,
	`desktop.ini`,
	`lost+found`,
	`Thumbs.db`,
}

var DefaultBaseDirectory = `~/.config/metabase`
var rootGroupToPath = make(map[string]string)
var CleanupIterations = 256
var SearchIndexFlushEveryNRecords = 250

type GroupListFunc func() ([]Group, error)
type PreInitializeFunc func(db *DB) error
type PostInitializeFunc func(db *DB, backend backends.Backend) error
type PostScanFunc func()

type DB struct {
	BaseDirectory      string                 `json:"base_dir"`
	AutomigrateModels  bool                   `json:"automigrate"`
	URI                string                 `json:"uri,omitempty"`
	MetadataURI        string                 `json:"metadata_uri,omitempty"`
	Indexer            string                 `json:"indexer,omitempty"`
	AdditionalIndexers map[string]string      `json:"additional_indexers,omitempty"`
	GlobalExclusions   []string               `json:"global_exclusions,omitempty"`
	ScanInProgress     bool                   `json:"scan_in_progress"`
	ExtractFields      []string               `json:"extract_fields,omitempty"`
	SkipMigrate        bool                   `json:"skip_migrate"`
	StatsDatabase      string                 `json:"stats_database"`
	StatsTags          map[string]interface{} `json:"stats_tags"`
	GroupLister        GroupListFunc          `json:"-"`
	PreInitialize      PreInitializeFunc      `json:"-"`
	PostInitialize     PostInitializeFunc     `json:"-"`
	db                 backends.Backend
	metadataDb         backends.Backend
	models             map[string]mapper.Mapper
	postscanCallbacks  []PostScanFunc
}

var Instance *DB

type Property struct {
	Key   string      `json:"key,identity"`
	Value interface{} `json:"value"`
	db    *DB
}

func ParseFilter(spec interface{}, fmtvalues ...interface{}) (filter.Filter, error) {
	if fmt.Sprintf("%v", spec) == `all` {
		return filter.All, nil
	}

	switch spec.(type) {
	case []string:
		return filter.Parse(strings.Join(spec.([]string), filter.CriteriaSeparator))
	case map[string]interface{}:
		return filter.FromMap(spec.(map[string]interface{}))
	case string, interface{}:
		if len(fmtvalues) > 0 {
			return filter.Parse(fmt.Sprintf(fmt.Sprintf("%v", spec), fmtvalues...))
		} else {
			return filter.Parse(fmt.Sprintf("%v", spec))
		}
	default:
		return filter.Filter{}, fmt.Errorf("Invalid argument type %T", spec)
	}
}

func NewDB() *DB {
	db := &DB{
		AutomigrateModels:  true,
		BaseDirectory:      DefaultBaseDirectory,
		GlobalExclusions:   DefaultGlobalExclusions,
		AdditionalIndexers: make(map[string]string),
		PreInitialize: func(_ *DB) error {
			return nil
		},
		PostInitialize: func(_ *DB, _ backends.Backend) error {
			return nil
		},
		models:    make(map[string]mapper.Mapper),
		StatsTags: make(map[string]interface{}),
	}

	db.GroupLister = func() ([]Group, error) {
		if Metadata == nil {
			return nil, fmt.Errorf("Cannot list groups: database not initialized")
		}

		var groups []Group

		f, _ := ParseFilter(map[string]interface{}{
			`parent`: fmt.Sprintf("is:%s", RootGroupName),
		})

		if err := Metadata.Find(f, &groups); err != nil {
			return nil, err
		}

		return groups, nil
	}

	return db
}

func (self *DB) RegisterModel(schema *dal.Collection, _db ...backends.Backend) error {
	var db backends.Backend

	if len(_db) == 0 {
		db = self.db
	} else {
		db = _db[0]
	}

	if _, ok := self.models[schema.Name]; !ok {
		self.models[schema.Name] = mapper.NewModel(db, schema)
		return nil
	} else {
		return fmt.Errorf("A model named %q is already registered", schema.Name)
	}
}

func (self *DB) Model(name string) (mapper.Mapper, bool) {
	model, ok := self.models[name]
	return model, ok
}

func (self *DB) UnregisterModel(name string) {
	if _, ok := self.models[name]; ok {
		delete(self.models, name)
	}
}

func (self *DB) RegisterPostScanEvent(fn PostScanFunc) {
	self.postscanCallbacks = append(self.postscanCallbacks, fn)
}

// Initialize the DB by opening the underlying database
func (self *DB) Initialize() error {
	filter.CriteriaSeparator = `;`
	filter.FieldTermSeparator = `=`
	filter.QueryUnescapeValues = true
	backends.BleveBatchFlushCount = SearchIndexFlushEveryNRecords

	// reuse the "json:" struct tag for loading pivot/dal.Record into/out of structs
	dal.RecordStructTag = `json`

	// give implementers the opportunity to do things to the database before applying defaults of our own
	if err := self.PreInitialize(self); err != nil {
		return err
	}

	if v, err := pathutil.ExpandUser(self.BaseDirectory); err == nil {
		self.BaseDirectory = v
	} else {
		return err
	}

	// setup stats
	if self.StatsDatabase == `` {
		stats.LocalStatsEnabled = false
	}

	if err := stats.Initialize(self.StatsDatabase, self.StatsTags); err != nil {
		return err
	}

	if self.URI == `` {
		self.URI = fmt.Sprintf("sqlite:///%s/info.db", self.BaseDirectory)
	}

	if self.MetadataURI == `` {
		self.MetadataURI = self.URI
	}

	if db, err := pivot.NewDatabaseWithOptions(self.URI, backends.ConnectOptions{
		Indexer:            self.Indexer,
		AdditionalIndexers: self.AdditionalIndexers,
	}); err == nil {
		self.db = db
		self.metadataDb = db
	} else {
		return err
	}

	if self.MetadataURI != self.URI {
		if db, err := pivot.NewDatabaseWithOptions(self.MetadataURI, backends.ConnectOptions{
			Indexer:            self.Indexer,
			AdditionalIndexers: self.AdditionalIndexers,
		}); err == nil {
			self.metadataDb = db
		} else {
			return err
		}
	}

	// now that we have a pivot Database instance, setup our local models
	if err := self.RegisterModel(MetadataSchema, self.metadataDb); err != nil {
		return err
	}

	Metadata, _ = self.models[MetadataSchema.Name]

	// give implementers a chance to do things to the underlying backend (e.g.: registering more models)
	if err := self.PostInitialize(self, self.db); err != nil {
		return err
	}

	// not too sure about this one...
	Instance = self

	if self.AutomigrateModels {
		for name, model := range self.models {
			log.Infof("Migrating model %q", name)

			if err := model.Migrate(); err != nil {
				return err
			}
		}
	}

	for _, pattern := range self.ExtractFields {
		if rx, err := regexp.Compile(pattern); err == nil {
			metadata.RegexpPatterns = append(metadata.RegexpPatterns, rx)
		} else {
			return err
		}
	}

	if err := self.refreshRootGroupPathCache(); err != nil {
		return err
	}

	return nil
}

// Update the label-to-realpath map (used by Entry.GetAbsolutePath)
func (self *DB) refreshRootGroupPathCache() error {
	if groups, err := self.GroupLister(); err == nil {
		for _, group := range groups {
			rootGroupToPath[group.ID] = group.Path
		}
	} else {
		return err
	}

	return nil
}

func (self *DB) GetPivotDatabase() backends.Backend {
	return self.db
}

func (self *DB) GetPivotMetadataDatabase() backends.Backend {
	return self.metadataDb
}

func (self *DB) AddGlobalExclusions(patterns ...string) {
	self.GlobalExclusions = append(self.GlobalExclusions, patterns...)
}

func (self *DB) Scan(deep bool, labels ...string) error {
	if self.ScanInProgress {
		log.Warningf("Another scan is already running")
		return fmt.Errorf("Scan already running")
	} else {
		self.ScanInProgress = true

		defer func() {
			self.Cleanup()
			self.ScanInProgress = false

			for _, fn := range self.postscanCallbacks {
				fn()
			}
		}()
	}

	passes := metadata.GetLoaders().Passes()

	if len(labels) == 0 {
		log.Debugf("Scanning all groups in %d passes", len(passes))
	} else {
		log.Debugf("Scanning groups %v in %d passes", labels, len(passes))
	}

	for _, pass := range metadata.GetLoaders().Passes() {
		if groups, err := self.GroupLister(); err == nil {
			for _, group := range groups {
				// update our label-to-realpath map (used by Entry.GetAbsolutePath)
				rootGroupToPath[group.ID] = group.Path

				group.DeepScan = deep
				group.CurrentPass = pass

				if len(labels) > 0 {
					skip := true

					for _, label := range labels {
						if group.ID == stringutil.Underscore(label) {
							skip = false
							break
						}
					}

					if skip {
						log.Debugf("PASS %d: Skipping group %s [%s]", pass, group.Path, group.ID)
						continue
					}
				}

				if err := group.Initialize(); err == nil {
					log.Debugf("PASS %d: Scanning group %s [%s]", pass, group.Path, group.ID)

					if err := group.Scan(); err == nil {
						defer group.RefreshStats()
					} else {
						if len(groups) == 1 {
							return err
						} else {
							log.Errorf("PASS %d: Error scanning group %q: %v", pass, group.ID, err)
						}
					}
				} else {
					if len(groups) == 1 {
						return err
					} else {
						log.Errorf("PASS %d:Error scanning group %q: %v", pass, group.ID, err)
					}
				}
			}
		} else {
			return err
		}
	}

	return nil
}

func (self *DB) GetDirectoriesByFile(filename string) []Group {
	foundGroups := make([]Group, 0)

	if groups, err := self.GroupLister(); err == nil {
		for _, group := range groups {
			if group.ContainsPath(filename) {
				foundGroups = append(foundGroups, group)
			}
		}

		return foundGroups
	}

	return nil
}

func (self *DB) Cleanup() error {
	if !self.ScanInProgress {
		self.ScanInProgress = true

		defer func() {
			self.ScanInProgress = false
		}()
	}

	var ids []string

	if groups, err := self.GroupLister(); err == nil {
		for _, group := range groups {
			ids = append(ids, group.ID)
		}
	} else {
		return err
	}

	if len(ids) == 0 {
		return fmt.Errorf("Preventing cleanup of empty directory set.")
	}

	log.Debugf("Cleaning up...")

	// cleanup files whose parent label no longer exists
	if f, err := ParseFilter(map[string]interface{}{
		`root_group`: fmt.Sprintf("not:%s", strings.Join(ids, `|`)),
	}); err == nil {
		f.Limit = -1

		totalRemoved := 0
		backend := Metadata.GetBackend()
		indexer := backend.WithSearch(``)

		if err := indexer.DeleteQuery(MetadataSchema.Name, f); err != nil {
			log.Warningf("Remove missing root_groups failed: %v", err)
		}

		cleanupFn := func() int {
			entriesToDelete := make([]interface{}, 0)
			allQuery := filter.Copy(&filter.All)
			allQuery.Fields = []string{`id`, `name`, `root_group`, `parent`}

			if err := Metadata.FindFunc(allQuery, Entry{}, func(entryI interface{}, err error) {
				if err == nil {
					if entry, ok := entryI.(*Entry); ok {
						// make sure the file actually exists
						if absPath, err := entry.GetAbsolutePath(); err == nil {
							if _, err := os.Stat(absPath); os.IsNotExist(err) {
								entriesToDelete = append(entriesToDelete, entry.ID)
								reportEntryDeletionStats(entry.RootGroup, entry)
								return
							}
						}

						// make sure the entry's parent exists
						if entry.Parent != `root` && !Metadata.Exists(entry.Parent) {
							entriesToDelete = append(entriesToDelete, entry.ID)
							reportEntryDeletionStats(entry.RootGroup, entry)
							return
						}
					}
				} else {
					log.Warningf("Error cleaning up database entries: %v", err)
				}
			}); err == nil {
				if l := len(entriesToDelete); l > 0 {
					if err := Metadata.Delete(entriesToDelete...); err == nil {
						log.Debugf("Removed %d entries", l)
						return l
					} else {
						log.Warningf("Error cleaning up database: %v", err)
					}
				}
			} else {
				log.Warningf("Failed to cleanup database: %v", err)
			}

			return -1
		}

		// cleanup until there's nothing left, an error occurs, or we've exceeded our CleanupIterations
		for i := 0; i < CleanupIterations; i++ {
			if removed := cleanupFn(); removed > 0 {
				totalRemoved += removed
			} else {
				break
			}
		}

		log.Infof("DB cleanup finished, deleted %d entries.", totalRemoved)
	} else {
		return err
	}

	return nil
}

func (self *DB) PollDirectories() {
	for {
		if !self.ScanInProgress {
			if groups, err := self.GroupLister(); err == nil {
				for _, group := range groups {
					lastCheckedAt := util.StartedAt

					if tm := group.GetLatestModifyTime(); !tm.IsZero() {
						lastCheckedAt = tm
					}

					// log.Debugf("[%v] Checking for file changes since %v", group.ID, lastCheckedAt)

					if err := group.WalkModifiedSince(lastCheckedAt, func(entry *Entry, isNew bool) error {
						if absPath, err := entry.GetAbsolutePath(); err == nil {
							if isNew {
								log.Noticef("[%v] Created: %v (%v)", group.ID, absPath, entry.LastModifiedTime())
							} else {
								log.Infof("[%v] Changed: %v (%v)", group.ID, absPath, entry.LastModifiedTime())
							}

							if err := group.ScanPath(absPath); err != nil {
								log.Warningf("[%v] Error scanning %v: %v", group.ID, absPath, err)
							}
						} else {
							log.Warningf("[%v] %v", group.ID, err)
						}

						return nil
					}); err != nil {
						log.Warningf("Failed to traverse %v: %v", group.ID, err)
					}
				}
			}
		}

		time.Sleep(10 * time.Second)
	}
}
