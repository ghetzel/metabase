package metabase

import (
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ghetzel/byteflood/util"
	"github.com/ghetzel/go-stockutil/pathutil"
	"github.com/ghetzel/go-stockutil/sliceutil"
	"github.com/ghetzel/go-stockutil/stringutil"
	"github.com/ghetzel/metabase/metadata"
	"github.com/ghetzel/mobius"
	"github.com/ghetzel/pivot"
	"github.com/ghetzel/pivot/backends"
	"github.com/ghetzel/pivot/dal"
	"github.com/ghetzel/pivot/filter"
	"github.com/ghetzel/pivot/mapper"
	"github.com/op/go-logging"
	"github.com/robfig/cron"
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
var SearchIndexFlushEveryNRecords = 1000

var changedEntries sync.Map

type GroupListFunc func() (GroupSet, error)
type PreInitializeFunc func(db *DB) error
type PostInitializeFunc func(db *DB, backend backends.Backend) error
type PostScanFunc func()

type DB struct {
	BaseDirectory      string                 `json:"base_dir"`
	AutomigrateModels  bool                   `json:"automigrate"`
	URI                string                 `json:"uri,omitempty"`
	Indexer            string                 `json:"indexer,omitempty"`
	MetadataURI        string                 `json:"metadata_uri,omitempty"`
	MetadataIndexer    string                 `json:"metadata_indexer,omitempty"`
	AdditionalIndexers []string               `json:"additional_indexers,omitempty"`
	GlobalExclusions   []string               `json:"global_exclusions,omitempty"`
	ScanInProgress     bool                   `json:"scan_in_progress"`
	ExtractFields      []string               `json:"extract_fields,omitempty"`
	SkipMigrate        bool                   `json:"skip_migrate"`
	SkipChecksum       bool                   `json:"skip_checksum"`
	StatsDatabase      string                 `json:"stats_database"`
	StatsTags          map[string]interface{} `json:"stats_tags"`
	GroupLister        GroupListFunc          `json:"-"`
	ScanInterval       string                 `json:"scan_interval"`
	PreInitialize      PreInitializeFunc      `json:"-"`
	PostInitialize     PostInitializeFunc     `json:"-"`
	db                 backends.Backend
	metadataDb         backends.Backend
	models             map[string]mapper.Mapper
	postscanCallbacks  []PostScanFunc
	scanSchedule       *cron.Cron
}

var Instance *DB

func ParseFilter(spec interface{}, fmtvalues ...interface{}) (*filter.Filter, error) {
	if fmt.Sprintf("%v", spec) == `all` {
		return filter.All(), nil
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
		return nil, fmt.Errorf("Invalid argument type %T", spec)
	}
}

func NewDB() *DB {
	db := &DB{
		AutomigrateModels:  true,
		BaseDirectory:      DefaultBaseDirectory,
		GlobalExclusions:   DefaultGlobalExclusions,
		AdditionalIndexers: make([]string, 0),
		PreInitialize: func(_ *DB) error {
			return nil
		},
		PostInitialize: func(_ *DB, _ backends.Backend) error {
			return nil
		},
		models:       make(map[string]mapper.Mapper),
		StatsTags:    make(map[string]interface{}),
		scanSchedule: cron.New(),
	}

	db.GroupLister = func() (GroupSet, error) {
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
	filter.QueryUnescapeValues = true

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
	if self.StatsDatabase != `` {
		if err := mobius.Initialize(self.StatsDatabase, self.StatsTags); err != nil {
			return err
		}
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
			Indexer:            self.MetadataIndexer,
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
			log.Debugf("Migrating model %q", name)

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

	if self.ScanInterval != `` {
		if err := self.scanSchedule.AddFunc(self.ScanInterval, func() {
			if err := self.Scan(false); err != nil {
				log.Warningf("Automatic scan error: %v", err)
			}

			if e := self.scanSchedule.Entries(); len(e) > 0 {
				log.Debugf("Automatic scan completed. Next scan at %v", e[0].Next)
			}
		}); err == nil {
			self.scanSchedule.Start()

			if e := self.scanSchedule.Entries(); len(e) > 0 {
				log.Debugf("Automatic scans scheduled. Next scan at %v", e[0].Next)
			}
		} else {
			return err
		}
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
	oldcount := backends.BleveBatchFlushCount
	backends.BleveBatchFlushCount = SearchIndexFlushEveryNRecords
	parentPathCache = sync.Map{}

	log.Debugf("Index record flush count: %d", backends.BleveBatchFlushCount)

	defer func() {
		backends.BleveBatchFlushCount = oldcount
		log.Debugf("Index record flush count reset to %d", backends.BleveBatchFlushCount)
		log.Debugf("Perfoming final backend flush")
		Metadata.GetBackend().Flush()
	}()

	startedAt := time.Now()

	if self.ScanInProgress {
		log.Warningf("Another scan is already running")
		return fmt.Errorf("Scan already running")
	} else {
		log.Infof("Scan started at %v", startedAt)
		self.ScanInProgress = true

		defer func() {
			self.Cleanup(true, !deep)
			self.ScanInProgress = false

			for _, fn := range self.postscanCallbacks {
				fn()
			}

			log.Infof("Scan completed in %v", time.Since(startedAt))
		}()
	}

	passes := metadata.GetLoaders().Passes()

	if len(labels) == 0 {
		log.Debugf("Scanning all groups in %d passes", len(passes))
	} else {
		log.Debugf("Scanning groups %v in %d passes", labels, len(passes))
	}

	groupsToSkipOnNextPass := make([]string, 0)
	groupPasses := make(map[string]int)

	if groups, err := self.GroupLister(); err == nil {
		sort.Sort(sort.Reverse(groups))

		for _, group := range groups {
			changedEntries = sync.Map{}
			group.db = self

			for _, pass := range passes {
				// will contain a list of IDs of groups underneath this top-level group
				// that should be scanned
				subgroups := make([]string, 0)

				if sliceutil.ContainsString(groupsToSkipOnNextPass, group.ID) {
					continue
				}

				// update our label-to-realpath map (used by Entry.GetAbsolutePath)
				rootGroupToPath[group.ID] = group.Path

				group.DeepScan = deep
				group.CurrentPass = pass

				if v, ok := groupPasses[group.ID]; ok {
					group.PassesDone = v
				}

				if len(labels) > 0 {
					skip := true

					for _, label := range labels {
						parts := strings.SplitN(label, `:`, 2)
						label = parts[0]

						if group.ID == stringutil.Underscore(label) {
							skip = false

							if len(parts) == 2 {
								subgroups = strings.Split(parts[1], `,`)
							}

							break
						}
					}

					if skip {
						// log.Debugf("PASS %d: Skipping group %s [%s]", pass, group.Path, group.ID)
						continue
					}
				}

				if err := group.Initialize(); err == nil {
					log.Infof("Scanning path %s", group.Path)

					log.Debugf("PASS %d: Scanning group %s (%d subgroups) [%s]", pass, group.Path, len(subgroups), group.ID)

					if err := group.Scan(subgroups); err == nil {
						defer group.RefreshStats()
					} else {
						if len(groups) == 1 {
							return err
						} else {
							log.Errorf("PASS %d: Error scanning group %q: %v", pass, group.ID, err)
						}
					}

					log.Debugf("PASS %d: Group %q encountered %d modified files", pass, group.ID, group.ModifiedFileCount)

					if !deep {
						if group.ModifiedFileCount == 0 {
							log.Debugf("PASS %d: Group %q will not be scanned in remaining passes", pass, group.ID)
							break
						}
					}
				} else {
					if len(groups) == 1 {
						return err
					} else {
						log.Errorf("PASS %d:Error scanning group %q: %v", pass, group.ID, err)
					}
				}

				log.Debugf("PASS %d: Flushing backend", pass)
				Metadata.GetBackend().Flush()

				groupPasses[group.ID] = (group.PassesDone + 1)
			}
		}
	} else {
		return fmt.Errorf("failed to list groups: %v", err)
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

func (self *DB) Cleanup(skipFileStats bool, skipRootGroupPrune bool) error {
	if !self.ScanInProgress {
		self.ScanInProgress = true

		defer func() {
			self.ScanInProgress = false
		}()
	}

	var ids []string
	var totalRemoved int

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

	if 1 == 2 {
		// cleanup files whose parent label no longer exists
		if f, err := ParseFilter(map[string]interface{}{
			`root_group`: fmt.Sprintf("not:%s", strings.Join(ids, `|`)),
		}); err == nil {
			f.Limit = 0
			f.Sort = nil

			backend := Metadata.GetBackend()
			indexer := backend.WithSearch(MetadataSchema)

			if i, ok := backend.(backends.Indexer); ok {
				indexer = i
			}

			log.Debugf("Cleanup: removing entries associated with deleted root groups")

			if err := indexer.DeleteQuery(MetadataSchema, f); err != nil {
				log.Warningf("Remove missing root_groups failed: %v", err)
			}
		} else {
			log.Errorf("Cleanup failed: %v", err)
			return err
		}
	}

	deleteFn := func(ids []interface{}) int {
		if l := len(ids); l > 0 {
			if err := Metadata.Delete(ids...); err == nil {
				log.Debugf("Removed %d entries", l)
				return l
			} else {
				log.Warningf("Error cleaning up database: %v", err)
			}
		}

		return 0
	}

	cleanupFn := func() int {
		entriesToDelete := make([]interface{}, 0)
		allQuery := filter.All()
		allQuery.Fields = []string{`id`, `name`, `root_group`, `parent`}

		if err := Metadata.FindFunc(allQuery, Entry{}, func(entryI interface{}, err error) {
			var entry *Entry

			if len(entriesToDelete) >= 1000 {
				deleteFn(entriesToDelete)
				entriesToDelete = nil
			}

			if e, ok := entryI.(*Entry); ok {
				entry = e
			}

			if err == nil {
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
			} else {
				log.Warningf("Error cleaning up database entries: %v", err)
			}
		}); err == nil {
			deleteFn(entriesToDelete)
		} else {
			log.Warningf("Failed to cleanup database: %v", err)
		}

		return -1
	}

	if !skipFileStats {
		// cleanup until there's nothing left, an error occurs, or we've exceeded our CleanupIterations
		log.Debugf("Cleanup: verifying existence of all files")

		for i := 0; i < CleanupIterations; i++ {
			if removed := cleanupFn(); removed > 0 {
				log.Debugf("Cleanup pass %d: removed %d files", i, removed)
				totalRemoved += removed
			} else {
				break
			}
		}
	}

	if totalRemoved == 0 {
		log.Debugf("Cleaned up %d entries.", totalRemoved)
	} else {
		log.Noticef("Cleaned up %d entries.", totalRemoved)
	}

	return nil
}

func (self *DB) PollDirectories() {
	for {
		if !self.ScanInProgress {
			if groups, err := self.GroupLister(); err == nil {
				for _, group := range groups {
					lastCheckedAt := util.StartedAt

					if tm, err := group.GetLatestModifyTime(); err == nil && !tm.IsZero() {
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
