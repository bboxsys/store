package store

import (
	"runtime"

	"github.com/dgraph-io/badger/v4"
)

// Metrics contains metadata about the store.
type Metrics struct {
	SizeLsm          int64          `json:"size_lsm"`
	SizeVLog         int64          `json:"size_vlog"`
	SizeTotal        int64          `json:"size_total"`
	NumKeys          int64          `json:"num_keys"`
	NumLevels        int            `json:"num_levels"`
	NumTables        int            `json:"num_tables"`
	LevelSizes       []LevelMetrics `json:"level_sizes"`
	BlockCacheHits   int64          `json:"block_cache_hits"`
	BlockCacheMisses int64          `json:"block_cache_misses"`
	DiskTotal        uint64         `json:"disk_total"`
	DiskFree         uint64         `json:"disk_free"`
	DiskUsedPct      float64        `json:"disk_used_pct"`
	PutOps           int64          `json:"put_ops"`
	GetOps           int64          `json:"get_ops"`
	DeleteOps        int64          `json:"delete_ops"`
}

// LevelMetrics contains per-level table information from BadgerDB.
type LevelMetrics struct {
	Level     int   `json:"level"`
	NumTables int   `json:"num_tables"`
	Size      int64 `json:"size"`
}

// NewMetrics creates a basic Metrics with LSM and VLog sizes (backward compat).
func NewMetrics(db *badger.DB) *Metrics {
	lsm, vlog := db.Size()
	return &Metrics{
		SizeLsm:  lsm,
		SizeVLog: vlog,
	}
}

// CollectMetrics gathers comprehensive metrics from the store.
func (s *Store) CollectMetrics() *Metrics {
	lsm, vlog := s.DB.Size()
	m := &Metrics{
		SizeLsm:   lsm,
		SizeVLog:  vlog,
		SizeTotal: lsm + vlog,
		PutOps:    s.putOps.Load(),
		GetOps:    s.getOps.Load(),
		DeleteOps: s.deleteOps.Load(),
	}

	// Table info per level
	tables := s.DB.Tables()
	totalTables := 0
	levelMap := make(map[int]LevelMetrics)
	for _, t := range tables {
		totalTables++
		lm := levelMap[t.Level]
		lm.Level = t.Level
		lm.NumTables++
		lm.Size += int64(t.OnDiskSize)
		levelMap[t.Level] = lm
	}
	m.NumTables = totalTables
	m.NumLevels = len(levelMap)
	for _, lm := range levelMap {
		m.LevelSizes = append(m.LevelSizes, lm)
	}

	// Block cache metrics
	bcm := s.DB.BlockCacheMetrics()
	if bcm != nil {
		m.BlockCacheHits = int64(bcm.Hits())
		m.BlockCacheMisses = int64(bcm.Misses())
	}

	// Key count
	m.NumKeys = s.countKeys()

	// Disk usage
	if s.Location != "" {
		total, free, usedPct := getDiskUsage(s.Location)
		m.DiskTotal = total
		m.DiskFree = free
		m.DiskUsedPct = usedPct
	}

	return m
}

// countKeys counts the total number of keys in the store using a keys-only iteration.
func (s *Store) countKeys() int64 {
	var count int64
	err := s.DB.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			count++
		}
		return nil
	})
	if err != nil {
		s.Logger.Error("error counting keys: " + err.Error())
	}
	return count
}

// getDiskUsage returns disk total, free, and used percentage for the given path.
// Uses platform-specific implementation.
func getDiskUsage(path string) (total uint64, free uint64, usedPct float64) {
	if runtime.GOOS == "windows" {
		return 0, 0, 0
	}
	return getDiskUsagePlatform(path)
}
