package metabase

import (
	"bytes"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func createManifestTestDestination(prefix string) (string, error) {
	prefix = `metabase-test-` + prefix
	name, err := ioutil.TempDir(``, prefix)

	if err != nil {
		return ``, err
	}

	return name, nil
}

func populateManifestTestDestination(base string) error {
	i := 2

	if err := os.MkdirAll(path.Join(base, `subdir1`), 0755); err != nil {
		return err
	}

	for name, size := range map[string]int{
		`subdir1/file.1`: 42,
		`file.top1`:      56,
	} {
		data := make([]byte, size)
		rand.Seed(int64(i))
		rand.Read(data)

		if err := ioutil.WriteFile(path.Join(base, name), data, 0644); err != nil {
			return err
		}

		i += 1
	}

	return nil
}

func getTestManifestItems() []ManifestItem {
	return []ManifestItem{
		{
			ID:           `test1a`,
			Type:         FileItem,
			RelativePath: `/subdir1/file.1`,
			Values: []ManifestValue{
				int64(42),
				`aae9c3aa50b937f1c2fef02853677d3f68a28193`,
			},
		}, {
			ID:           `test2`,
			Type:         FileItem,
			RelativePath: `/file.top1`,
			Values: []ManifestValue{
				int64(56),
				`b004ff62dd5510e33807ae38366553381451ed5b`,
			},
		},
	}
}

func TestSyncPolicyCompare(t *testing.T) {
	assert := require.New(t)

	policy := SyncPolicy{
		ID: `test`,
		Fields: []string{
			`name`, `size`, `enabled`,
		},
	}

	assert.True(policy.Compare(`name`, `tester`, `tester`))
	assert.True(policy.Compare(`size`, 0, 0))
	assert.True(policy.Compare(`size`, 0, `0`))
	assert.True(policy.Compare(`size`, `0`, 0))
	assert.True(policy.Compare(`size`, nil, nil))
	assert.True(policy.Compare(`enabled`, true, true))
	assert.True(policy.Compare(`enabled`, false, false))

	assert.False(policy.Compare(`name`, `tester`, `Tester`))
	assert.False(policy.Compare(`name`, `tester`, `other`))
	assert.False(policy.Compare(`size`, 123, 456))
	assert.False(policy.Compare(`size`, 123, nil))
	assert.False(policy.Compare(`size`, nil, 123))
	assert.False(policy.Compare(`enabled`, true, false))
	assert.False(policy.Compare(`enabled`, true, nil))
	assert.False(policy.Compare(`enabled`, false, true))
	assert.False(policy.Compare(`enabled`, nil, false))
}

func TestSyncManifestUpdates(t *testing.T) {
	assert := require.New(t)

	dir1, err := createManifestTestDestination(`dir1`)
	defer os.RemoveAll(dir1)
	assert.Nil(err)

	manifest := NewManifest(dir1, `file.size`, `checksum`)
	wantedItems := getTestManifestItems()
	manifest.Add(wantedItems...)

	policy := SyncPolicy{
		Fields: []string{
			`file.size`, `checksum`,
		},
	}

	// nothing there yet, should want everything
	updates, err := manifest.GetUpdateManifest(policy)
	assert.Nil(err)
	assert.NotNil(updates)
	assert.Equal(wantedItems, updates.Items)

	// create files
	assert.Nil(populateManifestTestDestination(dir1))

	updates, err = manifest.GetUpdateManifest(policy)
	assert.Nil(err)
	assert.NotNil(updates)
	assert.Equal(0, len(updates.Items))

	// remove a file
	assert.Nil(os.Remove(path.Join(dir1, `subdir1`, `file.1`)))
	updates, err = manifest.GetUpdateManifest(policy)
	assert.Nil(err)
	assert.NotNil(updates)
	assert.Equal(wantedItems[0:1], updates.Items)
}

func TestSyncManifestLoadTSV(t *testing.T) {
	assert := require.New(t)

	wantedItems := getTestManifestItems()
	manifest := NewManifest(`loadtsv`)

	lines := []string{
		"id\trelative_path\ttype\tfile.size\tchecsum",
		"test1a\t/subdir1/file.1\tfile\t42\taae9c3aa50b937f1c2fef02853677d3f68a28193",
		"test2\t/file.top1\tfile\t56\tb004ff62dd5510e33807ae38366553381451ed5b",
	}

	tsv := strings.Join(lines, "\n")
	reader := bytes.NewReader([]byte(tsv[:]))

	assert.Nil(manifest.LoadTSV(reader))

	assert.Equal(wantedItems, manifest.Items)
}
