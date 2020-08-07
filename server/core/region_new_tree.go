// Copyright 2020 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"bytes"
	"math/rand"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/pd/v4/pkg/btree"
	"go.uber.org/zap"
)

var _ btree.Item = &regionNewItem{}

type regionNewItem struct {
	timestamp uint64
	region    *RegionInfo
}

// Less returns true if the timestamp is less than the other.
func (r *regionNewItem) Less(other btree.Item) bool {
	left := r.timestamp
	right := other.(*regionNewItem).timestamp
	return left < right
}

func (r *regionNewItem) Contains(key []byte) bool {
	start, end := r.region.GetStartKey(), r.region.GetEndKey()
	return bytes.Compare(key, start) >= 0 && (len(end) == 0 || bytes.Compare(key, end) < 0)
}

type regionNewTree struct {
	tree *btree.BTree
	tmap map[uint64]*regionNewItem
}

func newRegionNewTree() *regionNewTree {
	return &regionNewTree{
		tree: btree.New(defaultBTreeDegree),
		tmap: make(map[uint64]*regionNewItem),
	}
}

func (t *regionNewTree) length() int {
	return t.tree.Len()
}

// getOverlaps gets the regions which are overlapped with the specified region range.
func (t *regionNewTree) getOverlaps(region *RegionInfo) []*regionNewItem {
	item := &regionNewItem{region: region}
	result := t.find(region)
	if result == nil {
		result = item
	}

	var overlaps []*regionNewItem
	t.tree.AscendGreaterOrEqual(result, func(i btree.Item) bool {
		over := i.(*regionNewItem)
		if len(region.GetEndKey()) > 0 && bytes.Compare(region.GetEndKey(), over.region.GetStartKey()) <= 0 {
			return false
		}
		if len(region.GetStartKey()) > 0 && bytes.Compare(region.GetStartKey(), over.region.GetStartKey()) >= 0 {
			return false
		}
		overlaps = append(overlaps, over)
		return true
	})
	return overlaps
}

// update updates the tree with the region.
// It finds and deletes all the overlapped regions first, and then
// insert the region.
func (t *regionNewTree) update(region *RegionInfo) []*RegionInfo {
	overlaps := t.getOverlaps(region)
	var regions []*RegionInfo
	for _, item := range overlaps {
		log.Debug("overlapping region",
			zap.Uint64("new-region-id", item.region.GetID()),
			zap.Stringer("delete-region", RegionToHexMeta(item.region.GetMeta())),
			zap.Stringer("update-region", RegionToHexMeta(region.GetMeta())))
		regions = append(regions, item.region)
		t.tree.Delete(item)
	}

	item := &regionNewItem{timestamp: uint64(time.Now().UnixNano()), region: region}
	t.tree.ReplaceOrInsert(item)
	t.tmap[region.GetID()] = item

	return regions
}

// remove removes a region if the region is in the tree.
// It will do nothing if it cannot find the region or the found region
// is not the same with the region.
func (t *regionNewTree) remove(region *RegionInfo) btree.Item {
	if t.length() == 0 {
		return nil
	}
	result := t.find(region)
	if result == nil || result.region.GetID() != region.GetID() {
		return nil
	}
	delete(t.tmap, region.GetID())

	return t.tree.Delete(result)
}

func (t *regionNewTree) find(region *RegionInfo) *regionNewItem {
	return t.tmap[region.GetID()]
}

func init() {
	rand.Seed(time.Now().UnixNano())
}
