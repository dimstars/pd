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

import "bytes"

type regionQueueNode struct {
	region *RegionInfo
	pre    *regionQueueNode
	next   *regionQueueNode
}

type regionQueue struct {
	start *regionQueueNode
	end   *regionQueueNode
	len   int
}

func newRegionQueue() *regionQueue {
	return &regionQueue{
		start: nil,
		end:   nil,
		len:   0,
	}
}

// getRegions gets all RegionInfo from regionQueue.
func (queue *regionQueue) getRegions() []*RegionInfo {
	var regions []*RegionInfo
	temp := queue.start
	for temp != nil {
		regions = append(regions, temp.region)
		temp = temp.next
	}
	return regions
}

// push adds region to the end of regionQueue.
func (queue *regionQueue) push(region *RegionInfo) *regionQueueNode {
	if region == nil {
		return nil
	}
	if queue.start == nil {
		queue.start = &regionQueueNode{
			region: region,
			pre:    nil,
			next:   nil,
		}
		queue.len++
		queue.end = queue.start
		return queue.start
	}
	queue.end.next = &regionQueueNode{
		region: region,
		pre:    queue.end,
		next:   nil,
	}
	queue.len++
	queue.end = queue.end.next
	return queue.end
}

// pop deletes the first region in regionQueue and return it.
func (queue *regionQueue) pop() *RegionInfo {
	if queue.start == nil {
		return nil
	}
	region := queue.start.region
	queue.start = queue.start.next
	if queue.start != nil {
		queue.start.pre = nil
	}
	queue.len--
	return region
}

// removeNode deletes the regionQueueNode in regionQueue.
func (queue *regionQueue) removeNode(node *regionQueueNode) {
	if node == nil {
		return
	}
	if node.pre != nil {
		node.pre.next = node.next
	} else {
		queue.start = node.next
	}
	if node.next != nil {
		node.next.pre = node.pre
	} else {
		queue.end = node.pre
	}
	queue.len--
}

type regionCache struct {
	regionQueue  *regionQueue
	queueNodeMap map[uint64]*regionQueueNode
}

func newRegionCache() *regionCache {
	return &regionCache{
		regionQueue:  newRegionQueue(),
		queueNodeMap: make(map[uint64]*regionQueueNode),
	}
}

func (cache *regionCache) length() int {
	return cache.regionQueue.len
}

// getNode gets the regionQueueNode which has the region with regionID.
func (cache *regionCache) getNode(regionID uint64) *regionQueueNode {
	if node, ok := cache.queueNodeMap[regionID]; ok {
		return node
	}
	return nil
}

// getRegions gets all RegionInfo from regionCache.
func (cache *regionCache) getRegions() []*RegionInfo {
	return cache.regionQueue.getRegions()
}

// update updates the RegionInfo or add it into regionCache.
func (cache *regionCache) update(region *RegionInfo) {
	if node := cache.getNode(region.GetID()); node != nil {
		node.region = region
		return
	}
	node := cache.regionQueue.push(region)
	if node != nil {
		cache.queueNodeMap[region.GetID()] = node
	}
}

// remove deletes the region in regionCache.
func (cache *regionCache) remove(region *RegionInfo) {
	if node, ok := cache.queueNodeMap[region.GetID()]; ok {
		cache.regionQueue.removeNode(node)
		delete(cache.queueNodeMap, region.GetID())
	}
}

// pop deletes the oldest region and return it.
func (cache *regionCache) pop() *RegionInfo {
	if region := cache.regionQueue.pop(); region != nil {
		delete(cache.queueNodeMap, region.GetID())
		return region
	}
	return nil
}

// randomRegion returns a random region from regionCache.
func (cache *regionCache) randomRegion(storeID uint64, ranges []KeyRange, n int, optPending RegionOption, optOther RegionOption, optAll RegionOption) *RegionInfo {
	if len(ranges) == 0 {
		ranges = []KeyRange{NewKeyRange("", "")}
	}

	for _, node := range cache.queueNodeMap {
		if !involved(node.region, ranges) {
			continue
		}
		for _, peer := range node.region.GetPendingPeers() {
			if peer.GetStoreId() == storeID && optPending(node.region) && optAll(node.region) {
				return node.region
			}
		}
	}
	for _, node := range cache.queueNodeMap {
		if !involved(node.region, ranges) {
			continue
		}
		for _, peer := range node.region.GetFollowers() {
			if peer.GetStoreId() == storeID && optOther(node.region) && optAll(node.region) {
				return node.region
			}
		}
	}
	for _, node := range cache.queueNodeMap {
		if !involved(node.region, ranges) {
			continue
		}
		peer := node.region.GetLeader()
		if peer.GetStoreId() == storeID && optOther(node.region) && optAll(node.region) {
			return node.region
		}
	}
	for _, node := range cache.queueNodeMap {
		if !involved(node.region, ranges) {
			continue
		}
		for _, peer := range node.region.GetLearners() {
			if peer.GetStoreId() == storeID && optOther(node.region) && optAll(node.region) {
				return node.region
			}
		}
	}
	return nil
}

func involved(region *RegionInfo, ranges []KeyRange) bool {
	for _, keyRange := range ranges {
		startKey := keyRange.StartKey
		endKey := keyRange.EndKey
		if len(startKey) > 0 && len(endKey) > 0 && bytes.Compare(startKey, endKey) > 0 {
			continue
		}
		if (len(startKey) == 0 || (len(region.GetStartKey()) > 0 && bytes.Compare(region.GetStartKey(), startKey) >= 0 )) && (len(endKey) == 0 || (len(region.GetEndKey()) > 0 && bytes.Compare(region.GetEndKey(), endKey) <= 0)) {
			return true
		}
	}
	return false
}
