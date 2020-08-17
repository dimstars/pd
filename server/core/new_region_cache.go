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

func (queue *regionQueue) length() int {
	return queue.len
}

// getRegions gets all RegionInfo from regionQueue.
func (queue *regionQueue) getRegions() []*RegionInfo {
	var cache []*RegionInfo
	temp := queue.start
	for temp != nil {
		cache = append(cache, temp.region)
		temp = temp.next
	}
	return cache
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

// getAt gets the region at the index in regionQueue.
func (queue *regionQueue) getAt(index int) *RegionInfo {
	if queue.len <= index {
		return nil
	}
	temp := queue.start
	for i := 0; i < index; i++ {
		temp = temp.next
	}
	return temp.region
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
	maxCount     int
	regionQueue  *regionQueue
	queueNodeMap map[uint64]*regionQueueNode
}

func newRegionCache(max int) *regionCache {
	return &regionCache{
		maxCount:     max,
		regionQueue:  newRegionQueue(),
		queueNodeMap: make(map[uint64]*regionQueueNode),
	}
}

// getNode gets the regionQueueNode which has the region with regionID.
func (cache *regionCache) getNode(regionID uint64) *regionQueueNode {
	if node, ok := cache.queueNodeMap[regionID]; ok {
		return node
	}
	return nil
}

// getRegion gets the region with regionID.
func (cache *regionCache) getRegion(regionID uint64) *RegionInfo {
	if node := cache.getNode(regionID); node != nil {
		return node.region
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
		if cache.regionQueue.len > cache.maxCount {
			region := cache.regionQueue.pop()
			delete(cache.queueNodeMap, region.GetID())
		}
	}
}

// remove deletes the region in regionCache.
func (cache *regionCache) remove(region *RegionInfo) {
	if node, ok := cache.queueNodeMap[region.GetID()]; ok {
		cache.regionQueue.removeNode(node)
		delete(cache.queueNodeMap, region.GetID())
	}
}

// randomRegion returns a random region from regionCache.
func (cache *regionCache) randomRegion() *RegionInfo {
	for _, node := range cache.queueNodeMap {
		return node.region
	}
	return nil
}
