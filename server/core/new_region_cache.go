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
	var regions []*RegionInfo
	temp := queue.start
	for temp != nil {
		regions = append(regions, temp.region)
		temp = temp.next
	}
	return regions
}

// findNode will find the regionQueueNode which has the region with regionID.
func (queue *regionQueue) findNode(regionID uint64) *regionQueueNode {
	temp := queue.start
	for temp != nil {
		if temp.region.GetID() == regionID {
			return temp
		}
		temp = temp.next
	}
	return nil
}

// update updates the RegionInfo or push it into regionQueue.
func (queue *regionQueue) update(region *RegionInfo) *regionQueueNode{
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
	originNode := queue.findNode(region.GetID())
	if originNode != nil {
		originNode.region = region
		return originNode
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

// remove deletes the region in regionQueue.
func (queue *regionQueue) remove(region *RegionInfo) {
	queue.removeNode(queue.findNode(region.GetID()))
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

// newRegionCache saves the new peer's statistics.
type newRegionCache struct {
	maxCount     int
	regionQueue  *regionQueue
	queueNodeMap map[uint64]*regionQueueNode // regionID -> regionQueueNode
}

func newNewRegionCache(max int) *newRegionCache {
	return &newRegionCache{
		maxCount:     max,
		regionQueue:  newRegionQueue(),
		queueNodeMap: make(map[uint64]*regionQueueNode),
	}
}

// add updates the RegionInfo or add it into newRegionCache.
func (newRegions *newRegionCache) add(region *RegionInfo) {
	node := newRegions.regionQueue.update(region)
	if node != nil {
		newRegions.queueNodeMap[region.GetID()] = node
		if newRegions.regionQueue.len > newRegions.maxCount {
			region := newRegions.regionQueue.pop()
			delete(newRegions.queueNodeMap, region.GetID())
		}
	}
}

// remove deletes the region in newRegionCache.
func (newRegions *newRegionCache) remove(region *RegionInfo) {
	if node, ok := newRegions.queueNodeMap[region.GetID()]; ok {
		newRegions.regionQueue.removeNode(node)
		delete(newRegions.queueNodeMap, region.GetID())
	}
}

// randomRegion returns a random region from newRegionCache.
func (newRegions *newRegionCache) randomRegion() *RegionInfo {
	for _, node := range newRegions.queueNodeMap {
		return node.region
	}
	return nil
}

// getRegions gets all RegionInfo from newRegionCache.
func (newRegions *newRegionCache) getRegions() []*RegionInfo {
	return newRegions.regionQueue.getRegions()
}
