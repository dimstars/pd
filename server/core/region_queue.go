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
func (queue *regionQueue) findNode(regionID uint64) *regionQueueNode{
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
func (queue *regionQueue) update(region *RegionInfo) {
	if region == nil {
		return
	}
	if queue.start == nil {
		queue.start = &regionQueueNode{
			region: region,
			next:   nil,
		}
		queue.len++
		queue.end = queue.start
		return
	}
	originNode := queue.findNode(region.GetID())
	if originNode != nil{
		originNode.region = region
		return
	}
	queue.end.next = &regionQueueNode{
		region: region,
		next:   nil,
	}
	queue.len++
	queue.end = queue.end.next
}

// pop deletes the first region in regionQueue and return it.
func (queue *regionQueue) pop() *RegionInfo {
	if queue.start == nil {
		return nil
	}
	region := queue.start.region
	queue.start = queue.start.next
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
	if queue.start == nil {
		return
	}
	if queue.start.region.GetID() == region.GetID() {
		queue.start = queue.start.next
		queue.len--
		return
	}
	temp := queue.start
	for temp.next != nil {
		if temp.next.region.GetID() == region.GetID() {
			temp.next = temp.next.next
			queue.len--
			return
		}
		temp = temp.next
	}
}
