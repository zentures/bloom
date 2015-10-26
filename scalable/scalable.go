// Copyright (c) 2014 Dataence, LLC. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package scalable

import (
	"fmt"
	"hash"
	"hash/fnv"
	"math"

	"github.com/zhenjl/bloom"
	"github.com/zhenjl/bloom/partitioned"
)

// ScalableBloom is an implementation of the Scalable Bloom Filter that "addresses the problem of having
// to choose an a priori maximum size for the set, and allows an arbitrary growth of the set being presented."
// Reference #2: Scalable Bloom Filters (http://gsd.di.uminho.pt/members/cbm/ps/dbloom.pdf)
type ScalableBloom struct {
	// h is the hash function used to get the list of h1..hk values
	// By default we use hash/fnv.New64(). User can also set their own using SetHasher()
	h hash.Hash

	// p is the fill ratio of the filter partitions. It's mainly used to calculate m at the start.
	// p is not checked when new items are added. So if the fill ratio goes above p, the likelihood
	// of false positives (error rate) will increase.
	//
	// By default we use the fill ratio of p = 0.5
	p float64

	// e is the desired error rate of the bloom filter. The lower the e, the higher the k.
	//
	// By default we use the error rate of e = 0.1% = 0.001. In some papers this is P (uppercase P)
	e float64

	// n is the number of elements the filter is predicted to hold while maintaining the error rate
	// or filter size (m). n is user supplied. But, in case you are interested, the formula is
	// n =~ m * ( (log(p) * log(1-p)) / abs(log e) )
	n uint

	// c is the number of items we have added to the filter
	c uint

	// r is the error tightening ratio with 0 < r < 1.
	// By default we use 0.9 as it result in better average space usage for wide ranges of growth.
	// See Scalable Bloom Filter paper for reference
	r float32

	// bfs is an array of bloom filters used by the scalable bloom filter
	bfs []bloom.Bloom

	// bfc is the bloom filter constructor (New()) that returns the bloom filter to use
	bfc func(uint) bloom.Bloom
}

var _ bloom.Bloom = (*ScalableBloom)(nil)

// New initializes a new partitioned bloom filter.
// n is the number of items this bloom filter predicted to hold.
func New(n uint) bloom.Bloom {
	var (
		p float64   = 0.5
		e float64   = 0.001
		r float32   = 0.9
		h hash.Hash = fnv.New64()
	)

	bf := &ScalableBloom{
		h: h,
		n: n,
		p: p,
		e: e,
		r: r,
	}

	bf.addBloomFilter()

	return bf
}

func (this *ScalableBloom) SetBloomFilter(f func(uint) bloom.Bloom) {
	this.bfc = f
}

func (this *ScalableBloom) SetHasher(h hash.Hash) {
	this.h = h
}

func (this *ScalableBloom) Reset() {
	if this.h == nil {
		this.h = fnv.New64()
	} else {
		this.h.Reset()
	}

	this.bfs = []bloom.Bloom{}
	this.c = 0
	this.addBloomFilter()
}

func (this *ScalableBloom) SetErrorProbability(e float64) {
	this.e = e
}

func (this *ScalableBloom) EstimatedFillRatio() float64 {
	return this.bfs[len(this.bfs)-1].EstimatedFillRatio()
}

func (this *ScalableBloom) FillRatio() float64 {
	// Since this has multiple bloom filters, we will return the average
	t := float64(0)
	for i := range this.bfs {
		t += this.bfs[i].FillRatio()
	}
	return t / float64(len(this.bfs))
}

func (this *ScalableBloom) Add(item []byte) bloom.Bloom {
	i := len(this.bfs) - 1

	if this.bfs[i].EstimatedFillRatio() > this.p {
		this.addBloomFilter()
		i++
	}

	this.bfs[i].Add(item)
	this.c++
	return this
}

func (this *ScalableBloom) Check(item []byte) bool {
	l := len(this.bfs)
	for i := l - 1; i >= 0; i-- {
		//fmt.Println("checking level ", i)
		if this.bfs[i].Check(item) {
			return true
		}
	}
	return false
}

func (this *ScalableBloom) Count() uint {
	return this.c
}

func (this *ScalableBloom) PrintStats() {
	fmt.Printf("n = %d, p = %f, e = %f\n", this.n, this.p, this.e)
	fmt.Println("Total items:", this.c)

	for i := range this.bfs {
		fmt.Printf("Scalable Bloom Filter #%d\n", i)
		fmt.Printf("-------------------------\n")
		this.bfs[i].PrintStats()
	}
}

func (this *ScalableBloom) addBloomFilter() {
	var bf bloom.Bloom
	if this.bfc == nil {
		bf = partitioned.New(this.n)
	} else {
		bf = this.bfc(this.n)
	}

	e := this.e * math.Pow(float64(this.r), float64(len(this.bfs)))

	bf.SetHasher(this.h)
	bf.SetErrorProbability(e)
	bf.Reset()

	this.bfs = append(this.bfs, bf)
	//fmt.Println("Added new bloom filter")
}
