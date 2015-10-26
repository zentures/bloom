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

package partitioned

import (
	"fmt"
	"hash"
	"hash/fnv"

	"encoding/binary"
	"math"

	"github.com/willf/bitset"
	"github.com/zhenjl/bloom"
)

// PartitionedBloom is a variant implementation of the standard bloom filter.
// Reference #1: Approximate Caches for Packet Classification (http://www.ieee-infocom.org/2004/Papers/45_3.PDF)
// Reference #2: Scalable Bloom Filters (http://gsd.di.uminho.pt/members/cbm/ps/dbloom.pdf)
//
// The name Partitioned Bloom Filter is my choice as there was no name assigned to this variant.
type PartitionedBloom struct {
	// h is the hash function used to get the list of h1..hk values
	// By default we use hash/fnv.New64(). User can also set their own using SetHasher()
	h hash.Hash

	// m is the total number of bits for this bloom filter. m for the partitioned bloom filter
	// will be divided into k partitions, or slices. So each partition contains Math.ceil(m/k) bits.
	//
	// m =~ n / ((log(p)*log(1-p))/abs(log e))
	m uint

	// k is the number of hash values used to set and test bits. Each filter partition will be
	// set/tested using a single hash value. Note that the number of hash functions may not be the
	// same as hash values. For example, our implementation uses 32-bit hash values. So a single
	// Murmur3 128bit hash function can be used as 4 32-bit hash values. A single FNV 64bit hash function
	// can be used as 2 32-bit has values.
	//
	// k = log2(1/e)
	// Given that our e is defaulted to 0.001, therefore k ~= 10, which means we need 10 hash values
	k uint

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

	// s is the size of the partition, or slice.
	// s = m / k
	s uint

	// b is the set of bit array holding the bloom filters. There will be k b's.
	b []*bitset.BitSet

	// bs holds the list of bits to be set/check based on the hash values
	bs []uint
}

var _ bloom.Bloom = (*PartitionedBloom)(nil)

// New initializes a new partitioned bloom filter.
// n is the number of items this bloom filter predicted to hold.
func New(n uint) bloom.Bloom {
	var (
		p float64 = 0.5
		e float64 = 0.001
		k uint    = bloom.K(e)
		m uint    = bloom.M(n, p, e)
		s uint    = bloom.S(m, k)
	)

	return &PartitionedBloom{
		h:  fnv.New64(),
		n:  n,
		p:  p,
		e:  e,
		k:  k,
		m:  m,
		s:  s,
		b:  makePartitions(k, s),
		bs: make([]uint, k),
	}
}

func (this *PartitionedBloom) SetHasher(h hash.Hash) {
	this.h = h
}

func (this *PartitionedBloom) Reset() {
	this.k = bloom.K(this.e)
	this.m = bloom.M(this.n, this.p, this.e)
	this.s = bloom.S(this.m, this.k)
	this.b = makePartitions(this.k, this.s)
	this.bs = make([]uint, this.k)

	if this.h == nil {
		this.h = fnv.New64()
	} else {
		this.h.Reset()
	}
}

func (this *PartitionedBloom) SetErrorProbability(e float64) {
	this.e = e
}

func (this *PartitionedBloom) EstimatedFillRatio() float64 {
	return 1 - math.Exp(-float64(this.c)/float64(this.s))
}

func (this *PartitionedBloom) FillRatio() float64 {
	// Since this is partitioned, we will return the average fill ratio of all partitions
	t := float64(0)
	for _, v := range this.b[:this.k] {
		t += (float64(v.Count()) / float64(this.s))
	}
	return t / float64(this.k)
}

func (this *PartitionedBloom) Add(item []byte) bloom.Bloom {
	this.bits(item)
	for i, v := range this.bs[:this.k] {
		this.b[i].Set(v)
	}
	this.c++
	return this
}

func (this *PartitionedBloom) Check(item []byte) bool {
	this.bits(item)
	for i, v := range this.bs[:this.k] {
		if !this.b[i].Test(v) {
			return false
		}
	}
	return true
}

func (this *PartitionedBloom) Count() uint {
	return this.c
}

func (this *PartitionedBloom) PrintStats() {
	fmt.Printf("m = %d, n = %d, k = %d, s = %d, p = %f, e = %f\n", this.m, this.n, this.k, this.s, this.p, this.e)
	fmt.Println("Total items:", this.c)

	for i, v := range this.b[:this.k] {
		c := v.Count()
		fmt.Printf("Bits in partition %d: %d (%.1f%%)\n", i, c, (float32(c)/float32(this.s))*100)
	}
}

func (this *PartitionedBloom) bits(item []byte) {
	this.h.Reset()
	this.h.Write(item)
	s := this.h.Sum(nil)
	a := binary.BigEndian.Uint32(s[4:8])
	b := binary.BigEndian.Uint32(s[0:4])

	// Reference: Less Hashing, Same Performance: Building a Better Bloom Filter
	// URL: http://www.eecs.harvard.edu/~kirsch/pubs/bbbf/rsa.pdf
	for i, _ := range this.bs[:this.k] {
		this.bs[i] = (uint(a) + uint(b)*uint(i)) % this.s
	}
}

func makePartitions(k, s uint) []*bitset.BitSet {
	b := make([]*bitset.BitSet, k)

	for i, _ := range b {
		b[i] = bitset.New(s)
	}

	return b
}
