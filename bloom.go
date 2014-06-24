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

package bloom

import (
	"hash"
	"math"
)

type Bloom interface {
	Add(key []byte) Bloom
	Check(key []byte) bool
	Count() uint
	PrintStats()
	SetHasher(hash.Hash)
	Reset()
	FillRatio() float64
	EstimatedFillRatio() float64
	SetErrorProbability(e float64)
}

func K(e float64) uint {
	return uint(math.Ceil(math.Log2(1 / e)))
}

func M(n uint, p, e float64) uint {
	// m =~ n / ((log(p)*log(1-p))/abs(log e))
	return uint(math.Ceil(float64(n) / ((math.Log(p) * math.Log(1-p)) / math.Abs(math.Log(e)))))
}

func S(m, k uint) uint {
	return uint(math.Ceil(float64(m) / float64(k)))
}
