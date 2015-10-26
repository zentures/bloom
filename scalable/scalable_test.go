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
	"bufio"
	"crypto/md5"
	"crypto/sha1"
	"fmt"
	"hash"
	"hash/crc64"
	"hash/fnv"
	"os"
	"testing"

	"github.com/spaolacci/murmur3"
	"github.com/zhenjl/bloom"
	"github.com/zhenjl/bloom/partitioned"
	"github.com/zhenjl/bloom/standard"
	"github.com/zhenjl/cityhash"
)

var (
	web2, web2a []string
)

func init() {
	file, err := os.Open("/usr/share/dict/web2")
	if err != nil {
		fmt.Println("Cannot open /usr/share/dict/web2 - " + err.Error())
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		web2 = append(web2, scanner.Text())
	}

	if err = scanner.Err(); err != nil {
		fmt.Println("Error reading file - " + err.Error())
	}

	file2, err2 := os.Open("/usr/share/dict/web2a")
	if err2 != nil {
		fmt.Println("Cannot open /usr/share/dict/web2a - " + err2.Error())
	}
	defer file2.Close()

	scanner = bufio.NewScanner(file2)
	for scanner.Scan() {
		web2a = append(web2a, scanner.Text())
	}

	if err2 = scanner.Err(); err2 != nil {
		fmt.Println("Error reading file - " + err2.Error())
	}
}

func testBloomFilter(t *testing.T, bf bloom.Bloom) {
	fn, fp := 0, 0

	for l := range web2 {
		if !(bf.Add([]byte(web2[l])).Check([]byte(web2[l]))) {
			fn++
		}
	}

	bf.PrintStats()

	for l := range web2a {
		if bf.Check([]byte(web2a[l])) {
			//fmt.Println("False Positive:", web2a[l])
			fp++
		}
	}

	fmt.Printf("Total false negatives: %d (%.4f%%)\n", fn, (float32(fn) / float32(len(web2)) * 100))
	fmt.Printf("Total false positives: %d (%.4f%%)\n", fp, (float32(fp) / float32(len(web2a)) * 100))
}

func TestBloomFilter(t *testing.T) {
	l := []uint{uint(len(web2)), 200000, 100000, 50000}
	h := []hash.Hash{fnv.New64(), crc64.New(crc64.MakeTable(crc64.ECMA)), murmur3.New64(), cityhash.New64(), md5.New(), sha1.New()}
	n := []string{"fnv.New64()", "crc64.New()", "murmur3.New64()", "cityhash.New64()", "md5.New()", "sha1.New()"}
	b := []func(uint) bloom.Bloom{standard.New, partitioned.New}
	bn := []string{"standard", "partitioned"}

	for i := range l {
		for j := range h {
			for k := range b {
				fmt.Printf("\n\nTesting %s with size %d using %s\n", n[j], l[i], bn[k])
				bf := New(l[i])
				bf.SetHasher(h[j])
				bf.(*ScalableBloom).SetBloomFilter(b[k])
				bf.Reset()
				testBloomFilter(t, bf)
			}
		}
	}
}

func BenchmarkBloomFNV64(b *testing.B) {
	var lines []string
	lines = append(lines, web2...)
	for len(lines) < b.N {
		lines = append(lines, web2...)
	}

	bf := New(uint(b.N))
	fn := 0

	b.ResetTimer()

	for l := 0; l < b.N; l++ {
		if !(bf.Add([]byte(lines[l])).Check([]byte(lines[l]))) {
			fn++
		}
	}

	b.StopTimer()
}

func BenchmarkBloomCRC64(b *testing.B) {
	var lines []string
	lines = append(lines, web2...)
	for len(lines) < b.N {
		lines = append(lines, web2...)
	}

	bf := New(uint(b.N))
	bf.SetHasher(crc64.New(crc64.MakeTable(crc64.ECMA)))
	bf.Reset()
	fn := 0

	b.ResetTimer()

	for l := 0; l < b.N; l++ {
		if !(bf.Add([]byte(lines[l])).Check([]byte(lines[l]))) {
			fn++
		}
	}

	b.StopTimer()
}

func BenchmarkBloomMurmur3(b *testing.B) {
	var lines []string
	lines = append(lines, web2...)
	for len(lines) < b.N {
		lines = append(lines, web2...)
	}

	bf := New(uint(b.N))
	bf.SetHasher(murmur3.New64())
	bf.Reset()
	fn := 0

	b.ResetTimer()

	for l := 0; l < b.N; l++ {
		if !(bf.Add([]byte(lines[l])).Check([]byte(lines[l]))) {
			fn++
		}
	}

	b.StopTimer()
}

func BenchmarkBloomCityHash(b *testing.B) {
	var lines []string
	lines = append(lines, web2...)
	for len(lines) < b.N {
		lines = append(lines, web2...)
	}

	bf := New(uint(b.N))
	bf.SetHasher(cityhash.New64())
	bf.Reset()
	fn := 0

	b.ResetTimer()

	for l := 0; l < b.N; l++ {
		if !(bf.Add([]byte(lines[l])).Check([]byte(lines[l]))) {
			fn++
		}
	}

	b.StopTimer()
}

func BenchmarkBloomMD5(b *testing.B) {
	var lines []string
	lines = append(lines, web2...)
	for len(lines) < b.N {
		lines = append(lines, web2...)
	}

	bf := New(uint(b.N))
	bf.SetHasher(md5.New())
	bf.Reset()
	fn := 0

	b.ResetTimer()

	for l := 0; l < b.N; l++ {
		if !(bf.Add([]byte(lines[l])).Check([]byte(lines[l]))) {
			fn++
		}
	}

	b.StopTimer()
}

func BenchmarkBloomSha1(b *testing.B) {
	var lines []string
	lines = append(lines, web2...)
	for len(lines) < b.N {
		lines = append(lines, web2...)
	}

	bf := New(uint(b.N))
	bf.SetHasher(sha1.New())
	bf.Reset()
	fn := 0

	b.ResetTimer()

	for l := 0; l < b.N; l++ {
		if !(bf.Add([]byte(lines[l])).Check([]byte(lines[l]))) {
			fn++
		}
	}

	b.StopTimer()
}
