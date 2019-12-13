package hashkit

import (
	"strconv"
	"strings"
)

type DistributeHash interface {
	AddNode(node string, spot int)
	DelNode(n string)
	GetNode(key []byte) (string, bool)
	Nodes() []string
}

type BucketHash struct {
	bucketCount uint
	bucketMap   map[uint][]string
	hash        func([]byte) uint

	powerOf2 bool
}

// bucketMappingConfig: "1,2,3->ip:port"
func NewBucketHash(bucketCount uint, bucketMappingList []string, hash func([]byte) uint) *BucketHash {
	bucketMap := make(map[uint][]string)
	for _, bucketMappingConfig := range bucketMappingList {
		parts := strings.Split(bucketMappingConfig, "->")
		if len(parts) != 2 {
			panic("incorrect bucketMapping config")
		}
		for _, b := range strings.Split(parts[0], ",") {
			bucket, err := strconv.ParseUint(b, 10, 32)
			if err != nil {
				panic("incorrect bucket " + b)
			}
			if bucket >= uint64(bucketCount) {
				panic("incorrect bucket" + b)
			}
			bucketMap[uint(bucket)] = strings.Split(parts[1], ",")
		}
	}

	return &BucketHash{
		bucketCount: bucketCount,
		bucketMap:   bucketMap,
		hash:        hash,
		powerOf2:    isPowerOf2(bucketCount),
	}
}

func (b *BucketHash) AddNode(node string, spot int) {
}

func (b *BucketHash) DelNode(n string) {
}

func (b *BucketHash) GetNode(key []byte) (addr string, ok bool) {
	addrs, ok := b.GetNodes(key)
	if ok {
		addr = addrs[0]
	}
	return
}

func (b *BucketHash) GetNodes(key []byte) (addrs []string, ok bool) {
	value := b.hash(key)
	bucket := mod(value, b.bucketCount, b.powerOf2)
	addrs, ok = b.bucketMap[bucket]
	return
}

func (b *BucketHash) Nodes() (ret []string) {
	uniq := make(map[string]interface{})
	for _, addrs := range b.bucketMap {
		for _, addr := range addrs {
			uniq[addr] = 1
		}
	}
	for u, _ := range uniq {
		ret = append(ret, u)
	}
	return
}

func isPowerOf2(n uint) bool {
	return n > 0 && ((n & (n - 1)) == 0)
}

func mod(origin uint, divisor uint, powerOf2 bool) uint {
	if powerOf2 {
		return origin & (divisor - 1)
	} else {
		return origin % divisor
	}
}
