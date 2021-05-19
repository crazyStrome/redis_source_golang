package hyperloglog

import "unsafe"

/**
 * 这个文件中是hll的一些辅助函数
 * 包括hash函数、sparse和dense的相关配置函数
 */
// SPARSE 的相关参数
const (
	// XZERO 的标志位: 0b0100 0000
	HLL_SPARSE_XZERO_BIT byte = 0x40
	// VAL 的标志位: 0b1000 0000
	HLL_SPARSE_VAL_BIT byte = 0x80
	// XZERO 最大长度: 16384
	HLL_SPARSE_XZERO_LEN uint16 = 1 << HLL_P
	// VAL 能表示的最大值: 32
	HLL_SPARSE_VAL_MAX_VALUE byte = 32
	// ZERO 能覆盖的寄存器最多为: 64
	HLL_SPARSE_ZERO_MAX_LEN byte = 64
	// HLL 从sparse转到dense的registers的长度
	HLL_SPARSE_MAX_BYTES = 500
)
const (
	HLL_P          byte   = 14
	HLL_REGISTERS  uint16 = 1 << HLL_P
	HLL_P_MASK            = HLL_REGISTERS - 1
	HLL_DENSE_BITS byte   = 6
	HLL_DENSE_SIZE byte   = 1 << HLL_DENSE_BITS
	HLL_DENSE_MASK        = HLL_DENSE_SIZE - 1
)
// hash 算法相关参数
const (
	BIG_M = 0xc6a4a7935bd1e995
	BIG_R = 47
	SEED = 0x1234ABCD
)

/**
 * 这一部分代码用来更新缓存card
 * 使用card[7]的最高位作为标志位
 * 当card[7]是1，表示缓存不可用
 * 当card[7]是0，表示缓存可用
 */
func (hll *hllhdr) validCache(cache uint64) {
	for i := 0; i < 8; i ++ {
		hll.card[i] = byte(cache & 0xff)
		cache >>= 8
	}
	// 最高位设置为0
	hll.card[7] &= 0x7f
}
func (hll *hllhdr) isCacheValid() bool {
	return hll.card[7] & (1 << 7) == 0
}
func (hll *hllhdr) invalidCache() {
	// 最高位设置为1
	hll.card[7] |= 1 << 7
}
func (hll *hllhdr) cacheGet() uint64 {
	var cache uint64
	for i := 7; i >= 0; i -- {
		cache |= uint64(hll.card[i])
		cache <<= 8
	}
	return cache
}

/**
 * sparse 的相关函数
 * 为了便于调试，sparse相关函数都用指针指向对应的寄存器字节
 * sparse使用byte数组储存操作码，操作码长度有1byte或者2byte
 * sparse的函数中，p表示byte数组中指向该操作码指针
 */
/**
 * 设置ZERO操作码
 * ZERO 表示为00xxxxxx
 * xxxxxx + 1 = size
 */
func sparseSetZERO(p *byte, size byte) {
	*p = 0x3f & (size - 1)
}
// 返回ZERO覆盖的寄存器数量
func sparseGetZERO(p *byte) byte {
	return (*p & 0x3f) + 1
}
/**
 * XZERO: 01xxxxxx yyyyyyyy
 * p -> 01xxxxxx
 * pn -> yyyyyyyy
 */
func sparseSetXZERO(p *byte, size uint16) {
	// pn 是p下一个字节的指针
	var pn = (*byte)(unsafe.Pointer(uintptr(unsafe.Pointer(p)) + 1))
	*pn = byte(0xff & (size - 1))
	*p = 0x40 | byte(0x3f & ((size - 1) >> 8))
}
func sparseGetXZERO(p *byte) uint16 {
	// pn 是下一个字节的指针
	var pn = (*byte)(unsafe.Pointer(uintptr(unsafe.Pointer(p)) + 1))
	return uint16(0x3f & *p) << 8 | uint16(*pn) + 1
}
/**
 * VAL: 1vvvvvxx
 * vvvvv val
 * xx+1 = size
 */
func sparseSetVAL(p *byte, val byte, size byte) {
	*p = 0
	// 设置 xx
	*p = 0x03 & (size - 1)
	// 设置 vvvvv
	*p |= (0x1f & val) << 2
	// 设置最高位为1
	*p |= 0x80
}
// 返回寄存器的数量
func sparseGetVALEN(p *byte) byte{
	return (0x03 & *p) + 1
}
// 返回寄存器的数量和数值
func sparseGetVAL(p *byte) byte {
	return (0x7c & *p) >> 2
}
func isSparseZERO(p *byte) bool {
	// ZERO 操作码 00xxxxxx
	return *p & 0xc0 == 0
}
func isSparseXZERO(p *byte) bool {
	// XZERO 操作码 01XX XXXX
	return *p & 0xc0 == HLL_SPARSE_XZERO_BIT
}
func isSparseVAL(p *byte) bool {
	// VAL 操作码 1vvv vvxx
	return *p & HLL_SPARSE_VAL_BIT != 0
}
/**
 * dense 的相关函数
 */
// index 是寄存器索引
func (hll *hllhdr) denseGet(index uint16) byte {
	var b0 = uint16(HLL_DENSE_BITS) * index / 8
	var fb = uint16(HLL_DENSE_BITS) * index % 8
	// b0 右移 fb 位
	var v1 = hll.registers[b0] >> fb
	// 如果b1位置有数据，即该位置没有超出范围
	var v2 byte
	if b0 + 1 < HLL_REGISTERS {
		v2 = hll.registers[b0 + 1] << (8 - fb)
	}
	var res = v1 | v2
	return res & HLL_DENSE_MASK
}
// denseSet 将第index个寄存器设置成val
func (hll *hllhdr) denseSet(index uint16, val byte) {
	// 清除 b0 字节
	var b0 = uint16(HLL_DENSE_BITS) * index / 8
	var fb = uint16(HLL_DENSE_BITS) * index % 8
	var mask = ^(HLL_DENSE_MASK << fb)
	hll.registers[b0] &= mask
	// 设置 b0 字节
	hll.registers[b0] |= val << fb

	//清除 b1 的相关位
	if b0 + 1 >= HLL_REGISTERS {
		// 如果超过了registers的索引范围
		// 直接返回
		return
	}
	mask = ^(HLL_DENSE_MASK >> (8 - fb))
	hll.registers[b0+1] &= mask
	// 设置b1的相关位
	hll.registers[b0+1] |= val >> (8-fb)
}

// MurmurHash64A (64-bit) algorithm by Austin Appleby.
func murmurHash64A(data []byte) (h uint64) {
	var k uint64
	var seed = uint64(SEED)

	h = seed ^ uint64(len(data))*BIG_M

	for l := len(data); l >= 8; l -= 8 {
		k = uint64(data[0]) | uint64(data[1])<<8 | uint64(data[2])<<16 | uint64(data[3])<<24 |
			uint64(data[4])<<32 | uint64(data[5])<<40 | uint64(data[6])<<48 | uint64(data[7])<<56

		k *= BIG_M
		k ^= k >> BIG_R
		k *= BIG_M

		h ^= k
		h *= BIG_M

		data = data[8:]
	}

	switch len(data) {
	case 7:
		h ^= uint64(data[6]) << 48
		fallthrough
	case 6:
		h ^= uint64(data[5]) << 40
		fallthrough
	case 5:
		h ^= uint64(data[4]) << 32
		fallthrough
	case 4:
		h ^= uint64(data[3]) << 24
		fallthrough
	case 3:
		h ^= uint64(data[2]) << 16
		fallthrough
	case 2:
		h ^= uint64(data[1]) << 8
		fallthrough
	case 1:
		h ^= uint64(data[0])
		h *= BIG_M
	}

	h ^= h >> BIG_R
	h *= BIG_M
	h ^= h >> BIG_R

	return
}

/**
 * 计算ele的hash值
 * 根据其后低14位
 */
func hllPatLen(ele string) (index uint16, count byte) {
	var hash = murmurHash64A([]byte(ele))
	index = uint16(hash & uint64(HLL_P_MASK))

	// 放置count到最后还是0，把第63位设置为1
	hash |= 1 << 63
	var bit = uint64(HLL_REGISTERS)
	// 第一个1出现的位置比如0b11110000 统计的count是5
	count = 1
	for hash & bit == 0 {
		count ++
		bit <<= 1
	}
	return
}