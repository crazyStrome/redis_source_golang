package hyperloglog

import (
	"errors"
	"math"
)
/**
 * 这个文件中的函数构成了hll的整体结构
 */
const (
	// HLL_DENSE 表示dense格式
	HLL_DENSE byte = 1
	// HLL_SPARSE 表示sparse格式
	HLL_SPARSE = 2
)

// Errors
var (
	ErrNoSuchEncoding = errors.New("no such encoding, please check your object")
	ErrInvalidFormat  = errors.New("invalid format")
	ErrWrongParam     = errors.New("wrong parameter")
)

var PE []float64
func init() {
	// PE 初始化一次
	PE = make([]float64, 64)
	PE[0] = 1.0
	for i := 1; i < 64; i ++ {
		PE[i] = 1/float64(uint64(1) << i)
	}
	/**
	 * PE[1] = 1/2
	 * PE[2] = 1/4
	 * PE[3] = 1/8
	 * ...
	 */
}
// hllhdr 储存有关hll的相关内容
type hllhdr struct {
	// encoding 表示hll的编码形式
	encoding  byte
	// card 是一层缓存，当hll没有添加新元素时
	// card 是有效缓存，否则计算之后更新card
	card      []byte
	// registers 是byte数组，储存hll编码的内容
	registers []byte
}

func (hll *hllhdr) addElement(ele string) error {
	if hll.encoding == HLL_SPARSE {
		return hll.sparseAdd(ele)
	} else if hll.encoding == HLL_DENSE {
		return hll.denseAdd(ele)
	}
	return ErrNoSuchEncoding
}
func (hll *hllhdr) denseAdd(ele string) error {
	var idx, cnt = hllPatLen(ele)
	var old = hll.denseGet(idx)
	if old < cnt {
		hll.denseSet(idx, cnt)
	}
	return nil
}
func (hll *hllhdr) sparseAdd(ele string) error {
	var idx, cnt = hllPatLen(ele)
	// sparse 最多只能储存不高于32的VAL
	if cnt > HLL_SPARSE_VAL_MAX_VALUE {
		// 转换成dense，然后添加到hll
		var err = hll.sparse2dense()
		if err != nil {
			return err
		}
		return hll.denseAdd(ele)
	}
	// 记录字节索引
	var curb uint16
	// 寄存器索引
	var first uint16
	var p *byte
	// span 是当前操作码覆盖的寄存器数量
	var span uint16
	var prevb, nextb int = -1, -1
	for curb < uint16(len(hll.registers)) {
		p = &hll.registers[curb]
		var oplen uint16 = 1
		if isSparseZERO(p) {
			span = uint16(sparseGetZERO(p))
		} else if isSparseXZERO(p) {
			oplen = 2
			span = sparseGetXZERO(p)
		} else {
			// VAL
			span = uint16(sparseGetVALEN(p))
		}
		// [first, first+span)包含这个idx字节
		if idx <= first + span -1 {
			break
		}
		prevb = int(curb)
		first += span
		curb += oplen
	}
	if span == 0 {
		return ErrInvalidFormat
	}

	// 取下一个操作码的字节索引
	nextb = int(curb) + 1
	if isSparseXZERO(p) {
		nextb = int(curb) + 2
	}
	if nextb >= len(hll.registers) {
		nextb = -1
	}

	// 缓存当前操作码覆盖的寄存器长度
	var isZero, isXzero, isVal bool
	var runlen uint16
	if isSparseZERO(p) {
		isZero = true
		runlen = uint16(sparseGetZERO(p))
	} else if isSparseXZERO(p) {
		isXzero = true
		runlen = sparseGetXZERO(p)
	} else {
		isVal = true
		runlen = uint16(sparseGetVALEN(p))
	}
	// 根据不同的情况更新hll，将新的操作码序列放到一个缓冲区中
	// 然后使用新序列替换旧序列

	// 这一块是D)选项的相关变量
	// goto 语句之间不能新定义变量
	// 因此放在最开始
	var seq []byte
	var curSeqBit int
	// 当前操作码覆盖的最后一个寄存器
	var last uint16
	var l uint16
	var oldlen int = 1

	if isVal {
		var oldcnt = sparseGetVAL(p)
		// A) 当前操作码是VAL，其值比cnt大，那就不需要更新hll
		if oldcnt >= cnt {
			return nil
		}
		// B) 当前操作码是VAL，长度是1，直接更新该操作码
		if runlen == 1 {
			sparseSetVAL(p, cnt, 1)
			goto updated
		}
	}
	// C) 当前操作码是ZERO，且长度为1，直接把它替换成长度为1
	// 值为cnt的VAL码
	if isZero && runlen == 1 {
		sparseSetVAL(p, cnt, 1)
		goto updated
	}
	// D) 最普遍的情况，一个ZERO或XZERO操作码分裂成多个操作码
	// 比如，XZERO-VAL-XZERO
	seq = make([]byte, 5)
	curSeqBit = 0
	// 当前操作码覆盖的最后一个寄存器
	last = first + span - 1
	l = 0
	if isZero || isXzero {
		if idx != first {
			l = idx - first
			// 使用XZERO
			if l > uint16(HLL_SPARSE_ZERO_MAX_LEN) {
				sparseSetXZERO(&seq[curSeqBit], l)
				curSeqBit += 2
			} else {
				sparseSetZERO(&seq[curSeqBit], byte(l))
				curSeqBit ++
			}
		}
		// 设置一个VAL，长度1， 值为cnt
		sparseSetVAL(&seq[curSeqBit], cnt, 1)
		curSeqBit ++
		// idx 后面的寄存器设置为一个ZERO或XZERO操作码
		if idx != last {
			l = last - idx
			if l > uint16(HLL_SPARSE_ZERO_MAX_LEN) {
				sparseSetXZERO(&seq[curSeqBit], l)
				curSeqBit += 2
			} else {
				sparseSetZERO(&seq[curSeqBit], byte(l))
				curSeqBit ++
			}
		}
	} else {
		// 将一个VAL分裂成三个VAL
		var curVal = sparseGetVAL(p)
		if idx != first {
			l = idx - first
			sparseSetVAL(&seq[curSeqBit], curVal, byte(l))
			curSeqBit ++
		}
		sparseSetVAL(&seq[curSeqBit], cnt, 1)
		curSeqBit ++
		if idx != last {
			l = last - idx
			sparseSetVAL(&seq[curSeqBit], curVal, byte(l))
			curSeqBit ++
		}
	}
	// 使用新序列替换旧序列，新序列长度为curSeqBit
	// 如果新长度超过HLL_SPARSE_MAX_BYTES，进行转换
	if isXzero {
		oldlen = 2
	}
	if len(hll.registers) + curSeqBit - oldlen > HLL_SPARSE_MAX_BYTES {
		// 转换成dense，然后添加到hll
		var err = hll.sparse2dense()
		if err != nil {
			return err
		}
		return hll.denseAdd(ele)
	}
	seq = seq[:curSeqBit]
	if nextb != -1 {
		seq = append(seq, hll.registers[nextb:]...)
	}
	hll.registers = append(hll.registers[:curb], seq...)

	// updated 用来将可能出现的两个相同VAL操作码合并
updated:
	var scanlen = 5
	var c int
	if prevb != -1 {
		c = prevb
	}
	for c < len(hll.registers) && scanlen > 0{
		p = &hll.registers[c]
		scanlen --
		if isSparseXZERO(p) {
			c += 2
			continue
		} else if isSparseZERO(p) {
			c ++
			continue
		}
		if c+1 < len(hll.registers) && isSparseVAL(&hll.registers[c+1]) {
			var v1 = sparseGetVAL(p)
			var l1 = sparseGetVALEN(p)
			var v2 = sparseGetVAL(&hll.registers[c+1])
			var l2 = sparseGetVALEN(&hll.registers[c+1])
			if v1 == v2 {
				var l = l2 + l1
				if l <= HLL_SPARSE_VAL_MAX_VALUE {
					sparseSetVAL(p, v1, l)
					copy(hll.registers[c+1:], hll.registers[c+2:])
					hll.registers = hll.registers[:len(hll.registers)-1]
					continue
				}
			}
		}
		c ++
	}
	return nil
}

/**
* ZERO操作码占用一个字节，表示为00xxxxxx，
* 后六位xxxxxx+1表示有N个连续的寄存器设置为0，
* 这个操作码可以表示1-64个连续的寄存器被设置为0。
* XZERO操作码占用两个字节，表示为01xxxxxx yyyyyyyy。

* xxxxxx是高位，yyyyyyyy是低位。这十四位+1表示有N个连续的寄存器设置为0.
* 这个操作码可以表示0-16384个寄存器被设置为0。

* VAL操作码占用一个字节，表示为1vvvvvxx。
* 它包含一个5bit的vvvvv表示寄存器值，2bit的xx+1表示有这么多个连续的寄存器被设置为vvvvv。
* 这个操作码表示可以表示1-4个寄存器被设置1-32的值。
 */
func createHll() *hllhdr {
	var hll = &hllhdr{
		encoding: HLL_SPARSE,
		card: make([]byte, 8),
	}
	var sparselen = HLL_REGISTERS / HLL_SPARSE_XZERO_LEN
	if HLL_REGISTERS % HLL_SPARSE_XZERO_LEN != 0 {
		sparselen ++
	}
	sparselen *= 2

	hll.registers = make([]byte, sparselen)
	// 全设置成XZERO
	var aux = HLL_REGISTERS
	var idx = 0
	for aux > 0 {
		var xzero = HLL_SPARSE_XZERO_LEN
		if xzero > aux {
			xzero = aux
		}
		sparseSetXZERO(&hll.registers[idx], xzero)
		idx += 2
		aux -= xzero
	}
	return hll
}

func (hll *hllhdr) sparse2dense() error {
	if hll.encoding == HLL_DENSE {
		return nil
	}
	var sps = hll.registers
	hll.registers = make([]byte, uint64(HLL_REGISTERS) * uint64(HLL_DENSE_BITS) / 8)
	var idx uint16
	var curBit uint16
	// 设置encoding
	hll.encoding = HLL_DENSE
	for curBit < uint16(len(sps)) {
		var p = &sps[curBit]
		var oplen uint16
		if isSparseZERO(p) {
			curBit ++
			oplen = uint16(sparseGetZERO(p))
			idx += oplen
		} else if isSparseXZERO(p) {
			curBit += 2
			oplen = sparseGetXZERO(p)
			idx += oplen
		} else {
			curBit ++
			var val byte
			val = sparseGetVAL(p)
			oplen = uint16(sparseGetVALEN(p))
			for oplen > 0 {
				oplen --
				hll.denseSet(idx, val)
				idx ++
			}
		}
	}
	if idx != HLL_REGISTERS {
		return ErrWrongParam
	}
	return nil
}
func (hll *hllhdr) hllCount() uint64 {
	// 加一层缓存
	if hll.isCacheValid() {
		return hll.cacheGet()
	}

	var m = float64(HLL_REGISTERS)
	var alpha = 0.7213 / (1 + 1.079 / m)
	var E float64
	var ez uint16
	if hll.encoding == HLL_SPARSE {
		E = hll.sparseSum(&ez)
	} else if hll.encoding == HLL_DENSE {
		E = hll.denseSum(&ez)
	}

	// 粗略估计
	E = (1/E)*alpha*m*m
	/**
	* 使用LINEAR COUNTING 算法统计基数小的情况，统计的ez不为0，说明有很多寄存器都是0
	* 对于更大的基数，低于72000，Hyperloglog使用线性统计的误差会增加，
	* HyperLogLog使用一个偏置bias来补偿误差
	 */
	if E < m*2.5 && ez != 0 {
		E = m * math.Log(m/float64(ez))
	} else if m == 16384 && E < 72000 {
		var bias = 5.9119*1.0e-18*(E*E*E*E)-
		  	1.4253*1.0e-12*(E*E*E)+
			1.2940*1.0e-7*(E*E)-
			5.2921*1.0e-3*E+ 83.3216
		E -= E*(bias/100)
	}
	var c = uint64(E)
	hll.validCache(c)
	return c
}
func (hll *hllhdr) sparseSum(ezp *uint16) float64 {
	var E float64
	var ez uint16
	var curBit, idx uint16
	var val byte
	var oplen uint16
	for curBit < uint16(len(hll.registers)) {
		var p = &hll.registers[curBit]
		if isSparseZERO(p) {
			oplen = uint16(sparseGetZERO(p))
			idx += oplen
			ez += oplen
			curBit ++
		} else if isSparseXZERO(p) {
			oplen = sparseGetXZERO(p)
			idx += oplen
			ez += oplen
			curBit += 2
		} else {
			// VAL 操作码
			val = sparseGetVAL(p)
			oplen = uint16(sparseGetVALEN(p))
			idx += oplen
			curBit ++
			E += PE[val] * float64(oplen)
		}
	}
	// 2^0 = 1
	E += float64(ez)
	*ezp = ez
	return E
}
func (hll *hllhdr) denseSum(ezp *uint16) float64 {
	var E float64
	var ez uint16
	var idx int
	var r = hll.registers
	var r0, r1, r2, r3, r4, r5, r6, r7, r8, r9, r10, r11, r12, r13, r14, r15 byte
	for i := 0; i < 1024; i ++ {
		// 一次循环计算16个寄存器
		r0 = r[idx] & 63
		if r0 == 0 {
			ez ++
		}
		r1 = (r[idx] >> 6 | r[idx+1] << 2) & 63
		if r1 == 0 {
			ez ++
		}
		r2 = (r[idx+1] >> 4 | r[idx+2] << 4) & 63
		if r2 == 0 {
			ez ++
		}
		r3 = (r[idx+2] >> 2) & 63
		if r3 == 0 {
			ez ++
		}
		r4 = r[idx+3] & 63
		if r4 == 0 {
			ez ++
		}
		r5 = (r[idx+3] >> 6 | r[idx+4] << 2) & 63
		if r5 == 0 {
			ez ++
		}
		r6 = (r[idx+4] >> 4 | r[idx+5] << 4) & 63
		if r6 == 0 {
			ez++
		}
		r7 = (r[idx+5] >> 2) & 63
		if r7 == 0 {
			ez++
		}
		r8 = r[idx+6] & 63
		if r8 == 0 {
			ez++
		}
		r9 = (r[idx+6] >> 6 | r[idx+7] << 2) & 63
		if r9 == 0 {
			ez++
		}
		r10 = (r[idx+7] >> 4 | r[idx+8] << 4) & 63
		if r10 == 0 {
			ez++
		}
		r11 = (r[idx+8] >> 2) & 63
		if r11 == 0 {
			ez++
		}
		r12 = r[idx+9] & 63
		if r12 == 0 {
			ez++
		}
		r13 = (r[idx+9] >> 6 | r[idx+10] << 2) & 63
		if r13 == 0 {
			ez++
		}
		r14 = (r[idx+10] >> 4 | r[idx+11] << 4) & 63
		if r14 == 0 {
			ez++
		}
		r15 = (r[idx+11] >> 2) & 63
		if r15 == 0 {
			ez++
		}
		E += (PE[r0] + PE[r1]) + (PE[r2] + PE[r3]) + (PE[r4] + PE[r5]) +
			(PE[r6] + PE[r7]) + (PE[r8] + PE[r9]) + (PE[r10] + PE[r11]) +
			(PE[r12] + PE[r13]) + (PE[r14] + PE[r15])
		idx += 12
	}
	*ezp = ez
	return E
}
