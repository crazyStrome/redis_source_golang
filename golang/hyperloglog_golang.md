redis使用hyperLogLog进行基数统计。基数统计就是计算集合中不重复元素的个数，它的应用主要为统计一段时间内登陆的用户数量等。

hyperloglog进行基数统计时，损失了部分精度，但是可以在很小的内存空间中统计非常大的数据。redis的hll(本文里代表hyperloglog)将所添加元素hash值分散到16384个桶中，每个桶中储存hash值的连续0綴长度+1，在统计时使用概率论的相关算法进行计算。具体的分析见我的另一篇文章：[redis的HyperLogLog解析](https://mp.weixin.qq.com/s/OllgM66xV8lt9Jql1kUDYQ)

本文使用golang实现了redis的hyperloglog，具体代码见[github](https://github.com/crazyStrome/redis_source_golang/tree/master/golang)

#  1.  辅助函数

##  1.1.  HLLPatLen

hll使用字节数组[]byte来储存每一个桶中的数据，在redis中，使用寄存器register来表示每个桶，register中的数据就是这个桶的值。例如，元素A的hash值为xxx....，hll从低位开始取14位数据作为寄存器的索引，找到对应的寄存器；然后统计剩余hash二进制串中第一个1出现的位置，储存到该位置的寄存器中。

因此有hllPatLen函数。

```golang
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
```

hllPatLen函数使用一个64位hash函数，这个在网上有很多实现，本文不关注其具体实现，只是拿过来使用。

index通过hash值和HLL_P_MASK进行AND操作获得。

然后从hash的第15位开始统计第一个1出现的位置，作为count返回。

##  1.2.  dense寄存器相关

hll的结构定义如下：

```golang
type hllhdr struct {
	// encoding 表示hll的编码形式
	encoding  byte
	// card 是一层缓存，当hll没有添加新元素时
	// card 是有效缓存，否则计算之后更新card
	card      []byte
	// registers 是byte数组，储存hll编码的内容
	registers []byte
}
```

hllhdr中的encoding表示当前hll的寄存器编码形式：DENSE和SPARSE。

hll的registers是字节数组，两种编码形式的寄存器都储存在这里。

dense编码形式使用6位二进制串储存每一个寄存器的内容，一个字节具有八个位，那么registers的一个字节中肯定包含两个寄存器的部分或全部二进制串。

redis使用一系列函数实现dense的编码形式，这些函数的实现原理详见文章开头的文章。

###  1.2.1.  denseSet

上代码：

```golang
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
```

denseSet中的index表示寄存器索引号，val是需要设置的新值。

###  1.2.2.  denseGet

```golang
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
```

代码中，index表示寄存器索引号。根据index计算出该寄存器从b0号字节开始，然后将b1字节和b0字节的相关内容合并在一起就是该寄存器的值。

##  1.3.  sparse寄存器相关

当hll统计的基数比较少时，使用dense储存的寄存器会有大量的零值，这会浪费很多空间，因此hll使用sparse编码形式来处理这些寄存器。

sparse使用三种操作码来表示一串连续的寄存器，在hll的registers中储存的就是这些操作码而不是dense形式的寄存器了。

* ZERO操作码占用一个字节，表示为00xxxxxx，后六位xxxxxx+1表示有N个连续的寄存器设置为0，这个操作码可以表示1-64个连续的寄存器被设置为0。
* XZERO操作码占用两个字节，表示为01xxxxxx yyyyyyyy。xxxxxx是高位，yyyyyyyy是低位。这十四位+1表示有N个连续的寄存器设置为0.这个操作码可以表示0-16384个寄存器被设置为0。
* VAL操作码占用一个字节，表示为1vvvvvxx。它包含一个5bit的vvvvv表示寄存器值，2bit的xx+1表示有这么多个连续的寄存器被设置为vvvvv。这个操作码表示可以表示1-4个寄存器被设置1-32的值。

在实现hll的sparse相关函数时，使用指针作为参数传入，这样调试更加方便。

###  1.3.1.  辨别操作码

要设置或获取sparse操作码，首先需要辨认操作码的类型。其实现也很简单，每个储存操作码的字节高两位是单独保留出来，用来区分不同操作码的。

```golang
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
```

在这些函数中，传入的参数p是hll的registers字节数组中某一个字节的指针，具体哪一个操作码覆盖哪一个寄存器的计算在函数之外进行。

###  1.3.2.  sparse配置

sparse配置的函数也很简单，详细原理见本文开头的文章，基本就是根据每个操作码的特性，获取该操作码覆盖的寄存器数量或者该操作码代表的寄存器值。

```golang
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
```

##  1.4.  cache相关

hll进行基数统计时，需要遍历所有的寄存器进行计算，占用的cpu时间和资源都很多，如果一段时间内hll没有添加新元素，而多次调用hll的基数统计时，就会重复计算相同的值，太浪费了。hll使用一个缓存来保存上次计算的内容，并通过一个位设置当前缓存是否有效。

```golang
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
```

hll统计的基数值是64位的，因此使用8个字节的card来储存。当card[7]的最高位是1的话，表示当前缓存不可用，需要重新计算。

redis在这里设计的很巧妙，当card[7]的最高位是0的时候，表示缓存有效。在获取缓存时直接将card拼接就可以了，不需要处理card[7]的最高位。

#  2.  添加数据

hll在添加新数据时分情况，如果是dense形式就调用denseAdd，否则调用sparseAdd函数。

##  2.1.  denseAdd

向hll的dense形式registers添加新数据ele的逻辑很简单：

1. 使用hllPatLen计算ele应该更新的寄存器索引idx以及寄存器值cnt
2. 使用denseGet直接获取idx寄存器的值old
3. 如果old不小于cnt，不更新；大概率，ele在之前已经添加到hll中了，这也是实现基数统计去重的重要环节
4. 如果old小于cnt，使用denseSet更新idx处的寄存器值

```golang
func (hll *hllhdr) denseAdd(ele string) error {
	var idx, cnt = hllPatLen(ele)
	var old = hll.denseGet(idx)
	if old < cnt {
		hll.denseSet(idx, cnt)
	}
	return nil
}
```

##  2.3.  sparseAdd

向sparse形式的registers添加ele的过程就很复杂了。sparse形式储存的registers中每一个字节或者两个连续的字节表示操作码，用来覆盖一串连续的寄存器。

使用hllPatLen计算ele的寄存器索引idx和值cnt。

每个操作码覆盖的寄存器数量不同，因此需要遍历registers中的每一个操作码直到找到idx号寄存器所在的操作码。

然后就是更新该位置处的操作码，分很多情况：

* 当前操作码是VAL，并且其值比cnt大，就不需要更新
* 当前操作码是VAL，其值比cnt小，直接更新该操作码
* 当前操作码是ZERO，长度是1，直接更新该操作码
* 当前操作码是ZERO或者XZERO，需要分裂成XZERO-VAL-XZERO

最后一种情况是最复杂的，本次实现中将新的操作码序列放在一个缓存中，然后更新registers中原来位置的操作码，在golang中使用append就能很方便实现。

#  3.  基数统计

hll使用hllCount函数实现基数统计，原理就是统计所有寄存器的值，然后配合统计公式进行计算。

hll根据其寄存器存储公式，分别调用denseSum和sparseSum。

hll的具体计算原理见本文开头的博客，思路也很简单。具体代码太长了，这里放sparseSum的代码：

```golang
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
```

#  4.  存储格式转换

前面提到hll在基数小的时候使用sparse形式储存它的所有寄存器。在sparseAdd的实现中，在以下情况会转换为dense形式：

* 计算的cnt大于sparse能储存的最大值：32

```go
if cnt > HLL_SPARSE_VAL_MAX_VALUE {
	// 转换成dense，然后添加到hll
	var err = hll.sparse2dense()
	if err != nil {
		return err
	}
	return hll.denseAdd(ele)

```

* 更新后的registers大于HLL_SPARSE_MAX_BYTES

```golang
var oldlen int = 1
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
```

hll使用sparse2dense函数进行储存形式转换。该函数的实现方式很简单，就是把当前sparse形式的registers一个个转换为dense的registers。具体逻辑如下：

1. 创建足够大小的字节数组：16384 * 6 / 8个字节
2. 遍历sparse的registers中的操作码；如果是ZERO或XZERO，覆盖指定数量的dense寄存器，这些寄存器初始化时就是0，因此直接跳过即可；如果是VAL，取出其储存的长度和内容，将接下来这么长的寄存器设置为VAL的内容
3. 最后将hll的encoding设置为HLL_DENSE

#  5.  测试

测试hll的误差，从1到MAX_UINT64添加到hll中，进行基数统计并计算误差值。

统计的结果数据量太大，导入到excel做表格时系统内存不够用，只截取前一部分数据分析如下：

![image-20210519102424587](https://gitee.com/crazstom/pics/raw/master/img/image-20210519102424587.png)

上图横坐标是实际值，纵坐标是误差=Abs(统计值-实际值)/实际值，按照该曲线趋势，统计到1200000时误差也不会超过10%。

使用测试文件中的TestAddAndCount单线程运行测试三个小时，当测到9443829时，hll的误差有12.18%。总体来说误差不是很大。

![image-20210519102720480](https://gitee.com/crazstom/pics/raw/master/img/image-20210519102720480.png)