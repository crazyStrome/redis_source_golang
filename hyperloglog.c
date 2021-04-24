/* hyperloglog.c - Redis HyperLogLog probabilistic cardinality approximation.
 * This file implements the algorithm and the exported Redis commands.
 *
 * Copyright (c) 2014, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "redis.h"

#include <stdint.h>
#include <math.h>

/* The Redis HyperLogLog implementation is based on the following ideas:
 *
 * * The use of a 64 bit hash function as proposed in [1], in order to don't
 *   limited to cardinalities up to 10^9, at the cost of just 1 additional
 *   bit per register.
 * * The use of 16384 6-bit registers for a great level of accuracy, using
 *   a total of 12k per key.
 *   使用16384个6位寄存器实现精确度的提高，hyperloglog的一个可以仅使用12k的内存
 * * The use of the Redis string data type. No new type is introduced.
 *   使用redis的string类型，不建立新数据类型
 * * No attempt is made to compress the data structure as in [1]. Also the
 *   algorithm used is the original HyperLogLog Algorithm as in [2], with
 *   the only difference that a 64 bit hash function is used, so no correction
 *   is performed for values near 2^32 as in [1].
 *
 * [1] Heule, Nunkesser, Hall: HyperLogLog in Practice: Algorithmic
 *     Engineering of a State of The Art Cardinality Estimation Algorithm.
 *
 * [2] P. Flajolet, Éric Fusy, O. Gandouet, and F. Meunier. Hyperloglog: The
 *     analysis of a near-optimal cardinality estimation algorithm.
 *
 * Redis uses two representations:
 * Redis使用两种表现形式
 *
 * 1) A "dense" representation where every entry is represented by
 *    a 6-bit integer.
 * 1) dense形式使用6位的数字表示
 * 2) A "sparse" representation using run length compression suitable
 *    for representing HyperLogLogs with many registers set to 0 in
 *    a memory efficient way.
 * 2) sparse形式将0进行压缩
 *
 *
 * HLL header
 * ===
 *
 * Both the dense and sparse representation have a 16 byte header as follows:
 *
 * +------+---+-----+----------+
 * | HYLL | E | N/U | Cardin.  |
 * +------+---+-----+----------+
 *
 * The first 4 bytes are a magic string set to the bytes "HYLL".
 * "E" is one byte encoding, currently set to HLL_DENSE or
 * E表示编码方式：HLL_DENSE或者HLL_SPARSE
 * HLL_SPARSE. N/U are three not used bytes.
 *
 * The "Cardin." field is a 64 bit integer stored in little endian format
 * with the latest cardinality computed that can be reused if the data
 * structure was not modified since the last computation (this is useful
 * because there are high probabilities that HLLADD operations don't
 * modify the actual data structure and hence the approximated cardinality).
 * Cardin 是用小端序储存的64位整数
 * 其中保存最近的基数计算结果
 * 如果从上一次计算到现在，数据结构没有没修改过，这个基数就可以重用
 * 这个是很有用的，因为HLLADD操作有很大概率不会修改实际数据结构
 *
 * When the most significant bit in the most significant byte of the cached
 * cardinality is set, it means that the data structure was modified and
 * we can't reuse the cached value that must be recomputed.
 * 当设置了缓存基数的最高有效字节中的最高有效位时，
 * 这意味着数据结构已修改，
 * 我们无法重用必须重新计算的缓存值。
 * 
 * Dense representation
 * Dense 形式
 * ===
 *
 * The dense representation used by Redis is the following:
 *
 * +--------+--------+--------+------//      //--+
 * |11000000|22221111|33333322|55444444 ....     |
 * +--------+--------+--------+------//      //--+
 *
 * The 6 bits counters are encoded one after the other starting from the
 * LSB to the MSB, and using the next bytes as needed.
 * 6位计数器从LSB到MSB依次编码，并根据需要使用下一个字节。
 *
 * Sparse representation
 * Sparse 形式
 * ===
 *
 * The sparse representation encodes registers using a run length
 * encoding composed of three opcodes, two using one byte, and one using
 * of two bytes. The opcodes are called ZERO, XZERO and VAL.
 * sparse 形式使用变长编码对寄存器进行编码，
 * 变长编码形式使用三个操作码组成，
 * 其中两个操作码分别使用一个字节
 * 另一个操作码使用两个字节
 * 这三个操作码分别是ZERO、XZERO、VAL
 *
 * ZERO opcode is represented as 00xxxxxx. The 6-bit integer represented
 * by the six bits 'xxxxxx', plus 1, means that there are N registers set
 * to 0. This opcode can represent from 1 to 64 contiguous registers set
 * to the value of 0.
 * ZERO 操作码表示成00xxxxxx，后六位数字+1表示有N个寄存器设置为0
 * 这个操作码可以表示1-64个连续的寄存器被设置为0了
 *
 * XZERO opcode is represented by two bytes 01xxxxxx yyyyyyyy. The 14-bit
 * integer represented by the bits 'xxxxxx' as most significant bits and
 * 'yyyyyyyy' as least significant bits, plus 1, means that there are N
 * registers set to 0. This opcode can represent from 0 to 16384 contiguous
 * registers set to the value of 0.
 * XZERO 操作码使用两个字节 01xxxxxx yyyyyyyy表示，这xxxxxx是高位，yyyyyyyy是低位
 * 这14位数字+1表示有N个寄存器设置为0
 * 这个操作码可以表示从0-16384个连续的寄存器为0
 *
 * VAL opcode is represented as 1vvvvvxx. It contains a 5-bit integer
 * representing the value of a register, and a 2-bit integer representing
 * the number of contiguous registers set to that value 'vvvvv'.
 * To obtain the value and run length, the integers vvvvv and xx must be
 * incremented by one. This opcode can represent values from 1 to 32,
 * repeated from 1 to 4 times.
 * VAL 操作码表示成1vvvvvxx，vvvvv表示一个寄存器的值，xx这个两位的数字表示有xx个寄存器的内容是vvvvv
 * 这个操作符可以表示1-32的值，可以重复1-4次
 *
 * The sparse representation can't represent registers with a value greater
 * than 32, however it is very unlikely that we find such a register in an
 * HLL with a cardinality where the sparse representation is still more
 * memory efficient than the dense representation. When this happens the
 * HLL is converted to the dense representation.
 * Sparse 形式无法表示内容超过32的寄存器，
 * 然而，很少见可以在HLL的基数寄存器中找到超过32的寄存器
 * 在这种情况下，sparse形式仍然比dense形式具有更高的内存效率
 * 当发生这种情况，HLL会转为dense形式
 *
 * The sparse representation is purely positional. For example a sparse
 * representation of an empty HLL is just: XZERO:16384.
 * Sparse 形式纯粹是位置性的，例如一个稀疏HLL的sparse形式表示为：XZERO：16384
 *
 * An HLL having only 3 non-zero registers at position 1000, 1020, 1021
 * respectively set to 2, 3, 3, is represented by the following three
 * opcodes:
 * 一个HLL，只在1000、1020、1021三个位置有非零寄存器，并且值为2，3，3
 * 表示为：
 *
 * XZERO:1000 (Registers 0-999 are set to 0)
 * VAL:2,1    (1 register set to value 2, that is register 1000)
 * ZERO:19    (Registers 1001-1019 set to 0)
 * VAL:3,2    (2 registers set to value 3, that is registers 1020,1021)
 * XZERO:15362 (Registers 1022-16383 set to 0)
 * 
 * XZERO：1000 0-999的寄存器都是0
 * VAL：2，1 一个寄存器值是2，位置1000的寄存器
 * ZERO：19 1001-1019的寄存器设为0
 * VAL：3，2 两个寄存器设置为3，分别是1020、1021位置的寄存器
 * XZERO：15362 从1022-16383的寄存器设为0
 *
 * In the example the sparse representation used just 7 bytes instead
 * of 12k in order to represent the HLL registers. In general for low
 * cardinality there is a big win in terms of space efficiency, traded
 * with CPU time since the sparse representation is slower to access:
 * 在这个例子中，sparse形式使用7个字节而不是12k来表示HLL寄存器。
 * 当基数比较小的时候会有很高的的空间利用率，换来的是CPU时间的增加
 *
 * The following table shows average cardinality vs bytes used, 100
 * samples per cardinality (when the set was not representable because
 * of registers with too big value, the dense representation size was used
 * as a sample).
 * 下面的表格显示的是平均基数 vs 使用的字节数，一个基数使用100个样本
 *
 * 100 267
 * 200 485
 * 300 678
 * 400 859
 * 500 1033
 * 600 1205
 * 700 1375
 * 800 1544
 * 900 1713
 * 1000 1882
 * 2000 3480
 * 3000 4879
 * 4000 6089
 * 5000 7138
 * 6000 8042
 * 7000 8823
 * 8000 9500
 * 9000 10088
 * 10000 10591
 *
 * The dense representation uses 12288 bytes, so there is a big win up to
 * a cardinality of ~2000-3000. For bigger cardinalities the constant times
 * involved in updating the sparse representation is not justified by the
 * memory savings. The exact maximum length of the sparse representation
 * when this implementation switches to the dense representation is
 * configured via the define server.hll_sparse_max_bytes.
 * dense 形式使用12288字节，因此spare在进行2000-3000的基数统计时效率很高
 * 对于更大的基数，sparse会使用更多的时间。
 * 通过定义server.hll_sparse_max_bytes实现切换为dense的最大长度
 */

struct hllhdr {
    char magic[4];      /* "HYLL" */
    // 编码格式：sparse或者dense
    uint8_t encoding;   /* HLL_DENSE or HLL_SPARSE. */
    uint8_t notused[3]; /* Reserved for future use, must be zero. */
    // 缓存的基数统计值
    uint8_t card[8];    /* Cached cardinality, little endian. */
    // 实际的寄存器们
    uint8_t registers[]; /* Data bytes. */
};

/* The cached cardinality MSB is used to signal validity of the cached value. */
// card的第八位设置为1表示缓存不可用
#define HLL_INVALIDATE_CACHE(hdr) (hdr)->card[7] |= (1<<7)
#define HLL_VALID_CACHE(hdr) (((hdr)->card[7] & (1<<7)) == 0)

#define HLL_P 14 /* The greater is P, the smaller the error. */
// HLL使用16384个寄存器：0b 0100 0000 0000 0000
#define HLL_REGISTERS (1<<HLL_P) /* With P=14, 16384 registers. */
// HLL的掩码用来找位置，0x0011 1111 1111 1111
#define HLL_P_MASK (HLL_REGISTERS-1) /* Mask to index register. */
#define HLL_BITS 6 /* Enough to count up to 63 leading zeroes. */
#define HLL_REGISTER_MAX ((1<<HLL_BITS)-1)
#define HLL_HDR_SIZE sizeof(struct hllhdr)
#define HLL_DENSE_SIZE (HLL_HDR_SIZE+((HLL_REGISTERS*HLL_BITS+7)/8))
#define HLL_DENSE 0 /* Dense encoding. */
#define HLL_SPARSE 1 /* Sparse encoding. */
#define HLL_RAW 255 /* Only used internally, never exposed. */
#define HLL_MAX_ENCODING 1

static char *invalid_hll_err = "-INVALIDOBJ Corrupted HLL object detected\r\n";

/* =========================== Low level bit macros ========================= */
// 用于进行位运算的宏

/* Macros to access the dense representation.
 * 针对dense形式的宏
 *
 * We need to get and set 6 bit counters in an array of 8 bit bytes.
 * We use macros to make sure the code is inlined since speed is critical
 * especially in order to compute the approximated cardinality in
 * HLLCOUNT where we need to access all the registers at once.
 * For the same reason we also want to avoid conditionals in this code path.
 * 我们需要获取或者设置8位字节数组的6位计数器，就是把一串8位数组切割成6位的数组
 * 我们使用宏来保证运行速度，尤其是计算HLLCOOUNT的近似基数统计时需要统计所有的寄存器
 * 
 *
 * +--------+--------+--------+------//
 * |11000000|22221111|33333322|55444444
 * +--------+--------+--------+------//
 *
 * Note: in the above representation the most significant bit (MSB)
 * of every byte is on the left. We start using bits from the LSB to MSB,
 * and so forth passing to the next byte.
 * 上面是大端字节序，最高位在左边
 * 我们使用最低位开始到最高位依次遍历
 * Example, we want to access to counter at pos = 1 ("111111" in the
 * illustration above).
 * 例如，我们想获取在位置1（从0开始）处的计数
 *
 * The index of the first byte b0 containing our data is:
 * 储存数据的第一个字节是1100 0000
 *
 *  b0 = 6 * pos / 8 = 0
 *
 *   +--------+
 *   |11000000|  <- Our byte at b0
 *   +--------+
 *
 * The position of the first bit (counting from the LSB = 0) in the byte
 * is given by:
 * 第一位（从最低位开始数）的位置这么计算：
 *
 *  fb = 6 * pos % 8 -> 6
 *
 * Right shift b0 of 'fb' bits.
 * 将b0右移fb位
 *
 *   +--------+
 *   |11000000|  <- Initial value of b0
 *   |00000011|  <- After right shift of 6 pos.
 *   +--------+
 *
 * Left shift b1 of bits 8-fb bits (2 bits)
 * 将b1左移8-fb位：
 *
 *   +--------+
 *   |22221111|  <- Initial value of b1
 *   |22111100|  <- After left shift of 2 bits.
 *   +--------+
 *
 * OR the two bits, and finally AND with 111111 (63 in decimal) to
 * clean the higher order bits we are not interested in:
 * 将这两个字节进行或操作，然后和111111与操作以消除高2位
 *
 *   +--------+
 *   |00000011|  <- b0 right shifted
 *   |22111100|  <- b1 left shifted
 *   |22111111|  <- b0 OR b1
 *   |  111111|  <- (b0 OR b1) AND 63, our value.
 *   +--------+
 *
 * We can try with a different example, like pos = 0. In this case
 * the 6-bit counter is actually contained in a single byte.
 * 我们使用另一个例子，比如0位置的内容。
 * 该情况下，这个六位的数字包含在一个字节中
 *
 *  b0 = 6 * pos / 8 = 0
 *
 *   +--------+
 *   |11000000|  <- Our byte at b0
 *   +--------+
 *
 *  fb = 6 * pos % 8 = 0
 *
 *  So we right shift of 0 bits (no shift in practice) and
 *  left shift the next byte of 8 bits, even if we don't use it,
 *  but this has the effect of clearing the bits so the result
 *  will not be affacted after the OR.
 * 因此需要把当前字节右移0位，下一字节左移8位，即使没有用到后一个字节的内容
 * 但是这会消除这些位，它就不会被之后的或运算影响
 *
 * -------------------------------------------------------------------------
 *
 * Setting the register is a bit more complex, let's assume that 'val'
 * is the value we want to set, already in the right range.
 * 设置寄存器就比较复杂了，假设val是需要设置的寄存器值
 *
 * We need two steps, in one we need to clear the bits, and in the other
 * we need to bitwise-OR the new bits.
 * 需要两步，第一步清除寄存器的相关位，第二部需要通过或操作设置新的位
 *
 * Let's try with 'pos' = 1, so our first byte at 'b' is 0,
 *
 * "fb" is 6 in this case.
 *
 *   +--------+
 *   |11000000|  <- Our byte at b0
 *   +--------+
 *
 * To create a AND-mask to clear the bits about this position, we just
 * initialize the mask with the value 63, left shift it of "fs" bits,
 * and finally invert the result.
 * 为了生成一个与掩码来清除该位置的相关位，使用一个初始值为63的掩码
 * 左移fb位，然后翻转结果
 *
 *   +--------+
 *   |00111111|  <- "mask" starts at 63
 *   |11000000|  <- "mask" after left shift of "ls" bits.
 *   |00111111|  <- "mask" after invert.
 *   +--------+
 *
 * Now we can bitwise-AND the byte at "b" with the mask, and bitwise-OR
 * it with "val" left-shifted of "ls" bits to set the new bits.
 * 让新生成的掩码和第0个字节进行与操作，就清除了相关的位
 * 然后将val左移fs位，和上述结果进行或操作，就设置了新值
 *
 * Now let's focus on the next byte b1:
 * 接下来是设置下一个字节b1
 *
 *   +--------+
 *   |22221111|  <- Initial value of b1
 *   +--------+
 *
 * To build the AND mask we start again with the 63 value, right shift
 * it by 8-fb bits, and invert it.
 * 使用63构建与掩码，右移8-fb位，然后翻转
 *
 *   +--------+
 *   |00111111|  <- "mask" set at 2&6-1
 *   |00001111|  <- "mask" after the right shift by 8-fb = 2 bits
 *   |11110000|  <- "mask" after bitwise not.
 *   +--------+
 *
 * Now we can mask it with b+1 to clear the old bits, and bitwise-OR
 * with "val" left-shifted by "rs" bits to set the new value.
 */

/* Note: if we access the last counter, we will also access the b+1 byte
 * that is out of the array, but sds strings always have an implicit null
 * term, so the byte exists, and we can skip the conditional (or the need
 * to allocate 1 byte more explicitly). */

/* Store the value of the register at position 'regnum' into variable 'target'.
 * 'p' is an array of unsigned bytes. */
#define HLL_DENSE_GET_REGISTER(target,p,regnum) do { \
    uint8_t *_p = (uint8_t*) p; \
    // 这个寄存器在第_byte个字节
    unsigned long _byte = regnum*HLL_BITS/8; \
    // 这个寄存器在第_byte个字节的第_fb位
    unsigned long _fb = regnum*HLL_BITS&7; \
    unsigned long _fb8 = 8 - _fb; \
    unsigned long b0 = _p[_byte]; \
    unsigned long b1 = _p[_byte+1]; \
    // b0的高fb位、b1的低fb8位
    target = ((b0 >> _fb) | (b1 << _fb8)) & HLL_REGISTER_MAX; \
} while(0)

/* Set the value of the register at position 'regnum' to 'val'.
 * 'p' is an array of unsigned bytes. */
#define HLL_DENSE_SET_REGISTER(p,regnum,val) do { \
    uint8_t *_p = (uint8_t*) p; \
    unsigned long _byte = regnum*HLL_BITS/8; \
    unsigned long _fb = regnum*HLL_BITS&7; \
    unsigned long _fb8 = 8 - _fb; \
    unsigned long _v = val; \
    _p[_byte] &= ~(HLL_REGISTER_MAX << _fb); \
    _p[_byte] |= _v << _fb; \
    _p[_byte+1] &= ~(HLL_REGISTER_MAX >> _fb8); \
    _p[_byte+1] |= _v >> _fb8; \
} while(0)

/* Macros to access the sparse representation.
 * The macros parameter is expected to be an uint8_t pointer. */
#define HLL_SPARSE_XZERO_BIT 0x40 /* 01xxxxxx */
#define HLL_SPARSE_VAL_BIT 0x80 /* 1vvvvvxx */
#define HLL_SPARSE_IS_ZERO(p) (((*(p)) & 0xc0) == 0) /* 00xxxxxx */
#define HLL_SPARSE_IS_XZERO(p) (((*(p)) & 0xc0) == HLL_SPARSE_XZERO_BIT)
#define HLL_SPARSE_IS_VAL(p) ((*(p)) & HLL_SPARSE_VAL_BIT)
#define HLL_SPARSE_ZERO_LEN(p) (((*(p)) & 0x3f)+1)
#define HLL_SPARSE_XZERO_LEN(p) (((((*(p)) & 0x3f) << 8) | (*((p)+1)))+1)
#define HLL_SPARSE_VAL_VALUE(p) ((((*(p)) >> 2) & 0x1f)+1)
#define HLL_SPARSE_VAL_LEN(p) (((*(p)) & 0x3)+1)
#define HLL_SPARSE_VAL_MAX_VALUE 32
#define HLL_SPARSE_VAL_MAX_LEN 4
#define HLL_SPARSE_ZERO_MAX_LEN 64
#define HLL_SPARSE_XZERO_MAX_LEN 16384
#define HLL_SPARSE_VAL_SET(p,val,len) do { \
    *(p) = (((val)-1)<<2|((len)-1))|HLL_SPARSE_VAL_BIT; \
} while(0)
#define HLL_SPARSE_ZERO_SET(p,len) do { \
    *(p) = (len)-1; \
} while(0)
#define HLL_SPARSE_XZERO_SET(p,len) do { \
    int _l = (len)-1; \
    *(p) = (_l>>8) | HLL_SPARSE_XZERO_BIT; \
    *((p)+1) = (_l&0xff); \
} while(0)

/* ========================= HyperLogLog algorithm  ========================= */

/* Our hash function is MurmurHash2, 64 bit version.
 * It was modified for Redis in order to provide the same result in
 * big and little endian archs (endian neutral). */
// 大端、小端顺序都能用
uint64_t MurmurHash64A (const void * key, int len, unsigned int seed) {
    const uint64_t m = 0xc6a4a7935bd1e995;
    const int r = 47;
    uint64_t h = seed ^ (len * m);
    const uint8_t *data = (const uint8_t *)key;
    const uint8_t *end = data + (len-(len&7));

    while(data != end) {
        uint64_t k;

#if (BYTE_ORDER == LITTLE_ENDIAN)
        k = *((uint64_t*)data);
#else
        k = (uint64_t) data[0];
        k |= (uint64_t) data[1] << 8;
        k |= (uint64_t) data[2] << 16;
        k |= (uint64_t) data[3] << 24;
        k |= (uint64_t) data[4] << 32;
        k |= (uint64_t) data[5] << 40;
        k |= (uint64_t) data[6] << 48;
        k |= (uint64_t) data[7] << 56;
#endif

        k *= m;
        k ^= k >> r;
        k *= m;
        h ^= k;
        h *= m;
        data += 8;
    }

    switch(len & 7) {
    case 7: h ^= (uint64_t)data[6] << 48;
    case 6: h ^= (uint64_t)data[5] << 40;
    case 5: h ^= (uint64_t)data[4] << 32;
    case 4: h ^= (uint64_t)data[3] << 24;
    case 3: h ^= (uint64_t)data[2] << 16;
    case 2: h ^= (uint64_t)data[1] << 8;
    case 1: h ^= (uint64_t)data[0];
            h *= m;
    };

    h ^= h >> r;
    h *= m;
    h ^= h >> r;
    return h;
}

/* Given a string element to add to the HyperLogLog, returns the length
 * of the pattern 000..1 of the element hash. As a side effect 'regp' is
 * set to the register index this element hashes to. */
int hllPatLen(unsigned char *ele, size_t elesize, long *regp) {
    uint64_t hash, bit, index;
    int count;

    /* Count the number of zeroes starting from bit HLL_REGISTERS
     * (that is a power of two corresponding to the first bit we don't use
     * as index). The max run can be 64-P+1 bits.
     * 从HLL_REGISTERS位开始统计零的数量
     * 
     *
     * Note that the final "1" ending the sequence of zeroes must be
     * included in the count, so if we find "001" the count is 3, and
     * the smallest count possible is no zeroes at all, just a 1 bit
     * at the first position, that is a count of 1.
     * 从0开始数的第一个1也包含在计数中，
     * 例如 001 统计为3，计数结果至少为1
     *
     * This may sound like inefficient, but actually in the average case
     * there are high probabilities to find a 1 after a few iterations. 
     * 听起来可能效率比较低，但是在实际中，经历几次迭代就可以找到第一个1了
     */
    hash = MurmurHash64A(ele,elesize,0xadc83b19ULL);
    index = hash & HLL_P_MASK; /* Register index. */
    // hash的第 63 位设置为 1
    hash |= ((uint64_t)1<<63); /* Make sure the loop terminates. */
    // bit = 1 << 14 = 0100 0000 0000 0000
    bit = HLL_REGISTERS; /* First bit not used to address the register. */
    count = 1; /* Initialized to 1 since we count the "00000...1" pattern. */
    // 从第 14位开始高位统计0的个数
    while((hash & bit) == 0) {
        count++;
        bit <<= 1;
    }
    *regp = (int) index;
    return count;
}

/* ================== Dense representation implementation  ================== */
// dense 实现

/* "Add" the element in the dense hyperloglog data structure.
 * Actually nothing is added, but the max 0 pattern counter of the subset
 * the element belongs to is incremented if needed.
 * 在hyperloglog中添加元素
 * 实际上并没有添加这个元素，只不过进行0的统计
 * 有可能替换其计数
 *
 * 'registers' is expected to have room for HLL_REGISTERS plus an
 * additional byte on the right. This requirement is met by sds strings
 * automatically since they are implicitly null terminated.
 * 寄存器需要有HLL_REGISTERS这么大的空间加上右侧额外的字节
 * 这是因为sds字符串会有一个null的终止符
 *
 * The function always succeed, however if as a result of the operation
 * the approximated cardinality changed, 1 is returned. Otherwise 0
 * is returned. */
int hllDenseAdd(uint8_t *registers, unsigned char *ele, size_t elesize) {
    uint8_t oldcount, count;
    long index;

    /* Update the register if this element produced a longer run of zeroes. */
    count = hllPatLen(ele,elesize,&index);
    HLL_DENSE_GET_REGISTER(oldcount,registers,index);
    if (count > oldcount) {
        HLL_DENSE_SET_REGISTER(registers,index,count);
        return 1;
    } else {
        return 0;
    }
}

/* Compute SUM(2^-reg) in the dense representation.
 * PE is an array with a pre-computer table of values 2^-reg indexed by reg.
 * As a side effect the integer pointed by 'ezp' is set to the number
 * of zero registers. 
 * 计算dense形式的 SUM(2^-reg)
 * PE是提前计算的 2^-reg 表
 * ezp 指向0寄存器的个数
 * */
double hllDenseSum(uint8_t *registers, double *PE, int *ezp) {
    double E = 0;
    // ez 是统计的0的个数
    int j, ez = 0;

    /* Redis default is to use 16384 registers 6 bits each. The code works
     * with other values by modifying the defines, but for our target value
     * we take a faster path with unrolled loops. */
    if (HLL_REGISTERS == 16384 && HLL_BITS == 6) {
        uint8_t *r = registers;
        unsigned long r0, r1, r2, r3, r4, r5, r6, r7, r8, r9,
                      r10, r11, r12, r13, r14, r15;
        // 1024 * 64 = 16384
        for (j = 0; j < 1024; j++) {
            /* Handle 16 registers per iteration. */
            r0 = r[0] & 63; if (r0 == 0) ez++;
            r1 = (r[0] >> 6 | r[1] << 2) & 63; if (r1 == 0) ez++;
            r2 = (r[1] >> 4 | r[2] << 4) & 63; if (r2 == 0) ez++;
            r3 = (r[2] >> 2) & 63; if (r3 == 0) ez++;
            r4 = r[3] & 63; if (r4 == 0) ez++;
            r5 = (r[3] >> 6 | r[4] << 2) & 63; if (r5 == 0) ez++;
            r6 = (r[4] >> 4 | r[5] << 4) & 63; if (r6 == 0) ez++;
            r7 = (r[5] >> 2) & 63; if (r7 == 0) ez++;
            r8 = r[6] & 63; if (r8 == 0) ez++;
            r9 = (r[6] >> 6 | r[7] << 2) & 63; if (r9 == 0) ez++;
            r10 = (r[7] >> 4 | r[8] << 4) & 63; if (r10 == 0) ez++;
            r11 = (r[8] >> 2) & 63; if (r11 == 0) ez++;
            r12 = r[9] & 63; if (r12 == 0) ez++;
            r13 = (r[9] >> 6 | r[10] << 2) & 63; if (r13 == 0) ez++;
            r14 = (r[10] >> 4 | r[11] << 4) & 63; if (r14 == 0) ez++;
            r15 = (r[11] >> 2) & 63; if (r15 == 0) ez++;

            /* Additional parens will allow the compiler to optimize the
             * code more with a loss of precision that is not very relevant
             * here (floating point math is not commutative!). */
            // 两两相加减小误差
            E += (PE[r0] + PE[r1]) + (PE[r2] + PE[r3]) + (PE[r4] + PE[r5]) +
                 (PE[r6] + PE[r7]) + (PE[r8] + PE[r9]) + (PE[r10] + PE[r11]) +
                 (PE[r12] + PE[r13]) + (PE[r14] + PE[r15]);
            r += 12;
        }
    } else {
        for (j = 0; j < HLL_REGISTERS; j++) {
            unsigned long reg;

            HLL_DENSE_GET_REGISTER(reg,registers,j);
            if (reg == 0) {
                ez++;
                /* Increment E at the end of the loop. */
            } else {
                E += PE[reg]; /* Precomputed 2^(-reg[j]). */
            }
        }
        E += ez; /* Add 2^0 'ez' times. */
    }
    *ezp = ez;
    return E;
}

/* ================== Sparse representation implementation  ================= */

/* Convert the HLL with sparse representation given as input in its dense
 * representation. Both representations are represented by SDS strings, and
 * the input representation is freed as a side effect.
 * sparse 转换为dense形式
 * 两种形式都是用sds表示
 * 输入的sparse形式数据会被释放
 *
 * The function returns REDIS_OK if the sparse representation was valid,
 * otherwise REDIS_ERR is returned if the representation was corrupted. */
int hllSparseToDense(robj *o) {
    sds sparse = o->ptr, dense;
    // 把sds指针转为hllhdr
    struct hllhdr *hdr, *oldhdr = (struct hllhdr*)sparse;
    int idx = 0, runlen, regval;
    // 获取起始和结束指针
    uint8_t *p = (uint8_t*)sparse, *end = p+sdslen(sparse);

    /* If the representation is already the right one return ASAP. */
    // 如果已经转换完成了，就直接返回
    hdr = (struct hllhdr*) sparse;
    if (hdr->encoding == HLL_DENSE) return REDIS_OK;

    /* Create a string of the right size filled with zero bytes.
     * Note that the cached cardinality is set to 0 as a side effect
     * that is exactly the cardinality of an empty HLL. */
    dense = sdsnewlen(NULL,HLL_DENSE_SIZE);
    hdr = (struct hllhdr*) dense;
    *hdr = *oldhdr; /* This will copy the magic and cached cardinality. */
    hdr->encoding = HLL_DENSE;

    /* Now read the sparse representation and set non-zero registers
     * accordingly. */
    // p 调到hllhdr 的data指针处
    p += HLL_HDR_SIZE;
    while(p < end) {
        if (HLL_SPARSE_IS_ZERO(p)) {
            runlen = HLL_SPARSE_ZERO_LEN(p);
            idx += runlen;
            p++;
        } else if (HLL_SPARSE_IS_XZERO(p)) {
            runlen = HLL_SPARSE_XZERO_LEN(p);
            idx += runlen;
            p += 2;
        } else {
            runlen = HLL_SPARSE_VAL_LEN(p);
            regval = HLL_SPARSE_VAL_VALUE(p);
            while(runlen--) {
                HLL_DENSE_SET_REGISTER(hdr->registers,idx,regval);
                idx++;
            }
            p++;
        }
    }

    /* If the sparse representation was valid, we expect to find idx
     * set to HLL_REGISTERS. */
    if (idx != HLL_REGISTERS) {
        sdsfree(dense);
        return REDIS_ERR;
    }

    /* Free the old representation and set the new one. */
    sdsfree(o->ptr);
    o->ptr = dense;
    return REDIS_OK;
}

/* "Add" the element in the sparse hyperloglog data structure.
 * Actually nothing is added, but the max 0 pattern counter of the subset
 * the element belongs to is incremented if needed.
 * 把新元素添加到sparse形式的hyperloglog数据结构中
 * 实际上不会添加新元素，只不过把统计的0后缀添加在上面
 *
 * The object 'o' is the String object holding the HLL. The function requires
 * a reference to the object in order to be able to enlarge the string if
 * needed.
 *
 * On success, the function returns 1 if the cardinality changed, or 0
 * if the register for this element was not updated.
 * On error (if the representation is invalid) -1 is returned.
 *
 * As a side effect the function may promote the HLL representation from
 * sparse to dense: this happens when a register requires to be set to a value
 * not representable with the sparse representation, or when the resulting
 * size would be greater than server.hll_sparse_max_bytes. */
int hllSparseAdd(robj *o, unsigned char *ele, size_t elesize) {
    struct hllhdr *hdr;
    uint8_t oldcount, count, *sparse, *end, *p, *prev, *next;
    long index, first, span;
    long is_zero = 0, is_xzero = 0, is_val = 0, runlen = 0;

    /* Update the register if this element produced a longer run of zeroes. */
    count = hllPatLen(ele,elesize,&index);

    /* If the count is too big to be representable by the sparse representation
     * switch to dense representation. */
    // 32
    if (count > HLL_SPARSE_VAL_MAX_VALUE) goto promote;

    /* When updating a sparse representation, sometimes we may need to
     * enlarge the buffer for up to 3 bytes in the worst case (XZERO split
     * into XZERO-VAL-XZERO). Make sure there is enough space right now
     * so that the pointers we take during the execution of the function
     * will be valid all the time. 
     * 在添加数据时可能会出现 XZERO分裂成 XZERO-VAL-XZERO
     * 通过sdsMakeRoomFor保证有足够的空间进行数据的添加
     * */
    o->ptr = sdsMakeRoomFor(o->ptr,3);

    /* Step 1: we need to locate the opcode we need to modify to check
     * if a value update is actually needed. */
    sparse = p = ((uint8_t*)o->ptr) + HLL_HDR_SIZE;
    end = p + sdslen(o->ptr) - HLL_HDR_SIZE;

    first = 0;
    prev = NULL; /* Points to previos opcode at the end of the loop. */
    next = NULL; /* Points to the next opcode at the end of the loop. */
    span = 0;
    while(p < end) {
        long oplen;

        /* Set span to the number of registers covered by this opcode.
         *
         * This is the most performance critical loop of the sparse
         * representation. Sorting the conditionals from the most to the
         * least frequent opcode in many-bytes sparse HLLs is faster. */
        oplen = 1;
        if (HLL_SPARSE_IS_ZERO(p)) {
            span = HLL_SPARSE_ZERO_LEN(p);
        } else if (HLL_SPARSE_IS_VAL(p)) {
            span = HLL_SPARSE_VAL_LEN(p);
        } else { /* XZERO. */
            span = HLL_SPARSE_XZERO_LEN(p);
            oplen = 2;
        }
        /* Break if this opcode covers the register as 'index'. */
        if (index <= first+span-1) break;
        prev = p;
        p += oplen;
        first += span;
    }
    if (span == 0) return -1; /* Invalid format. */
    // 新数据在p指向的数据包含的区间里

    next = HLL_SPARSE_IS_XZERO(p) ? p+2 : p+1;
    if (next >= end) next = NULL;

    /* Cache current opcode type to avoid using the macro again and
     * again for something that will not change.
     * Also cache the run-length of the opcode. */
    if (HLL_SPARSE_IS_ZERO(p)) {
        is_zero = 1;
        runlen = HLL_SPARSE_ZERO_LEN(p);
    } else if (HLL_SPARSE_IS_XZERO(p)) {
        is_xzero = 1;
        runlen = HLL_SPARSE_XZERO_LEN(p);
    } else {
        is_val = 1;
        runlen = HLL_SPARSE_VAL_LEN(p);
    }

    /* Step 2: After the loop:
     *
     * 'first' stores to the index of the first register covered
     *  by the current opcode, which is pointed by 'p'.
     * first 保存了包含当前操作码的第一个寄存器，指针是p
     *
     * 'next' ad 'prev' store respectively the next and previous opcode,
     *  or NULL if the opcode at 'p' is respectively the last or first.
     * next 和 prev 储存它之后和之前的操作码
     * 如果p是最开始或最后的操作码，next或prev就是null
     *
     * 'span' is set to the number of registers covered by the current
     *  opcode.
     * span 是当前操作码覆盖的寄存器数量
     *
     * There are different cases in order to update the data structure
     * in place without generating it from scratch:
     * 有不同的情况来更新hll的数据结构：
     *
     * A) If it is a VAL opcode already set to a value >= our 'count'
     *    no update is needed, regardless of the VAL run-length field.
     *    In this case PFADD returns 0 since no changes are performed.
     * A) 如果它是一个VAL操作码并且其值比新值计算的count要大，就不需要更新了
     * PFADD 会返回0表示没有改变发生
     *
     * B) If it is a VAL opcode with len = 1 (representing only our
     *    register) and the value is less than 'count', we just update it
     *    since this is a trivial case. 
     * B) 如果当前操作码是VAL操作码并且长度是1，并且其值小于count，那就直接更新它了
     * */
    if (is_val) {
        oldcount = HLL_SPARSE_VAL_VALUE(p);
        /* Case A. */
        if (oldcount >= count) return 0;

        /* Case B. */
        if (runlen == 1) {
            HLL_SPARSE_VAL_SET(p,count,1);
            goto updated;
        }
    }

    /* C) Another trivial to handle case is a ZERO opcode with a len of 1.
     * We can just replace it with a VAL opcode with our value and len of 1. 
     * C) 另一种需要处理的情况是：一个ZERO操作码并且长度是1，
     * 那就直接把它替换成长度为1，值为计算值的VAL操作码
     */
    if (is_zero && runlen == 1) {
        HLL_SPARSE_VAL_SET(p,count,1);
        goto updated;
    }

    /* D) General case.
     * D) 普遍的情况

     * The other cases are more complex: our register requires to be updated
     * and is either currently represented by a VAL opcode with len > 1,
     * by a ZERO opcode with len > 1, or by an XZERO opcode.
     * 需要更新的是长度大于1的VAL操作码、长度大于1的ZERO操作码或者一个XZERO操作码
     *
     * In those cases the original opcode must be split into muliple
     * opcodes. The worst case is an XZERO split in the middle resuling into
     * XZERO - VAL - XZERO, so the resulting sequence max length is
     * 5 bytes.
     * 在这种情况下，原来的操作码必须分裂成多个操作码
     * 最糟糕的情况就是XZERO操作码分裂成XZERO-VAL-XZERO，这个操作需要最多5个字节
     *
     * We perform the split writing the new sequence into the 'new' buffer
     * with 'newlen' as length. Later the new sequence is inserted in place
     * of the old one, possibly moving what is on the right a few bytes
     * if the new sequence is longer than the older one.
     * 通过将新的操作码序列写到new buffer，长度是newlen
     * 之后新的操作码序列会插入到旧操作码的位置，可能将右侧的字节向后移动，
     * 因为新的操作码序列比原来的操作码序列要长
     *  */
    uint8_t seq[5], *n = seq;
    int last = first+span-1; /* Last register covered by the sequence. */
    int len;

    if (is_zero || is_xzero) {
        /* Handle splitting of ZERO / XZERO. */
        // 把ZERO、XZERO分裂开
        if (index != first) {
            len = index-first;
            // 如果新元素插入的位置index要远大于操作码的起始位置
            // 就是用XZERO
            if (len > HLL_SPARSE_ZERO_MAX_LEN) {
                HLL_SPARSE_XZERO_SET(n,len);
                n += 2;
            } else {
                HLL_SPARSE_ZERO_SET(n,len);
                n++;
            }
        }
        // 设置一个VAL 操作码，长度为1，值为count
        HLL_SPARSE_VAL_SET(n,count,1);
        n++;
        // 设置index后面的ZERO操作码或者XZERO操作码
        if (index != last) {
            len = last-index;
            if (len > HLL_SPARSE_ZERO_MAX_LEN) {
                HLL_SPARSE_XZERO_SET(n,len);
                n += 2;
            } else {
                HLL_SPARSE_ZERO_SET(n,len);
                n++;
            }
        }
    } else {
        /* Handle splitting of VAL. */
        // 分裂VAL操作码
        // 和上面的逻辑一致
        int curval = HLL_SPARSE_VAL_VALUE(p);

        if (index != first) {
            len = index-first;
            HLL_SPARSE_VAL_SET(n,curval,len);
            n++;
        }
        HLL_SPARSE_VAL_SET(n,count,1);
        n++;
        if (index != last) {
            len = last-index;
            HLL_SPARSE_VAL_SET(n,curval,len);
            n++;
        }
    }

    /* Step 3: substitute the new sequence with the old one.
     *
     * Note that we already allocated space on the sds string
     * calling sdsMakeRoomFor(). 
     * 第三步：使用新序列替换旧序列
     * 前提，已经有足够的空间了
     * */
     int seqlen = n-seq;
     int oldlen = is_xzero ? 2 : 1;
     int deltalen = seqlen-oldlen;

     if (deltalen > 0 &&
         sdslen(o->ptr)+deltalen > server.hll_sparse_max_bytes) goto promote;
     if (deltalen && next) memmove(next+deltalen,next,end-next);
     sdsIncrLen(o->ptr,deltalen);
     memcpy(p,seq,seqlen);
     end += deltalen;

updated:
    /* Step 4: Merge adjacent values if possible.
     *
     * The representation was updated, however the resulting representation
     * may not be optimal: adjacent VAL opcodes can sometimes be merged into
     * a single one. */
    p = prev ? prev : sparse;
    int scanlen = 5; /* Scan up to 5 upcodes starting from prev. */
    while (p < end && scanlen--) {
        if (HLL_SPARSE_IS_XZERO(p)) {
            p += 2;
            continue;
        } else if (HLL_SPARSE_IS_ZERO(p)) {
            p++;
            continue;
        }
        /* We need two adjacent VAL opcodes to try a merge, having
         * the same value, and a len that fits the VAL opcode max len. */
        if (p+1 < end && HLL_SPARSE_IS_VAL(p+1)) {
            int v1 = HLL_SPARSE_VAL_VALUE(p);
            int v2 = HLL_SPARSE_VAL_VALUE(p+1);
            if (v1 == v2) {
                int len = HLL_SPARSE_VAL_LEN(p)+HLL_SPARSE_VAL_LEN(p+1);
                if (len <= HLL_SPARSE_VAL_MAX_LEN) {
                    HLL_SPARSE_VAL_SET(p+1,v1,len);
                    memmove(p,p+1,end-p);
                    sdsIncrLen(o->ptr,-1);
                    end--;
                    /* After a merge we reiterate without incrementing 'p'
                     * in order to try to merge the just merged value with
                     * a value on its right. */
                    continue;
                }
            }
        }
        p++;
    }

    /* Invalidate the cached cardinality. */
    hdr = o->ptr;
    HLL_INVALIDATE_CACHE(hdr);
    return 1;

promote: /* Promote to dense representation. */
    if (hllSparseToDense(o) == REDIS_ERR) return -1; /* Corrupted HLL. */
    hdr = o->ptr;

    /* We need to call hllDenseAdd() to perform the operation after the
     * conversion. However the result must be 1, since if we need to
     * convert from sparse to dense a register requires to be updated.
     *
     * Note that this in turn means that PFADD will make sure the command
     * is propagated to slaves / AOF, so if there is a sparse -> dense
     * convertion, it will be performed in all the slaves as well. */
    int dense_retval = hllDenseAdd(hdr->registers, ele, elesize);
    redisAssert(dense_retval == 1);
    return dense_retval;
}

/* Compute SUM(2^-reg) in the sparse representation.
 * PE is an array with a pre-computer table of values 2^-reg indexed by reg.
 * As a side effect the integer pointed by 'ezp' is set to the number
 * of zero registers. 
 * 计算sparse形式的 SUM(2^-reg) 
 * */
double hllSparseSum(uint8_t *sparse, int sparselen, double *PE, int *ezp, int *invalid) {
    double E = 0;
    int ez = 0, idx = 0, runlen, regval;
    uint8_t *end = sparse+sparselen, *p = sparse;

    while(p < end) {
        if (HLL_SPARSE_IS_ZERO(p)) {
            runlen = HLL_SPARSE_ZERO_LEN(p);
            idx += runlen;
            ez += runlen;
            /* Increment E at the end of the loop. */
            p++;
        } else if (HLL_SPARSE_IS_XZERO(p)) {
            runlen = HLL_SPARSE_XZERO_LEN(p);
            idx += runlen;
            ez += runlen;
            /* Increment E at the end of the loop. */
            p += 2;
        } else {
            runlen = HLL_SPARSE_VAL_LEN(p);
            regval = HLL_SPARSE_VAL_VALUE(p);
            idx += runlen;
            E += PE[regval]*runlen;
            p++;
        }
    }
    if (idx != HLL_REGISTERS && invalid) *invalid = 1;
    E += ez; /* Add 2^0 'ez' times. */
    *ezp = ez;
    return E;
}

/* ========================= HyperLogLog Count ==============================
 * This is the core of the algorithm where the approximated count is computed.
 * The function uses the lower level hllDenseSum() and hllSparseSum() functions
 * as helpers to compute the SUM(2^-reg) part of the computation, which is
 * representation-specific, while all the rest is common. 
 * 这是进行基数统计的核心算法
 * 这个函数使用hllDenseSum() and hllSparseSum()辅助计算 SUM(2^-reg)的一部分
 * 剩下的就都是公共的
 * */

/* Implements the SUM operation for uint8_t data type which is only used
 * internally as speedup for PFCOUNT with multiple keys. */
double hllRawSum(uint8_t *registers, double *PE, int *ezp) {
    double E = 0;
    int j, ez = 0;
    uint64_t *word = (uint64_t*) registers;
    uint8_t *bytes;

    for (j = 0; j < HLL_REGISTERS/8; j++) {
        // 一次循环统计8个字节
        if (*word == 0) {
            ez += 8;
        } else {
            bytes = (uint8_t*) word;
            if (bytes[0]) E += PE[bytes[0]]; else ez++;
            if (bytes[1]) E += PE[bytes[1]]; else ez++;
            if (bytes[2]) E += PE[bytes[2]]; else ez++;
            if (bytes[3]) E += PE[bytes[3]]; else ez++;
            if (bytes[4]) E += PE[bytes[4]]; else ez++;
            if (bytes[5]) E += PE[bytes[5]]; else ez++;
            if (bytes[6]) E += PE[bytes[6]]; else ez++;
            if (bytes[7]) E += PE[bytes[7]]; else ez++;
        }
        word++;
    }
    E += ez; /* 2^(-reg[j]) is 1 when m is 0, add it 'ez' times for every
                zero register in the HLL. */
    *ezp = ez;
    return E;
}

/* Return the approximated cardinality of the set based on the harmonic
 * mean of the registers values. 'hdr' points to the start of the SDS
 * representing the String object holding the HLL representation.
 * 返回hll的近似基数统计值，基于寄存器值的调和平均数
 * hdr 指向持有hll 结构的String 对象
 *
 * If the sparse representation of the HLL object is not valid, the integer
 * pointed by 'invalid' is set to non-zero, otherwise it is left untouched.
 *
 * hllCount() supports a special internal-only encoding of HLL_RAW, that
 * is, hdr->registers will point to an uint8_t array of HLL_REGISTERS element.
 * This is useful in order to speedup PFCOUNT when called against multiple
 * keys (no need to work with 6-bit integers encoding). 
 * hllCount() 支持一种特殊的编码形式：HLL_RAW，
 * hdr->registers 指向一个uint8_t数组
 * 这种方式会加速PFCOUNT的计算，因为他使用的不是6位计数器了
 * */
uint64_t hllCount(struct hllhdr *hdr, int *invalid) {
    double m = HLL_REGISTERS;
    double E, alpha = 0.7213/(1+1.079/m);
    int j, ez; /* Number of registers equal to 0. */

    /* We precompute 2^(-reg[j]) in a small table in order to
     * speedup the computation of SUM(2^-register[0..i]). 
     * 提前计算了 2^(-reg[j]) 并储存在PE中
     * 用来加速后续 SUM(2^-register[0..i]) 的计算
     */
    static int initialized = 0;
    static double PE[64];
    // 使用initialized来初始化一次PE
    if (!initialized) {
        PE[0] = 1; /* 2^(-reg[j]) is 1 when m is 0. */
        for (j = 1; j < 64; j++) {
            /* 2^(-reg[j]) is the same as 1/2^reg[j]. */
            PE[j] = 1.0/(1ULL << j);
        }
        /**
         * PE[1] = 1/2
         * PE[2] = 1/4
         * PE[3] = 1/8
         * ...
         */
        initialized = 1;
    }

    /* Compute SUM(2^-register[0..i]). */
    if (hdr->encoding == HLL_DENSE) {
        E = hllDenseSum(hdr->registers,PE,&ez);
    } else if (hdr->encoding == HLL_SPARSE) {
        E = hllSparseSum(hdr->registers,
                         sdslen((sds)hdr)-HLL_HDR_SIZE,PE,&ez,invalid);
    } else if (hdr->encoding == HLL_RAW) {
        E = hllRawSum(hdr->registers,PE,&ez);
    } else {
        redisPanic("Unknown HyperLogLog encoding in hllCount()");
    }

    /* Muliply the inverse of E for alpha_m * m^2 to have the raw estimate. */
    // 粗略统计
    E = (1/E)*alpha*m*m;

    /* Use the LINEARCOUNTING algorithm for small cardinalities.
     * For larger values but up to 72000 HyperLogLog raw approximation is
     * used since linear counting error starts to increase. However HyperLogLog
     * shows a strong bias in the range 2.5*16384 - 72000, so we try to
     * compensate for it. 
     * 使用LINEARCOUNTING 算法统计基数小的情况，统计的ez不为0，说明有很多寄存器都是0
     * 对于更大的基数，低于72000，Hyperloglog使用线性统计的误差会增加，
     * HyperLogLog使用一个偏执bias来补偿误差
     * 
     * */
    if (E < m*2.5 && ez != 0) {
        E = m*log(m/ez); /* LINEARCOUNTING() */
    } else if (m == 16384 && E < 72000) {
        /* We did polynomial regression of the bias for this range, this
         * way we can compute the bias for a given cardinality and correct
         * according to it. Only apply the correction for P=14 that's what
         * we use and the value the correction was verified with. */
        double bias = 5.9119*1.0e-18*(E*E*E*E)
                      -1.4253*1.0e-12*(E*E*E)+
                      1.2940*1.0e-7*(E*E)
                      -5.2921*1.0e-3*E+
                      83.3216;
        E -= E*(bias/100);
    }
    /* We don't apply the correction for E > 1/30 of 2^32 since we use
     * a 64 bit function and 6 bit counters. To apply the correction for
     * 1/30 of 2^64 is not needed since it would require a huge set
     * to approach such a value. */
    return (uint64_t) E;
}

/* Call hllDenseAdd() or hllSparseAdd() according to the HLL encoding. */
int hllAdd(robj *o, unsigned char *ele, size_t elesize) {
    struct hllhdr *hdr = o->ptr;
    switch(hdr->encoding) {
    case HLL_DENSE: return hllDenseAdd(hdr->registers,ele,elesize);
    case HLL_SPARSE: return hllSparseAdd(o,ele,elesize);
    default: return -1; /* Invalid representation. */
    }
}

/* Merge by computing MAX(registers[i],hll[i]) the HyperLogLog 'hll'
 * with an array of uint8_t HLL_REGISTERS registers pointed by 'max'.
 *
 * The hll object must be already validated via isHLLObjectOrReply()
 * or in some other way.
 *
 * If the HyperLogLog is sparse and is found to be invalid, REDIS_ERR
 * is returned, otherwise the function always succeeds. */
int hllMerge(uint8_t *max, robj *hll) {
    struct hllhdr *hdr = hll->ptr;
    int i;

    if (hdr->encoding == HLL_DENSE) {
        uint8_t val;

        for (i = 0; i < HLL_REGISTERS; i++) {
            HLL_DENSE_GET_REGISTER(val,hdr->registers,i);
            if (val > max[i]) max[i] = val;
        }
    } else {
        uint8_t *p = hll->ptr, *end = p + sdslen(hll->ptr);
        long runlen, regval;

        p += HLL_HDR_SIZE;
        i = 0;
        while(p < end) {
            if (HLL_SPARSE_IS_ZERO(p)) {
                runlen = HLL_SPARSE_ZERO_LEN(p);
                i += runlen;
                p++;
            } else if (HLL_SPARSE_IS_XZERO(p)) {
                runlen = HLL_SPARSE_XZERO_LEN(p);
                i += runlen;
                p += 2;
            } else {
                runlen = HLL_SPARSE_VAL_LEN(p);
                regval = HLL_SPARSE_VAL_VALUE(p);
                while(runlen--) {
                    if (regval > max[i]) max[i] = regval;
                    i++;
                }
                p++;
            }
        }
        if (i != HLL_REGISTERS) return REDIS_ERR;
    }
    return REDIS_OK;
}

/* ========================== HyperLogLog commands ========================== */

/* Create an HLL object. We always create the HLL using sparse encoding.
 * This will be upgraded to the dense representation as needed. */
robj *createHLLObject(void) {
    robj *o;
    struct hllhdr *hdr;
    sds s;
    uint8_t *p;
    int sparselen = HLL_HDR_SIZE +
                    (((HLL_REGISTERS+(HLL_SPARSE_XZERO_MAX_LEN-1)) /
                     HLL_SPARSE_XZERO_MAX_LEN)*2);
    int aux;

    /* Populate the sparse representation with as many XZERO opcodes as
     * needed to represent all the registers. */
    aux = HLL_REGISTERS;
    s = sdsnewlen(NULL,sparselen);
    p = (uint8_t*)s + HLL_HDR_SIZE;
    // 全部设置成XZERO
    while(aux) {
        int xzero = HLL_SPARSE_XZERO_MAX_LEN;
        if (xzero > aux) xzero = aux;
        HLL_SPARSE_XZERO_SET(p,xzero);
        p += 2;
        aux -= xzero;
    }
    redisAssert((p-(uint8_t*)s) == sparselen);

    /* Create the actual object. */
    o = createObject(REDIS_STRING,s);
    hdr = o->ptr;
    memcpy(hdr->magic,"HYLL",4);
    hdr->encoding = HLL_SPARSE;
    return o;
}

/* Check if the object is a String with a valid HLL representation.
 * Return REDIS_OK if this is true, otherwise reply to the client
 * with an error and return REDIS_ERR. 
 * 检查传入的对象是不是包括HLL的string类型
 * */
int isHLLObjectOrReply(redisClient *c, robj *o) {
    struct hllhdr *hdr;

    /* Key exists, check type */
    if (checkType(c,o,REDIS_STRING))
        return REDIS_ERR; /* Error already sent. */

    if (stringObjectLen(o) < sizeof(*hdr)) goto invalid;
    hdr = o->ptr;

    /* Magic should be "HYLL". */
    if (hdr->magic[0] != 'H' || hdr->magic[1] != 'Y' ||
        hdr->magic[2] != 'L' || hdr->magic[3] != 'L') goto invalid;

    if (hdr->encoding > HLL_MAX_ENCODING) goto invalid;

    /* Dense representation string length should match exactly. */
    if (hdr->encoding == HLL_DENSE &&
        stringObjectLen(o) != HLL_DENSE_SIZE) goto invalid;

    /* All tests passed. */
    return REDIS_OK;

invalid:
    addReplySds(c,
        sdsnew("-WRONGTYPE Key is not a valid "
               "HyperLogLog string value.\r\n"));
    return REDIS_ERR;
}

/* PFADD var ele ele ele ... ele => :0 or :1 */
void pfaddCommand(redisClient *c) {
    // 进缓存
    robj *o = lookupKeyWrite(c->db,c->argv[1]);
    struct hllhdr *hdr;
    int updated = 0, j;

    if (o == NULL) {
        /* Create the key with a string value of the exact length to
         * hold our HLL data structure. sdsnewlen() when NULL is passed
         * is guaranteed to return bytes initialized to zero. */
        o = createHLLObject();
        dbAdd(c->db,c->argv[1],o);
        updated++;
    } else {
        if (isHLLObjectOrReply(c,o) != REDIS_OK) return;
        o = dbUnshareStringValue(c->db,c->argv[1],o);
    }
    /* Perform the low level ADD operation for every element. */
    for (j = 2; j < c->argc; j++) {
        int retval = hllAdd(o, (unsigned char*)c->argv[j]->ptr,
                               sdslen(c->argv[j]->ptr));
        switch(retval) {
        case 1:
            updated++;
            break;
        case -1:
            addReplySds(c,sdsnew(invalid_hll_err));
            return;
        }
    }
    hdr = o->ptr;
    if (updated) {
        signalModifiedKey(c->db,c->argv[1]);
        notifyKeyspaceEvent(REDIS_NOTIFY_STRING,"pfadd",c->argv[1],c->db->id);
        server.dirty++;
        HLL_INVALIDATE_CACHE(hdr);
    }
    addReply(c, updated ? shared.cone : shared.czero);
}

/* PFCOUNT var -> approximated cardinality of set. */
void pfcountCommand(redisClient *c) {
    robj *o;
    struct hllhdr *hdr;
    uint64_t card;

    /* Case 1: multi-key keys, cardinality of the union.
     *
     * When multiple keys are specified, PFCOUNT actually computes
     * the cardinality of the merge of the N HLLs specified. */
    if (c->argc > 2) {
        uint8_t max[HLL_HDR_SIZE+HLL_REGISTERS], *registers;
        int j;

        /* Compute an HLL with M[i] = MAX(M[i]_j). */
        memset(max,0,sizeof(max));
        hdr = (struct hllhdr*) max;
        hdr->encoding = HLL_RAW; /* Special internal-only encoding. */
        registers = max + HLL_HDR_SIZE;
        for (j = 1; j < c->argc; j++) {
            /* Check type and size. */
            robj *o = lookupKeyRead(c->db,c->argv[j]);
            if (o == NULL) continue; /* Assume empty HLL for non existing var.*/
            if (isHLLObjectOrReply(c,o) != REDIS_OK) return;

            /* Merge with this HLL with our 'max' HHL by setting max[i]
             * to MAX(max[i],hll[i]). */
            if (hllMerge(registers,o) == REDIS_ERR) {
                addReplySds(c,sdsnew(invalid_hll_err));
                return;
            }
        }

        /* Compute cardinality of the resulting set. */
        addReplyLongLong(c,hllCount(hdr,NULL));
        return;
    }

    /* Case 2: cardinality of the single HLL.
     *
     * The user specified a single key. Either return the cached value
     * or compute one and update the cache. */
    o = lookupKeyRead(c->db,c->argv[1]);
    if (o == NULL) {
        /* No key? Cardinality is zero since no element was added, otherwise
         * we would have a key as HLLADD creates it as a side effect. */
        addReply(c,shared.czero);
    } else {
        if (isHLLObjectOrReply(c,o) != REDIS_OK) return;
        o = dbUnshareStringValue(c->db,c->argv[1],o);

        /* Check if the cached cardinality is valid. */
        hdr = o->ptr;
        if (HLL_VALID_CACHE(hdr)) {
            /* Just return the cached value. */
            card = (uint64_t)hdr->card[0];
            card |= (uint64_t)hdr->card[1] << 8;
            card |= (uint64_t)hdr->card[2] << 16;
            card |= (uint64_t)hdr->card[3] << 24;
            card |= (uint64_t)hdr->card[4] << 32;
            card |= (uint64_t)hdr->card[5] << 40;
            card |= (uint64_t)hdr->card[6] << 48;
            card |= (uint64_t)hdr->card[7] << 56;
        } else {
            int invalid = 0;
            /* Recompute it and update the cached value. */
            card = hllCount(hdr,&invalid);
            if (invalid) {
                addReplySds(c,sdsnew(invalid_hll_err));
                return;
            }
            hdr->card[0] = card & 0xff;
            hdr->card[1] = (card >> 8) & 0xff;
            hdr->card[2] = (card >> 16) & 0xff;
            hdr->card[3] = (card >> 24) & 0xff;
            hdr->card[4] = (card >> 32) & 0xff;
            hdr->card[5] = (card >> 40) & 0xff;
            hdr->card[6] = (card >> 48) & 0xff;
            hdr->card[7] = (card >> 56) & 0xff;
            /* This is not considered a read-only command even if the
             * data structure is not modified, since the cached value
             * may be modified and given that the HLL is a Redis string
             * we need to propagate the change. */
            signalModifiedKey(c->db,c->argv[1]);
            server.dirty++;
        }
        addReplyLongLong(c,card);
    }
}

/* PFMERGE dest src1 src2 src3 ... srcN => OK */
void pfmergeCommand(redisClient *c) {
    uint8_t max[HLL_REGISTERS];
    struct hllhdr *hdr;
    int j;

    /* Compute an HLL with M[i] = MAX(M[i]_j).
     * We we the maximum into the max array of registers. We'll write
     * it to the target variable later. */
    memset(max,0,sizeof(max));
    for (j = 1; j < c->argc; j++) {
        /* Check type and size. */
        robj *o = lookupKeyRead(c->db,c->argv[j]);
        if (o == NULL) continue; /* Assume empty HLL for non existing var. */
        if (isHLLObjectOrReply(c,o) != REDIS_OK) return;

        /* Merge with this HLL with our 'max' HHL by setting max[i]
         * to MAX(max[i],hll[i]). */
        if (hllMerge(max,o) == REDIS_ERR) {
            addReplySds(c,sdsnew(invalid_hll_err));
            return;
        }
    }

    /* Create / unshare the destination key's value if needed. */
    robj *o = lookupKeyWrite(c->db,c->argv[1]);
    if (o == NULL) {
        /* Create the key with a string value of the exact length to
         * hold our HLL data structure. sdsnewlen() when NULL is passed
         * is guaranteed to return bytes initialized to zero. */
        o = createHLLObject();
        dbAdd(c->db,c->argv[1],o);
    } else {
        /* If key exists we are sure it's of the right type/size
         * since we checked when merging the different HLLs, so we
         * don't check again. */
        o = dbUnshareStringValue(c->db,c->argv[1],o);
    }

    /* Only support dense objects as destination. */
    if (hllSparseToDense(o) == REDIS_ERR) {
        addReplySds(c,sdsnew(invalid_hll_err));
        return;
    }

    /* Write the resulting HLL to the destination HLL registers and
     * invalidate the cached value. */
    hdr = o->ptr;
    for (j = 0; j < HLL_REGISTERS; j++) {
        HLL_DENSE_SET_REGISTER(hdr->registers,j,max[j]);
    }
    HLL_INVALIDATE_CACHE(hdr);

    signalModifiedKey(c->db,c->argv[1]);
    /* We generate an PFADD event for PFMERGE for semantical simplicity
     * since in theory this is a mass-add of elements. */
    notifyKeyspaceEvent(REDIS_NOTIFY_STRING,"pfadd",c->argv[1],c->db->id);
    server.dirty++;
    addReply(c,shared.ok);
}

/* ========================== Testing / Debugging  ========================== */

/* PFSELFTEST
 * This command performs a self-test of the HLL registers implementation.
 * Something that is not easy to test from within the outside. */
#define HLL_TEST_CYCLES 1000
void pfselftestCommand(redisClient *c) {
    unsigned int j, i;
    sds bitcounters = sdsnewlen(NULL,HLL_DENSE_SIZE);
    struct hllhdr *hdr = (struct hllhdr*) bitcounters, *hdr2;
    robj *o = NULL;
    uint8_t bytecounters[HLL_REGISTERS];

    /* Test 1: access registers.
     * The test is conceived to test that the different counters of our data
     * structure are accessible and that setting their values both result in
     * the correct value to be retained and not affect adjacent values. */
    for (j = 0; j < HLL_TEST_CYCLES; j++) {
        /* Set the HLL counters and an array of unsigned byes of the
         * same size to the same set of random values. */
        for (i = 0; i < HLL_REGISTERS; i++) {
            unsigned int r = rand() & HLL_REGISTER_MAX;

            bytecounters[i] = r;
            HLL_DENSE_SET_REGISTER(hdr->registers,i,r);
        }
        /* Check that we are able to retrieve the same values. */
        for (i = 0; i < HLL_REGISTERS; i++) {
            unsigned int val;

            HLL_DENSE_GET_REGISTER(val,hdr->registers,i);
            if (val != bytecounters[i]) {
                addReplyErrorFormat(c,
                    "TESTFAILED Register %d should be %d but is %d",
                    i, (int) bytecounters[i], (int) val);
                goto cleanup;
            }
        }
    }

    /* Test 2: approximation error.
     * The test adds unique elements and check that the estimated value
     * is always reasonable bounds.
     *
     * We check that the error is smaller than a few times than the expected
     * standard error, to make it very unlikely for the test to fail because
     * of a "bad" run.
     *
     * The test is performed with both dense and sparse HLLs at the same
     * time also verifying that the computed cardinality is the same. */
    memset(hdr->registers,0,HLL_DENSE_SIZE-HLL_HDR_SIZE);
    o = createHLLObject();
    double relerr = 1.04/sqrt(HLL_REGISTERS);
    int64_t checkpoint = 1;
    uint64_t seed = (uint64_t)rand() | (uint64_t)rand() << 32;
    uint64_t ele;
    for (j = 1; j <= 10000000; j++) {
        ele = j ^ seed;
        hllDenseAdd(hdr->registers,(unsigned char*)&ele,sizeof(ele));
        hllAdd(o,(unsigned char*)&ele,sizeof(ele));

        /* Make sure that for small cardinalities we use sparse
         * encoding. */
        if (j == checkpoint && j < server.hll_sparse_max_bytes/2) {
            hdr2 = o->ptr;
            if (hdr2->encoding != HLL_SPARSE) {
                addReplyError(c, "TESTFAILED sparse encoding not used");
                goto cleanup;
            }
        }

        /* Check that dense and sparse representations agree. */
        if (j == checkpoint && hllCount(hdr,NULL) != hllCount(o->ptr,NULL)) {
                addReplyError(c, "TESTFAILED dense/sparse disagree");
                goto cleanup;
        }

        /* Check error. */
        if (j == checkpoint) {
            int64_t abserr = checkpoint - (int64_t)hllCount(hdr,NULL);
            uint64_t maxerr = ceil(relerr*6*checkpoint);

            /* Adjust the max error we expect for cardinality 10
             * since from time to time it is statistically likely to get
             * much higher error due to collision, resulting into a false
             * positive. */
            if (j == 10) maxerr = 1;

            if (abserr < 0) abserr = -abserr;
            if (abserr > (int64_t)maxerr) {
                addReplyErrorFormat(c,
                    "TESTFAILED Too big error. card:%llu abserr:%llu",
                    (unsigned long long) checkpoint,
                    (unsigned long long) abserr);
                goto cleanup;
            }
            checkpoint *= 10;
        }
    }

    /* Success! */
    addReply(c,shared.ok);

cleanup:
    sdsfree(bitcounters);
    if (o) decrRefCount(o);
}

/* PFDEBUG <subcommand> <key> ... args ...
 * Different debugging related operations about the HLL implementation. */
void pfdebugCommand(redisClient *c) {
    char *cmd = c->argv[1]->ptr;
    struct hllhdr *hdr;
    robj *o;
    int j;

    o = lookupKeyRead(c->db,c->argv[2]);
    if (o == NULL) {
        addReplyError(c,"The specified key does not exist");
        return;
    }
    if (isHLLObjectOrReply(c,o) != REDIS_OK) return;
    o = dbUnshareStringValue(c->db,c->argv[2],o);
    hdr = o->ptr;

    /* PFDEBUG GETREG <key> */
    if (!strcasecmp(cmd,"getreg")) {
        if (c->argc != 3) goto arityerr;

        if (hdr->encoding == HLL_SPARSE) {
            if (hllSparseToDense(o) == REDIS_ERR) {
                addReplySds(c,sdsnew(invalid_hll_err));
                return;
            }
            server.dirty++; /* Force propagation on encoding change. */
        }

        hdr = o->ptr;
        addReplyMultiBulkLen(c,HLL_REGISTERS);
        for (j = 0; j < HLL_REGISTERS; j++) {
            uint8_t val;

            HLL_DENSE_GET_REGISTER(val,hdr->registers,j);
            addReplyLongLong(c,val);
        }
    }
    /* PFDEBUG DECODE <key> */
    else if (!strcasecmp(cmd,"decode")) {
        if (c->argc != 3) goto arityerr;

        uint8_t *p = o->ptr, *end = p+sdslen(o->ptr);
        sds decoded = sdsempty();

        if (hdr->encoding != HLL_SPARSE) {
            addReplyError(c,"HLL encoding is not sparse");
            return;
        }

        p += HLL_HDR_SIZE;
        while(p < end) {
            int runlen, regval;

            if (HLL_SPARSE_IS_ZERO(p)) {
                runlen = HLL_SPARSE_ZERO_LEN(p);
                p++;
                decoded = sdscatprintf(decoded,"z:%d ",runlen);
            } else if (HLL_SPARSE_IS_XZERO(p)) {
                runlen = HLL_SPARSE_XZERO_LEN(p);
                p += 2;
                decoded = sdscatprintf(decoded,"Z:%d ",runlen);
            } else {
                runlen = HLL_SPARSE_VAL_LEN(p);
                regval = HLL_SPARSE_VAL_VALUE(p);
                p++;
                decoded = sdscatprintf(decoded,"v:%d,%d ",regval,runlen);
            }
        }
        decoded = sdstrim(decoded," ");
        addReplyBulkCBuffer(c,decoded,sdslen(decoded));
        sdsfree(decoded);
    }
    /* PFDEBUG ENCODING <key> */
    else if (!strcasecmp(cmd,"encoding")) {
        char *encodingstr[2] = {"dense","sparse"};
        if (c->argc != 3) goto arityerr;

        addReplyStatus(c,encodingstr[hdr->encoding]);
    }
    /* PFDEBUG TODENSE <key> */
    else if (!strcasecmp(cmd,"todense")) {
        int conv = 0;
        if (c->argc != 3) goto arityerr;

        if (hdr->encoding == HLL_SPARSE) {
            if (hllSparseToDense(o) == REDIS_ERR) {
                addReplySds(c,sdsnew(invalid_hll_err));
                return;
            }
            conv = 1;
            server.dirty++; /* Force propagation on encoding change. */
        }
        addReply(c,conv ? shared.cone : shared.czero);
    } else {
        addReplyErrorFormat(c,"Unknown PFDEBUG subcommand '%s'", cmd);
    }
    return;

arityerr:
    addReplyErrorFormat(c,
        "Wrong number of arguments for the '%s' subcommand",cmd);
}

