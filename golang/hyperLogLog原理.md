#  1.  需求

现在有个需求，需要统一段时间内登陆的用户数量。这一段时间里可能先后登陆了用户a、b、c、b、c、a、d这么多人，可能有人重复登陆，我们在处理时就需要去重然后返回用户数量。

上面的情况就是基数统计：获取一个集合S去重之后的集合大小。

一种方法就是使用hashmap，hashmap的key是用户名，value是它登陆的次数。在64位机器中，golang在一条记录上使用24个字节。当用户成千上万乃至上亿的时候，需要的内存空间就非常大了。

为了节省内存空间，还有其他方法：$B^+$树、Bitmap等，Redis中使用HyperLogLog来进行粗略的统计，12k内存可以统计$2^{64}$个数据。

#  2.  伯努利实验

HyperLogLog的原理和伯努利实验有关。

投掷硬币，正面和反面出现的概率都是50%。那一直投掷硬币直到第t次投掷出现正面，这个过程就是一次伯努利实验。

如果进行n次伯努利实验，第1次实验，n=1，对应的投掷次数为$t_1$；第n次实验，对应的投掷次数为$t_n$。

进行完这么多次实验之后，肯定有一个最大的投掷次数$t_{max}$

1. n次伯努利实验的投掷次数都不大于 $t_{max}$
2. n次伯努利实验中，至少有一次投掷次数等于 $ t_{max}$

n和$ t_{max}$存在如下估算关系：
$$
n=2^{t_{max}}
$$
具体的推导我就不说了，概率论的相关内容。

通过多次伯努利实验的 $ t_{max}$可以推导得到进行的伯努利实验的次数n。那么如果把HyperLogLog的一次ADD当做一次伯努利实验，那么通过计算每次伯努利实验的最大投掷次数$ t_{max}$应该就可以求出HyperLogLog统计的元素数量n了。这就是HyperLogLog的基本原理。

HyperLogLog的第i次ADD的投掷次数$t_i$怎么计算呢？每次ADD的元素的hash值是一系列0和1组合的字节码，那么就可以通过统计从某个位置、某个方向开始第一个1所在的位置来计算$ t_i$。例如，0b1010 1000，从最低位向最高位计算得到$t=4$。

#  3.  HyperLogLog

HyperLogLog基于LogLogCounting等算法，它使用一个几乎均匀的hash函数获取需要统计的元素的hash值，然后通过分桶平均消除误差。HLL(之后均代表HyperLogLog)把hash值分成一个一个的桶，并且用hash值的前k个位来寻找它的桶位置，桶的数量表示成：
$$
m=2^k
$$
如下图，LSB表示最低位，MSB表示最高位，这个hash值表示为大端字节序。k=6，说明一共有64个桶。而下图的hash值表示的桶位置是0b00 1101=13。

![分桶](https://gitee.com/crazstom/pics/raw/master/img/%E5%88%86%E6%A1%B6.svg)

接下来计算上图hash值中后L-k的序列中第一个1出现的位置：6。因此在索引号为13的桶中进行后续操作，如果桶中的数字比6小就设置为6，否则就不变。

统计每个桶中储存的值的平均数，就可以计算得到估算的基数值。

HLL中使用调和平均数进行计算：
$$
H=\frac{n}{\frac{1}{x_1}+\frac{1}{x_2}+...+\frac{1}{x_n}}=\frac{n}{\sum_{i=1}^n\frac{1}{x_i}}
$$
它的基数估算公式就是：
$$
\hat{n}=\frac{\alpha_mm^2}{\sum_{i=0}^m2^{-M[i]}}
$$
其中，M[i]表示第i个桶中的数值，表示为该hash值下第一个1对应的最大位置。

还有：
$$
\alpha_m=(m\int_0^\infty(log_2(\frac{2+u}{1+u}))^mdu)^{-1}
$$
HLL的执行步骤可以通过这个[HLL模拟网站](http://content.research.neustar.biz/blog/hll.html)进行了解，建立大致的印象为后续的内容铺垫。

可以看出，HLL占用很少的内存来实现非常大的基数统计，相应的实现也必然很复杂，下一节就是分析其在redis中的实现。

#  4.  redis HLL实现

##  4.1.  HLL介绍

redis HLL代码：

> https://github.com/crazyStrome/redis_source_golang

redis使用register（寄存器）来表示hash定位的桶，其中的内容就是统计的hash值的L-k那一部分中第一个1的最大位置。下面是redis代码中的HLL介绍。

redis使用[1]中提出的64位hash函数，通过在每个寄存器或桶中增加1bit，将基数统计的上限提高到$10^9$以上。

> [1] Heule, Nunkesser, Hall: HyperLogLog in Practice: Algorithmic Engineering of a State of The Art Cardinality Estimation Algorithm.*

redis使用它的sds来储存HLL，并不创建新的数据类型。

redis不对[1]中的数据结构进行额外的压缩。redis使用[2]中的HyperLogLog的算法，唯一不同的是redis使用64位hash函数。

> [2] P. Flajolet, Éric Fusy, O. Gandouet, and F. Meunier. Hyperloglog: The analysis of a near-optimal cardinality estimation algorithm.*

redis在HLL中使用两种不同的数据储存形式：

1. dense形式。HLL的每个entry（每个寄存器中的内容）使用一个6位的数字表示。
2. sparse形式。当HLL中有很多寄存器为0时对这些寄存器进行压缩，以提高内存利用率。

###  4.1.1.  HLL header

redis使用hllhdr来持有一个HLL：

```golang
struct hllhdr {
    char magic[4];      /* "HYLL" */
    // 编码格式：sparse或者dense
    uint8_t encoding;   /* HLL_DENSE or HLL_SPARSE. */
    uint8_t notused[3]; /* Reserved for future use, must be zero. */
    // 缓存的基数统计值
    uint8_t card[8];    /* Cached cardinality, little endian. */
    // 实际的寄存器们，dense有16384个6bit寄存器
    uint8_t registers[]; /* Data bytes. */
};
```

dense和sparse形式都使用16字节的header。

* 前四个字节是HYLL，固定不变。
* encoding占用一个字节，表示HLL的编码形式：HLL_DENSE和HLL_SPARSE。
* notused占用三个字节，进行占位用的。
* card占用八个字节，用小端字节序储存的64位整数，保存最近的基数计算结果。如果从上一次基数计算到现在，数据结构都没有修改过，card中的内容可以重新使用。redis使用card最高位表示该数据是否可用，如果最高位是1，表明数据被修改了，就需要重新计算基数并缓存到card中。

registers表示这个HLL所持有的dense或sparse形式的数据。

###  4.1.2.  dense形式

HLL的dense编码形式使用的寄存器都是6bit，并且是连续排列。一个字节是8bit，因此一个字节会同时持有两个寄存器的部分或全部bit。

![dense](https://gitee.com/crazstom/pics/raw/master/img/dense.svg)

dense形式的寄存器都是从LSB到MSB进行编码，即从最低位到最高位。如果当前字节不够储存寄存器的剩余bit，就会根据需要使用下一个字节。

上图中，从左到右有三个字节，包含了四个寄存器0-3。第0个字节的后六位储存的是0号寄存器的内容，第0个字节的前两位和第1个字节的后四位储存的是1号寄存器的内容，以此类推，这就是dense的储存形式。

###  4.1.3.  sparse形式

HLL使用三种操作码实现对其数据结构的sparse编码。这三个操作码分别是ZERO、XZERO和VAL，其中两个操作码分别使用一个字节，另一个操作码使用两个字节。

下面介绍这三种操作码。

ZERO操作码占用一个字节，表示为00xxxxxx，后六位xxxxxx+1表示有N个连续的寄存器设置为0，这个操作码可以表示1-64个连续的寄存器被设置为0。

XZERO操作码占用两个字节，表示为01xxxxxx yyyyyyyy。xxxxxx是高位，yyyyyyyy是低位。这十四位+1表示有N个连续的寄存器设置为0.这个操作码可以表示0-16384个寄存器被设置为0。

VAL操作码占用一个字节，表示为1vvvvvxx。它包含一个5bit的vvvvv表示寄存器值，2bit的xx+1表示有这么多个连续的寄存器被设置为vvvvv。这个操作码表示可以表示1-4个寄存器被设置1-32的值。

sparse无法表示寄存器值超过32的寄存器，但是在HLL中超过32的寄存器值很少见。当没有这种情况时，sparse要比dense具有更高的内存效率，而如果有寄存器值超过32时，HLL会从sparse转换为dense。

sparse用来表示某个位置的寄存器内容，它是位置性的。例如，一个空HLL表示成01111111 11111111，这说明有16384个寄存器被设置为0，记作XZERO:16384。

再例如，一个HLL只有三个寄存器值不是0，位置分别是1000，1020，1021，值分别是2，3，3，这个HLL的sparse表示为：

> XZERO:1000 0-999号寄存器设置为0
>
> VAL:2,1 1个寄存器设置为2，即1000号寄存器
>
> ZERO:19 1001-1019号寄存器为0
>
> VAL:3,2 两个寄存器设置为3，分别是1020、1021号寄存器
>
> XZERO:15362 从1022-16383号寄存器设置为0

当基数比较小的时候，HLL有很高的的利用率。上面的例子中使用了7个字节表示所有的HLL寄存器，而dense使用12k的内存。

但是，sparse在进行2000-3000的基数计算时效率高，但是基数更大时，sparse转换成dense效率更高。通过定义server.hll_sparse_max_bytes实现sparse切换为dense时sparse的最大长度。

##  4.2.  redis HLL 实现代码

hllhdr定义：

```golang
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
```

###  4.2.1.  相关定义和宏

redis运行很快的原因就是使用了大量的宏定义。

前面说到，redis使用card作为缓存提高HLL的效率。而card是否可用通过其最高位来判断的，当最高位设置为1时，说明HLL被修改了，card的缓存不可用，需要重新计算。

```golang
/* The cached cardinality MSB is used to signal validity of the cached value. */
// card的第八位设置为1表示缓存不可用
#define HLL_INVALIDATE_CACHE(hdr) (hdr)->card[7] |= (1<<7)
#define HLL_VALID_CACHE(hdr) (((hdr)->card[7] & (1<<7)) == 0)
```

剩余的就是一些常用定义和宏。

```golang
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
```

###  4.2.2.  位运算的宏

redis使用一系列的宏简化HLL的dense和sparse形式的相关位运算。

#### 4.2.2.1. dense运算

HLL使用8bit的字节数组registers来储存dense的6bit寄存器组。因此需要把8bit数组分成6bit数组并进行取值和设置值的操作。redis使用宏来保证运行速度。

![dense](https://gitee.com/crazstom/pics/raw/master/img/dense.svg)

上图表示的是大端字节序，最高位(MSB)在左边。redis从最低位向最高位依次遍历。

(1)  取寄存器值

例如，获取在位置pos=1处的寄存器值，寄存器编号从0开始。

保存1号寄存器部分内容的第一个字节是b0：1100 0000。

```c
b0 = 6 * pos / 8
```

1号寄存器的第一位是这么计算的：

```c
fb = 6 * pos % 8 -> 6
```

把b0右移fb位：1100 0000 >> fb = 0000 0011

把b1左移8-fb位：2222 1111 << (8-fb) = 2211 1100

将这两个字节进行OR操作：0000 0011 | 2211 1100 = 2211 1111

然后将结果和0011 1111进行AND操作以消除高2位：2211 1111 & 0011 1111 = 0011 1111，这就是1号寄存器的值。

另一个例子，获取0号寄存器的内容。这个情况下，这个寄存器的六位都在一个字节b0中：1100 0000。

```c
b0 = 6 * pos / 8 = 0
```

0号寄存器的第一位的位置：

```c
fb = 6 * pos % 8 = 0
```

因此将当前字节b0右移0位：1100 0000 >> 0 = 1100 0000

下一字节b1左移8位：2222 1111 << 8 = 0000 0000

移动后的两个字节进行OR操作：1100 0000 | 0000 0000 = 1100 0000

然后将结果和0011 1111进行AND操作消除高2位：1100 0000 & 0011 1111 = 0000 0000

因此寄存器0的内容就是00 0000

(2)  设置寄存器值

设置寄存器的值就比较复杂了，假设val=0b 00cd efgh是需要设置的新值。需要两步，第一步清除寄存器的相关位，第二部通过或操作设置新的位。

例如，设置1号寄存器的值，它的第一个字节是b0。

这个例子中，fb=6。

为了生成一个AND掩码来清除b0的相关位，先生成一个值为63的初始掩码0b0011 1111。左移fb位，然后取反码：!(0011 1111 << fb) = 0011 1111。

让新掩码和b0进行AND操作以清除寄存器1的相关位：0011 1111 & 1100 0000 = 0000 0000

把val左移fb位，和上述结果进行OR操作就设置了b0的新值：b0 = (val << fb) OR b0 = (00cd efgh << 6) OR  0000 0000 = gh00 0000

接下来就是设置b1的相关位。b1的初始值为2222 1111。

使用63构建AND掩码，右移8-fb位，然后翻转：!(0011 1111 >> (8-fb)) = 1111 0000。

将新掩码和b1进行AND操作以清除相关位：b1 = 1111 0000 & 2222 1111 = 2222 0000

然后将val = 0b 00cd efgh右移8-fb位，然后和b1进行OR操作以设置相关位：b1 = (00cd efgh >> (8-fb)) | b1 = 2222 cdef

(3)  redis实现代码

redis的实现代码就很简单，有点C基础的都能看懂。

```c
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

```

#### 4.2.2.2. sparse运算

sparse形式就不需要查找对应的位了，但是需要区分它的操作码类型、操作码覆盖的寄存器范围、以及相应的操作码设置。

ZERO表示为00xx xxx，XZERO表示为01xx xxxx xxxx xxxx，VAL表示为1vvv vvxx。因此通过每个字节的前两位可以判断对应的操作码。

```c
#define HLL_SPARSE_XZERO_BIT 0x40 /* 01xxxxxx */
#define HLL_SPARSE_VAL_BIT 0x80 /* 1vvvvvxx */
#define HLL_SPARSE_IS_ZERO(p) (((*(p)) & 0xc0) == 0) /* 00xxxxxx */
#define HLL_SPARSE_IS_XZERO(p) (((*(p)) & 0xc0) == HLL_SPARSE_XZERO_BIT)
#define HLL_SPARSE_IS_VAL(p) ((*(p)) & HLL_SPARSE_VAL_BIT)
```

ZERO的后六位表示寄存器的长度，XZERO的后14位表示连续寄存器的长度，VAL的后两位是寄存器的长度，vvvvv是寄存器值，通过这些特征可以获得这三种操作码的相关信息。

```c
#define HLL_SPARSE_ZERO_LEN(p) (((*(p)) & 0x3f)+1)
#define HLL_SPARSE_XZERO_LEN(p) (((((*(p)) & 0x3f) << 8) | (*((p)+1)))+1)
#define HLL_SPARSE_VAL_VALUE(p) ((((*(p)) >> 2) & 0x1f)+1)
#define HLL_SPARSE_VAL_LEN(p) (((*(p)) & 0x3)+1)
```

VAL的操作码后两位是寄存器长度，中间5位vvvvv是寄存器值，因此设置VAL操作码的val和长度len的代码如下：

```c
#define HLL_SPARSE_VAL_SET(p,val,len) do { \
    *(p) = (((val)-1)<<2|((len)-1))|HLL_SPARSE_VAL_BIT; \
} while(0)
```

其余的设置ZERO、XZERO的长度也是这个原理：

```c
#define HLL_SPARSE_ZERO_SET(p,len) do { \
    *(p) = (len)-1; \
} while(0)
#define HLL_SPARSE_XZERO_SET(p,len) do { \
    int _l = (len)-1; \
    *(p) = (_l>>8) | HLL_SPARSE_XZERO_BIT; \
    *((p)+1) = (_l&0xff); \
} while(0)

```

###  4.2.3.  相关函数

HLL使用了一个通用的hash算法，在不同的计算机架构上都可以使用，这里不再叙述，本文只关注HLL的实现算法。

####  4.2.3.1.  ADD

HLL只有添加数据才能进行统计。redis实现了两种ADD，分别应用于dense、sparse。

(1)  hllPatLen

其中，这两种形式都需要一个函数来统计该数据hash值的第一个1的位置。

```c
int hllPatLen(unsigned char *ele, size_t elesize, long *regp) 
```

当客户端将一个string添加给HLL时，这个函数返回其部分hash子串的最长0綴，也是第一个1出现的位置，然后加1返回。

![分桶](https://gitee.com/crazstom/pics/raw/master/img/%E5%88%86%E6%A1%B6.svg)

具体如下步骤：

1. 计算添加HLL的string元素的64位hash值
2. 通过hash值和0b0011 1111进行AND操作以获得对应的桶索引，上图对应13号桶
3. 然后从第6位开始统计最长0綴：5，将其加一就是第一个1所在的位置

(2)  hllAdd

```c
/* Call hllDenseAdd() or hllSparseAdd() according to the HLL encoding. */
int hllAdd(robj *o, unsigned char *ele, size_t elesize)
```

所有要添加给HLL的数据都通过hllAdd这个函数进行。

这个函数通过o指向的hllhdr的编码形式不同，分别调用hllDenseAdd或hllSparseAdd。

```c
    switch(hdr->encoding) {
    case HLL_DENSE: return hllDenseAdd(hdr->registers,ele,elesize);
    case HLL_SPARSE: return hllSparseAdd(o,ele,elesize);
    default: return -1; /* Invalid representation. */
```

(3)  hllDenseAdd

```c
int hllDenseAdd(uint8_t *registers, unsigned char *ele, size_t elesize)
```

这个函数向HLL的dense形式寄存器添加元素ele。

参数registers是以sds的形式储存的，因此其长度包含16384个6bit寄存器加上sds的null终止符。

这个函数的逻辑就很简单了。

1. 使用hllPatLen获得ele的hash值对应的桶索引index和最长0綴count
2. 获取index位置寄存器的值oldcount
3. 如果oldcount < count，就将该位置寄存器设置为count，返回1；否则返回0

(4)  hllSparseAdd

```c
int hllSparseAdd(robj *o, unsigned char *ele, size_t elesize)
```

这个函数向HLL的sparse形式数据结构中添加元素ele。

实际上也不会把ele添加进去，而是把计算的最长0綴添加在HLL中。

参数o实际上是一个持有HLL的string对象，这个函数使用这样的对象是有好处的，就是可以在需要的时候扩展这个string，在它后面添加需要的字节。

在函数中，HLL可能会从sparse转换为dense形式储存数据。原因有两个：需要设置的寄存器值超过了sparse支持的范围；添加之后的sparse数据的大小超过了server.hll_sparse_max_bytes。

首先，计算新元素的索引index和最长0綴count。

如果count > 32，说明sparse无法使用了，转换为dense形式，然后调用hllDenseAdd并返回。

如果count <= 32，进行后续计算。

在添加数据时可能出现XZERO分别成XZERO-VAL-XZERO的情况，这也是最复杂的情况，因此使用sdsMakeRoomFor添加足够的空间。

1. 步骤一：定位需要修改的操作码

XZERO占用两个字节，ZERO和VAL占用一个字节；通过这些操作码中储存的长度与index比较以获得需要更新的操作码位置：*p。

2. 步骤二：在另一块内存中更新操作码

经过步骤一的计算后，变量first指的是p指向的当前操作码中包含的第一个寄存器索引位置。

变量next和prev分别保存后面和前面一个操作码。如果next是null说明p指向的操作码是最后一个；如果prev是null说明p指向的操作码是第一个。

变量span当前操作码覆盖的寄存器数量。

更新操作码具有不同的情况：

A)  如果当前操作码是VAL并且其值 >= count，那就不需要更新当前操作码，函数返回0表示没有数据更新

B)   如果当前操作码是VAL并且长度是1，说明当前操作码只覆盖一个寄存器，那直接更新这个操作码的值为count

C)  如果当前操作码是ZERO并且长度是1，那直接把这个操作码改成VAL并且更新值为count，长度为1

D)  剩下的就是更普遍的情况了。例如，当前操作码是长度大于1的VAL、长度大于1的ZERO或者一个XZERO。这种情况下，原来的操作码需要分裂成多个操作码。最复杂的就是XZERO分裂成XZERO-VAL-XZERO，这将需要5个字节储存。

redis将新的操作码序列写到一个额外的缓冲区new，长度是newlen。之后这个新序列会替换就序列。新序列可能会比旧序列长，因此在插入新序列时可能要将原操作码右侧的字节向后移动。

3. 步骤三：使用新序列替换旧操作码序列，具体看代码。
4. 步骤四：如果连续的操作码值相同的话，合并这些操作码。

hllSparseAdd的代码太长了，不适合在博客中贴出来，可以看我在文章开头提交到github的代码。

####  4.2.3.2.  count

(1)  hllCount

```c
uint64_t hllCount(struct hllhdr *hdr, int *invalid)
```

这个函数返回HLL的近似基数统计值，基于所有寄存器的调和平均数。如果这个HLL对象的sparse是无效的，invalid会设置为非零值。

hllCount支持一个特殊的内部编码：HLL_RAW。HLL_RAW使用8bit寄存器而不是HLL_DENSE的6bit寄存器，这样会加速PFCOUNT的计算。

在函数中，为了加速计算，redis提前构建了一个表PE，长度为64，$PE[i] = \frac{1}{2^i}$。这样之后就可以查表进行计算。

```c
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
```

接下来，根据HLL的编码形式选择hllDenseSum、hllSparseSum或hllRawSum计算$SUM(2^{-register[0..i]})$，记为E。

```c
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
```

根据E的范围采取不同的算法：当基数统计小于HLL桶的四分之一时使用LINEARCOUNTING算法；当基数统计更大时，通过一个bias变量调节计算误差。

```c
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
```

(2)  hllDenseSum

```c
double hllDenseSum(uint8_t *registers, double *PE, int *ezp)
```

这个函数是计算dense形式下各个寄存器的$SUM(2^{-register[0..i]})$。

最简单的就是通过之前的宏定义在16384次循环中获取每个寄存器的值，并通过传入的PE表计算总和。而在redis中，一次循环计算16个寄存器，循环1024次以加速计算。在计算时，PE表中的元素是double值，redis将每个寄存器值映射在PE表中的结果两两相加以消除误差。

```c
/* Additional parens will allow the compiler to optimize the
             * code more with a loss of precision that is not very relevant
             * here (floating point math is not commutative!). */
            // 两两相加减小误差
            E += (PE[r0] + PE[r1]) + (PE[r2] + PE[r3]) + (PE[r4] + PE[r5]) +
                 (PE[r6] + PE[r7]) + (PE[r8] + PE[r9]) + (PE[r10] + PE[r11]) +
                 (PE[r12] + PE[r13]) + (PE[r14] + PE[r15]);
```

(3)  hllSparseSum

```c
double hllSparseSum(uint8_t *sparse, int sparselen, double *PE, int *ezp, int *invalid) 
```

这个函数就是将sparse指向的每个字节进行识别，判断是哪一种操作码，然后通过查PE表计算总和返回。

(4)  hllRawSum

```c
double hllRawSum(uint8_t *registers, double *PE, int *ezp) 
```

这个函数和之前的思路一样，它使用8bit寄存器储存内容，并且在一次循环中计算8个寄存器的内容。

####  4.2.3.3.  sparse to dense

```c
int hllSparseToDense(robj *o) 
```

这个函数用来将sparse转换为dense，转换条件是：新元素的最长0綴 > 32或当前sparse形式的数据长度超过了server.hll_sparse_max_bytes。

HLL中，redis使用sds来储存sparse和dense形式的数据。

该函数的实现逻辑也比较简单，前提是本文之前关于dense、sparse以及各种操作码的内容都理解了。

1. 使用sdsnewlen分配HLL_DENSE_SIZE大小的内存dense，初始化index索引为0
2. 遍历参数o指向的sparse数据，并以此设置index指向的dense寄存器内容：
    * 如果是ZERO和XZERO，直接设置该操作码覆盖的寄存器为0，实际上也没有设置，直接跳过这些寄存器了，增加index相应的值。
    * 如果是VAL，取出该操作码的值和长度，将其覆盖的dense寄存器设置为该值，增加index。

3. 释放sparse数据占用的内存。

#  5.  总结

乍一开始可能觉得HyperLogLog好难，还要学概率，还有什么伯努利过程，但是它就是单纯的一个公式就解决了，耐心看下去还是很有收获的，下一步可以根据这些内容使用golang实现一个自己的HyperLogLog来加深理解。

