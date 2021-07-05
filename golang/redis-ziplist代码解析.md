#  ziplist

##  1.  介绍

首先是官方介绍：

> The ziplist is a specially encoded dually linked list that is designed to be very memory efficient. It stores both strings and integer values, where integers are encoded as actual integers instead of a series of characters. It allows push and pop operations on either side of the list in O(1) time. However, because every operation requires a reallocation of the memory used by the ziplist, the actual complexity is related to the amount of memory used by the ziplist.

ziplist是一个经过特殊编码的双向链表，旨在提高内存效率。它可以储存字符串和整型值，其中，整型值被编码为实际整数而不是作为字符储存。ziplist可以在O(1)的时间内，在其左侧和右侧进行pop和push操作。但是，实际每个操作都需要重新分配ziplist使用的内存，因此实际的复杂性与ziplist使用的内存量有关。

###  1.1.  内存分布

每个ziplist占用的内存布局如下：

![zlmemory](https://gitee.com/crazstom/pics/raw/master/img/zlmemory.svg)

其中，

* zlbytes是一个无符号整数，表示当前ziplist占用的总字节数。
* zltail是ziplist最后一个entry的指针相对于ziplist最开始的偏移量。通过它，不需要完全遍历ziplist就可以对最后的entry进行操作。
* zllen是ziplist的entry数量。当zllen比2**16-2大时，需要完全遍历entry列表来获取entry的总数目。
* zlend是一个单字节的特殊值，等于255，标识着ziplist的内存结束点。

###  1.2.  entry

ziplist每个entry都有一个header，header中有两部分内容。第一部分内容是前一个entry的字节数prevrawlen。第二部分内容是该entry的编码encoding，如果是字符串编码，会附带储存其长度len。每一个entry中的结构如下，content为entry实际储存的数据，包括整型和字符串。

![entry](https://gitee.com/crazstom/pics/raw/master/img/entry.svg)

第一部分中，上一个entry的长度prevrawlen使用如下方式编码：

* 如果其长度小于254字节，entry使用一个字节储存
* 如果其长度大于等于254字节，当前entry使用5个字节。第一个字节设置为254，表示后面的四个字节储存的是上一个entry的长度

第二部分信息编码形式encoding取决于当前entry数据类型：

* 如果需要储存的数据是字符串，这部分信息的前两位设置为对应的编码，后续内容储存字符串的长度
* 如果需要储存的数据是整数，这部分信息的前两位设置为11，之后的两位用来确定整数类型。例如：

> |00pppppp| - 1 byte
>
> 长度小于等于63字节的string，后6位储存其长度
>
> |01pppppp|qqqqqqqq| - 2 bytes
>
> 长度小于等于16383字节的string，后14位储存其长度
>
> |10______|qqqqqqqq|rrrrrrrr|ssssssss|tttttttt| - 5 bytes
>
> 长度大于等于16384字节的string，后32位储存其长度
>
> |11000000| - 1 byte
>
> Integer encoded as int16_t (2 bytes).
>
> |11010000| - 1 byte
>
> Integer encoded as int32_t (4 bytes).
>
> |11100000| - 1 byte
>
> Integer encoded as int64_t (8 bytes).
>
> |11110000| - 1 byte
>
> Integer encoded as 24 bit signed (3 bytes).
>
> |11111110| - 1 byte
>
> Integer encoded as 8 bit signed (1 byte).
>
> |1111xxxx| - (with xxxx between 0000 and 1101) immediate 4 bit integer.
>
> xxxx这四位中，0000、1111、1110都不能使用，因此其编码后的值范围为1-13。
>
> |11111111| - End of ziplist.

##  2.  函数和宏实现

###  2.1.  宏定义

redis就是通过一堆宏定义来加速程序执行的。

####  2.1.1.  ZIP_IS_STR(enc)

通过entry中的编码enc来判断当前entry中持有的数据类型是不是string

```c
#define ZIP_IS_STR(enc) (((enc) & ZIP_STR_MASK) < ZIP_STR_MASK)
```

ZIP_STR_MASK是0b0011_0000，将它与enc进行AND操作，如果enc是string类型，那么其前两位应该是00、01或10，因此计算之后的数值应该小于ZIP_STR_MASK的。

####  2.1.2.  ZIPLIST_BYTES(zl)

这个宏就是把ziplist最开头的zlbytes提取出来，代码也很简单，通过指针取zl对应的值。

```c
#define ZIPLIST_BYTES(zl)       (*((uint32_t*)(zl)))
```

####  2.1.3.  ZIPLIST_TAIL_OFFSET(zl)

这个宏是提取ziplist的zltail内容，它的指针则是对zl偏移一个uint32_t大小(zlbytes的长度)获得的。

```c
#define ZIPLIST_TAIL_OFFSET(zl) (*((uint32_t*)((zl)+sizeof(uint32_t))))
```

####  2.1.4.  ZIPLIST_LENGTH(zl)

获取zllen的内容，其指针的获取同上，只不过需要偏移两个uint32_t的内存大小。

```c
#define ZIPLIST_LENGTH(zl)      (*((uint16_t*)((zl)+sizeof(uint32_t)*2)))
```

####  2.1.5.  ZIPLIST_ENTRY相关

redis使用三个宏来定位ziplist中entry的首尾位置。

首先计算了ziplist中第一个entry到ziplist开头的偏移地址，其实就是zlbytes、zltail和zllen占用的内存大小。

```c
#define ZIPLIST_HEADER_SIZE     (sizeof(uint32_t)*2+sizeof(uint16_t))
```

接下来通过宏获得ziplist第一个entry的指针地址。

```c
#define ZIPLIST_ENTRY_HEAD(zl)  ((zl)+ZIPLIST_HEADER_SIZE)
```

那么ziplist的最后一个entry的指针地址也可以通过2.1.3的宏获得。

```c
#define ZIPLIST_ENTRY_TAIL(zl)  ((zl)+intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl)))
```

entry列表的地址边界也可以获得，ziplist最后是一个字节的zlend，因此，zl偏移zlbytes-1就可以获得其指针了。

```c
#define ZIPLIST_ENTRY_END(zl)   ((zl)+intrev32ifbe(ZIPLIST_BYTES(zl))-1)
```

####  2.1.6.  ZIPLIST_INCR_LENGTH(zl,incr)

这个宏用来调整ziplist的entry数量，即zllen。因为ziplist每次只pop或push一个数据，因此这个宏的incr一般为1或-1。

代码如下，当ZIPLIST_LENGTH(zl)大于UINT16_MAX时，就已经不再更新zllen了，之后统计ziplist长度就需要进行遍历。

```c
#define ZIPLIST_INCR_LENGTH(zl,incr) { \
    if (ZIPLIST_LENGTH(zl) < UINT16_MAX) \
        ZIPLIST_LENGTH(zl) = intrev16ifbe(intrev16ifbe(ZIPLIST_LENGTH(zl))+incr); \
}
```

我当时迷糊了好久，为啥第三行代码ZIPLIST_LENGTH(zl)的结果可以直接赋值呢？我翻了好久也没翻出来，突然发现，ZIPLIST_LENGTH(zl)是宏定义。它在编译时是将定义的代码直接插入到第三行代码中的，这样就可以进行指针赋值了。

####  2.1.7.  ZIP_ENTRY_ENCODING(ptr, encoding)

这个宏是用来设置entry的encoding字段，具体entry的结构见后续章节。

代码如下：

```c
#define ZIP_ENTRY_ENCODING(ptr, encoding) do {  \
    (encoding) = (ptr[0]); \
    if ((encoding) < ZIP_STR_MASK) (encoding) &= ZIP_STR_MASK; \
} while(0)
```

代码中第三行，如果encoding小于ZIP_STR_MASK(0b1100_0000)，就通过AND操作将encoding后6位设置为0。

####  2.1.8.  ZIP_DECODE_LENGTH(*ptr*, *encoding*, *lensize*, *len*)

这个宏的实现目标为：根据encoding设置lensize，即len占用的字节数；根据ptr指向的数据设置len。

接下来介绍lensize和len的设置，具体编码参考1.2章的encoding格式：

* 如果encoding==ZIP_STR_06B(0b0000_0000)，它的lensize为1，len即为ptr[0]的后六位。
* 如果encoding==ZIP_STR_14B(0b0100_0000)，lensize为2，len为((ptr[0]&0x3f)<<8)|ptr[1]。
* 如果encoding==ZIP_STR_32B(0b1100_0000)，lensize为5，len为ptr[1]-ptr[4]组成的uint64类型整数。
* 否则的话encoding>0b1100_0000，说明其是数字，则调用zipIntSize(encoding)设置它的len，lensize为1.

```c
static unsigned int zipIntSize(unsigned char encoding) {
    switch(encoding) {
    case ZIP_INT_8B:  return 1;
    case ZIP_INT_16B: return 2;
    case ZIP_INT_24B: return 3;
    case ZIP_INT_32B: return 4;
    case ZIP_INT_64B: return 8;
    default: return 0; /* 4 bit immediate */
    }
    assert(NULL);
    return 0;
}
```

####  2.1.9.  ZIP_DECODE_PREVLENSIZE(*ptr*, *prevlensize*)和ZIP_DECODE_PREVLEN(*ptr*, *prevlensize*, *prevlen*)

在1.2章节中，entry会记录上一个entry的大小，如果超过254则使用5个字节来储存其长度，否则只使用1个字节就够了。因此，ZIP_DECODE_PREVLENSIZE(ptr,prevlensize)就是用来设置entry中的prevlensize字段。

```c
#define ZIP_DECODE_PREVLENSIZE(ptr, prevlensize) do {                          \
    if ((ptr)[0] < ZIP_BIGLEN) {                                               \
        (prevlensize) = 1;                                                     \
    } else {                                                                   \
        (prevlensize) = 5;                                                     \
    }                                                                          \
} while(0);
```

那ZIP_DECODE_PREVLEN(*ptr*, *prevlensize*, *prevlen*)就很简单了，就是把ptr[1]-ptr[4]的数据复制到prevlen。

```c
#define ZIP_DECODE_PREVLEN(ptr, prevlensize, prevlen) do {                     \
    ZIP_DECODE_PREVLENSIZE(ptr, prevlensize);                                  \
    if ((prevlensize) == 1) {                                                  \
        (prevlen) = (ptr)[0];                                                  \
    } else if ((prevlensize) == 5) {                                           \
        assert(sizeof((prevlensize)) == 4);                                    \
        memcpy(&(prevlen), ((char*)(ptr)) + 1, 4);                             \
        memrev32ifbe(&prevlen);                                                \
    }                                                                          \
} while(0);
```

###  2.2.  函数

ziplist的函数实现，从ziplist的创建、插入等操作开始叙述。

####  2.2.1.  char *ziplistNew(void)

顾名思义，这个函数就是产生一个新的ziplist。

代码：

```c
unsigned char *ziplistNew(void) {
    unsigned int bytes = ZIPLIST_HEADER_SIZE+1;
    unsigned char *zl = zmalloc(bytes);
    ZIPLIST_BYTES(zl) = intrev32ifbe(bytes);
    ZIPLIST_TAIL_OFFSET(zl) = intrev32ifbe(ZIPLIST_HEADER_SIZE);
    ZIPLIST_LENGTH(zl) = 0;
    zl[bytes-1] = ZIP_END;
    return zl;
}
```

见1.1章节ziplist的内存结构，ziplist初始时没有entry，因此其结构中只存在zlbytes、zltail、zllen和zlend四个，其中，zlbytes和zltail分别占用4个字节，zllen占用2个字节，zlend占用一个字节。因此代码中的bytes计算得11个字节。

分配完对应的内存后，使用上一章的宏定义设置ziplist的相关参数。

最后，把zl[bytes-1]即最后一个字节设置为0xff，即ZIP_END。

####  2.2.2.  zlentry zipEntry(unsigned char *p)

这个函数用来生成一个新的entry，entry的结构如下：

```c
typedef struct zlentry {
    // prevrawlen 表示上一个entry的大小、
    // prevrawlensize 表示prevrawlen占用的字节数
    unsigned int prevrawlensize, prevrawlen;
    // len 表示当前entry的长度
    // lensize 表示len占用的字节数
    unsigned int lensize, len;
    // headersize 表示entry的头部占用字节数
    // headersize = prevrawlensize + lensize
    unsigned int headersize;
    // encoding 表示当前entry使用的编码
    unsigned char encoding;
    // p 指向的数据是当前entry的实际内容，包括prevrawlen、encoding和content
    unsigned char *p;
} zlentry;
```

因此，zipEntry的实现也很简单，entry的prevrawlen和len都储存在p指向的地址中，使用2.1.9的相关宏进行设置。

上面的entry结构是便于同前端交互而实现的结构，实际ziplist中的entry中只有prevrawlen、len/encoding以及数据content组成。

####  2.2.3.  数据插入

ziplist支持从头部和尾部进行数据的插入，这两种方法都需要使用ziplistInsert实现。函数签名如下：

```c
unsigned char *__ziplistInsert(unsigned char *zl, unsigned char *p, unsigned char *s, unsigned int slen)
```

函数中zl就是实际的ziplist实体，p为需要插入的entry指针位置，s为需要插入的字符串，长度为slen。

实现逻辑如下：

1. 计算prevlensize和prevlen，如果p在zl的最后位置，则通过ptail进行计算，否则直接通过p就可以计算得到。

```c
    if (p[0] != ZIP_END) {
        // 如果 p 不是 zl 的最后位置，
        // 新数据是插在 p 后面的，因此通过 p 可以获得
        // prevlensize 和 prevlen
        ZIP_DECODE_PREVLEN(p, prevlensize, prevlen);
    } else {
        // p 是 zl 的最后位置
        // 新数据插入在ptail后面
        // 那么需要获取ptail的长度给prevlen
        unsigned char *ptail = ZIPLIST_ENTRY_TAIL(zl);
        if (ptail[0] != ZIP_END) {
            prevlen = zipRawEntryLength(ptail);
        }
    }
```

2. 使用zipTryEncoding(s,slen,&value,&encoding)判断能否将s字符串转换为数字，并设置相应的encoding；如果无法转换，ziplist将使用字符串编码方式进行储存。
3. 使用ziplistResize(zl,curlen+reqlen+nextdiff)增加zl的内存，并进行内存移动操作，留出可以放置新entry的空间。
4. 新entry下一跳entry的prevlen需要进行调整，在1.2章节中，prevrawlen可能为1或5个字节。在本节中，如果新entry的长度len一个字节就够编码，而下一跳entry的prerawlen之前使用5个字节编码，那么就需要进行内存的调整。具体见__ziplistCascadeUpdate(zl,p+reqlen)的代码实现。
5. 根据需要插入的数据类型（string或整数）添加到ziplist中，并增加zllen。

####  2.2.4.  查找

函数签名如下，p为需要查找的entry列表起点，vstr为需要查找的数据，长度为vlen，每次查找时跳过skip个entry进行匹配。例如，skip=2，第一次比较entry0，那么entry1、entry2都会跳过不进行比较，下一次直接比较entry3。

```c
unsigned char *ziplistFind(unsigned char *p, unsigned char *vstr, unsigned int vlen, unsigned int skip)
```

这个函数的实现思路为，从第一个entry依次遍历到最后一个entry，即p[0]==ZIP_END。

1. 通过宏定义获取p指向entry的prevlensize、lensize和len。
2. q=p+prevlensize+lensize，得到当前entry的数据所在地址。
3. 根据encoding，进行分别比较。如果为string，则使用memcmp比较，否则使用zipTryEncoding将vstr转为整数进行比较。
4. 在循环中，使用skipcnt进行跳过。
5. 将p=q+len，指向下一个entry。

####  2.2.5.  遍历

在对ziplist进行遍历时需要获取下一个entry的地址，遍历分前向和后向遍历，因此，ziplist也有两种实现方式。

```c
unsigned char *ziplistNext(unsigned char *zl, unsigned char *p)
unsigned char *ziplistPrev(unsigned char *zl, unsigned char *p)
```

在ziplistNext中，如果p已经遍历到zl的最后位置ZIP_END，下一个entry就是NULL；否则的话使用zipRawEntryLength获取p的entry占用字节数，然后将p向后移动相应的字节。如果移动后p指向ZIP_END，则返回NULL。

在ziplistPrev中，如果p指向ZIP_END，那么p的上一个entry就是zltail指向的entry，将其返回即可；如果p是zl的第一个entry，那么返回NULL；否则的话使用ZIP_DECODE_PREVLEN计算当前entry中的prevlen，将p-prevlen并返回。

####  2.2.6.  删除

函数签名如下，删除从p开始的num个entry，并返回删除后的ziplist。

```c
unsigned char *__ziplistDelete(unsigned char *zl, unsigned char *p, unsigned int num)
```

实现思路如下：

1. 计算需要删除的entry占用的内存字节数totlen。
2. 如果totlen为0，直接返回zl
3. 如果totlen>0，说明需要进行内存的移动。
4. 删除entry时，第num+1个entry的prevrawlen就需要重新调整，可能从1字节变为5字节，因此需要使用zipPrevLenByteDiff函数判断需要调整的字节数nextdiff，然后p向前移动nextdiff个字节数。
5. 重新设置zlbytes、zllen、zltail等参数，返回zl

####  2.2.7.  entry数量

获取zl中entry的数量就比较简单了。如果zllen小于UINT16_MAX，直接就可以返回zllen；否则的话需要从第一个entry遍历到最后一个entry进行统计。代码就不贴了，可以看源代码中的ziplistLen(unsigned char *zl)函数。

##  总结

ziplist当数据量比较大的时候，其查找还是很消耗资源的，需要进行完全遍历，时间复杂度最坏情况为O(n)；它的插入和删除的时间复杂度也并不一直是O(1)。因此在使用时也需要配合其他数据结构使用。

刚开始接触ziplist的时候不知道它的具体原理是什么，但是只要了解ziplist的内存分布以及entry的内存分布，它的相关函数实现逻辑基本就了然于胸了，不过具体的细节还是需要阅读源码来提高具体认识的。

