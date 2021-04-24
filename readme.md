##  内容

这个文件夹下包含redis的源码以及[golang代码实现](./golang)

redis代码阅读顺序参考[如何阅读redis源码-知乎](https://www.zhihu.com/question/28677076)

目前阅读的redis代码有：

* 内存分配[zmalloc.c](./zmalloc.c)和[zmalloc.h](./zmalloc.h)

* 动态字符串[sds.h](./sds.h)和[sds.c](./sds.c)

* 双端链表[adlist.c](./adlist.c)和[adlist.h](./adlist.h)

* 字典[dict.h](./dict.h)和[dict.c](./dict.c)

* 跳跃表[server.h](./server.h)文件里面关于zskiplist结构和zskiplistNode结构，以及[t_zset.c](./t_zset.c)中所有zsl开头的函数，比如 zslCreate、zslInsert、zslDeleteNode等等
* 基数统计[hyperloglog.c ](./hyperloglog.c )中的 hllhdr 结构， 以及所有以 hll 开头的函数