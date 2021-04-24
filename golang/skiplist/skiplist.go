package skiplist

import (
	"fmt"
	"math/rand"
	"reflect"
)

// SKIPLIST_MAXLEVEL 表示跳表索引的最多层数
const SKIPLIST_MAXLEVEL = 32
// SKIPLIST_P 表示索引层数上升的概率
const SKIPLIST_P = 0.25
// skiplistLevel 表示skiplist每一层持有的数据结构
type skiplistLevel struct {
	// 该层节点的下一个节点，redis使用forward
	next *skiplistNode
	// 该层节点到下一节点中间间隔的跳数
	span int
}
// skiplistNode 表示skiplist的每一个节点
type skiplistNode struct {
	// robj 代表该节点的数据
	robj interface{}
	// score 表示该节点的分数，以便排序
	score float64
	// prev 表示该节点的上一节点，redis 中使用backward
	prev *skiplistNode
	// levels 表示该节点在每一层中到下一节点的数据
	levels []skiplistLevel
}
// skiplist 持有一个跳表的完整数据
type skiplist struct {
	// header和tail表示跳表的头结点和尾节点
	header, tail *skiplistNode
	// length 表示跳表的长度
	length int
	// level 表示该跳表索引的层数
	level int
}

func createSkiplist() *skiplist {
	var sl = new(skiplist)
	sl.header = nil
	sl.tail = nil
	sl.length = 0
	sl.level = 1
	sl.header = createSkiplistNode(SKIPLIST_MAXLEVEL, 0, nil)
	// 设置header节点每一层索引的初始数据
	for i := 0; i < SKIPLIST_MAXLEVEL; i ++ {
		sl.header.levels[i].next = nil
		sl.header.levels[i].span = 0
	}
	sl.header.prev = nil
	sl.tail = nil
	return sl
}
func createSkiplistNode(level int, score float64, obj interface{}) *skiplistNode {
	var sln = new(skiplistNode)
	//sln.prev = nil
	sln.score = score
	sln.robj = obj
	sln.levels = make([]skiplistLevel, level)
	for i := 0; i < level; i ++ {
		sln.levels[i] = skiplistLevel{}
	}
	return sln
}
func (sl *skiplist) insert(score float64, robj interface{}) *skiplistNode {
	var update = make([]*skiplistNode, SKIPLIST_MAXLEVEL)
	var x *skiplistNode
	var rank = make([]int, SKIPLIST_MAXLEVEL)

	x = sl.header
	for i := sl.level-1; i >= 0; i -- {
		if i == sl.level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}
		// 寻找比score和robj小的最近节点
		for x.levels[i].next != nil && (x.levels[i].next.score < score ||
					(x.levels[i].next.score == score && compareObj(robj, x.levels[i].next.robj))) {
			rank[i] += x.levels[i].span
			x = x.levels[i].next
		}
		update[i] = x
	}
	var level = randomLevel()
	if level > sl.level {
		for i := sl.level; i < level; i ++ {
			rank[i] = 0
			update[i] = sl.header
			update[i].levels[i].span = sl.length
		}
		sl.level = level
	}
	x = createSkiplistNode(level, score, robj)
	for i := 0; i < level; i ++ {
		x.levels[i].next = update[i].levels[i].next
		update[i].levels[i].next = x

		x.levels[i].span = update[i].levels[i].span - (rank[0]-rank[i])
		update[i].levels[i].span = rank[0] - rank[i] + 1
	}

	for i := level-1; i < sl.level; i ++ {
		update[i].levels[i].span ++
	}

	// 如果当前节点是插入的第一个节点，它的prev是nil
	if update[0] == sl.header {
		x.prev = nil
	} else {
		x.prev = update[0]
	}
	if x.levels[0].next != nil {
		x.levels[0].next.prev = x
	} else {
		sl.tail = x
	}
	sl.length ++
	return x
}
// delete 删除跳表中的数据,return true表示删除成功，false表示没有该节点
func (sl *skiplist) delete(score float64, robj interface{}) bool {
	var update = make([]*skiplistNode, SKIPLIST_MAXLEVEL)
	var x *skiplistNode

	// 查找最近节点
	x = sl.header
	for i := sl.level-1; i >= 0; i -- {
		for x.levels[i].next != nil && (x.levels[i].next.score < score || 
			(x.levels[i].next.score == score && compareObj(robj, x.levels[i].next.robj))) {
			x = x.levels[i].next
		}
		update[i] = x
	}
	// x之后的节点可能是需要删除的节点，也可能不是
	x = x.levels[0].next
	if x != nil && x.score == score && equalObj(x.robj, robj) {
		sl.deleteNode(update, x)
		return true
	}
	// 没有找到该节点
	return false
}
// deleteNode 删除该节点
func (sl *skiplist) deleteNode(update []*skiplistNode, x *skiplistNode) {
	for i := 0; i < sl.level; i ++ {
		if update[i].levels[i].next == x {
			update[i].levels[i].span += x.levels[i].span - 1
			update[i].levels[i].next = x.levels[i].next
		} else {
			// x的level比当前level i要小
			update[i].levels[i].span --
		}
	}
	// 设置后续节点
	if x.levels[0].next != nil {
		x.levels[0].next.prev = x.prev
	} else {
		sl.tail = x.prev
	}
	// 更新level值
	for sl.level > 1 && sl.header.levels[sl.level-1].next == nil {
		sl.level --
	}
	sl.length --
}
// equalObj 判断两个对象是否相同
func equalObj(obj1, obj2 interface{}) bool {
	return (!compareObj(obj1, obj2)) && (!compareObj(obj2, obj1))
}
// compareObj 如果obj1>obj2，返回true
func compareObj(obj1, obj2 interface{}) bool {
	var t1, t2 reflect.Type
	t1 = reflect.TypeOf(obj1)
	t2 = reflect.TypeOf(obj2)
	if t1.Kind() != t2.Kind() {
		compareObj(fmt.Sprint(obj1), fmt.Sprint(obj2))
	}
	var v1, v2 reflect.Value
	v1 = reflect.ValueOf(obj1)
	v2 = reflect.ValueOf(obj2)
	switch t1.Kind() {
	case reflect.Int:
		return v1.Int() > v2.Int()
	case reflect.Float64, reflect.Float32:
		return v1.Float() > v2.Float()
	case reflect.String:
		return v1.String() > v2.String()
	}
	return false
}
func randomLevel() int {
	var level = 1
	for rand.Float64() < SKIPLIST_P {
		level ++
	}
	if level < SKIPLIST_MAXLEVEL {
		return level
	}
	return SKIPLIST_MAXLEVEL
}
func printSkiplist(sl *skiplist) {
	var x = sl.header
	fmt.Printf("Skiplist %+v has [%d] level, [%d] length\n", sl, sl.level, sl.length)

	for x.levels[0].next != nil {
		//var prev = x
		x = x.levels[0].next
		fmt.Printf("%v, %v, level: [%d]\n", x.robj, x.score, len(x.levels))
	}
}