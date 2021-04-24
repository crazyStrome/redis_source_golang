package skiplist

import (
	"fmt"
	"testing"
	"unsafe"
)
func TestSize(m *testing.T) {
	var a string
	var b int
	fmt.Println(unsafe.Sizeof(a) + unsafe.Sizeof(b))
}
func TestCreation(t *testing.T) {
	fmt.Println("-----------TestCreation-----------")
	var sl = createSkiplist()
	printSkiplist(sl)
	sl.insert(20.0, "hello")
	printSkiplist(sl)
	sl.insert(20.3, "world")
	printSkiplist(sl)
	var s = "sjldajdlksajdlkajd"
	for i := range s {
		sl.insert(float64(i), string(s[i]))
	}
	//for i := range s {
	//	fmt.Println(sl.delete(float64(i), string(s[i])))
	//}
	fmt.Println(sl.delete(20.0, "hello"))
	printSkiplist(sl)
	//fmt.Println(20.0 == 20.0)

	// fmt.Println(sl)
	//fmt.Println(sl)
	//var n = createSkiplistNode(2, 1, "hi")
	//fmt.Println(n)
	//fmt.Println(equalObj("a", "a"))
}
