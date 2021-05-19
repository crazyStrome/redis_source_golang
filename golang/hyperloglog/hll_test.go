package hyperloglog

import (
	"fmt"
	"math"
	"os"
	"testing"
)

func TestHll(t *testing.T) {
	var a = []byte{'1', '2'}
	fmt.Println(a)
	var p = &a[0]
	//var hll = createHll()
	sparseSetXZERO(p, 10000)
	for i := range a {
		fmt.Printf("%b\n", a[i])
	}
}
func TestDenseAndSparse(t *testing.T) {
	//初始的hll的registers是两个字节的数组
	var hll = createHll()
	fmt.Printf("the hll is %+v\n", hll)

	// hll初始是sparse
	fmt.Printf("sparse in [%d] is XZERO: [%v]\n", 0, isSparseXZERO(&hll.registers[0]))
	fmt.Printf("sparse in [%d] xzero len is [%v]\n", 0, sparseGetXZERO(&hll.registers[0]))

	// 设置 0 号字节的ZERO和 1 号字节的VAL
	var a = []byte{'1', '2'}
	var p0 = &a[0]
	var p1 = &a[1]
	sparseSetZERO(p0, 30)
	fmt.Printf("after set ZERO, a is [%+v], p0 -> [%d], the code len is [%d]\n", a, *p0, sparseGetZERO(p0))
	sparseSetVAL(p1, 20, 3)
	var v = sparseGetVAL(p1)
	var s = sparseGetVALEN(p1)
	fmt.Printf("after set ZERO, a is [%+v], p1 -> [%d], the code val and size if [%d], [%d]\n", a, *p1, v, s)
}
func TestAddAndCount(t *testing.T) {
	var hll = createHll()
	var c uint64
	var i uint64 = 1
	var maxPrecision float64
	var f, _ = os.Create("data.txt")
	for i < math.MaxUint64 {
		err := hll.addElement(fmt.Sprint(i))
		if err != nil {
			fmt.Println("error:", err)
			return
		}
		var count = hll.hllCount()
		var delta uint64
		if i > count {
			delta = i-count
		} else {
			delta = count - i
		}
		var p = float64(delta)/float64(i)
		if p > maxPrecision {
			maxPrecision = p
		}
		fmt.Printf("i : [%d] count: [%d] delta: [%d] precision: [%f]\n", i, count, delta, p)
		f.Write([]byte(fmt.Sprintf("%d %d %d %f\n", i, count, delta, p)))
		if uint64(i) != count {
			c ++
		}
		i ++
		//time.Sleep(10 * time.Millisecond)
	}
	f.Close()
	fmt.Printf("total [%d] errors, max precision is [%f]\n", c, maxPrecision)
}
