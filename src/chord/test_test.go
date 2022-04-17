package chord

import "math"
import "math/rand"


func TestChordBasic() {
	m := 4
	n_nodes := 5
	a := make([]int, math.Pow(2, m))
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(a), func(i, j int) { a[i], a[j] = a[j], a[i] })
	ids := a[:n_nodes]

	chord_nodes := Make([]&Node, n_nodes)
	for i := 0; i < n_nodes; i++ {
		if i == 0 {
"127.0.0.1:" + port
		}
	}
}
