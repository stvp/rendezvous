package rendezvous

import (
	"fmt"
	"reflect"
	"testing"
)

var (
	sampleKeys = []string{
		"d25ffecc2a229597f0043ef41aba2705",
		"0aaa9660fb2a7c515014d08b8b9907bb",
		"1c255a812ae7a2de0065a1660085bc28",
		"155b444d7dd18006b78db5ff97b058a6",
		"0f123cdaccae0339440b4736f6d73398",
		"d3c7407ca7615be760892164e6c0569a",
		"f84fb057568f83da880d29bdba4dc44a",
		"095b3e933521283f169a9e7e8c3cb933",
		"700e8a771299f2aa6ec19425b0955d0a",
		"082de10c8d63b47c4ee869eaf3ffb76b",
		"558757456b971d9b298e55f1f6c73679",
		"356ceba27adc65e6fba525e728db1186",
		"c5d85597adabe7be75b1a73e692e471f",
		"ce54de057d499bf644744868cb73d97e",
		"e94b1dc17a55f49c3eb4426c21e36ce0",
		"1b82c951b447795493d337afea4fa2ce",
		"08877a49a5bf24e0f5f4a76f838f6039",
		"eb11c41eb0ddd5e1c5b65f86bb75aa23",
		"0cfe4adfa3082edbddfcc20aa5056f79",
		"50c23fd0fd4765e809c5a1ff6094cf3d",
	}
)

func TestGet(t *testing.T) {
	table := New([]string{"a", "b", "c", "d", "e"})

	testcases := []struct {
		key          string
		expectedNode string
	}{
		{"", "a"},
		{"foo", "e"},
		{"bar", "b"},
	}

	for _, testcase := range testcases {
		gotNode := table.Get(testcase.key)
		if gotNode != testcase.expectedNode {
			t.Errorf("got: %#v, expected: %#v", gotNode, testcase.expectedNode)
		}
	}
}

func BenchmarkGet5nodes(b *testing.B) {
	table := New([]string{"a", "b", "c", "d", "e"})
	for i := 0; i < b.N; i++ {
		table.Get(sampleKeys[i%len(sampleKeys)])
	}
}

func BenchmarkGet10nodes(b *testing.B) {
	table := New([]string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"})
	for i := 0; i < b.N; i++ {
		table.Get(sampleKeys[i%len(sampleKeys)])
	}
}

func TestGetN(t *testing.T) {
	table := New([]string{"a", "b", "c", "d", "e"})

	testcases := []struct {
		count         int
		key           string
		expectedNodes []string
	}{
		{1, "foo", []string{"e"}},
		{2, "bar", []string{"b", "d"}},
		{3, "baz", []string{"d", "a", "b"}},
		{2, "biz", []string{"e", "c"}},
		{0, "boz", []string{}},
		{100, "floo", []string{"c", "b", "a", "d", "e"}},
	}

	for _, testcase := range testcases {
		gotNodes := table.GetN(testcase.count, testcase.key)
		if !reflect.DeepEqual(gotNodes, testcase.expectedNodes) {
			t.Errorf("got: %#v, expected: %#v", gotNodes, testcase.expectedNodes)
		}
	}
}

func TestDistribution(t *testing.T) {
	table := New([]string{"a", "b", "c", "d", "e"})
	got := map[string]int{"a": 0, "b": 0, "c": 0, "d": 0, "e": 0}
	for _, key := range sampleKeys {
		for i := 999; i < 1192; i++ {
			k := fmt.Sprintf("/%d/%s", i, key)
			slot := table.Get(k)
			got[slot] = got[slot] + 1
		}
	}
	t.Logf("%#v\n", got)
}

func BenchmarkGetN3_5nodes(b *testing.B) {
	table := New([]string{"a", "b", "c", "d", "e"})
	for i := 0; i < b.N; i++ {
		table.GetN(3, sampleKeys[i%len(sampleKeys)])
	}
}

func BenchmarkGetN5_5nodes(b *testing.B) {
	table := New([]string{"a", "b", "c", "d", "e"})
	for i := 0; i < b.N; i++ {
		table.GetN(5, sampleKeys[i%len(sampleKeys)])
	}
}

func BenchmarkGetN3_10nodes(b *testing.B) {
	table := New([]string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"})
	for i := 0; i < b.N; i++ {
		table.GetN(3, sampleKeys[i%len(sampleKeys)])
	}
}

func BenchmarkGetN5_10nodes(b *testing.B) {
	table := New([]string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"})
	for i := 0; i < b.N; i++ {
		table.GetN(5, sampleKeys[i%len(sampleKeys)])
	}
}
