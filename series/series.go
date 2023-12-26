package series

import (
	"fmt"
	"gonum.org/v1/gonum/stat"
	"math"
	"reflect"
	"sort"
	"strings"
)

// Series 是一个用于操作符合特定类型结构的元素数组的数据结构。它们足够灵活，可以转换为其他 Series 类型，并考虑缺失或非有效元素。Series 的强大之处主要在于能够比较和子集化不同类型的 Series。

type Series struct {
	Name     string   // Series 的名称
	elements Elements // 元素的值
	t        Type     // Series 的类型

	// deprecated: use Error() instead
	Err error
}

// Elements 是表示 Series 中元素数组的接口。
type Elements interface {
	Elem(int) Element
	Len() int
}

// Element 是为 Series 的元素定义方法的接口。
type Element interface {
	// Set Setter 方法
	Set(interface{})

	// Eq 比较方法
	Eq(Element) bool
	Neq(Element) bool
	Less(Element) bool
	LessEq(Element) bool
	Greater(Element) bool
	GreaterEq(Element) bool

	// Copy 访问器/转换方法
	Copy() Element
	Val() ElementValue
	String() string
	Int() (int, error)
	Float() float64
	Bool() (bool, error)

	// IsNA 信息方法
	IsNA() bool
	Type() Type
}

// intElements 是 Int 类型元素的具体实现。
type intElements []intElement

func (e intElements) Len() int           { return len(e) }
func (e intElements) Elem(i int) Element { return &e[i] }

// stringElements 是 String 类型元素的具体实现。
type stringElements []stringElement

func (e stringElements) Len() int           { return len(e) }
func (e stringElements) Elem(i int) Element { return &e[i] }

// floatElements 是 Float 类型元素的具体实现。
type floatElements []floatElement

func (e floatElements) Len() int           { return len(e) }
func (e floatElements) Elem(i int) Element { return &e[i] }

// boolElements 是 Bool 类型元素的具体实现。
type boolElements []boolElement

func (e boolElements) Len() int           { return len(e) }
func (e boolElements) Elem(i int) Element { return &e[i] }

// ElementValue 表示可用于编组或解组 Elements 的值。
type ElementValue interface{}

// MapFunction 定义了一个映射函数的签名，允许进行相当灵活的 MAP 实现，用于在 Series 中映射函数到每个元素并返回一个新的 Series 对象。该函数必须与 Series 中的数据底层类型兼容。换句话说，当使用 Float 类型的 Series 时，通过参数 `f` 传递的函数不应期望其他类型，而应期望处理类型为 Float 的 Element(s)。
type MapFunction func(Element) Element

// Comparator 是一种更具类型安全性的用于比较的方便别名。
type Comparator string

// 支持的比较器
const (
	Eq        Comparator = "=="   // 等于
	Neq       Comparator = "!="   // 不等于
	Greater   Comparator = ">"    // 大于
	GreaterEq Comparator = ">="   // 大于等于
	Less      Comparator = "<"    // 小于
	LessEq    Comparator = "<="   // 小于等于
	In        Comparator = "in"   // 包含
	CompFunc  Comparator = "func" // 用户定义的比较函数
)

// compFunc 定义了用户定义的比较函数。在内部用于类型断言。
type compFunc = func(el Element) bool

// Type 是一种更具类型安全性的用于表示 Series 类型的别名。
type Type string

// 支持的 Series 类型
const (
	String Type = "string"
	Int    Type = "int"
	Float  Type = "float"
	Bool   Type = "bool"
)

// Indexes 表示可用于选择 Series 子集元素的元素。目前支持以下类型：
//
//	int            // 匹配给定索引号
//	[]int          // 匹配所有给定索引号
//	[]bool         // 匹配标记为 true 的 Series 中的所有元素
//	Series [Int]   // 与 []int 相同
//	Series [Bool]  // 与 []bool 相同
type Indexes interface{}

// New 是通用的 Series 构造函数。
func New(values interface{}, t Type, name string) Series {
	ret := Series{
		Name: name,
		t:    t,
	}

	// 预先分配元素
	preAlloc := func(n int) {
		switch t {
		case String:
			ret.elements = make(stringElements, n)
		case Int:
			ret.elements = make(intElements, n)
		case Float:
			ret.elements = make(floatElements, n)
		case Bool:
			ret.elements = make(boolElements, n)
		default:
			panic(fmt.Sprintf("unknown type %v", t))
		}
	}

	if values == nil {
		preAlloc(1)
		ret.elements.Elem(0).Set(nil)
		return ret
	}

	switch v := values.(type) {
	case []string:
		l := len(v)
		preAlloc(l)
		for i := 0; i < l; i++ {
			ret.elements.Elem(i).Set(v[i])
		}
	case []float64:
		l := len(v)
		preAlloc(l)
		for i := 0; i < l; i++ {
			ret.elements.Elem(i).Set(v[i])
		}
	case []int:
		l := len(v)
		preAlloc(l)
		for i := 0; i < l; i++ {
			ret.elements.Elem(i).Set(v[i])
		}
	case []bool:
		l := len(v)
		preAlloc(l)
		for i := 0; i < l; i++ {
			ret.elements.Elem(i).Set(v[i])
		}
	case Series:
		l := v.Len()
		preAlloc(l)
		for i := 0; i < l; i++ {
			ret.elements.Elem(i).Set(v.elements.Elem(i))
		}
	default:
		switch reflect.TypeOf(values).Kind() {
		case reflect.Slice:
			v := reflect.ValueOf(values)
			l := v.Len()
			preAlloc(v.Len())
			for i := 0; i < l; i++ {
				val := v.Index(i).Interface()
				ret.elements.Elem(i).Set(val)
			}
		default:
			preAlloc(1)
			v := reflect.ValueOf(values)
			val := v.Interface()
			ret.elements.Elem(0).Set(val)
		}
	}

	return ret
}

// Strings 是 String Series 的构造函数。
func Strings(values interface{}) Series {
	return New(values, String, "")
}

// Ints 是 Int Series 的构造函数。
func Ints(values interface{}) Series {
	return New(values, Int, "")
}

// Floats 是 Float Series 的构造函数。
func Floats(values interface{}) Series {
	return New(values, Float, "")
}

// Bools 是 Bool Series 的构造函数。
func Bools(values interface{}) Series {
	return New(values, Bool, "")
}

// Empty 返回与相同类型的空 Series。
func (s Series) Empty() Series {
	return New([]int{}, s.t, s.Name)
}

// 返回错误或 nil（如果未发生错误）
func (s *Series) Error() error {
	return s.Err
}

// Append 将新元素添加到 Series 的末尾。使用 Append 时，将直接修改 Series。
func (s *Series) Append(values interface{}) {
	if err := s.Err; err != nil {
		return
	}
	news := New(values, s.t, s.Name)
	switch s.t {
	case String:
		s.elements = append(s.elements.(stringElements), news.elements.(stringElements)...)
	case Int:
		s.elements = append(s.elements.(intElements), news.elements.(intElements)...)
	case Float:
		s.elements = append(s.elements.(floatElements), news.elements.(floatElements)...)
	case Bool:
		s.elements = append(s.elements.(boolElements), news.elements.(boolElements)...)
	}
}

// Concat 连接两个 Series。它将返回一个包含两个 Series 元素的新 Series。
func (s Series) Concat(x Series) Series {
	if err := s.Err; err != nil {
		return s
	}
	if err := x.Err; err != nil {
		s.Err = fmt.Errorf("concat error: argument has errors: %v", err)
		return s
	}
	y := s.Copy()
	y.Append(x)
	return y
}

// Subset 根据给定的 Indexes 返回 Series 的子集。
func (s Series) Subset(indexes Indexes) Series {
	if err := s.Err; err != nil {
		return s
	}
	idx, err := parseIndexes(s.Len(), indexes)
	if err != nil {
		s.Err = err
		return s
	}
	ret := Series{
		Name: s.Name,
		t:    s.t,
	}
	switch s.t {
	case String:
		elements := make(stringElements, len(idx))
		for k, i := range idx {
			elements[k] = s.elements.(stringElements)[i]
		}
		ret.elements = elements
	case Int:
		elements := make(intElements, len(idx))
		for k, i := range idx {
			elements[k] = s.elements.(intElements)[i]
		}
		ret.elements = elements
	case Float:
		elements := make(floatElements, len(idx))
		for k, i := range idx {
			elements[k] = s.elements.(floatElements)[i]
		}
		ret.elements = elements
	case Bool:
		elements := make(boolElements, len(idx))
		for k, i := range idx {
			elements[k] = s.elements.(boolElements)[i]
		}
		ret.elements = elements
	default:
		panic("unknown series type")
	}
	return ret
}

// Set 方法设置 Series 的索引处的值并返回自身的引用。原始 Series 会被修改。
func (s Series) Set(indexes Indexes, newValue Series) Series {
	if err := s.Err; err != nil {
		return s
	}
	if err := newValue.Err; err != nil {
		s.Err = fmt.Errorf("set error: 参数存在错误: %v", err)
		return s
	}
	idx, err := parseIndexes(s.Len(), indexes)
	if err != nil {
		s.Err = err
		return s
	}
	if len(idx) != newValue.Len() {
		s.Err = fmt.Errorf("set error: 维度不匹配")
		return s
	}
	for k, i := range idx {
		if i < 0 || i >= s.Len() {
			s.Err = fmt.Errorf("set error: 索引超出范围")
			return s
		}
		s.elements.Elem(i).Set(newValue.elements.Elem(k))
	}
	return s
}

// HasNaN 方法检查 Series 是否包含 NaN 元素。
func (s Series) HasNaN() bool {
	for i := 0; i < s.Len(); i++ {
		if s.elements.Elem(i).IsNA() {
			return true
		}
	}
	return false
}

// IsNaN 方法返回一个标识哪些元素是 NaN 的数组。
func (s Series) IsNaN() []bool {
	ret := make([]bool, s.Len())
	for i := 0; i < s.Len(); i++ {
		ret[i] = s.elements.Elem(i).IsNA()
	}
	return ret
}

// Compare 方法比较 Series 的值与其他元素。为此，要比较的元素首先转换为与调用方相同类型的 Series。
func (s Series) Compare(comparator Comparator, comparando interface{}) Series {
	if err := s.Err; err != nil {
		return s
	}
	compareElements := func(a, b Element, c Comparator) (bool, error) {
		var ret bool
		switch c {
		case Eq:
			ret = a.Eq(b)
		case Neq:
			ret = a.Neq(b)
		case Greater:
			ret = a.Greater(b)
		case GreaterEq:
			ret = a.GreaterEq(b)
		case Less:
			ret = a.Less(b)
		case LessEq:
			ret = a.LessEq(b)
		default:
			return false, fmt.Errorf("未知比较器: %v", c)
		}
		return ret, nil
	}

	bools := make([]bool, s.Len())

	// CompFunc 比较器比较
	if comparator == CompFunc {
		f, ok := comparando.(compFunc)
		if !ok {
			panic("comparando 不是一个 func(el Element) bool 类型的比较函数")
		}

		for i := 0; i < s.Len(); i++ {
			e := s.elements.Elem(i)
			bools[i] = f(e)
		}

		return Bools(bools)
	}

	comp := New(comparando, s.t, "")
	// In 比较器比较
	if comparator == In {
		for i := 0; i < s.Len(); i++ {
			e := s.elements.Elem(i)
			b := false
			for j := 0; j < comp.Len(); j++ {
				m := comp.elements.Elem(j)
				c, err := compareElements(e, m, Eq)
				if err != nil {
					s = s.Empty()
					s.Err = err
					return s
				}
				if c {
					b = true
					break
				}
			}
			bools[i] = b
		}
		return Bools(bools)
	}

	// 单一元素比较
	if comp.Len() == 1 {
		for i := 0; i < s.Len(); i++ {
			e := s.elements.Elem(i)
			c, err := compareElements(e, comp.elements.Elem(0), comparator)
			if err != nil {
				s = s.Empty()
				s.Err = err
				return s
			}
			bools[i] = c
		}
		return Bools(bools)
	}

	// 多元素比较
	if s.Len() != comp.Len() {
		s := s.Empty()
		s.Err = fmt.Errorf("无法比较: 长度不匹配")
		return s
	}
	for i := 0; i < s.Len(); i++ {
		e := s.elements.Elem(i)
		c, err := compareElements(e, comp.elements.Elem(i), comparator)
		if err != nil {
			s = s.Empty()
			s.Err = err
			return s
		}
		bools[i] = c
	}
	return Bools(bools)
}

// Copy 方法将返回 Series 的副本。
func (s Series) Copy() Series {
	name := s.Name
	t := s.t
	err := s.Err
	var elements Elements
	switch s.t {
	case String:
		elements = make(stringElements, s.Len())
		copy(elements.(stringElements), s.elements.(stringElements))
	case Float:
		elements = make(floatElements, s.Len())
		copy(elements.(floatElements), s.elements.(floatElements))
	case Bool:
		elements = make(boolElements, s.Len())
		copy(elements.(boolElements), s.elements.(boolElements))
	case Int:
		elements = make(intElements, s.Len())
		copy(elements.(intElements), s.elements.(intElements))
	}
	ret := Series{
		Name:     name,
		t:        t,
		elements: elements,
		Err:      err,
	}
	return ret
}

// Records 方法将 Series 的元素作为 []string 返回。
func (s Series) Records() []string {
	ret := make([]string, s.Len())
	for i := 0; i < s.Len(); i++ {
		e := s.elements.Elem(i)
		ret[i] = e.String()
	}
	return ret
}

// Float 方法将 Series 的元素作为 []float64 返回。如果元素无法转换为 float64 或包含 NaN，则返回 NaN 的 float 表示。
func (s Series) Float() []float64 {
	ret := make([]float64, s.Len())
	for i := 0; i < s.Len(); i++ {
		e := s.elements.Elem(i)
		ret[i] = e.Float()
	}
	return ret
}

// Int 方法将 Series 的元素作为 []int 返回，如果转换不可能则返回错误。
func (s Series) Int() ([]int, error) {
	ret := make([]int, s.Len())
	for i := 0; i < s.Len(); i++ {
		e := s.elements.Elem(i)
		val, err := e.Int()
		if err != nil {
			return nil, err
		}
		ret[i] = val
	}
	return ret, nil
}

// Bool 方法将 Series 的元素作为 []bool 返回，如果转换不可能则返回错误。
func (s Series) Bool() ([]bool, error) {
	ret := make([]bool, s.Len())
	for i := 0; i < s.Len(); i++ {
		e := s.elements.Elem(i)
		val, err := e.Bool()
		if err != nil {
			return nil, err
		}
		ret[i] = val
	}
	return ret, nil
}

// Type 方法返回给定 Series 的类型。
func (s Series) Type() Type {
	return s.t
}

// Len 方法返回给定 Series 的长度。
func (s Series) Len() int {
	return s.elements.Len()
}

// String 实现了 Series 的 Stringer 接口。
func (s Series) String() string {
	return fmt.Sprint(s.elements)
}

// Str 方法打印关于给定 Series 的一些额外信息。
func (s Series) Str() string {
	var ret []string
	// 如果名称存在，则打印名称
	if s.Name != "" {
		ret = append(ret, "Name: "+s.Name)
	}
	ret = append(ret, "Type: "+fmt.Sprint(s.t))
	ret = append(ret, "Length: "+fmt.Sprint(s.Len()))
	if s.Len() != 0 {
		ret = append(ret, "Values: "+fmt.Sprint(s))
	}
	return strings.Join(ret, "\n")
}

// Val 方法返回给定索引处的 Series 的值。如果索引超出范围，则会引发 panic。
func (s Series) Val(i int) interface{} {
	return s.elements.Elem(i).Val()
}

// Elem 方法返回给定索引处的 Series 的元素。如果索引超出范围，则会引发 panic。
func (s Series) Elem(i int) Element {
	return s.elements.Elem(i)
}

// parseIndexes 方法解析给定 Series 的索引，长度为 `l`。不进行越界检查。
func parseIndexes(l int, indexes Indexes) ([]int, error) {
	var idx []int
	switch idxs := indexes.(type) {
	case []int:
		idx = idxs
	case int:
		idx = []int{idxs}
	case []bool:
		bools := idxs
		if len(bools) != l {
			return nil, fmt.Errorf("索引错误: 索引维度不匹配")
		}
		for i, b := range bools {
			if b {
				idx = append(idx, i)
			}
		}
	case Series:
		s := idxs
		if err := s.Err; err != nil {
			return nil, fmt.Errorf("索引错误: 新值存在错误: %v", err)
		}
		if s.HasNaN() {
			return nil, fmt.Errorf("索引错误: 索引包含 NaN")
		}
		switch s.t {
		case Int:
			return s.Int()
		case Bool:
			bools, err := s.Bool()
			if err != nil {
				return nil, fmt.Errorf("索引错误: %v", err)
			}
			return parseIndexes(l, bools)
		default:
			return nil, fmt.Errorf("索引错误: 未知索引模式")
		}
	default:
		return nil, fmt.Errorf("索引错误: 未知索引模式")
	}
	return idx, nil
}

// Order 方法返回排序 Series 所需的索引。NaN 元素按出现顺序推送到末尾。
func (s Series) Order(reverse bool) []int {
	var ie indexedElements
	var nasIdx []int
	for i := 0; i < s.Len(); i++ {
		e := s.elements.Elem(i)
		if e.IsNA() {
			nasIdx = append(nasIdx, i)
		} else {
			ie = append(ie, indexedElement{i, e})
		}
	}
	var srt sort.Interface
	srt = ie
	if reverse {
		srt = sort.Reverse(srt)
	}
	sort.Stable(srt)
	var ret []int
	for _, e := range ie {
		ret = append(ret, e.index)
	}
	return append(ret, nasIdx...)
}

// indexedElement 结构表示带索引的元素。
type indexedElement struct {
	index   int
	element Element
}

// indexedElements 是 indexedElement 的切片。
type indexedElements []indexedElement

// Len 方法返回切片的长度。
func (e indexedElements) Len() int { return len(e) }

// Less 方法比较两个元素的大小。
func (e indexedElements) Less(i, j int) bool { return e[i].element.Less(e[j].element) }

// Swap 方法交换两个元素。
func (e indexedElements) Swap(i, j int) { e[i], e[j] = e[j], e[i] }

// StdDev 方法计算 Series 的标准差。
func (s Series) StdDev() float64 {
	stdDev := stat.StdDev(s.Float(), nil)
	return stdDev
}

// Mean 方法计算 Series 的平均值。
func (s Series) Mean() float64 {
	stdDev := stat.Mean(s.Float(), nil)
	return stdDev
}

// Median 方法计算中间值或中位数，与平均值相反，它不太容易受到异常值的影响。
func (s Series) Median() float64 {
	if s.elements.Len() == 0 ||
		s.Type() == String ||
		s.Type() == Bool {
		return math.NaN()
	}
	ix := s.Order(false)
	newElem := make([]Element, len(ix))

	for newpos, oldpos := range ix {
		newElem[newpos] = s.elements.Elem(oldpos)
	}

	// 当长度为奇数时，我们只需取长度(list)/2的值作为中位数。
	if len(newElem)%2 != 0 {
		return newElem[len(newElem)/2].Float()
	}
	// 当长度为偶数时，我们取列表的中间两个元素，中位数是它们的平均值。
	return (newElem[(len(newElem)/2)-1].Float() +
		newElem[len(newElem)/2].Float()) * 0.5
}

// Max 方法返回 Series 中的最大元素值。
func (s Series) Max() float64 {
	if s.elements.Len() == 0 || s.Type() == String {
		return math.NaN()
	}

	max := s.elements.Elem(0)
	for i := 1; i < s.elements.Len(); i++ {
		elem := s.elements.Elem(i)
		if elem.Greater(max) {
			max = elem
		}
	}
	return max.Float()
}

// MaxStr 方法返回字符串类型 Series 中的最大元素值。
func (s Series) MaxStr() string {
	if s.elements.Len() == 0 || s.Type() != String {
		return ""
	}

	max := s.elements.Elem(0)
	for i := 1; i < s.elements.Len(); i++ {
		elem := s.elements.Elem(i)
		if elem.Greater(max) {
			max = elem
		}
	}
	return max.String()
}

// Min 方法返回 Series 中的最小元素值。
func (s Series) Min() float64 {
	if s.elements.Len() == 0 || s.Type() == String {
		return math.NaN()
	}

	min := s.elements.Elem(0)
	for i := 1; i < s.elements.Len(); i++ {
		elem := s.elements.Elem(i)
		if elem.Less(min) {
			min = elem
		}
	}
	return min.Float()
}

// MinStr 方法返回字符串类型 Series 中的最小元素值。
func (s Series) MinStr() string {
	if s.elements.Len() == 0 || s.Type() != String {
		return ""
	}

	min := s.elements.Elem(0)
	for i := 1; i < s.elements.Len(); i++ {
		elem := s.elements.Elem(i)
		if elem.Less(min) {
			min = elem
		}
	}
	return min.String()
}

// Quantile 方法返回 Series 样本，使得 x 大于或等于样本比例 p。
// 注意: 当以字符串类型调用时，gonum/stat 会引发 panic。
func (s Series) Quantile(p float64) float64 {
	if s.Type() == String || s.Len() == 0 {
		return math.NaN()
	}

	ordered := s.Subset(s.Order(false)).Float()

	return stat.Quantile(p, stat.Empirical, ordered, nil)
}

// Map 方法将 MapFunction 函数应用于每个 Series 元素，并返回一个新的 Series 对象。
// 函数必须与 Series 数据底层类型兼容。
// 换句话说，当处理 Float 类型 Series 时，通过参数 `f` 传递的函数不应期望另一种类型，
// 而是期望处理类型为 Float 的 Element(s)。
func (s Series) Map(f MapFunction) Series {
	mappedValues := make([]Element, s.Len())
	for i := 0; i < s.Len(); i++ {
		value := f(s.elements.Elem(i))
		mappedValues[i] = value
	}
	return New(mappedValues, s.Type(), s.Name)
}

// Sum 方法计算 Series 的和。
func (s Series) Sum() float64 {
	if s.elements.Len() == 0 || s.Type() == String || s.Type() == Bool {
		return math.NaN()
	}
	sFloat := s.Float()
	sum := sFloat[0]
	for i := 1; i < len(sFloat); i++ {
		elem := sFloat[i]
		sum += elem
	}
	return sum
}

// Slice 方法从 j 到 k-1 的索引处对 Series 进行切片。
func (s Series) Slice(j, k int) Series {
	if s.Err != nil {
		return s
	}

	if j > k || j < 0 || k >= s.Len() {
		empty := s.Empty()
		empty.Err = fmt.Errorf("slice index out of bounds")
		return empty
	}

	idxs := make([]int, k-j)
	for i := 0; j+i < k; i++ {
		idxs[i] = j + i
	}

	return s.Subset(idxs)
}
