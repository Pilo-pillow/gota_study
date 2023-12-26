package dataframe

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"golang.org/x/net/html"
	"golang.org/x/net/html/atom"
	"io"
	"reflect"
	"sort"
	"strconv"
	"stream/go-sdk/test/gota_study/series"
	"strings"
	"unicode/utf8"
)

// DataFrame 是一个表示带有命名列的数据表的数据结构。
// 它包含一个 series.Series 切片表示列，以及行数、列数和错误字段来处理潜在错误。
type DataFrame struct {
	columns []series.Series
	ncols   int
	nrows   int

	Err error
}

// New 使用提供的 series 创建一个新的 DataFrame。
// 它检查错误，复制系列，并初始化 DataFrame。
func New(se ...series.Series) DataFrame {
	if se == nil || len(se) == 0 {
		return DataFrame{Err: fmt.Errorf("empty DataFrame")}
	}

	columns := make([]series.Series, len(se))
	for i, s := range se {
		columns[i] = s.Copy()
	}
	nrows, ncols, err := checkColumnsDimensions(columns...)
	if err != nil {
		return DataFrame{Err: err}
	}

	df := DataFrame{
		columns: columns,
		ncols:   ncols,
		nrows:   nrows,
	}
	colnames := df.Names()
	fixColnames(colnames)
	for i, colname := range colnames {
		df.columns[i].Name = colname
	}
	return df
}

// checkColumnsDimensions 检查提供的 series 的维度。
// 它返回行数、列数以及任何错误。
func checkColumnsDimensions(se ...series.Series) (nrows, ncols int, err error) {
	ncols = len(se)
	nrows = -1
	if se == nil || ncols == 0 {
		err = fmt.Errorf("no Series given")
		return
	}
	for i, s := range se {
		if s.Err != nil {
			err = fmt.Errorf("error on series %d: %v", i, s.Err)
			return
		}
		if nrows == -1 {
			nrows = s.Len()
		}
		if nrows != s.Len() {
			err = fmt.Errorf("arguments have different dimensions")
			return
		}
	}
	return
}

// Copy 创建 DataFrame 的深层副本。
func (df DataFrame) Copy() DataFrame {
	copy := New(df.columns...)
	if df.Err != nil {
		copy.Err = df.Err
	}
	return copy
}

// String 返回 DataFrame 的字符串表示。
func (df DataFrame) String() (str string) {
	return df.print(true, true, true, true, 10, 70, "DataFrame")
}

// Error 返回与 DataFrame 相关联的错误。
func (df *DataFrame) Error() error {
	return df.Err
}

// print 生成 DataFrame 的格式化字符串表示。
func (df DataFrame) print(
	shortRows, shortCols, showDims, showTypes bool,
	maxRows int,
	maxCharsTotal int,
	class string) (str string) {

	addRightPadding := func(s string, nchar int) string {
		if utf8.RuneCountInString(s) < nchar {
			return s + strings.Repeat(" ", nchar-utf8.RuneCountInString(s))
		}
		return s
	}

	addLeftPadding := func(s string, nchar int) string {
		if utf8.RuneCountInString(s) < nchar {
			return strings.Repeat(" ", nchar-utf8.RuneCountInString(s)) + s
		}
		return s
	}

	if df.Err != nil {
		str = fmt.Sprintf("%s error: %v", class, df.Err)
		return
	}
	nrows, ncols := df.Dims()
	if nrows == 0 || ncols == 0 {
		str = fmt.Sprintf("Empty %s", class)
		return
	}
	idx := make([]int, maxRows)
	for i := 0; i < len(idx); i++ {
		idx[i] = i
	}
	var records [][]string
	shortening := false
	if shortRows && nrows > maxRows {
		shortening = true
		df = df.Subset(idx)
		records = df.Records()
	} else {
		records = df.Records()
	}

	if showDims {
		str += fmt.Sprintf("[%dx%d] %s\n\n", nrows, ncols, class)
	}

	for i := 0; i < df.nrows+1; i++ {
		add := ""
		if i != 0 {
			add = strconv.Itoa(i-1) + ":"
		}
		records[i] = append([]string{add}, records[i]...)
	}
	if shortening {
		dots := make([]string, ncols+1)
		for i := 1; i < ncols+1; i++ {
			dots[i] = "..."
		}
		records = append(records, dots)
	}
	types := df.Types()
	typesrow := make([]string, ncols)
	for i := 0; i < ncols; i++ {
		typesrow[i] = fmt.Sprintf("<%v>", types[i])
	}
	typesrow = append([]string{""}, typesrow...)

	if showTypes {
		records = append(records, typesrow)
	}

	maxChars := make([]int, df.ncols+1)
	for i := 0; i < len(records); i++ {
		for j := 0; j < df.ncols+1; j++ {

			records[i][j] = strconv.Quote(records[i][j])
			records[i][j] = records[i][j][1 : len(records[i][j])-1]

			if len(records[i][j]) > maxChars[j] {
				maxChars[j] = utf8.RuneCountInString(records[i][j])
			}
		}
	}
	maxCols := len(records[0])
	var notShowing []string
	if shortCols {
		maxCharsCum := 0
		for colnum, m := range maxChars {
			maxCharsCum += m
			if maxCharsCum > maxCharsTotal {
				maxCols = colnum
				break
			}
		}
		notShowingNames := records[0][maxCols:]
		notShowingTypes := typesrow[maxCols:]
		notShowing = make([]string, len(notShowingNames))
		for i := 0; i < len(notShowingNames); i++ {
			notShowing[i] = fmt.Sprintf("%s %s", notShowingNames[i], notShowingTypes[i])
		}
	}
	for i := 0; i < len(records); i++ {

		records[i][0] = addLeftPadding(records[i][0], maxChars[0]+1)
		for j := 1; j < df.ncols; j++ {
			records[i][j] = addRightPadding(records[i][j], maxChars[j])
		}
		records[i] = records[i][0:maxCols]
		if shortCols && len(notShowing) != 0 {
			records[i] = append(records[i], "...")
		}

		str += strings.Join(records[i], " ")
		str += "\n"
	}
	if shortCols && len(notShowing) != 0 {
		var notShown string
		var notShownArr [][]string
		cum := 0
		i := 0
		for n, ns := range notShowing {
			cum += len(ns)
			if cum > maxCharsTotal {
				notShownArr = append(notShownArr, notShowing[i:n])
				cum = 0
				i = n
			}
		}
		if i < len(notShowing) {
			notShownArr = append(notShownArr, notShowing[i:])
		}
		for k, ns := range notShownArr {
			notShown += strings.Join(ns, ", ")
			if k != len(notShownArr)-1 {
				notShown += ","
			}
			notShown += "\n"
		}
		str += fmt.Sprintf("\nNot Showing: %s", notShown)
	}
	return str
}

// Set 方法用新的DataFrame替换指定索引处的值。
// 它返回修改后的DataFrame。
func (df DataFrame) Set(indexes series.Indexes, newvalues DataFrame) DataFrame {
	if df.Err != nil {
		return df
	}
	if newvalues.Err != nil {
		return DataFrame{Err: fmt.Errorf("参数存在错误：%v", newvalues.Err)}
	}
	if df.ncols != newvalues.ncols {
		return DataFrame{Err: fmt.Errorf("列数不同")}
	}
	columns := make([]series.Series, df.ncols)
	for i, s := range df.columns {
		columns[i] = s.Set(indexes, newvalues.columns[i])
		if columns[i].Err != nil {
			df = DataFrame{Err: fmt.Errorf("在第%d列设置错误：%v", i, columns[i].Err)}
			return df
		}
	}
	return df
}

// Subset 方法返回一个根据指定索引选择的行的新DataFrame。
func (df DataFrame) Subset(indexes series.Indexes) DataFrame {
	if df.Err != nil {
		return df
	}
	columns := make([]series.Series, df.ncols)
	for i, column := range df.columns {
		s := column.Subset(indexes)
		columns[i] = s
	}
	nrows, ncols, err := checkColumnsDimensions(columns...)
	if err != nil {
		return DataFrame{Err: err}
	}
	return DataFrame{
		columns: columns,
		ncols:   ncols,
		nrows:   nrows,
	}
}

type SelectIndexes interface{}

// Select 方法根据提供的索引返回一个根据所选列进行选择的新DataFrame。
func (df DataFrame) Select(indexes SelectIndexes) DataFrame {
	if df.Err != nil {
		return df
	}
	idx, err := parseSelectIndexes(df.ncols, indexes, df.Names())
	if err != nil {
		return DataFrame{Err: fmt.Errorf("can't select columns: %v", err)}
	}
	columns := make([]series.Series, len(idx))
	for k, i := range idx {
		if i < 0 || i >= df.ncols {
			return DataFrame{Err: fmt.Errorf("can't select columns: index out of range")}
		}
		columns[k] = df.columns[i].Copy()
	}
	nrows, ncols, err := checkColumnsDimensions(columns...)
	if err != nil {
		return DataFrame{Err: err}
	}
	df = DataFrame{
		columns: columns,
		ncols:   ncols,
		nrows:   nrows,
	}
	colnames := df.Names()
	fixColnames(colnames)
	for i, colname := range colnames {
		df.columns[i].Name = colname
	}
	return df
}

// Drop 方法返回一个根据提供的索引删除列的新DataFrame。
func (df DataFrame) Drop(indexes SelectIndexes) DataFrame {
	if df.Err != nil {
		return df
	}
	idx, err := parseSelectIndexes(df.ncols, indexes, df.Names())
	if err != nil {
		return DataFrame{Err: fmt.Errorf("无法选择列：%v", err)}
	}
	var columns []series.Series
	for k, col := range df.columns {
		if !inIntSlice(k, idx) {
			columns = append(columns, col.Copy())
		}
	}
	nrows, ncols, err := checkColumnsDimensions(columns...)
	if err != nil {
		return DataFrame{Err: err}
	}
	df = DataFrame{
		columns: columns,
		ncols:   ncols,
		nrows:   nrows,
	}
	colnames := df.Names()
	fixColnames(colnames)
	for i, colname := range colnames {
		df.columns[i].Name = colname
	}
	return df
}

// KEY_ERROR 是用于标识键错误的常量。
const KEY_ERROR = "KEY_ERROR"

// GroupBy 方法按指定的列名对DataFrame进行分组，并返回Groups结构。
func (df DataFrame) GroupBy(colnames ...string) *Groups {
	if len(colnames) <= 0 {
		return nil
	}
	groupDataFrame := make(map[string]DataFrame)
	groupSeries := make(map[string][]map[string]interface{})

	// 检查列名是否存在于DataFrame中。
	for _, c := range colnames {
		if idx := findInStringSlice(c, df.Names()); idx == -1 {
			return &Groups{Err: fmt.Errorf("GroupBy: 无法找到列名：%s", c)}
		}
	}

	// 按指定的列对DataFrame进行分组。
	for _, s := range df.Maps() {
		key := ""
		for i, c := range colnames {
			format := ""
			if i == 0 {
				format = "%s%"
			} else {
				format = "%s_%"
			}
			switch s[c].(type) {
			case string, bool:
				format += "s"
			case int, int16, int32, int64:
				format += "d"
			case float32, float64:
				format += "f"
			default:
				return &Groups{Err: fmt.Errorf("GroupBy: 未找到类型")}
			}
			key = fmt.Sprintf(format, key, s[c])
		}
		groupSeries[key] = append(groupSeries[key], s)
	}

	// 确定列类型。
	colTypes := map[string]series.Type{}
	for _, c := range df.columns {
		colTypes[c.Name] = c.Type()
	}

	// 为每个组创建DataFrame。
	for k, cMaps := range groupSeries {
		groupDataFrame[k] = LoadMaps(cMaps, WithTypes(colTypes))
	}
	groups := &Groups{groups: groupDataFrame, colnames: colnames}
	return groups
}

// AggregationType 定义聚合操作的类型。
type AggregationType int

const (
	Aggregation_MAX AggregationType = iota + 1
	Aggregation_MIN
	Aggregation_MEAN
	Aggregation_MEDIAN
	Aggregation_STD
	Aggregation_SUM
	Aggregation_COUNT
)

// Groups 表示分组的数据并支持聚合操作。
type Groups struct {
	groups      map[string]DataFrame // 分组数据的映射，以分组的名称作为键，对应的值为DataFrame对象
	colnames    []string             // 列名的切片
	aggregation DataFrame            // 聚合结果的DataFrame对象
	Err         error                // 错误信息
}

// Aggregation 方法按照给定的AggregationType和列名对Groups进行聚合操作。
// 它返回包含聚合结果的新DataFrame。
func (gps Groups) Aggregation(typs []AggregationType, colnames []string) DataFrame {
	if gps.groups == nil {
		return DataFrame{Err: fmt.Errorf("Aggregation: 输入为nil")}
	}
	if len(typs) != len(colnames) {
		return DataFrame{Err: fmt.Errorf("Aggregation: len(typs) != len(colanmes)")}
	}
	dfMaps := make([]map[string]interface{}, 0)
	for _, df := range gps.groups {
		targetMap := df.Maps()[0]
		curMap := make(map[string]interface{})

		for _, c := range gps.colnames {
			if value, ok := targetMap[c]; ok {
				curMap[c] = value
			} else {
				return DataFrame{Err: fmt.Errorf("Aggregation: 无法找到列名：%s", c)}
			}
		}

		for i, c := range colnames {
			curSeries := df.Col(c)
			var value float64
			switch typs[i] {
			case Aggregation_MAX:
				value = curSeries.Max()
			case Aggregation_MEAN:
				value = curSeries.Mean()
			case Aggregation_MEDIAN:
				value = curSeries.Median()
			case Aggregation_MIN:
				value = curSeries.Min()
			case Aggregation_STD:
				value = curSeries.StdDev()
			case Aggregation_SUM:
				value = curSeries.Sum()
			case Aggregation_COUNT:
				value = float64(curSeries.Len())
			default:
				return DataFrame{Err: fmt.Errorf("Aggregation: 未找到该方法：%s", typs[i])}

			}
			curMap[fmt.Sprintf("%s_%s", c, typs[i])] = value
		}
		dfMaps = append(dfMaps, curMap)

	}

	colTypes := map[string]series.Type{}
	for k := range dfMaps[0] {
		switch dfMaps[0][k].(type) {
		case string:
			colTypes[k] = series.String
		case int, int16, int32, int64:
			colTypes[k] = series.Int
		case float32, float64:
			colTypes[k] = series.Float
		default:
			continue
		}
	}

	gps.aggregation = LoadMaps(dfMaps, WithTypes(colTypes))
	return gps.aggregation
}

// GetGroups 方法返回Groups中的分组数据。
func (g Groups) GetGroups() map[string]DataFrame {
	return g.groups
}

// Rename 方法用新的列名替换指定的旧列名。
// 它返回修改后的DataFrame。
func (df DataFrame) Rename(newname, oldname string) DataFrame {
	if df.Err != nil {
		return df
	}

	colnames := df.Names()
	idx := findInStringSlice(oldname, colnames)
	if idx == -1 {
		return DataFrame{Err: fmt.Errorf("rename: 无法找到列名")}
	}

	copy := df.Copy()
	copy.columns[idx].Name = newname
	return copy
}

// CBind 方法将两个DataFrame按列拼接。
// 它返回包含拼接结果的新DataFrame。
func (df DataFrame) CBind(dfb DataFrame) DataFrame {
	if df.Err != nil {
		return df
	}
	if dfb.Err != nil {
		return dfb
	}
	cols := append(df.columns, dfb.columns...)
	return New(cols...)
}

// RBind 方法将两个DataFrame按行拼接。
// 它返回包含拼接结果的新DataFrame。
func (df DataFrame) RBind(dfb DataFrame) DataFrame {
	if df.Err != nil {
		return df
	}
	if dfb.Err != nil {
		return dfb
	}
	expandedSeries := make([]series.Series, df.ncols)
	for k, v := range df.Names() {
		idx := findInStringSlice(v, dfb.Names())
		if idx == -1 {
			return DataFrame{Err: fmt.Errorf("rbind: 列名不兼容")}
		}

		originalSeries := df.columns[k]
		addedSeries := dfb.columns[idx]
		newSeries := originalSeries.Concat(addedSeries)
		if err := newSeries.Err; err != nil {
			return DataFrame{Err: fmt.Errorf("rbind: %v", err)}
		}
		expandedSeries[k] = newSeries
	}
	return New(expandedSeries...)
}

// Concat 方法将两个DataFrame按列拼接，保留唯一列。
// 它返回包含拼接结果的新DataFrame。
func (df DataFrame) Concat(dfb DataFrame) DataFrame {
	if df.Err != nil {
		return df
	}
	if dfb.Err != nil {
		return dfb
	}

	uniques := make(map[string]struct{})
	cols := []string{}
	for _, t := range []DataFrame{df, dfb} {
		for _, u := range t.Names() {
			if _, ok := uniques[u]; !ok {
				uniques[u] = struct{}{}
				cols = append(cols, u)
			}
		}
	}

	expandedSeries := make([]series.Series, len(cols))
	for k, v := range cols {
		aidx := findInStringSlice(v, df.Names())
		bidx := findInStringSlice(v, dfb.Names())

		var a, b series.Series
		if aidx != -1 {
			a = df.columns[aidx]
		} else {
			bb := dfb.columns[bidx]
			a = series.New(make([]struct{}, df.nrows), bb.Type(), bb.Name)
		}
		if bidx != -1 {
			b = dfb.columns[bidx]
		} else {
			b = series.New(make([]struct{}, dfb.nrows), a.Type(), a.Name)
		}
		newSeries := a.Concat(b)
		if err := newSeries.Err; err != nil {
			return DataFrame{Err: fmt.Errorf("concat: %v", err)}
		}
		expandedSeries[k] = newSeries
	}
	return New(expandedSeries...)
}

// Mutate 方法用提供的Series替换DataFrame中的某一列。
// 它返回修改后的DataFrame。
func (df DataFrame) Mutate(s series.Series) DataFrame {
	if df.Err != nil {
		return df
	}
	if s.Len() != df.nrows {
		return DataFrame{Err: fmt.Errorf("mutate: 维度不匹配")}
	}
	df = df.Copy()

	columns := df.columns
	if idx := findInStringSlice(s.Name, df.Names()); idx != -1 {
		columns[idx] = s
	} else {
		columns = append(columns, s)
	}
	nrows, ncols, err := checkColumnsDimensions(columns...)
	if err != nil {
		return DataFrame{Err: err}
	}
	df = DataFrame{
		columns: columns,
		ncols:   ncols,
		nrows:   nrows,
	}
	colnames := df.Names()
	fixColnames(colnames)
	for i, colname := range colnames {
		df.columns[i].Name = colname
	}
	return df
}

// F 结构表示一个过滤器，用于根据列名、列索引、比较器和比较值进行过滤。
type F struct {
	Colidx     int
	Colname    string
	Comparator series.Comparator
	Comparando interface{}
}

// Filter 方法根据提供的过滤器过滤DataFrame，并返回新的DataFrame。
func (df DataFrame) Filter(filters ...F) DataFrame {
	return df.FilterAggregation(Or, filters...)
}

// Aggregation 是一个整数常量，表示聚合操作的类型。
type Aggregation int

// String方法返回Aggregation的字符串表示。
func (a Aggregation) String() string {
	switch a {
	case Or:
		return "or"
	case And:
		return "and"
	}
	return fmt.Sprintf("未知的聚合类型 %d", a)
}

// 定义Aggregation常量。
const (
	Or Aggregation = iota
	And
)

// FilterAggregation 方法根据提供的Aggregation类型和过滤器进行过滤DataFrame，并返回新的DataFrame。
func (df DataFrame) FilterAggregation(agg Aggregation, filters ...F) DataFrame {
	if df.Err != nil {
		return df
	}

	compResults := make([]series.Series, len(filters))
	for i, f := range filters {
		var idx int
		if f.Colname == "" {
			idx = f.Colidx
		} else {
			idx = findInStringSlice(f.Colname, df.Names())
			if idx < 0 {
				return DataFrame{Err: fmt.Errorf("filter: 无法找到列名")}
			}
		}
		res := df.columns[idx].Compare(f.Comparator, f.Comparando)
		if err := res.Err; err != nil {
			return DataFrame{Err: fmt.Errorf("filter: %v", err)}
		}
		compResults[i] = res
	}

	if len(compResults) == 0 {
		return df.Copy()
	}

	res, err := compResults[0].Bool()
	if err != nil {
		return DataFrame{Err: fmt.Errorf("filter: %v", err)}
	}
	for i := 1; i < len(compResults); i++ {
		nextRes, err := compResults[i].Bool()
		if err != nil {
			return DataFrame{Err: fmt.Errorf("filter: %v", err)}
		}
		for j := 0; j < len(res); j++ {
			switch agg {
			case Or:
				res[j] = res[j] || nextRes[j]
			case And:
				res[j] = res[j] && nextRes[j]
			default:
				panic(agg)
			}
		}
	}
	return df.Subset(res)
}

// Order 结构表示排序的参数，包括列名和是否降序。
type Order struct {
	Colname string
	Reverse bool
}

// Sort 函数返回一个升序排序的Order结构。
func Sort(colname string) Order {
	return Order{colname, false}
}

// RevSort 函数返回一个降序排序的Order结构。
func RevSort(colname string) Order {
	return Order{colname, true}
}

// Arrange 方法按照指定的排序参数对DataFrame进行排序。
// 它返回排序后的新DataFrame。
func (df DataFrame) Arrange(order ...Order) DataFrame {
	if df.Err != nil {
		return df
	}
	if order == nil || len(order) == 0 {
		return DataFrame{Err: fmt.Errorf("rename: 无参数")}
	}

	for i := 0; i < len(order); i++ {
		colname := order[i].Colname
		if df.colIndex(colname) == -1 {
			return DataFrame{Err: fmt.Errorf("colname %s 不存在", colname)}
		}
	}

	origIdx := make([]int, df.nrows)
	for i := 0; i < df.nrows; i++ {
		origIdx[i] = i
	}

	swapOrigIdx := func(newidx []int) {
		newOrigIdx := make([]int, len(newidx))
		for k, i := range newidx {
			newOrigIdx[k] = origIdx[i]
		}
		origIdx = newOrigIdx
	}

	suborder := origIdx
	for i := len(order) - 1; i >= 0; i-- {
		colname := order[i].Colname
		idx := df.colIndex(colname)
		nextSeries := df.columns[idx].Subset(suborder)
		suborder = nextSeries.Order(order[i].Reverse)
		swapOrigIdx(suborder)
	}
	return df.Subset(origIdx)
}

// Capply 方法对DataFrame的每一列应用给定的函数。
// 它返回包含应用结果的新DataFrame。
func (df DataFrame) Capply(f func(series.Series) series.Series) DataFrame {
	if df.Err != nil {
		return df
	}
	columns := make([]series.Series, df.ncols)
	for i, s := range df.columns {
		applied := f(s)
		applied.Name = s.Name
		columns[i] = applied
	}
	return New(columns...)
}

// Rapply 方法对DataFrame的每一行应用给定的函数。
// 它返回包含应用结果的新DataFrame。
func (df DataFrame) Rapply(f func(series.Series) series.Series) DataFrame {
	// 检查原始DataFrame是否有错误，如果有，则返回相同的DataFrame。
	if df.Err != nil {
		return df
	}

	// 辅助函数，用于检测一组序列类型中的共同类型。
	detectType := func(types []series.Type) series.Type {
		var hasStrings, hasFloats, hasInts, hasBools bool
		// 遍历类型并根据每种类型的存在情况设置标志。
		for _, t := range types {
			switch t {
			case series.String:
				hasStrings = true
			case series.Float:
				hasFloats = true
			case series.Int:
				hasInts = true
			case series.Bool:
				hasBools = true
			}
		}
		// 根据检测到的标志返回共同的类型。
		switch {
		case hasStrings:
			return series.String
		case hasBools:
			return series.Bool
		case hasFloats:
			return series.Float
		case hasInts:
			return series.Int
		default:
			// 如果没有找到支持的类型，则引发错误。
			panic("不支持的类型")
		}
	}

	// 获取DataFrame中列的类型。
	types := df.Types()
	// 确定行的共同类型。
	rowType := detectType(types)

	// 初始化二维数组以存储转换后的元素。
	elements := make([][]series.Element, df.nrows)
	// 用于跟踪每行的长度。
	rowlen := -1

	// 遍历DataFrame中的每一行。
	for i := 0; i < df.nrows; i++ {
		// 创建一个具有共同类型的新空行序列。
		row := series.New(nil, rowType, "").Empty()
		// 将每列的元素附加到行序列。
		for _, col := range df.columns {
			row.Append(col.Elem(i))
		}
		// 将给定函数应用于行。
		row = f(row)
		// 检查应用函数时是否发生错误。
		if row.Err != nil {
			return DataFrame{Err: fmt.Errorf("在行 %d 上应用函数时发生错误: %v", i, row.Err)}
		}

		// 检查行长度是否一致。
		if rowlen != -1 && rowlen != row.Len() {
			return DataFrame{Err: fmt.Errorf("应用函数时发生错误: 行具有不同的长度")}
		}
		rowlen = row.Len()

		// 将行序列转换为元素切片并存储在elements数组中。
		rowElems := make([]series.Element, rowlen)
		for j := 0; j < rowlen; j++ {
			rowElems[j] = row.Elem(j)
		}
		elements[i] = rowElems
	}

	// 初始化数组以存储转换后的列。
	columns := make([]series.Series, rowlen)
	// 遍历每一列。
	for j := 0; j < rowlen; j++ {
		// 为每个行中的列元素创建类型切片。
		types := make([]series.Type, df.nrows)
		for i := 0; i < df.nrows; i++ {
			types[i] = elements[i][j].Type()
		}
		// 确定列的共同类型。
		colType := detectType(types)
		// 创建一个具有共同类型的新空列序列。
		s := series.New(nil, colType, "").Empty()
		// 将每行的元素附加到列序列。
		for i := 0; i < df.nrows; i++ {
			s.Append(elements[i][j])
		}
		// 将列序列存储在columns数组中。
		columns[j] = s
	}

	// 检查转换后的列的维度。
	nrows, ncols, err := checkColumnsDimensions(columns...)
	if err != nil {
		return DataFrame{Err: err}
	}

	// 创建一个带有转换后的列和更新维度的新DataFrame。
	df = DataFrame{
		columns: columns,
		ncols:   ncols,
		nrows:   nrows,
	}
	// 获取DataFrame的列名。
	colnames := df.Names()
	// 修复列名中的任何问题。
	fixColnames(colnames)
	// 更新DataFrame中列的名称。
	for i, colname := range colnames {
		df.columns[i].Name = colname
	}
	// 返回最终转换后的DataFrame。
	return df
}

// LoadOption 是用于配置加载选项的函数类型。
type LoadOption func(*loadOptions)

// loadOptions结构包含加载DataFrame时的各种选项。
type loadOptions struct {
	defaultType series.Type            // 默认系列类型
	detectTypes bool                   // 是否自动检测系列类型
	hasHeader   bool                   // 是否有表头
	names       []string               // 系列名列表
	nanValues   []string               // NaN值列表
	delimiter   rune                   // 分隔符
	lazyQuotes  bool                   // 懒惰引号模式
	comment     rune                   // 注释符号
	types       map[string]series.Type // 系列类型映射表
}

// DefaultType 函数返回一个LoadOption，用于设置默认列类型。
func DefaultType(t series.Type) LoadOption {
	return func(c *loadOptions) {
		c.defaultType = t
	}
}

// DetectTypes 函数返回一个LoadOption，用于启用或禁用类型检测。
func DetectTypes(b bool) LoadOption {
	return func(c *loadOptions) {
		c.detectTypes = b
	}
}

// HasHeader 函数返回一个LoadOption，用于设置是否包含表头。
func HasHeader(b bool) LoadOption {
	return func(c *loadOptions) {
		c.hasHeader = b
	}
}

// Names 函数返回一个LoadOption，用于设置列名。
func Names(names ...string) LoadOption {
	return func(c *loadOptions) {
		c.names = names
	}
}

// NaNValues 函数返回一个LoadOption，用于设置NaN值的字符串表示。
func NaNValues(nanValues []string) LoadOption {
	return func(c *loadOptions) {
		c.nanValues = nanValues
	}
}

// WithTypes 函数返回一个LoadOption，用于设置列的具体类型。
func WithTypes(coltypes map[string]series.Type) LoadOption {
	return func(c *loadOptions) {
		c.types = coltypes
	}
}

// WithDelimiter 函数返回一个LoadOption，用于设置分隔符。
func WithDelimiter(b rune) LoadOption {
	return func(c *loadOptions) {
		c.delimiter = b
	}
}

// WithLazyQuotes 函数返回一个LoadOption，用于设置是否启用惰性引号。
func WithLazyQuotes(b bool) LoadOption {
	return func(c *loadOptions) {
		c.lazyQuotes = b
	}
}

// WithComments 函数返回一个LoadOption，用于设置注释字符。
func WithComments(b rune) LoadOption {
	return func(c *loadOptions) {
		c.comment = b
	}
}

// LoadStructs 函数从给定的切片中加载结构体数据，并返回一个DataFrame。
// 可以使用LoadOption配置加载过程。
func LoadStructs(i interface{}, options ...LoadOption) DataFrame {
	if i == nil {
		return DataFrame{Err: fmt.Errorf("load: 无法从 <nil> 值创建DataFrame")}
	}

	cfg := loadOptions{
		defaultType: series.String,
		detectTypes: true,
		hasHeader:   true,
		nanValues:   []string{"NA", "NaN", "<nil>"},
	}

	for _, option := range options {
		option(&cfg)
	}

	tpy, val := reflect.TypeOf(i), reflect.ValueOf(i)
	switch tpy.Kind() {
	case reflect.Slice:
		if tpy.Elem().Kind() != reflect.Struct {
			return DataFrame{Err: fmt.Errorf(
				"load: 类型 %s (%s %s) 不受支持，必须是 []struct", tpy.Name(), tpy.Elem().Kind(), tpy.Kind())}
		}
		if val.Len() == 0 {
			return DataFrame{Err: fmt.Errorf("load: 无法从空切片创建DataFrame")}
		}

		numFields := val.Index(0).Type().NumField()
		var columns []series.Series
		for j := 0; j < numFields; j++ {

			if !val.Index(0).Field(j).CanInterface() {
				continue
			}
			field := val.Index(0).Type().Field(j)
			fieldName := field.Name
			fieldType := field.Type.String()

			fieldTags := field.Tag.Get("dataframe")
			if fieldTags == "-" {
				continue
			}
			tagOpts := strings.Split(fieldTags, ",")
			if len(tagOpts) > 2 {
				return DataFrame{Err: fmt.Errorf("字段 %s 上的结构体标签格式错误: %s", fieldName, fieldTags)}
			}
			if len(tagOpts) > 0 {
				if name := strings.TrimSpace(tagOpts[0]); name != "" {
					fieldName = name
				}
				if len(tagOpts) == 2 {
					if tagType := strings.TrimSpace(tagOpts[1]); tagType != "" {
						fieldType = tagType
					}
				}
			}

			var t series.Type
			if cfgtype, ok := cfg.types[fieldName]; ok {
				t = cfgtype
			} else {

				if cfg.detectTypes {

					parsedType, err := parseType(fieldType)
					if err != nil {
						return DataFrame{Err: err}
					}
					t = parsedType
				} else {
					t = cfg.defaultType
				}
			}

			elements := make([]interface{}, val.Len())
			for i := 0; i < val.Len(); i++ {
				fieldValue := val.Index(i).Field(j)
				elements[i] = fieldValue.Interface()

				if findInStringSlice(fmt.Sprint(elements[i]), cfg.nanValues) != -1 {
					elements[i] = nil
				}
			}

			if !cfg.hasHeader {
				tmp := make([]interface{}, 1)
				tmp[0] = fieldName
				elements = append(tmp, elements...)
				fieldName = ""
			}
			columns = append(columns, series.New(elements, t, fieldName))
		}
		return New(columns...)
	}
	return DataFrame{Err: fmt.Errorf(
		"load: 类型 %s (%s) 不受支持，必须是 []struct", tpy.Name(), tpy.Kind())}
}

// parseType 将字符串类型映射为 series.Type。
func parseType(s string) (series.Type, error) {
	switch s {
	case "float", "float64", "float32":
		return series.Float, nil
	case "int", "int64", "int32", "int16", "int8":
		return series.Int, nil
	case "string":
		return series.String, nil
	case "bool":
		return series.Bool, nil
	}
	return "", fmt.Errorf("类型 (%s) 不受支持", s)
}

// LoadRecords 从字符串切片记录加载 DataFrame。
func LoadRecords(records [][]string, options ...LoadOption) DataFrame {
	cfg := loadOptions{
		defaultType: series.String,
		detectTypes: true,
		hasHeader:   true,
		nanValues:   []string{"NA", "NaN", "<nil>"},
	}

	for _, option := range options {
		option(&cfg)
	}

	if len(records) == 0 {
		return DataFrame{Err: fmt.Errorf("load records: 空 DataFrame")}
	}
	if cfg.hasHeader && len(records) <= 1 {
		return DataFrame{Err: fmt.Errorf("load records: 空 DataFrame")}
	}
	if cfg.names != nil && len(cfg.names) != len(records[0]) {
		if len(cfg.names) > len(records[0]) {
			return DataFrame{Err: fmt.Errorf("load records: 列名过多")}
		}
		return DataFrame{Err: fmt.Errorf("load records: 列名不足")}
	}

	headers := make([]string, len(records[0]))
	if cfg.hasHeader {
		headers = records[0]
		records = records[1:]
	}
	if cfg.names != nil {
		headers = cfg.names
	}

	types := make([]series.Type, len(headers))
	rawcols := make([][]string, len(headers))
	for i, colname := range headers {
		rawcol := make([]string, len(records))
		for j := 0; j < len(records); j++ {
			rawcol[j] = records[j][i]
			if findInStringSlice(rawcol[j], cfg.nanValues) != -1 {
				rawcol[j] = "NaN"
			}
		}
		rawcols[i] = rawcol

		t, ok := cfg.types[colname]
		if !ok {
			t = cfg.defaultType
			if cfg.detectTypes {
				if l, err := findType(rawcol); err == nil {
					t = l
				}
			}
		}
		types[i] = t
	}

	columns := make([]series.Series, len(headers))
	for i, colname := range headers {
		col := series.New(rawcols[i], types[i], colname)
		if col.Err != nil {
			return DataFrame{Err: col.Err}
		}
		columns[i] = col
	}
	nrows, ncols, err := checkColumnsDimensions(columns...)
	if err != nil {
		return DataFrame{Err: err}
	}
	df := DataFrame{
		columns: columns,
		ncols:   ncols,
		nrows:   nrows,
	}

	colnames := df.Names()
	fixColnames(colnames)
	for i, colname := range colnames {
		df.columns[i].Name = colname
	}
	return df
}

// LoadMaps 从 map 数组加载 DataFrame。
func LoadMaps(maps []map[string]interface{}, options ...LoadOption) DataFrame {
	if len(maps) == 0 {
		return DataFrame{Err: fmt.Errorf("load maps: 空数组")}
	}
	inStrSlice := func(i string, s []string) bool {
		for _, v := range s {
			if v == i {
				return true
			}
		}
		return false
	}

	var colnames []string
	for _, v := range maps {
		for k := range v {
			if exists := inStrSlice(k, colnames); !exists {
				colnames = append(colnames, k)
			}
		}
	}
	sort.Strings(colnames)
	records := make([][]string, len(maps)+1)
	records[0] = colnames
	for k, m := range maps {
		row := make([]string, len(colnames))
		for i, colname := range colnames {
			element := ""
			val, ok := m[colname]
			if ok {
				element = fmt.Sprint(val)
			}
			row[i] = element
		}
		records[k+1] = row
	}
	return LoadRecords(records, options...)
}

type Matrix interface {
	Dims() (r, c int)
	At(i, j int) float64
}

// LoadMatrix 从矩阵加载 DataFrame。
func LoadMatrix(mat Matrix) DataFrame {
	nrows, ncols := mat.Dims()
	columns := make([]series.Series, ncols)
	for i := 0; i < ncols; i++ {
		floats := make([]float64, nrows)
		for j := 0; j < nrows; j++ {
			floats[j] = mat.At(j, i)
		}
		columns[i] = series.Floats(floats)
	}
	nrows, ncols, err := checkColumnsDimensions(columns...)
	if err != nil {
		return DataFrame{Err: err}
	}
	df := DataFrame{
		columns: columns,
		ncols:   ncols,
		nrows:   nrows,
	}
	colnames := df.Names()
	fixColnames(colnames)
	for i, colname := range colnames {
		df.columns[i].Name = colname
	}
	return df
}

// ReadCSV 从 CSV 格式的输入读取 DataFrame。
func ReadCSV(r io.Reader, options ...LoadOption) DataFrame {
	csvReader := csv.NewReader(r)
	cfg := loadOptions{
		delimiter:  ',',
		lazyQuotes: false,
		comment:    0,
	}
	for _, option := range options {
		option(&cfg)
	}

	csvReader.Comma = cfg.delimiter
	csvReader.LazyQuotes = cfg.lazyQuotes
	csvReader.Comment = cfg.comment

	records, err := csvReader.ReadAll()
	if err != nil {
		return DataFrame{Err: err}
	}
	return LoadRecords(records, options...)
}

// ReadJSON 从 JSON 格式的输入读取 DataFrame。
func ReadJSON(r io.Reader, options ...LoadOption) DataFrame {
	var m []map[string]interface{}
	d := json.NewDecoder(r)
	d.UseNumber()
	err := d.Decode(&m)
	if err != nil {
		return DataFrame{Err: err}
	}
	return LoadMaps(m, options...)
}

// WriteOption 定义写操作的选项类型。
type WriteOption func(*writeOptions)

// writeOptions 包含写操作的选项。
type writeOptions struct {
	writeHeader bool
}

// WriteHeader 指定是否写入 CSV 或 JSON 文件的列头。
func WriteHeader(b bool) WriteOption {
	return func(c *writeOptions) {
		c.writeHeader = b
	}
}

// WriteCSV 将 DataFrame 写入 CSV 格式。
func (df DataFrame) WriteCSV(w io.Writer, options ...WriteOption) error {
	if df.Err != nil {
		return df.Err
	}

	cfg := writeOptions{
		writeHeader: true,
	}

	for _, option := range options {
		option(&cfg)
	}

	records := df.Records()
	if !cfg.writeHeader {
		records = records[1:]
	}

	return csv.NewWriter(w).WriteAll(records)
}

// WriteJSON 将 DataFrame 写入 JSON 格式。
func (df DataFrame) WriteJSON(w io.Writer) error {
	if df.Err != nil {
		return df.Err
	}
	return json.NewEncoder(w).Encode(df.Maps())
}

// remainder 包含 HTML 表格中的元素索引、文本和行数。
type remainder struct {
	index int
	text  string
	nrows int
}

// readRows 从 HTML 表格中读取行。
func readRows(trs []*html.Node) [][]string {
	rems := []remainder{}
	rows := [][]string{}
	for _, tr := range trs {
		xrems := []remainder{}
		row := []string{}
		index := 0
		text := ""
		for j, td := 0, tr.FirstChild; td != nil; j, td = j+1, td.NextSibling {
			if td.Type == html.ElementNode && td.DataAtom == atom.Td {

				for len(rems) > 0 {
					v := rems[0]
					if v.index > index {
						break
					}
					v, rems = rems[0], rems[1:]
					row = append(row, v.text)
					if v.nrows > 1 {
						xrems = append(xrems, remainder{v.index, v.text, v.nrows - 1})
					}
					index++
				}

				rowspan, colspan := 1, 1
				for _, attr := range td.Attr {
					switch attr.Key {
					case "rowspan":
						if k, err := strconv.Atoi(attr.Val); err == nil {
							rowspan = k
						}
					case "colspan":
						if k, err := strconv.Atoi(attr.Val); err == nil {
							colspan = k
						}
					}
				}
				for c := td.FirstChild; c != nil; c = c.NextSibling {
					if c.Type == html.TextNode {
						text = strings.TrimSpace(c.Data)
					}
				}

				for k := 0; k < colspan; k++ {
					row = append(row, text)
					if rowspan > 1 {
						xrems = append(xrems, remainder{index, text, rowspan - 1})
					}
					index++
				}
			}
		}
		for j := 0; j < len(rems); j++ {
			v := rems[j]
			row = append(row, v.text)
			if v.nrows > 1 {
				xrems = append(xrems, remainder{v.index, v.text, v.nrows - 1})
			}
		}
		rows = append(rows, row)
		rems = xrems
	}
	for len(rems) > 0 {
		xrems := []remainder{}
		row := []string{}
		for i := 0; i < len(rems); i++ {
			v := rems[i]
			row = append(row, v.text)
			if v.nrows > 1 {
				xrems = append(xrems, remainder{v.index, v.text, v.nrows - 1})
			}
		}
		rows = append(rows, row)
		rems = xrems
	}
	return rows
}

// ReadHTML 从 HTML 格式的输入读取多个 DataFrame。每个 DataFrame 对应一个 HTML 表格。
func ReadHTML(r io.Reader, options ...LoadOption) []DataFrame {
	var err error
	var dfs []DataFrame
	var doc *html.Node
	var f func(*html.Node)

	doc, err = html.Parse(r)
	if err != nil {
		return []DataFrame{DataFrame{Err: err}}
	}

	f = func(n *html.Node) {
		if n.Type == html.ElementNode && n.DataAtom == atom.Table {
			trs := []*html.Node{}
			for c := n.FirstChild; c != nil; c = c.NextSibling {
				if c.Type == html.ElementNode && c.DataAtom == atom.Tbody {
					for cc := c.FirstChild; cc != nil; cc = cc.NextSibling {
						if cc.Type == html.ElementNode && (cc.DataAtom == atom.Th || cc.DataAtom == atom.Tr) {
							trs = append(trs, cc)
						}
					}
				}
			}

			df := LoadRecords(readRows(trs), options...)
			if df.Err == nil {
				dfs = append(dfs, df)
			}
			return
		}

		for c := n.FirstChild; c != nil; c = c.NextSibling {
			f(c)
		}
	}

	f(doc)
	return dfs
}

// Names 返回 DataFrame 的列名。
func (df DataFrame) Names() []string {
	colnames := make([]string, df.ncols)
	for i, s := range df.columns {
		colnames[i] = s.Name
	}
	return colnames
}

// Types 返回 DataFrame 的列类型。
func (df DataFrame) Types() []series.Type {
	coltypes := make([]series.Type, df.ncols)
	for i, s := range df.columns {
		coltypes[i] = s.Type()
	}
	return coltypes
}

// SetNames 设置 DataFrame 的列名。
func (df DataFrame) SetNames(colnames ...string) error {
	if df.Err != nil {
		return df.Err
	}
	if len(colnames) != df.ncols {
		return fmt.Errorf("设置列名: 维度不匹配")
	}
	for k, s := range colnames {
		df.columns[k].Name = s
	}
	return nil
}

// Dims 返回 DataFrame 的行数和列数。
func (df DataFrame) Dims() (int, int) {
	return df.Nrow(), df.Ncol()
}

// Nrow 返回 DataFrame 的行数。
func (df DataFrame) Nrow() int {
	return df.nrows
}

// Ncol 返回 DataFrame 的列数。
func (df DataFrame) Ncol() int {
	return df.ncols
}

// Col 根据列名返回 DataFrame 的列。
func (df DataFrame) Col(colname string) series.Series {
	if df.Err != nil {
		return series.Series{Err: df.Err}
	}

	idx := findInStringSlice(colname, df.Names())
	if idx < 0 {
		return series.Series{Err: fmt.Errorf("未知列名")}
	}
	return df.columns[idx].Copy()
}

// InnerJoin 执行内连接操作，将两个 DataFrame 按照指定的键连接。
func (df DataFrame) InnerJoin(b DataFrame, keys ...string) DataFrame {
	if len(keys) == 0 {
		return DataFrame{Err: fmt.Errorf("未指定连接键")}
	}

	var iKeysA []int
	var iKeysB []int
	var errorArr []string
	for _, key := range keys {
		i := df.colIndex(key)
		if i < 0 {
			errorArr = append(errorArr, fmt.Sprintf("在左侧 DataFrame 中找不到键 %q", key))
		}
		iKeysA = append(iKeysA, i)
		j := b.colIndex(key)
		if j < 0 {
			errorArr = append(errorArr, fmt.Sprintf("在右侧 DataFrame 中找不到键 %q", key))
		}
		iKeysB = append(iKeysB, j)
	}
	if len(errorArr) != 0 {
		return DataFrame{Err: fmt.Errorf(strings.Join(errorArr, "\n"))}
	}

	aCols := df.columns
	bCols := b.columns

	var newCols []series.Series
	for _, i := range iKeysA {
		newCols = append(newCols, aCols[i].Empty())
	}
	var iNotKeysA []int
	for i := 0; i < df.ncols; i++ {
		if !inIntSlice(i, iKeysA) {
			iNotKeysA = append(iNotKeysA, i)
			newCols = append(newCols, aCols[i].Empty())
		}
	}
	var iNotKeysB []int
	for i := 0; i < b.ncols; i++ {
		if !inIntSlice(i, iKeysB) {
			iNotKeysB = append(iNotKeysB, i)
			newCols = append(newCols, bCols[i].Empty())
		}
	}

	for i := 0; i < df.nrows; i++ {
		for j := 0; j < b.nrows; j++ {
			match := true
			for k := range keys {
				aElem := aCols[iKeysA[k]].Elem(i)
				bElem := bCols[iKeysB[k]].Elem(j)
				match = match && aElem.Eq(bElem)
			}
			if match {
				ii := 0
				for _, k := range iKeysA {
					elem := aCols[k].Elem(i)
					newCols[ii].Append(elem)
					ii++
				}
				for _, k := range iNotKeysA {
					elem := aCols[k].Elem(i)
					newCols[ii].Append(elem)
					ii++
				}
				for _, k := range iNotKeysB {
					elem := bCols[k].Elem(j)
					newCols[ii].Append(elem)
					ii++
				}
			}
		}
	}
	return New(newCols...)
}

// LeftJoin 执行左连接操作，将两个 DataFrame 按照指定的键连接。
func (df DataFrame) LeftJoin(b DataFrame, keys ...string) DataFrame {
	if len(keys) == 0 {
		return DataFrame{Err: fmt.Errorf("未指定连接键")}
	}

	var iKeysA []int
	var iKeysB []int
	var errorArr []string
	for _, key := range keys {
		i := df.colIndex(key)
		if i < 0 {
			errorArr = append(errorArr, fmt.Sprintf("在左侧 DataFrame 中找不到键 %q", key))
		}
		iKeysA = append(iKeysA, i)
		j := b.colIndex(key)
		if j < 0 {
			errorArr = append(errorArr, fmt.Sprintf("在右侧 DataFrame 中找不到键 %q", key))
		}
		iKeysB = append(iKeysB, j)
	}
	if len(errorArr) != 0 {
		return DataFrame{Err: fmt.Errorf(strings.Join(errorArr, "\n"))}
	}

	aCols := df.columns
	bCols := b.columns

	var newCols []series.Series
	for _, i := range iKeysA {
		newCols = append(newCols, aCols[i].Empty())
	}
	var iNotKeysA []int
	for i := 0; i < df.ncols; i++ {
		if !inIntSlice(i, iKeysA) {
			iNotKeysA = append(iNotKeysA, i)
			newCols = append(newCols, aCols[i].Empty())
		}
	}
	var iNotKeysB []int
	for i := 0; i < b.ncols; i++ {
		if !inIntSlice(i, iKeysB) {
			iNotKeysB = append(iNotKeysB, i)
			newCols = append(newCols, bCols[i].Empty())
		}
	}

	for i := 0; i < df.nrows; i++ {
		matched := false
		for j := 0; j < b.nrows; j++ {
			match := true
			for k := range keys {
				aElem := aCols[iKeysA[k]].Elem(i)
				bElem := bCols[iKeysB[k]].Elem(j)
				match = match && aElem.Eq(bElem)
			}
			if match {
				matched = true
				ii := 0
				for _, k := range iKeysA {
					elem := aCols[k].Elem(i)
					newCols[ii].Append(elem)
					ii++
				}
				for _, k := range iNotKeysA {
					elem := aCols[k].Elem(i)
					newCols[ii].Append(elem)
					ii++
				}
				for _, k := range iNotKeysB {
					elem := bCols[k].Elem(j)
					newCols[ii].Append(elem)
					ii++
				}
			}
		}
		if !matched {
			ii := 0
			for _, k := range iKeysA {
				elem := aCols[k].Elem(i)
				newCols[ii].Append(elem)
				ii++
			}
			for _, k := range iNotKeysA {
				elem := aCols[k].Elem(i)
				newCols[ii].Append(elem)
				ii++
			}
			for range iNotKeysB {
				newCols[ii].Append(nil)
				ii++
			}
		}
	}
	return New(newCols...)
}

// RightJoin 执行右连接操作，将两个 DataFrame 按照指定的键连接。
func (df DataFrame) RightJoin(b DataFrame, keys ...string) DataFrame {
	if len(keys) == 0 {
		return DataFrame{Err: fmt.Errorf("未指定连接键")}
	}

	var iKeysA []int
	var iKeysB []int
	var errorArr []string
	for _, key := range keys {
		i := df.colIndex(key)
		if i < 0 {
			errorArr = append(errorArr, fmt.Sprintf("在左侧 DataFrame 中找不到键 %q", key))
		}
		iKeysA = append(iKeysA, i)
		j := b.colIndex(key)
		if j < 0 {
			errorArr = append(errorArr, fmt.Sprintf("在右侧 DataFrame 中找不到键 %q", key))
		}
		iKeysB = append(iKeysB, j)
	}
	if len(errorArr) != 0 {
		return DataFrame{Err: fmt.Errorf(strings.Join(errorArr, "\n"))}
	}

	aCols := df.columns
	bCols := b.columns

	var newCols []series.Series
	for _, i := range iKeysA {
		newCols = append(newCols, aCols[i].Empty())
	}
	var iNotKeysA []int
	for i := 0; i < df.ncols; i++ {
		if !inIntSlice(i, iKeysA) {
			iNotKeysA = append(iNotKeysA, i)
			newCols = append(newCols, aCols[i].Empty())
		}
	}
	var iNotKeysB []int
	for i := 0; i < b.ncols; i++ {
		if !inIntSlice(i, iKeysB) {
			iNotKeysB = append(iNotKeysB, i)
			newCols = append(newCols, bCols[i].Empty())
		}
	}

	var yesmatched []struct{ i, j int }
	var nonmatched []int
	for j := 0; j < b.nrows; j++ {
		matched := false
		for i := 0; i < df.nrows; i++ {
			match := true
			for k := range keys {
				aElem := aCols[iKeysA[k]].Elem(i)
				bElem := bCols[iKeysB[k]].Elem(j)
				match = match && aElem.Eq(bElem)
			}
			if match {
				matched = true
				yesmatched = append(yesmatched, struct{ i, j int }{i, j})
			}
		}
		if !matched {
			nonmatched = append(nonmatched, j)
		}
	}
	for _, v := range yesmatched {
		i := v.i
		j := v.j
		ii := 0
		for _, k := range iKeysA {
			elem := aCols[k].Elem(i)
			newCols[ii].Append(elem)
			ii++
		}
		for _, k := range iNotKeysA {
			elem := aCols[k].Elem(i)
			newCols[ii].Append(elem)
			ii++
		}
		for _, k := range iNotKeysB {
			elem := bCols[k].Elem(j)
			newCols[ii].Append(elem)
			ii++
		}
	}
	for _, j := range nonmatched {
		ii := 0
		for _, k := range iKeysB {
			elem := bCols[k].Elem(j)
			newCols[ii].Append(elem)
			ii++
		}
		for range iNotKeysA {
			newCols[ii].Append(nil)
			ii++
		}
		for _, k := range iNotKeysB {
			elem := bCols[k].Elem(j)
			newCols[ii].Append(elem)
			ii++
		}
	}
	return New(newCols...)
}

// OuterJoin 执行外连接操作，将两个 DataFrame 按照指定的键连接。
func (df DataFrame) OuterJoin(b DataFrame, keys ...string) DataFrame {
	if len(keys) == 0 {
		return DataFrame{Err: fmt.Errorf("未指定连接键")}
	}

	var iKeysA []int
	var iKeysB []int
	var errorArr []string
	for _, key := range keys {
		i := df.colIndex(key)
		if i < 0 {
			errorArr = append(errorArr, fmt.Sprintf("在左侧 DataFrame 中找不到键 %q", key))
		}
		iKeysA = append(iKeysA, i)
		j := b.colIndex(key)
		if j < 0 {
			errorArr = append(errorArr, fmt.Sprintf("在右侧 DataFrame 中找不到键 %q", key))
		}
		iKeysB = append(iKeysB, j)
	}
	if len(errorArr) != 0 {
		return DataFrame{Err: fmt.Errorf(strings.Join(errorArr, "\n"))}
	}

	aCols := df.columns
	bCols := b.columns

	var newCols []series.Series
	for _, i := range iKeysA {
		newCols = append(newCols, aCols[i].Empty())
	}
	var iNotKeysA []int
	for i := 0; i < df.ncols; i++ {
		if !inIntSlice(i, iKeysA) {
			iNotKeysA = append(iNotKeysA, i)
			newCols = append(newCols, aCols[i].Empty())
		}
	}
	var iNotKeysB []int
	for i := 0; i < b.ncols; i++ {
		if !inIntSlice(i, iKeysB) {
			iNotKeysB = append(iNotKeysB, i)
			newCols = append(newCols, bCols[i].Empty())
		}
	}

	for i := 0; i < df.nrows; i++ {
		matched := false
		for j := 0; j < b.nrows; j++ {
			match := true
			for k := range keys {
				aElem := aCols[iKeysA[k]].Elem(i)
				bElem := bCols[iKeysB[k]].Elem(j)
				match = match && aElem.Eq(bElem)
			}
			if match {
				matched = true
				ii := 0
				for _, k := range iKeysA {
					elem := aCols[k].Elem(i)
					newCols[ii].Append(elem)
					ii++
				}
				for _, k := range iNotKeysA {
					elem := aCols[k].Elem(i)
					newCols[ii].Append(elem)
					ii++
				}
				for _, k := range iNotKeysB {
					elem := bCols[k].Elem(j)
					newCols[ii].Append(elem)
					ii++
				}
			}
		}
		if !matched {
			ii := 0
			for _, k := range iKeysA {
				elem := aCols[k].Elem(i)
				newCols[ii].Append(elem)
				ii++
			}
			for _, k := range iNotKeysA {
				elem := aCols[k].Elem(i)
				newCols[ii].Append(elem)
				ii++
			}
			for range iNotKeysB {
				newCols[ii].Append(nil)
				ii++
			}
		}
	}
	for j := 0; j < b.nrows; j++ {
		matched := false
		for i := 0; i < df.nrows; i++ {
			match := true
			for k := range keys {
				aElem := aCols[iKeysA[k]].Elem(i)
				bElem := bCols[iKeysB[k]].Elem(j)
				match = match && aElem.Eq(bElem)
			}
			if match {
				matched = true
			}
		}
		if !matched {
			ii := 0
			for _, k := range iKeysB {
				elem := bCols[k].Elem(j)
				newCols[ii].Append(elem)
				ii++
			}
			for range iNotKeysA {
				newCols[ii].Append(nil)
				ii++
			}
			for _, k := range iNotKeysB {
				elem := bCols[k].Elem(j)
				newCols[ii].Append(elem)
				ii++
			}
		}
	}
	return New(newCols...)
}

// CrossJoin 执行交叉连接操作，返回两个 DataFrame 的笛卡尔积。
func (df DataFrame) CrossJoin(b DataFrame) DataFrame {
	aCols := df.columns
	bCols := b.columns

	var newCols []series.Series
	for i := 0; i < df.ncols; i++ {
		newCols = append(newCols, aCols[i].Empty())
	}
	for i := 0; i < b.ncols; i++ {
		newCols = append(newCols, bCols[i].Empty())
	}

	for i := 0; i < df.nrows; i++ {
		for j := 0; j < b.nrows; j++ {
			for ii := 0; ii < df.ncols; ii++ {
				elem := aCols[ii].Elem(i)
				newCols[ii].Append(elem)
			}
			for ii := 0; ii < b.ncols; ii++ {
				jj := ii + df.ncols
				elem := bCols[ii].Elem(j)
				newCols[jj].Append(elem)
			}
		}
	}
	return New(newCols...)
}

// colIndex 返回列名称在 DataFrame 中的索引，如果找不到返回 -1。
func (df DataFrame) colIndex(s string) int {
	for k, v := range df.Names() {
		if v == s {
			return k
		}
	}
	return -1
}

// Records 返回 DataFrame 的记录，以二维字符串切片形式返回。
func (df DataFrame) Records() [][]string {
	var records [][]string
	records = append(records, df.Names())
	if df.ncols == 0 || df.nrows == 0 {
		return records
	}
	var tRecords [][]string
	for _, col := range df.columns {
		tRecords = append(tRecords, col.Records())
	}
	records = append(records, transposeRecords(tRecords)...)
	return records
}

// Maps 返回 DataFrame 的记录，以一维字符串切片映射为键值对形式返回。
func (df DataFrame) Maps() []map[string]interface{} {
	maps := make([]map[string]interface{}, df.nrows)
	colnames := df.Names()
	for i := 0; i < df.nrows; i++ {
		m := make(map[string]interface{})
		for k, v := range colnames {
			val := df.columns[k].Val(i)
			m[v] = val
		}
		maps[i] = m
	}
	return maps
}

// Elem 返回指定行和列位置的 DataFrame 单元格元素。
func (df DataFrame) Elem(r, c int) series.Element {
	return df.columns[c].Elem(r)
}

// fixColnames 修复列名，处理重复和缺失的列名，保证列名的唯一性。
func fixColnames(colnames []string) {
	dupnamesidx := make(map[string][]int)
	var missingnames []int
	for i := 0; i < len(colnames); i++ {
		a := colnames[i]
		if a == "" {
			missingnames = append(missingnames, i)
			continue
		}
		dupnamesidx[a] = append(dupnamesidx[a], i)
	}

	for k, places := range dupnamesidx {
		if len(places) < 2 {
			delete(dupnamesidx, k)
		}
	}

	counter := 0
	for _, i := range missingnames {
		proposedName := fmt.Sprintf("X%d", counter)
		for findInStringSlice(proposedName, colnames) != -1 {
			counter++
			proposedName = fmt.Sprintf("X%d", counter)
		}
		colnames[i] = proposedName
		counter++
	}

	var keys []string
	for k := range dupnamesidx {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, name := range keys {
		idx := dupnamesidx[name]
		if name == "" {
			name = "X"
		}
		counter := 0
		for _, i := range idx {
			proposedName := fmt.Sprintf("%s_%d", name, counter)
			for findInStringSlice(proposedName, colnames) != -1 {
				counter++
				proposedName = fmt.Sprintf("%s_%d", name, counter)
			}
			colnames[i] = proposedName
			counter++
		}
	}
}

// findInStringSlice 在字符串切片中查找指定字符串的索引，找不到返回 -1。
func findInStringSlice(str string, s []string) int {
	for i, e := range s {
		if e == str {
			return i
		}
	}
	return -1
}

// parseSelectIndexes 解析选择的索引，返回索引的整数切片。
func parseSelectIndexes(l int, indexes SelectIndexes, colnames []string) ([]int, error) {
	var idx []int
	switch indexes.(type) {
	case []int:
		idx = indexes.([]int)
	case int:
		idx = []int{indexes.(int)}
	case []bool:
		bools := indexes.([]bool)
		if len(bools) != l {
			return nil, fmt.Errorf("索引错误：索引维度不匹配")
		}
		for i, b := range bools {
			if b {
				idx = append(idx, i)
			}
		}
	case string:
		s := indexes.(string)
		i := findInStringSlice(s, colnames)
		if i < 0 {
			return nil, fmt.Errorf("无法选择列：找不到列名 %q", s)
		}
		idx = append(idx, i)
	case []string:
		xs := indexes.([]string)
		for _, s := range xs {
			i := findInStringSlice(s, colnames)
			if i < 0 {
				return nil, fmt.Errorf("无法选择列：找不到列名 %q", s)
			}
			idx = append(idx, i)
		}
	case series.Series:
		s := indexes.(series.Series)
		if err := s.Err; err != nil {
			return nil, fmt.Errorf("索引错误：新值存在错误：%v", err)
		}
		if s.HasNaN() {
			return nil, fmt.Errorf("索引错误：索引包含 NaN")
		}
		switch s.Type() {
		case series.Int:
			return s.Int()
		case series.Bool:
			bools, err := s.Bool()
			if err != nil {
				return nil, fmt.Errorf("索引错误：%v", err)
			}
			return parseSelectIndexes(l, bools, colnames)
		case series.String:
			xs := indexes.(series.Series).Records()
			return parseSelectIndexes(l, xs, colnames)
		default:
			return nil, fmt.Errorf("索引错误：未知的索引模式")
		}
	default:
		return nil, fmt.Errorf("索引错误：未知的索引模式")
	}
	return idx, nil
}

// findType 查找字符串切片的元素类型，返回对应的 series.Type。
func findType(arr []string) (series.Type, error) {
	var hasFloats, hasInts, hasBools, hasStrings bool
	for _, str := range arr {
		if str == "" || str == "NaN" {
			continue
		}
		if _, err := strconv.Atoi(str); err == nil {
			hasInts = true
			continue
		}
		if _, err := strconv.ParseFloat(str, 64); err == nil {
			hasFloats = true
			continue
		}
		if str == "true" || str == "false" {
			hasBools = true
			continue
		}
		hasStrings = true
	}

	switch {
	case hasStrings:
		return series.String, nil
	case hasBools:
		return series.Bool, nil
	case hasFloats:
		return series.Float, nil
	case hasInts:
		return series.Int, nil
	default:
		return series.String, fmt.Errorf("无法检测到类型")
	}
}

// transposeRecords 转置二维字符串切片。
func transposeRecords(x [][]string) [][]string {
	n := len(x)
	if n == 0 {
		return x
	}
	m := len(x[0])
	y := make([][]string, m)
	for i := 0; i < m; i++ {
		z := make([]string, n)
		for j := 0; j < n; j++ {
			z[j] = x[j][i]
		}
		y[i] = z
	}
	return y
}

// inIntSlice 检查整数是否存在于整数切片中。
func inIntSlice(i int, is []int) bool {
	for _, v := range is {
		if v == i {
			return true
		}
	}
	return false
}

// Describe 返回 DataFrame 的描述性统计信息。
func (df DataFrame) Describe() DataFrame {
	labels := series.Strings([]string{
		"平均值",
		"中位数",
		"标准差",
		"最小值",
		"25%",
		"50%",
		"75%",
		"最大值",
	})
	labels.Name = "列名"

	ss := []series.Series{labels}

	for _, col := range df.columns {
		var newCol series.Series
		switch col.Type() {
		case series.String:
			newCol = series.New([]string{
				"-",
				"-",
				"-",
				col.MinStr(),
				"-",
				"-",
				"-",
				col.MaxStr(),
			},
				col.Type(),
				col.Name,
			)
		case series.Bool:
			fallthrough
		case series.Float:
			fallthrough
		case series.Int:
			newCol = series.New([]float64{
				col.Mean(),
				col.Median(),
				col.StdDev(),
				col.Min(),
				col.Quantile(0.25),
				col.Quantile(0.50),
				col.Quantile(0.75),
				col.Max(),
			},
				series.Float,
				col.Name,
			)
		}
		ss = append(ss, newCol)
	}

	ddf := New(ss...)
	return ddf
}
