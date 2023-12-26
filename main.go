package main

import (
	"fmt"
	"stream/go-sdk/test/gota_study/dataframe"
	"stream/go-sdk/test/gota_study/series"
	"strings"
)

func main() {
	dataFrame := dataframe.New(
		series.New([]string{"Alice", "Bob", "Charlie"}, series.String, "Name"),
		series.New([]int{25, 30, 35}, series.Int, "Age"),
		series.New([]int{50000, 60000, 70000}, series.Int, "Salary"),
	)
	fmt.Println(dataFrame)

	mapData := []map[string]interface{}{
		{"Name": "Alice", "Age": 25, "Salary": 50000},
		{"Name": "Bob", "Age": 30, "Salary": 60000},
		{"Name": "Charlie", "Age": 35, "Salary": 70000},
	}
	dataFrameMap := dataframe.LoadMaps(mapData)
	fmt.Println(dataFrameMap)

	csvData := `
Name,Age,Colour,Height(ft)
刘备,44,Red,6.7
关羽,40,Blue,5.7
张飞,40,Blue,5.7
曹操,40,Blue,5.7`
	dataFrameCSV := dataframe.ReadCSV(strings.NewReader(csvData))
	fmt.Println(dataFrameCSV)

	type Person struct {
		Name   string
		Age    int
		Salary int
	}

	structData := []Person{
		{"Alice", 25, 50000},
		{"Bob", 30, 60000},
		{"Charlie", 35, 70000},
	}
	dataFrameStruct := dataframe.LoadStructs(structData)
	fmt.Println(dataFrameStruct)

	jsonData := `[
  {
    "Name": "刘备",
    "Age": 44,
    "Colour": "Red",
    "Height(ft)": 6.7
  },
  {
    "Name": "关羽",
    "Age": 40,
    "Colour": "Blue",
    "Height(ft)": 5.7
  }
]`
	dataFrameJSON := dataframe.ReadJSON(strings.NewReader(jsonData))
	fmt.Println(dataFrameJSON)

	rowN, colN := dataFrame.Dims()
	fmt.Println("dataFrame的维度为:(rowN*colN)", rowN, colN)
	types := dataFrame.Types()
	fmt.Println("dataFrame 的数据类型为:", types)
	names := dataFrame.Names()
	fmt.Println("dataFrame 的列名为:", names)
	nrow := dataFrame.Nrow()
	fmt.Println("dataFrame 的行数为:", nrow)
	ncol := dataFrame.Ncol()
	fmt.Println("dataFrame 的列数为:", ncol)
	s := series.New([]int{25, 30, 35}, series.Int, "Age")
	isNaN := s.IsNaN()
	fmt.Println("s 是否有NaN值:", isNaN)
	mean := s.Mean()
	fmt.Println("s 的平均值为:", mean)
	cpSeries := s.Copy()
	fmt.Println("copySeries 的数据为:", cpSeries)
	hasNaN := s.HasNaN()
	fmt.Println("s 是否有NaN值:", hasNaN)
	records := s.Records()
	fmt.Println("s 的记录为:", records)
	s.Len()
	for i, m := range dataFrame.Maps() {
		fmt.Println(i)
		fmt.Println(m)
	}
}
