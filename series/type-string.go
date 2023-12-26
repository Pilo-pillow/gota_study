package series

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// stringElement 表示 Series 中的字符串元素。
type stringElement struct {
	e   string
	nan bool
}

// 强制 stringElement 结构实现 Element 接口。
var _ Element = (*stringElement)(nil)

// Set 方法将给定的值设置为字符串元素。
// 如果值为 "NaN"，则标记为 NaN。
func (e *stringElement) Set(value interface{}) {
	e.nan = false
	switch val := value.(type) {
	case string:
		e.e = string(val)
		if e.e == "NaN" {
			e.nan = true
			return
		}
	case int:
		e.e = strconv.Itoa(val)
	case float64:
		e.e = strconv.FormatFloat(value.(float64), 'f', 6, 64)
	case bool:
		b := value.(bool)
		if b {
			e.e = "true"
		} else {
			e.e = "false"
		}
	case Element:
		e.e = val.String()
	default:
		e.nan = true
		return
	}
}

// Copy 方法返回字符串元素的副本。
func (e stringElement) Copy() Element {
	if e.IsNA() {
		return &stringElement{"", true}
	}
	return &stringElement{e.e, false}
}

// IsNA 方法检查字符串元素是否为 NaN。
func (e stringElement) IsNA() bool {
	return e.nan
}

// Type 方法返回字符串元素的类型。
func (e stringElement) Type() Type {
	return String
}

// Val 方法返回字符串元素的值。
func (e stringElement) Val() ElementValue {
	if e.IsNA() {
		return nil
	}
	return string(e.e)
}

// String 方法返回字符串元素的字符串表示。
func (e stringElement) String() string {
	if e.IsNA() {
		return "NaN"
	}
	return string(e.e)
}

// Int 方法将字符串元素转换为整数。
func (e stringElement) Int() (int, error) {
	if e.IsNA() {
		return 0, fmt.Errorf("can't convert NaN to int")
	}
	return strconv.Atoi(e.e)
}

// Float 方法将字符串元素转换为浮点数。
func (e stringElement) Float() float64 {
	if e.IsNA() {
		return math.NaN()
	}
	f, err := strconv.ParseFloat(e.e, 64)
	if err != nil {
		return math.NaN()
	}
	return f
}

// Bool 方法将字符串元素转换为布尔值。
func (e stringElement) Bool() (bool, error) {
	if e.IsNA() {
		return false, fmt.Errorf("can't convert NaN to bool")
	}
	switch strings.ToLower(e.e) {
	case "true", "t", "1":
		return true, nil
	case "false", "f", "0":
		return false, nil
	}
	return false, fmt.Errorf("can't convert String \"%v\" to bool", e.e)
}

// Eq 方法检查字符串元素是否等于另一个元素。
func (e stringElement) Eq(elem Element) bool {
	if e.IsNA() || elem.IsNA() {
		return false
	}
	return e.e == elem.String()
}

// Neq 方法检查字符串元素是否不等于另一个元素。
func (e stringElement) Neq(elem Element) bool {
	if e.IsNA() || elem.IsNA() {
		return false
	}
	return e.e != elem.String()
}

// Less 方法检查字符串元素是否小于另一个元素。
func (e stringElement) Less(elem Element) bool {
	if e.IsNA() || elem.IsNA() {
		return false
	}
	return e.e < elem.String()
}

// LessEq 方法检查字符串元素是否小于或等于另一个元素。
func (e stringElement) LessEq(elem Element) bool {
	if e.IsNA() || elem.IsNA() {
		return false
	}
	return e.e <= elem.String()
}

// Greater 方法检查字符串元素是否大于另一个元素。
func (e stringElement) Greater(elem Element) bool {
	if e.IsNA() || elem.IsNA() {
		return false
	}
	return e.e > elem.String()
}

// GreaterEq 方法检查字符串元素是否大于或等于另一个元素。
func (e stringElement) GreaterEq(elem Element) bool {
	if e.IsNA() || elem.IsNA() {
		return false
	}
	return e.e >= elem.String()
}
