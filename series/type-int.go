package series

import (
	"fmt"
	"math"
	"strconv"
)

// intElement 表示 Series 中的整数元素。
type intElement struct {
	e   int
	nan bool
}

// 强制 intElement 结构实现 Element 接口。
var _ Element = (*intElement)(nil)

// Set 方法将给定的值设置为整数元素。
// 如果值为 "NaN" 或转换失败，则标记为 NaN。
func (e *intElement) Set(value interface{}) {
	e.nan = false
	switch val := value.(type) {
	case string:
		if val == "NaN" {
			e.nan = true
			return
		}
		i, err := strconv.Atoi(value.(string))
		if err != nil {
			e.nan = true
			return
		}
		e.e = i
	case int:
		e.e = val
	case float64:
		f := val
		if math.IsNaN(f) ||
			math.IsInf(f, 0) ||
			math.IsInf(f, 1) {
			e.nan = true
			return
		}
		e.e = int(f)
	case bool:
		b := val
		if b {
			e.e = 1
		} else {
			e.e = 0
		}
	case Element:
		v, err := val.Int()
		if err != nil {
			e.nan = true
			return
		}
		e.e = v
	default:
		e.nan = true
		return
	}
}

// Copy 方法返回整数元素的副本。
func (e intElement) Copy() Element {
	if e.IsNA() {
		return &intElement{0, true}
	}
	return &intElement{e.e, false}
}

// IsNA 方法检查整数元素是否为 NaN。
func (e intElement) IsNA() bool {
	return e.nan
}

// Type 方法返回整数元素的类型。
func (e intElement) Type() Type {
	return Int
}

// Val 方法返回整数元素的值。
func (e intElement) Val() ElementValue {
	if e.IsNA() {
		return nil
	}
	return e.e
}

// String 方法返回整数元素的字符串表示。
func (e intElement) String() string {
	if e.IsNA() {
		return "NaN"
	}
	return fmt.Sprint(e.e)
}

// Int 方法返回整数元素的整数值。
func (e intElement) Int() (int, error) {
	if e.IsNA() {
		return 0, fmt.Errorf("can't convert NaN to int")
	}
	return int(e.e), nil
}

// Float 方法将整数元素转换为浮点数。
func (e intElement) Float() float64 {
	if e.IsNA() {
		return math.NaN()
	}
	return float64(e.e)
}

// Bool 方法将整数元素转换为布尔值。
func (e intElement) Bool() (bool, error) {
	if e.IsNA() {
		return false, fmt.Errorf("can't convert NaN to bool")
	}
	switch e.e {
	case 1:
		return true, nil
	case 0:
		return false, nil
	}
	return false, fmt.Errorf("can't convert Int \"%v\" to bool", e.e)
}

// Eq 方法检查整数元素是否等于另一个元素。
func (e intElement) Eq(elem Element) bool {
	i, err := elem.Int()
	if err != nil || e.IsNA() {
		return false
	}
	return e.e == i
}

// Neq 方法检查整数元素是否不等于另一个元素。
func (e intElement) Neq(elem Element) bool {
	i, err := elem.Int()
	if err != nil || e.IsNA() {
		return false
	}
	return e.e != i
}

// Less 方法检查整数元素是否小于另一个元素。
func (e intElement) Less(elem Element) bool {
	i, err := elem.Int()
	if err != nil || e.IsNA() {
		return false
	}
	return e.e < i
}

// LessEq 方法检查整数元素是否小于或等于另一个元素。
func (e intElement) LessEq(elem Element) bool {
	i, err := elem.Int()
	if err != nil || e.IsNA() {
		return false
	}
	return e.e <= i
}

// Greater 方法检查整数元素是否大于另一个元素。
func (e intElement) Greater(elem Element) bool {
	i, err := elem.Int()
	if err != nil || e.IsNA() {
		return false
	}
	return e.e > i
}

// GreaterEq 方法检查整数元素是否大于或等于另一个元素。
func (e intElement) GreaterEq(elem Element) bool {
	i, err := elem.Int()
	if err != nil || e.IsNA() {
		return false
	}
	return e.e >= i
}
