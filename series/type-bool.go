package series

import (
	"fmt"
	"math"
	"strings"
)

// boolElement 表示 Series 中的布尔元素。
type boolElement struct {
	e   bool
	nan bool
}

// 强制 boolElement 结构实现 Element 接口。
var _ Element = (*boolElement)(nil)

// Set 方法将给定的值设置为布尔元素。
// 如果值为 "NaN" 或转换失败，则标记为 NaN。
func (e *boolElement) Set(value interface{}) {
	e.nan = false
	switch val := value.(type) {
	case string:
		if val == "NaN" {
			e.nan = true
			return
		}
		switch strings.ToLower(value.(string)) {
		case "true", "t", "1":
			e.e = true
		case "false", "f", "0":
			e.e = false
		default:
			e.nan = true
			return
		}
	case int:
		switch val {
		case 1:
			e.e = true
		case 0:
			e.e = false
		default:
			e.nan = true
			return
		}
	case float64:
		switch val {
		case 1:
			e.e = true
		case 0:
			e.e = false
		default:
			e.nan = true
			return
		}
	case bool:
		e.e = val
	case Element:
		b, err := value.(Element).Bool()
		if err != nil {
			e.nan = true
			return
		}
		e.e = b
	default:
		e.nan = true
		return
	}
}

// Copy 方法返回布尔元素的副本。
func (e boolElement) Copy() Element {
	if e.IsNA() {
		return &boolElement{false, true}
	}
	return &boolElement{e.e, false}
}

// IsNA 方法检查布尔元素是否为 NaN。
func (e boolElement) IsNA() bool {
	return e.nan
}

// Type 方法返回布尔元素的类型。
func (e boolElement) Type() Type {
	return Bool
}

// Val 方法返回布尔元素的值。
func (e boolElement) Val() ElementValue {
	if e.IsNA() {
		return nil
	}
	return bool(e.e)
}

// String 方法返回布尔元素的字符串表示。
func (e boolElement) String() string {
	if e.IsNA() {
		return "NaN"
	}
	if e.e {
		return "true"
	}
	return "false"
}

// Int 方法将布尔元素转换为整数。
func (e boolElement) Int() (int, error) {
	if e.IsNA() {
		return 0, fmt.Errorf("can't convert NaN to int")
	}
	if e.e {
		return 1, nil
	}
	return 0, nil
}

// Float 方法将布尔元素转换为浮点数。
func (e boolElement) Float() float64 {
	if e.IsNA() {
		return math.NaN()
	}
	if e.e {
		return 1.0
	}
	return 0.0
}

// Bool 方法返回布尔元素的布尔值。
func (e boolElement) Bool() (bool, error) {
	if e.IsNA() {
		return false, fmt.Errorf("can't convert NaN to bool")
	}
	return bool(e.e), nil
}

// Eq 方法检查布尔元素是否等于另一个元素。
func (e boolElement) Eq(elem Element) bool {
	b, err := elem.Bool()
	if err != nil || e.IsNA() {
		return false
	}
	return e.e == b
}

// Neq 方法检查布尔元素是否不等于另一个元素。
func (e boolElement) Neq(elem Element) bool {
	b, err := elem.Bool()
	if err != nil || e.IsNA() {
		return false
	}
	return e.e != b
}

// Less 方法检查布尔元素是否小于另一个元素。
func (e boolElement) Less(elem Element) bool {
	b, err := elem.Bool()
	if err != nil || e.IsNA() {
		return false
	}
	return !e.e && b
}

// LessEq 方法检查布尔元素是否小于或等于另一个元素。
func (e boolElement) LessEq(elem Element) bool {
	b, err := elem.Bool()
	if err != nil || e.IsNA() {
		return false
	}
	return !e.e || b
}

// Greater 方法检查布尔元素是否大于另一个元素。
func (e boolElement) Greater(elem Element) bool {
	b, err := elem.Bool()
	if err != nil || e.IsNA() {
		return false
	}
	return e.e && !b
}

// GreaterEq 方法检查布尔元素是否大于或等于另一个元素。
func (e boolElement) GreaterEq(elem Element) bool {
	b, err := elem.Bool()
	if err != nil || e.IsNA() {
		return false
	}
	return e.e || !b
}
