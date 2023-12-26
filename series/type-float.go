package series

import (
	"fmt"
	"math"
	"strconv"
)

// floatElement 表示一个带有 float64 值的元素。
type floatElement struct {
	e   float64 // 实际的 float 值
	nan bool    // 标志，表示值是否为 NaN（非数值）
}

// 确保 floatElement 实现了 Element 接口。
var _ Element = (*floatElement)(nil)

// Set 根据输入值的类型设置 floatElement 的值。
func (e *floatElement) Set(value interface{}) {
	e.nan = false // 重置 nan 标志
	switch val := value.(type) {
	case string:
		if val == "NaN" {
			e.nan = true
			return
		}
		f, err := strconv.ParseFloat(value.(string), 64)
		if err != nil {
			e.nan = true
			return
		}
		e.e = f
	case int:
		e.e = float64(val)
	case float64:
		e.e = float64(val)
	case bool:
		b := val
		if b {
			e.e = 1
		} else {
			e.e = 0
		}
	case Element:
		e.e = val.Float()
	default:
		e.nan = true
		return
	}
}

// Copy 返回 floatElement 的副本。
func (e floatElement) Copy() Element {
	if e.IsNA() {
		return &floatElement{0.0, true}
	}
	return &floatElement{e.e, false}
}

// IsNA 返回是否为缺失值（NaN）。
func (e floatElement) IsNA() bool {
	if e.nan || math.IsNaN(e.e) {
		return true
	}
	return false
}

// Type 返回元素的类型。
func (e floatElement) Type() Type {
	return Float
}

// Val 返回元素的值。
func (e floatElement) Val() ElementValue {
	if e.IsNA() {
		return nil
	}
	return float64(e.e)
}

// String 返回元素的字符串表示。
func (e floatElement) String() string {
	if e.IsNA() {
		return "NaN"
	}
	return fmt.Sprintf("%f", e.e)
}

// Int 将元素转换为整数。
func (e floatElement) Int() (int, error) {
	if e.IsNA() {
		return 0, fmt.Errorf("无法将 NaN 转换为整数")
	}
	f := e.e
	if math.IsInf(f, 1) || math.IsInf(f, -1) {
		return 0, fmt.Errorf("无法将 Inf 转换为整数")
	}
	if math.IsNaN(f) {
		return 0, fmt.Errorf("无法将 NaN 转换为整数")
	}
	return int(f), nil
}

// Float 返回元素的 float64 值。
func (e floatElement) Float() float64 {
	if e.IsNA() {
		return math.NaN()
	}
	return float64(e.e)
}

// Bool 将元素转换为布尔值。
func (e floatElement) Bool() (bool, error) {
	if e.IsNA() {
		return false, fmt.Errorf("无法将 NaN 转换为布尔值")
	}
	switch e.e {
	case 1:
		return true, nil
	case 0:
		return false, nil
	}
	return false, fmt.Errorf("无法将浮点数 \"%v\" 转换为布尔值", e.e)
}

// Eq 比较两个元素是否相等。
func (e floatElement) Eq(elem Element) bool {
	f := elem.Float()
	if e.IsNA() || math.IsNaN(f) {
		return false
	}
	return e.e == f
}

// Neq 比较两个元素是否不相等。
func (e floatElement) Neq(elem Element) bool {
	f := elem.Float()
	if e.IsNA() || math.IsNaN(f) {
		return false
	}
	return e.e != f
}

// Less 比较两个元素是否小于。
func (e floatElement) Less(elem Element) bool {
	f := elem.Float()
	if e.IsNA() || math.IsNaN(f) {
		return false
	}
	return e.e < f
}

// LessEq 比较两个元素是否小于等于。
func (e floatElement) LessEq(elem Element) bool {
	f := elem.Float()
	if e.IsNA() || math.IsNaN(f) {
		return false
	}
	return e.e <= f
}

// Greater 比较两个元素是否大于。
func (e floatElement) Greater(elem Element) bool {
	f := elem.Float()
	if e.IsNA() || math.IsNaN(f) {
		return false
	}
	return e.e > f
}

// GreaterEq 比较两个元素是否大于等于。
func (e floatElement) GreaterEq(elem Element) bool {
	f := elem.Float()
	if e.IsNA() || math.IsNaN(f) {
		return false
	}
	return e.e >= f
}
