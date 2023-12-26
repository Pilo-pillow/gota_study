package series

/**
*这段代码定义了一个RollingWindow类，用于进行滚动窗口计算。
*类中有三个方法：
*- Rolling方法用于创建一个新的RollingWindow对象。
*- Mean方法返回滚动均值，通过调用getBlocks方法获取每个窗口大小的子序列，并计算它们的均值。
*- StdDev方法返回滚动标准差，同样通过调用getBlocks方法获取每个窗口大小的子序列，并计算它们的标准差。
*其中，getBlocks方法是核心方法，用于获取每个窗口大小的子序列。
*它通过遍历原始序列的每个元素，如果元素的位置小于窗口大小，则将一个空的Series对象添加到结果中;
*否则，通过计算窗口的索引范围，截取对应的子序列并将其添加到结果中。
*这段代码使用了自定义的Series类型，可能在其他地方定义。
*可以根据实际情况进行调整和补充。
 */

// RollingWindow 用于滚动窗口计算
type RollingWindow struct {
	window int    // 窗口大小
	series Series // 原始序列
}

// Rolling 创建新的 RollingWindow
func (s Series) Rolling(window int) RollingWindow {
	return RollingWindow{
		window: window,
		series: s,
	}
}

// Mean 返回滚动均值
func (r RollingWindow) Mean() (s Series) {
	s = New([]float64{}, Float, "Mean")
	for _, block := range r.getBlocks() {
		s.Append(block.Mean())
	}
	return
}

// StdDev 返回滚动标准差
func (r RollingWindow) StdDev() (s Series) {
	s = New([]float64{}, Float, "StdDev")
	for _, block := range r.getBlocks() {
		s.Append(block.StdDev())
	}
	return
}

// getBlocks 获取每个窗口大小的子序列
func (r RollingWindow) getBlocks() (blocks []Series) {
	for i := 1; i <= r.series.Len(); i++ {
		if i < r.window {
			blocks = append(blocks, r.series.Empty())
			continue
		}
		index := []int{}
		for j := i - r.window; j < i; j++ {
			index = append(index, j)
		}
		blocks = append(blocks, r.series.Subset(index))
	}
	return
}
