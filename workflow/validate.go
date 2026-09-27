package workflow

import "fmt"

// Validate 在加载阶段校验工作流定义：
// 重复步骤、引用不存在的依赖、环依赖分别返回不同 Kind 的 DefinitionError。
func Validate(steps []Step) error {
	seen := make(map[string]bool, len(steps))
	for _, s := range steps {
		if s.Name == "" {
			return &DefinitionError{Kind: ErrKindDuplicateStep, Step: s.Name, Detail: "step name must not be empty"}
		}
		if seen[s.Name] {
			return &DefinitionError{Kind: ErrKindDuplicateStep, Step: s.Name, Detail: "step defined more than once"}
		}
		seen[s.Name] = true
	}
	for _, s := range steps {
		for _, dep := range s.Depends {
			if !seen[dep] {
				return &DefinitionError{Kind: ErrKindMissingDep, Step: s.Name, Detail: fmt.Sprintf("depends on unknown step %q", dep)}
			}
		}
	}
	_, err := topoOrder(steps)
	return err
}

// topoOrder 返回拓扑排序后的步骤名（依赖在前）；存在环时返回 ErrKindCycle。
func topoOrder(steps []Step) ([]string, error) {
	const (
		white = 0 // 未访问
		gray  = 1 // 访问中（在栈上）
		black = 2 // 已完成
	)
	color := make(map[string]int, len(steps))
	deps := make(map[string][]string, len(steps))
	for _, s := range steps {
		deps[s.Name] = s.Depends
	}
	var order []string
	var visit func(name string, path []string) error
	visit = func(name string, path []string) error {
		switch color[name] {
		case black:
			return nil
		case gray:
			// 找到环：从 path 中截取环的部分
			start := 0
			for i, n := range path {
				if n == name {
					start = i
					break
				}
			}
			cycle := append(append([]string{}, path[start:]...), name)
			return &DefinitionError{Kind: ErrKindCycle, Step: name, Detail: fmt.Sprintf("dependency cycle detected: %v", cycle)}
		}
		color[name] = gray
		for _, dep := range deps[name] {
			if err := visit(dep, append(path, name)); err != nil {
				return err
			}
		}
		color[name] = black
		order = append(order, name)
		return nil
	}
	for _, s := range steps {
		if err := visit(s.Name, nil); err != nil {
			return nil, err
		}
	}
	return order, nil
}
