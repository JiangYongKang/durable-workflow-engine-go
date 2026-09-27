package workflow

import "fmt"

// Validate 在加载阶段校验工作流定义。
// 重复步骤、未知依赖、环依赖分别返回可区分的错误（errors.Is）。
func (d *Definition) Validate() error {
	if len(d.Steps) == 0 {
		return fmt.Errorf("workflow: definition has no steps")
	}
	seen := make(map[string]struct{}, len(d.Steps))
	for _, s := range d.Steps {
		if s.Name == "" {
			return fmt.Errorf("workflow: step with empty name")
		}
		if _, dup := seen[s.Name]; dup {
			return fmt.Errorf("%w: %q", ErrDuplicateStep, s.Name)
		}
		seen[s.Name] = struct{}{}
		if s.Handler == nil {
			return fmt.Errorf("workflow: step %q has nil handler", s.Name)
		}
	}
	for _, s := range d.Steps {
		for _, dep := range s.DependsOn {
			if dep == s.Name {
				return fmt.Errorf("%w: %q depends on itself", ErrCyclicDependency, s.Name)
			}
			if _, ok := seen[dep]; !ok {
				return fmt.Errorf("%w: step %q depends on %q", ErrUnknownDependency, s.Name, dep)
			}
		}
	}
	if _, err := d.topoSort(); err != nil {
		return err
	}
	return nil
}

// topoOrder 返回拓扑排序后的步骤名（依赖在前）。调用前需已通过 Validate。
func (d *Definition) topoOrder() []string {
	order, _ := d.topoSort()
	return order
}

// topoSort 用 Kahn 算法做拓扑排序；发现环时返回 ErrCyclicDependency。
func (d *Definition) topoSort() ([]string, error) {
	indegree := make(map[string]int, len(d.Steps))
	dependents := make(map[string][]string, len(d.Steps))
	for _, s := range d.Steps {
		if _, ok := indegree[s.Name]; !ok {
			indegree[s.Name] = 0
		}
		for _, dep := range s.DependsOn {
			indegree[s.Name]++
			dependents[dep] = append(dependents[dep], s.Name)
		}
	}
	queue := make([]string, 0, len(d.Steps))
	for _, s := range d.Steps { // 按定义顺序入队，保证结果稳定
		if indegree[s.Name] == 0 {
			queue = append(queue, s.Name)
		}
	}
	order := make([]string, 0, len(d.Steps))
	for len(queue) > 0 {
		name := queue[0]
		queue = queue[1:]
		order = append(order, name)
		for _, next := range dependents[name] {
			indegree[next]--
			if indegree[next] == 0 {
				queue = append(queue, next)
			}
		}
	}
	if len(order) != len(d.Steps) {
		// 找出仍在环上的步骤，便于定位
		var inCycle []string
		for name, deg := range indegree {
			if deg > 0 {
				inCycle = append(inCycle, name)
			}
		}
		return nil, fmt.Errorf("%w: steps %v", ErrCyclicDependency, inCycle)
	}
	return order, nil
}
