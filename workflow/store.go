package workflow

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
)

// Store 以本地 JSON 文件持久化运行状态，模拟可重启的存储。
// 所有写操作先写临时文件再原子 rename，避免崩溃留下半写状态。
type Store struct {
	mu   sync.Mutex
	path string
	data map[string]*RunState
}

// OpenStore 打开（或创建）位于 path 的状态文件。
func OpenStore(path string) (*Store, error) {
	s := &Store{path: path, data: map[string]*RunState{}}
	raw, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return s, nil
	}
	if err != nil {
		return nil, fmt.Errorf("workflow: read store: %w", err)
	}
	if len(raw) == 0 {
		return s, nil
	}
	if err := json.Unmarshal(raw, &s.data); err != nil {
		return nil, fmt.Errorf("workflow: parse store %s: %w", path, err)
	}
	if s.data == nil {
		s.data = map[string]*RunState{}
	}
	return s, nil
}

// LoadRun 读取一次运行的状态；不存在时返回 nil, nil。
// 返回的是深拷贝，调用方修改不会影响存储内容。
func (s *Store) LoadRun(runID string) (*RunState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.data[runID]
	if !ok {
		return nil, nil
	}
	return cloneRunState(st), nil
}

// SaveRun 原子地持久化一次运行的状态（深拷贝后落盘）。
func (s *Store) SaveRun(state *RunState) error {
	if state == nil || state.RunID == "" {
		return fmt.Errorf("workflow: cannot save run with empty id")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[state.RunID] = cloneRunState(state)
	return s.flushLocked()
}

func (s *Store) flushLocked() error {
	raw, err := json.MarshalIndent(s.data, "", "  ")
	if err != nil {
		return fmt.Errorf("workflow: encode store: %w", err)
	}
	dir := filepath.Dir(s.path)
	tmp, err := os.CreateTemp(dir, ".workflow-*.tmp")
	if err != nil {
		return fmt.Errorf("workflow: create temp store: %w", err)
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(raw); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("workflow: write temp store: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("workflow: sync temp store: %w", err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("workflow: close temp store: %w", err)
	}
	if err := os.Rename(tmpName, s.path); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("workflow: replace store: %w", err)
	}
	return nil
}

func cloneRunState(st *RunState) *RunState {
	out := &RunState{
		RunID:  st.RunID,
		Status: st.Status,
		Steps:  make(map[string]*StepRecord, len(st.Steps)),
	}
	for name, rec := range st.Steps {
		cp := *rec
		out.Steps[name] = &cp
	}
	return out
}
