package workflow

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
)

// Store 用本地 JSON 文件持久化运行状态，模拟 durable storage。
// 所有方法都可在多 goroutine 下安全调用。
type Store struct {
	mu   sync.Mutex
	path string
}

// NewStore 创建（或加载已有的）本地状态文件。
func NewStore(path string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	return &Store{path: path}, nil
}

// Load 读取当前持久化的运行记录；不存在时返回 nil, nil。
func (s *Store) Load() (*RunRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := os.ReadFile(s.path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var rec RunRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		return nil, err
	}
	return &rec, nil
}

// Save 原子地写入运行记录（先写临时文件再 rename），
// 保证进程在任意时刻崩溃都不会留下半写的状态文件。
func (s *Store) Save(rec *RunRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}
