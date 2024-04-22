package cm

import (
	"errors"
	"os"
	"testing"

	"k8s.io/utils/cpuset"
)

type FileSystem interface {
	MkdirAll(path string, perm os.FileMode) error
	WriteFile(filename string, data []byte, perm os.FileMode) error
}

type MockFileSystem struct {
	MkdirAllFunc  func(path string, perm os.FileMode) error
	WriteFileFunc func(filename string, data []byte, perm os.FileMode) error
}

func (m *MockFileSystem) MkdirAll(path string, perm os.FileMode) error {
	if m.MkdirAllFunc != nil {
		return m.MkdirAllFunc(path, perm)
	}
	return nil
}

func (m *MockFileSystem) WriteFile(filename string, data []byte, perm os.FileMode) error {
	if m.WriteFileFunc != nil {
		return m.WriteFileFunc(filename, data, perm)
	}
	return nil
}

func Test_writeCpuRtMultiRuntimeFile(t *testing.T) {
	mockFS := &MockFileSystem{}
	mockFS.MkdirAllFunc = func(path string, perm os.FileMode) error {
		// Check if the path is the expected one
		expectedPath := "/sys/fs/cgroup/cpu,cpuacct/kubepods/burstable/podb2aab547-2e0d-413a-b0c6-81183b6cdb8c"
		if path == expectedPath {
			// Simulate permission denied error for the expected path
			return errors.New("mkdir permission denied")
		}
		// For other paths, return nil
		return nil
	}
	mockFS.WriteFileFunc = func(filename string, data []byte, perm os.FileMode) error {
		// Mock behavior for WriteFile
		return nil
	}

	err := writeCpuRtMultiRuntimeFile("/sys/fs/cgroup/cpu,cpuacct/kubepods/burstable/podb2aab547-2e0d-413a-b0c6-81183b6cdb8c", cpuset.New(1, 2, 3), 10000000)
	if err == nil {
		t.Error("Expected error but got nil")
	} else if err.Error() != "mkdir permission denied" {
		t.Errorf("Unexpected error: %v", err)
	}
}
