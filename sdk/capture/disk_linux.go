//go:build linux

package capture

import (
	"fmt"
	"syscall"
)

func checkFreeSpace(dir string) error {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(dir, &stat); err != nil {
		return err
	}
	if uint64(stat.Bavail)*uint64(stat.Bsize) < 2<<30 {
		return fmt.Errorf("capture requires 2 GiB filesystem reserve")
	}
	return nil
}
