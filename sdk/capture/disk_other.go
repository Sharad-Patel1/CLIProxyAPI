//go:build !linux

package capture

// The deployment is Linux. Other targets retain the spool size ceiling.
func checkFreeSpace(string) error { return nil }
