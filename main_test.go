package ssproc

// We don't want to use this because we can't call pgtest.Close() before detecting the goroutine leaks.
//func TestMain(m *testing.M) {
//	goleak.VerifyTestMain(m)
//}
