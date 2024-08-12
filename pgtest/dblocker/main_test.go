package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"
)

var client = &http.Client{
	Timeout: 15 * time.Minute,
}

func TestLock(t *testing.T) {
	t.Skip()
	resp, err := client.Get("http://localhost:9191/lock")
	if err != nil {
		panic(err)
	}

	bytes, err := io.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}

	fmt.Println(string(bytes))
}

func TestUnlock(t *testing.T) {
	t.Skip()
	body := bytes.NewBuffer([]byte("postgresql://tester:LegacyCodeIsOneWithNoTest@localhost:9090/tester9"))
	resp, err := client.Post("http://localhost:9191/unlock", "text/plain", body)
	if err != nil {
		panic(err)
	}

	if resp.StatusCode != http.StatusOK {
		panic(resp.Status)
	}
}
