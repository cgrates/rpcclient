// +build integration

package rpcclient

import (
	"os"
	"testing"
)

type mockLogWriter struct {
	logOutput string
}

func (mLW *mockLogWriter) Alert(m string) error {
	return nil
}

func (mLW *mockLogWriter) Close() error {
	return nil
}

func (mLW *mockLogWriter) Crit(m string) error {
	mLW.logOutput = m
	return nil
}

func (mLW *mockLogWriter) Debug(m string) error {
	return nil
}

func (mLW *mockLogWriter) Emerg(m string) error {
	return nil
}

func (mLW *mockLogWriter) Err(m string) error {
	return nil
}

func (mLW *mockLogWriter) Info(m string) error {
	return nil
}

func (mLW *mockLogWriter) Notice(m string) error {
	return nil
}

func (mLW *mockLogWriter) Warning(m string) error {
	return nil
}

func (mLW *mockLogWriter) Write(b []byte) (int, error) {
	return 0, nil
}

func TestRPCClientloadTLSConfig(t *testing.T) {
	tmp := &mockLogWriter{}
	logger = tmp
	dirPath := "/tmp/TestRPCClientloadTLSConfig"
	clientCrt := ""
	clientKey := ""
	caPath := dirPath + "/file.txt"
	err := os.Mkdir(dirPath, 0755)

	if err != nil {
		t.Error(err)
	}
	defer os.RemoveAll(dirPath)

	err = os.WriteFile(caPath, []byte("testText2"), 0664)
	if err != nil {
		t.Error(err)
	}

	explog := "Cannot append certificate authority"
	rcv, err := loadTLSConfig(clientCrt, clientKey, caPath)
	rcvlog := tmp.logOutput

	if err != nil {
		t.Fatalf("\nexpected: <%+v>, \nreceived: <%+v>", nil, err)
	}

	if rcv != nil {
		t.Errorf("\nexpected: <%+v>, \nreceived: <%+v>", nil, rcv)
	}

	if rcvlog != explog {
		t.Errorf("\nexpected: <%+v>, \nreceived: <%+v>", explog, rcvlog)
	}
}
