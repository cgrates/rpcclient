// +build integration

package rpcclient

// func TestRPCClientloadTLSConfig(t *testing.T) {
// 	dirPath := "/tmp/TestRPCClientloadTLSConfig"
// 	clientCrt := ""
// 	clientKey := ""
// 	caPath := dirPath + "/file.txt"
// 	err := os.Mkdir(dirPath, 0755)

// 	if err != nil {
// 		t.Error(err)
// 	}
// 	defer os.RemoveAll(dirPath)

// 	err = os.WriteFile(caPath, []byte("testText2"), 0664)
// 	if err != nil {
// 		t.Error(err)
// 	}

// 	rcv, err := loadTLSConfig(clientCrt, clientKey, caPath)
// 	fmt.Println(rcv, err)
// }
