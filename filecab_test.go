package filecab

import (
    "os"
    "testing"
    "fmt"
    "log"
    "strconv"
    "time"
    "github.com/stretchr/testify/assert"
)

var fc *Filecab

func TestFilecab(t *testing.T) {
    r := map[string]string{
        "id": "accounts/",
        "name": "Mickey",
        "birthdate": "2001-01-01",
        "quote": "life is fun\nI like life",
    }
    err := fc.Save(r)
    assert.Nil(t, err)
    
    r2 := map[string]string{
        "id": "accounts/",
        "name": "Minnie",
        "birthdate": "2002-02-02",
        "quote": "I want to succeed\nat everything",
    }
    err = fc.Save(r2)
    assert.Nil(t, err)
    
    start := time.Now()
    for i := 0; i < 10000; i++ {
        r := map[string]string{
            "id": "accounts/",
            "name": "Mr. " + strconv.Itoa(i),
            "birthdate": "2001-01-01",
            "quote": "I want to succeed\nat everything",
        }
        err = fc.Save(r)
        assert.Nil(t, err)
    }
    fmt.Println("it took", time.Since(start))
}

func ExampleSerialize() {
    r := map[string]string{
        "name": "Drew",
        "birthdate": "1984-11-12",
        "quote": "life is fun\nI like life",
    }

    v := serializeRecord(r)
    fmt.Println(v)
    // Output:
    // birthdate: 1984-11-12
    // name: Drew
    // quote:
    //     life is fun
    //     I like life
}

func TestMain(m *testing.M) {
    setup()
    code := m.Run()
    // teardown()
    os.Exit(code)
}

// code is Golang
func setup() {
    localDir := "/home/ubuntu/filecab/filecab_userdata"
    if localDir == "/home/ubuntu/filecab/filecab_userdata" { // double check
        if localDir == "/home/ubuntu/filecab/filecab_userdata" { // triple check
            err := os.RemoveAll(localDir)
            if err != nil {
                log.Fatal(err)
            }
        }
    }
    fc = New(localDir)
}



