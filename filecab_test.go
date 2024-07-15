package filecab

import (
    "os"
    "testing"
    "fmt"
    "log"
    "strconv"
    "time"
    "github.com/stretchr/testify/assert"
    "database/sql"
    "encoding/json"
    _ "github.com/mattn/go-sqlite3"
)

var fc *Filecab

const maxLoop = 10000

// TODO: rwlock
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
    for i := 0; i < maxLoop; i++ {
        r := map[string]string{
            "id": "accounts/",
            "name": "Mr. " + strconv.Itoa(i),
            "birthdate": "2001-01-01",
            "quote": "I want to succeed\nat everything",
        }
        err = fc.Save(r)
        assert.Nil(t, err)
    }
    fmt.Println("writing took", time.Since(start))
    
    start = time.Now()
    records, err := fc.Load("accounts")
    assert.Nil(t, err)
    fmt.Println("number of records: ", len(records))
    // indentJSON, err := json.MarshalIndent(records, "", "  ")
    // assert.Nil(t, err)
    // fmt.Println(string(indentJSON))
    _ = json.Marshal
    fmt.Println("reading took", time.Since(start))
    
}

// write a similar test for Go to insert same number of records to
// a sqlite database
func TestSqliteInsertion(t *testing.T) {
    
    os.Remove("./mydatabase.db")
    db, err := sql.Open("sqlite3", "./mydatabase.db")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()
    _, err = db.Exec(`
        PRAGMA journal_mode = WAL;
        -- PRAGMA busy_timeout = 5000;
        -- PRAGMA synchronous = NORMAL;
        -- PRAGMA cache_size = 1000000000;
        -- PRAGMA foreign_keys = true;
        -- PRAGMA temp_store = memory;
    `)
    if err != nil {
        log.Fatal(err)
    }
    
    // explain PRAGMA journal_mode = WAL;

    // The `PRAGMA journal_mode = WAL;` statement in SQLite sets the journal mode of the database to Write-Ahead Logging (WAL). The primary purpose of the journal mode is to determine how SQLite handles transactions and ensures data integrity.
    // ### What is Write-Ahead Logging (WAL)?
    // In traditional rollback journal mode, SQLite uses a rollback journal to temporarily save the state of the database before any changes are made during a transaction. If a transaction fails, SQLite can use the rollback journal to revert the database to its previous state.
    // Write-Ahead Logging, on the other hand, is a more sophisticated journaling mode that offers several advantages over the traditional rollback journal:
    // 1. **Concurrency**:
    //    - **Better Concurrency**: WAL mode allows for better concurrent read and write access. Readers do not block writers, and writers do not block readers, meaning that the database can be read while a write is ongoing.
    // 2. **Checkpointing**:
    //    - **Checkpoint Process**: WAL maintains a separate log file where it records changes. Periodically, the changes in the WAL file are merged into the main database file by a process called "checkpointing."
    //    - In WAL mode, SQLite does not overwrite the original database file until a checkpoint operation is performed, which can be done automatically or manually.
    // 3. **Performance**:
    //    - **Improved Write Performance**: Writes in WAL mode are generally faster because they result in sequential writes to the WAL file, as opposed to random writes in rollback journal mode.
    //    - However, the read operations can sometimes be slightly slower if the WAL file is large, as SQLite has to read from both the main database file and the WAL file.
    // 4. **Recovery**:
    //    - **Enhanced Recovery**: In the event of a crash, WAL mode can provide faster recovery times because it replays the WAL log to restore the database to a consistent state.
    // ### Example Usage of PRAGMA journal_mode
    // ```go
    // _, err = db.Exec(`PRAGMA journal_mode = WAL;`)
    // if err != nil {
    //     log.Fatal(err)
    // }
    // ```
    // With this PRAGMA setting, you're configuring SQLite to use WAL mode for better performance and enhanced concurrency. It should be noted that while WAL mode has many benefits, it's essential to understand its behavior and ensure that your application handles checkpointing appropriately.
    // ### Additional Considerations
    // - **Checkpointing Frequency**: The frequency and timing of checkpoints can affect the performance and the size of the WAL file.
    // - **Disk Space**: The WAL file can grow in size depending on the volume of write operations and the frequency of checkpoints.
    // Switching to WAL mode is often beneficial, but you should consider these factors in the context of your specific application requirements.
    
    
    assert.Nil(t, err)
    defer db.Close()
    _, err = db.Exec(`CREATE TABLE accounts (id TEXT, name TEXT, birthdate TEXT, quote TEXT)`)
    assert.Nil(t, err)
    stmt, err := db.Prepare(`INSERT INTO accounts (id, name, birthdate, quote) VALUES (?, ?, ?, ?)`)
    assert.Nil(t, err)
    defer stmt.Close()
    r := []interface{}{"accounts/", "Mickey", "2001-01-01", "life is fun\nI like life"}
    _, err = stmt.Exec(r...)
    assert.Nil(t, err)
    r2 := []interface{}{"accounts/", "Minnie", "2002-02-02", "I want to succeed\nat everything"}
    _, err = stmt.Exec(r2...)
    assert.Nil(t, err)
    start := time.Now()
    for i := 0; i < maxLoop; i++ {
        r := []interface{}{
            "accounts/",
            "Mr. " + strconv.Itoa(i),
            "2001-01-01",
            "I want to succeed\nat everything",
        }
        _, err = stmt.Exec(r...)
        assert.Nil(t, err)
    }
    fmt.Println("sqlite write took", time.Since(start))
    
    // add code to select * from accounts
    // and marshal in to a []map[string]string
    start = time.Now()
    rows, err := db.Query(`SELECT * FROM accounts`)
    assert.Nil(t, err)
    defer rows.Close()
    var accounts []map[string]string
    cols, err := rows.Columns()
    assert.Nil(t, err)
    for rows.Next() {
        columns := make([]interface{}, len(cols))
        columnPointers := make([]interface{}, len(cols))
        for i := range columns {
            columnPointers[i] = &columns[i]
        }
        err = rows.Scan(columnPointers...)
        assert.Nil(t, err)
        account := make(map[string]string)
        for i, colName := range cols {
            val := columnPointers[i].(*interface{})
            account[colName] = fmt.Sprintf("%s", *val)
        }
        accounts = append(accounts, account)
    }
    // indentJSON, err := json.MarshalIndent(accounts, "", "  ")
    // assert.Nil(t, err)
    // fmt.Println(string(indentJSON))
    fmt.Println("sqlite read took", time.Since(start))
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




