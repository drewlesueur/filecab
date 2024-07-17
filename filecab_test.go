package filecab

import (
    "os"
    "testing"
    "fmt"
    "log"
    "strings"
    "strconv"
    "time"
    "context"
    "github.com/stretchr/testify/assert"
    "database/sql"
    "encoding/json"
    _ "github.com/mattn/go-sqlite3"
    "go.mongodb.org/mongo-driver/bson"
    "go.mongodb.org/mongo-driver/mongo"
    "go.mongodb.org/mongo-driver/mongo/options"
)

var fc *Filecab

const maxLoop = 10_000
// const maxLoop = 10
const repeat = 1
const extraFields = 100
// const maxLoop = 10

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
            "quote": strings.Repeat("I want to succeed\nat everything\n", repeat),
        }
        for j := 0; j < extraFields; j++ {
            jStr := strconv.Itoa(j)
            r["field" + jStr] = "value" + jStr
        }
        // err = fc.Save(r)
        err = fc.Save(r)
        assert.Nil(t, err)
    }

    fmt.Println("writing took", time.Since(start), "_lime")
    
    // start = time.Now()
    // records, err := fc.Load("accounts")
    // assert.Nil(t, err)
    // fmt.Println("number of records: ", len(records))
    // fmt.Println("reading took", time.Since(start), "_lime")
    
    start = time.Now()
    records, err := fc.Load3("accounts")
    assert.Nil(t, err)
    fmt.Println("number of records: ", len(records))
    fmt.Println("reading2 took", time.Since(start), "_lime")

    start = time.Now()
    for i, r := range records {
        updatedR := map[string]string{
            "id": r["id"],
            "camping": "camping in " + strconv.Itoa(i) + " trees",
        }
        err = fc.Save(updatedR)
        assert.Nil(t, err)
    }
    fmt.Println("updating took", time.Since(start), "_lime")
    
    start = time.Now()
    records, err = fc.Load3("accounts/history")
    assert.Nil(t, err)
    fmt.Println("number of records: ", len(records))
    fmt.Println("reading history took", time.Since(start), "_lime")
    // indentJSON, err := json.MarshalIndent(records, "", "  ")
    // assert.Nil(t, err)
    // fmt.Println(string(indentJSON))
    _ = json.Marshal
    
    
    
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
    
    assert.Nil(t, err)
    defer db.Close()
    // _, err = db.Exec(`CREATE TABLE accounts (id TEXT, name TEXT, birthdate TEXT, quote TEXT, camping TEXT)`)
    // make id the primary key auto increment

    _, err = db.Exec(`
        CREATE TABLE accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            birthdate TEXT,
            quote TEXT,
            camping TEXT
        )
    `)
    
    
    assert.Nil(t, err)
    stmt, err := db.Prepare(`INSERT INTO accounts (name, birthdate, quote) VALUES (?, ?, ?)`)
    assert.Nil(t, err)
    defer stmt.Close()
    r := []interface{}{"Mickey", "2001-01-01", "life is fun\nI like life"}
    _, err = stmt.Exec(r...)
    assert.Nil(t, err)
    r2 := []interface{}{"Minnie", "2002-02-02", "I want to succeed\nat everything"}
    _, err = stmt.Exec(r2...)
    assert.Nil(t, err)
    start := time.Now()
    for i := 0; i < maxLoop; i++ {
        r := []interface{}{
            "Mr. " + strconv.Itoa(i),
            "2001-01-01",
            strings.Repeat("I want to succeed\nat everything\n", repeat),
        }
        _, err = stmt.Exec(r...)
        assert.Nil(t, err)
    }
    fmt.Println("sqlite write took", time.Since(start), "_orangered")
    
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
    fmt.Println("sqlite read took", time.Since(start), "_orangered")
    start = time.Now()
    for i, account := range accounts {
        account["camping"] = "camping in " + strconv.Itoa(i) + " trees"
        _, err = db.Exec(`UPDATE accounts SET camping = ? WHERE id = ?`, account["camping"], account["id"])
        assert.Nil(t, err)
    }
    fmt.Println("sqlite update took", time.Since(start), "_orangered")
}






// mongod --port 27018 --dbpath /home/ubuntu/delme_my_mongo --bind_ip 127.0.0.1
// are there more flags we can use for optimization

// write the same function but for mongodb
// delete the existing mongo db and create it as part of the test
func TestMongoInsertion(t *testing.T) {
    clientOptions := options.Client().ApplyURI("mongodb://localhost:27018")
    client, err := mongo.Connect(context.TODO(), clientOptions)
    assert.Nil(t, err)
    defer client.Disconnect(context.TODO())
    err = client.Database("delme_my_db").Drop(context.TODO())
    assert.Nil(t, err)
    db := client.Database("delme_my_db")
    col := db.Collection("accounts")
    r := bson.D{
        {"name", "Mickey"},
        {"birthdate", "2001-01-01"},
        {"quote", "life is fun\nI like life"},
    }
    _, err = col.InsertOne(context.TODO(), r)
    assert.Nil(t, err)
    r2 := bson.D{
        {"name", "Minnie"},
        {"birthdate", "2002-02-02"},
        {"quote", "I want to succeed\nat everything"},
    }
    _, err = col.InsertOne(context.TODO(), r2)
    assert.Nil(t, err)
    start := time.Now()
    for i := 0; i < maxLoop; i++ {
        r := bson.D{
            {"name", "Mr. " + strconv.Itoa(i)},
            {"birthdate", "2001-01-01"},
            {"quote",  strings.Repeat("I want to succeed\nat everything\n", repeat)},
        }
        for j := 0; j < extraFields; j++ {
            jStr := strconv.Itoa(j)
            r = append(r, bson.E{"field" + jStr, "value" + jStr})
        }
        _, err = col.InsertOne(context.TODO(), r)
        assert.Nil(t, err)
    }
    fmt.Println("mongo write took", time.Since(start), "_saddlebrown")

    start = time.Now()
    cursor, err := col.Find(context.TODO(), bson.D{})
    assert.Nil(t, err)
    defer cursor.Close(context.TODO())
    var accounts []map[string]string
    for cursor.Next(context.TODO()) {
        var result bson.M
        err := cursor.Decode(&result)
        assert.Nil(t, err)
        account := make(map[string]string)
        for k, v := range result {
            account[k] = fmt.Sprintf("%v", v)
        }
        accounts = append(accounts, account)
    }
    fmt.Println("mongo read took", time.Since(start), "_saddlebrown")

    start = time.Now()
    for i, account := range accounts {
        account["camping"] = "camping in " + strconv.Itoa(i) + " trees"
        filter := bson.D{
            {"_id", account["_id"]},
        }
        update := bson.D{
            {"$set", bson.D{
                {"camping", account["camping"]},
            }},
        }
        _, err = col.UpdateOne(context.TODO(), filter, update)
        assert.Nil(t, err)
    }
    fmt.Println("mongo update took", time.Since(start), "_saddlebrown")
    
}

// now give me just the code to do similar updates for the second function, and time it like the first





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




