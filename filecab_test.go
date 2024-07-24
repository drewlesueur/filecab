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

const maxLoop = 1_000 * 1
// const maxLoop = 10
const repeat = 1
const extraFields = 100
// const maxLoop = 10

// TODO: rwlock
func TestFilecab(t *testing.T) {
    fmt.Println("")
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
    records, err := fc.LoadRange("accounts", 500, 100)
    fmt.Println("number of records: ", len(records))
    assert.Nil(t, err)
    fmt.Println("reading5 took", time.Since(start), "_lime")
    // indentJSON, err := json.MarshalIndent(records, "", "  ")
    // assert.Nil(t, err)
    // fmt.Println(string(indentJSON))

    start = time.Now()
    records, err = fc.LoadAll("accounts")
    assert.Nil(t, err)
    fmt.Println("number of records: ", len(records))
    // indentJSON, err := json.MarshalIndent(records, "", "  ")
    // assert.Nil(t, err)
    // fmt.Println(string(indentJSON))
    fmt.Println("reading4 took", time.Since(start), "_lime")

    start = time.Now()
    for i, r := range records {
        updatedR := map[string]string{
            "id": r["id"],
            "camping": "camping in " + strconv.Itoa(i) + " trees",
        }
        err = fc.Save(updatedR)
        if err != nil {
            fmt.Println(err, "_orangered")
        }
        assert.Nil(t, err)
    }
    fmt.Println("updating took", time.Since(start), "_lime")
    
    // start = time.Now()
    // records, err = fc.Load3("accounts/history")
    // assert.Nil(t, err)
    // fmt.Println("number of records: ", len(records))
    // fmt.Println("reading history took", time.Since(start), "_lime")
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
            camping TEXT,
            field0 TEXT,
            field1 TEXT,
            field2 TEXT,
            field3 TEXT,
            field4 TEXT,
            field5 TEXT,
            field6 TEXT,
            field7 TEXT,
            field8 TEXT,
            field9 TEXT,
            field10 TEXT,
            field11 TEXT,
            field12 TEXT,
            field13 TEXT,
            field14 TEXT,
            field15 TEXT,
            field16 TEXT,
            field17 TEXT,
            field18 TEXT,
            field19 TEXT,
            field20 TEXT,
            field21 TEXT,
            field22 TEXT,
            field23 TEXT,
            field24 TEXT,
            field25 TEXT,
            field26 TEXT,
            field27 TEXT,
            field28 TEXT,
            field29 TEXT,
            field30 TEXT,
            field31 TEXT,
            field32 TEXT,
            field33 TEXT,
            field34 TEXT,
            field35 TEXT,
            field36 TEXT,
            field37 TEXT,
            field38 TEXT,
            field39 TEXT,
            field40 TEXT,
            field41 TEXT,
            field42 TEXT,
            field43 TEXT,
            field44 TEXT,
            field45 TEXT,
            field46 TEXT,
            field47 TEXT,
            field48 TEXT,
            field49 TEXT,
            field50 TEXT,
            field51 TEXT,
            field52 TEXT,
            field53 TEXT,
            field54 TEXT,
            field55 TEXT,
            field56 TEXT,
            field57 TEXT,
            field58 TEXT,
            field59 TEXT,
            field60 TEXT,
            field61 TEXT,
            field62 TEXT,
            field63 TEXT,
            field64 TEXT,
            field65 TEXT,
            field66 TEXT,
            field67 TEXT,
            field68 TEXT,
            field69 TEXT,
            field70 TEXT,
            field71 TEXT,
            field72 TEXT,
            field73 TEXT,
            field74 TEXT,
            field75 TEXT,
            field76 TEXT,
            field77 TEXT,
            field78 TEXT,
            field79 TEXT,
            field80 TEXT,
            field81 TEXT,
            field82 TEXT,
            field83 TEXT,
            field84 TEXT,
            field85 TEXT,
            field86 TEXT,
            field87 TEXT,
            field88 TEXT,
            field89 TEXT,
            field90 TEXT,
            field91 TEXT,
            field92 TEXT,
            field93 TEXT,
            field94 TEXT,
            field95 TEXT,
            field96 TEXT,
            field97 TEXT,
            field98 TEXT,
            field99 TEXT
        )
    `)


    
    assert.Nil(t, err)
    // stmt, err := db.Prepare(`INSERT INTO accounts (name, birthdate, quote) VALUES (?, ?, ?)`)
    // update the insert to handle all the fields
    // format itnine in each line
// "?, ".repeat(103)
    stmt, err := db.Prepare(`INSERT INTO accounts (
            name, birthdate, quote, field0, field1, field2, field3, 
            field4, field5, field6, field7, field8, field9, field10, field11,
            field12, field13, field14, field15, field16, field17, field18, 
            field19, field20, field21, field22, field23, field24, field25, 
            field26, field27, field28, field29, field30, field31, field32, 
            field33, field34, field35, field36, field37, field38, field39, 
            field40, field41, field42, field43, field44, field45, field46, 
            field47, field48, field49, field50, field51, field52, field53, 
            field54, field55, field56, field57, field58, field59, field60, 
            field61, field62, field63, field64, field65, field66, field67, 
            field68, field69, field70, field71, field72, field73, field74, 
            field75, field76, field77, field78, field79, field80, field81, 
            field82, field83, field84, field85, field86, field87, field88, 
            field89, field90, field91, field92, field93, field94, field95, 
            field96, field97, field98, field99
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
    assert.Nil(t, err)
    defer stmt.Close()
    // r := []interface{}{"Mickey", "2001-01-01", "life is fun\nI like life"}
    // _, err = stmt.Exec(r...)
    // assert.Nil(t, err)
    // r2 := []interface{}{"Minnie", "2002-02-02", "I want to succeed\nat everything"}
    // _, err = stmt.Exec(r2...)
    // assert.Nil(t, err)
    start := time.Now()
    for i := 0; i < maxLoop; i++ {
        r := []interface{}{
            "Mr. " + strconv.Itoa(i),
            "2001-01-01",
            strings.Repeat("I want to succeed\nat everything\n", repeat),
        }
        for j := 0; j < extraFields; j++ {
            jStr := strconv.Itoa(j)
            r = append(r, "value" + jStr)
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

    // start = time.Now()
    // cursor, err := col.Find(context.TODO(), bson.D{})
    // assert.Nil(t, err)
    // defer cursor.Close(context.TODO())
    // var accounts []map[string]string
    // for cursor.Next(context.TODO()) {
    //     var result bson.M
    //     err := cursor.Decode(&result)
    //     assert.Nil(t, err)
    //     account := make(map[string]string)
    //     for k, v := range result {
    //         account[k] = fmt.Sprintf("%v", v)
    //     }
    //     accounts = append(accounts, account)
    // }
    // fmt.Println("mongo read took", time.Since(start), "_saddlebrown")
    
    
    start = time.Now()
    cur, err := col.Find(context.TODO(), bson.D{})
    assert.Nil(t, err)
    defer cur.Close(context.TODO())
    var accounts []map[string]string
    err = cur.All(context.TODO(), &accounts)
    assert.Nil(t, err)
    fmt.Println("mongo read2 took", time.Since(start), "_saddlebrown")
    // indentJSON, err := json.MarshalIndent(accounts, "", "  ")
    // assert.Nil(t, err)
    // fmt.Println(string(indentJSON))

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

func ExampleToBase60() {
    fmt.Println(toBase60(60 * 60 * 60 * 60))
    
    // Output:
    // 10000
}




