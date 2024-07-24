package filecab


import (
    "sync"
    "strings"
    "strconv"
    "sort"
    "time"
    "fmt"
    "io"
    "math/rand"
    "encoding/hex"
    "context"
    "regexp"
    "os"
    "bytes"
    "path/filepath"
	"compress/gzip"
)


      
// created and updated are in log?
// user as well
// id is the full path
// example id is
// accounts/2024/07/13/drew1
// type Record map[string]string{}
// cache files, etc
// catch systen changes
// cache and invalidation


type Filecab struct {
    mu     sync.RWMutex
    RootDir string
    // cachedDir map[string]bool
    openFiles map[string]*os.File
    conds map[string]*sync.Cond
    condCounts map[string]int
}

func New(rootDir string) *Filecab {
    if rootDir == "/" {
        panic("Root directory cannot be '/'")
    }
    f := &Filecab{
        RootDir: rootDir,
        // cachedDir: map[string]bool{},
        openFiles: map[string]*os.File{},
        conds: map[string]*sync.Cond{},
        condCounts: map[string]int{},
    }
    // f.CondTimer()
    return f
    
    
}

// func (f *Filecab) CondTimer() {
//     ticker := time.NewTicker(2 * time.Second)
//     go func() {
//         for range ticker.C {
//             f.mu.RLock()
//             for _, cond := range f.conds {
//                 cond.Broadcast()
//             }
//             f.mu.RUnlock()
//         }
//     }()
// }

// update this code to also add a prev symmlink to the previous record
// to make a doubly linked list basically
func (f *Filecab) Save(record map[string]string) error {
    f.mu.Lock()
    defer f.mu.Unlock()
    return f.saveInternal(true, record)
}


func (f *Filecab) openFile(filePath string, keepOpen map[string]bool) (*os.File, error) {
    file, ok := f.openFiles[filePath]
    if !ok {
        newFile, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
        if err != nil {
            return nil, err
        }
        file = newFile
        f.openFiles[filePath] = file
        if len(f.openFiles) > 100 {
            for openPath, openFile := range f.openFiles {
                if filePath != openPath && !keepOpen[openPath] {
                    openFile.Close()
                    delete(f.openFiles, openPath)
                    break
                }
            }
        }
    }
    return file, nil
}

type MetaFiles struct {
    RecordHist *os.File
    ParentHist *os.File
    ParentOrder *os.File
    RecordHistPath string
    ParentHistPath string
    ParentOrderPath string
}
// func (f *Filecab) MetaFilesForRecord(record map[string]string], includeOrder bool) (*ThreeFilesAndName, error) {
//         parts := strings.Split(record["id"], "/"+recordsName+"/")
//         parentId := strings.Join(parts[0:len(parts)-1], "/"+recordsName+"/")
//         parentHist := f.RootDir + "/" + usedId + "/history.txt"
//         recordHist := f.RootDir + "/" + record["id"] + "/history.txt"
//         // localRecordId := parts[len(parts) - 1]
//         recordOrder := f.RootDir + "/" + parentDir + "/order.txt"
//         
//         recordHistFile, err := f.openFile(recordHist)
//         if err != nil {
//             return nil, err
//         }
//         parentHistFile, err := f.openFile(parentHist)
//         if err != nil {
//             return nil, err
//         }
//         recordOrderFile, err := f.openFile(recordOrder)
//         if err != nil {
//             return nil, err
//         }
//         return &ThreeFilesAndName{
//             RecordHist: recordHistFile,
//             ParentHist: parentHistFile,
//             ParentOrder: recordOrderFile,
//         }, nil
// }


func (f *Filecab) MetaFilesForRecord(record map[string]string, includeOrder bool) (*MetaFiles, error) {
    parts := strings.Split(record["id"], "/"+recordsName+"/")
    parentId := strings.Join(parts[0:len(parts)-1], "/"+recordsName+"/")
    parentHist := f.RootDir + "/" + parentId + "/history.txt"
    recordHist := f.RootDir + "/" + record["id"] + "/history.txt"
    var recordOrder string
    // localRecordId := parts[len(parts) - 1]
    
    keepOpen := map[string]bool{
        parentHist: true,
        recordHist: true,
    }
    if includeOrder {
        recordOrder = f.RootDir + "/" + parentId + "/order.txt"
        keepOpen[recordOrder] = true
    }
    recordHistFile, err := f.openFile(recordHist, keepOpen)
    if err != nil {
        return nil, err
    }
    parentHistFile, err := f.openFile(parentHist, keepOpen)
    if err != nil {
        return nil, err
    }
    var recordOrderFile *os.File
    if includeOrder {
        recordOrderFile, err = f.openFile(recordOrder, keepOpen)
        if err != nil {
            return nil, err
        }
    }
    return &MetaFiles{
        RecordHist:  recordHistFile,
        RecordHistPath:  recordHist,
        ParentHist:  parentHistFile,
        ParentHistPath:  parentHist,
        ParentOrder: recordOrderFile,
        ParentOrderPath: recordOrder,
    }, nil

}

// func (f *Filecab) saveHistory(record map[string]string, serializedBytes []byte, file *os.File) error {
//     if _, err := file.Write(serializedBytes); err != nil {
//         return err
//     }
//     return nil
// }

func (f *Filecab) saveHistory(record map[string]string, serializedBytes []byte, file *os.File) (int64, error) {
    if _, err := file.Write(serializedBytes); err != nil {
        return 0, err
    }
    // return 0, nil
    
    stat, err := file.Stat()
    if err != nil {
        return 0, err
    }
    return stat.Size(), nil
}

const idSize = 31
func (f *Filecab) saveOrder(record map[string]string, file *os.File) error {
    parts := strings.Split(record["id"], "/"+recordsName+"/")
    localRecordId := parts[len(parts) - 1]
    if len(localRecordId) < idSize {
        localRecordId = localRecordId + strings.Repeat("_", 63-len(localRecordId)) 
    }
    if _, err := file.Write([]byte(localRecordId + "\n")); err != nil {
        return err
     }
    return nil
 }


const singleFileHistory = true
// const singleFileHistory = false

const linkedList = false
// const linkedList = true

// write a function in Go to represent a number as base 60 with the following characters
var timeEncoding = [61]string{
    "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", 
    "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", 
    "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", 
    "u", "v", "x", "y", "z", "A", "B", "C", "D", "E", 
    "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", 
    "P", "Q", "R", "S", "T", "U", "B", "W", "X", "Y", 
    "Z",
}
func toBase60(n int) string {
	if n == 0 {
		return timeEncoding[0]
	}
	var result string
	for n > 0 {
		remainder := n % 60
		result = timeEncoding[remainder] + result
		n /= 60
	}
	return result
}

func encodeDate(t time.Time) string {
    // 11 chars with slash
    year := strconv.Itoa(t.Year())
    month := int(t.Month())
    day := t.Day()
    hour := t.Hour()
    minute := t.Minute()
    second := t.Second()
    frame := t.Nanosecond() / 16666667
    return year + timeEncoding[month] + timeEncoding[day] + "/" + timeEncoding[hour] + timeEncoding[minute] + timeEncoding[second] + timeEncoding[frame]
}


const recordsName = "R"
// const recordsName = "records"
func (f *Filecab) saveInternal(doLog bool, record map[string]string) error {
    // if !doLog {
    //     return nil
    // }
    isNew := false
    var originalID = ""
    if strings.HasSuffix(record["id"], "/") {
        originalID = record["id"]
        // localRecordId := now.Format("2006_01_02/15_04_05_") + fmt.Sprintf("%03d", now.Nanosecond()/1e6) + "_" + generateUniqueID() + "_" + nameize(record["name"])
        // localRecordId := now.Format("20060102/150405") + fmt.Sprintf("%03d", now.Nanosecond()/1e6) + "_" + generateUniqueID() + "_" + nameize(record["name"])
        now := time.Now()
        localRecordId := encodeDate(now) + "_" + generateUniqueID() + "_" + nameize(record["name"])
        //               11             1     6                    1     32
        record["id"] += recordsName + "/" + localRecordId
        // record["id"] += recordsName + "/" + generateUniqueID() + "_" + nameize(record["name"])
        isNew = true
    }
    
    record["id"] = strings.ReplaceAll(record["id"], "..", "")
    fullDir := f.RootDir + "/" + record["id"]
    filePath := fullDir + "/" + "record.txt"
    
    if isNew {
        // fmt.Println("creating", record["id"], "with", len(record), "fields")
        timeStr := time.Now().Format(time.RFC3339Nano)
        if doLog {
            record["updated_at"] = timeStr
            record["created_at"] = timeStr
        }
        serializedBytes := serializeRecordToBytes(record)

        var err error
        err = os.MkdirAll(fullDir, os.ModePerm)
        if err != nil {
            return err
        }
        errCh := make(chan error, 12)
        var errChCount = 0

        if record["override_symlink"] == "" {
            errChCount++
            go func() {
                err := os.WriteFile(filePath, serializedBytes, 0644)
                errCh <- err
            }()
        } else {
            errChCount++
            go func() {
                errCh <- os.Symlink(record["override_symlink"], filePath)
            }()
        }
        // errChCount++
        // go func() {
        //     errCh <- f.saveOrder(record)
        // }()

        if doLog {
            if singleFileHistory {
                metaFiles, err := f.MetaFilesForRecord(record, true)
                if err != nil {
                    return err
                }
                // errChCount++
                // go func() {
                //     size, err := f.saveHistory(record, serializedBytes, metaFiles.RecordHist)
                //     _ = size
                //     errCh <- err
                // }()
                size, err := f.saveHistory(record, serializedBytes, metaFiles.RecordHist)
                _ = size
                if err != nil {
                    return err
                }
                f.BroadcastForFile(metaFiles.RecordHistPath)
                // todo: wrangle the version
                errChCount++
                go func() {
                    size, err := f.saveHistory(record, serializedBytes, metaFiles.ParentHist)
                    _ = size
                    f.BroadcastForFile(metaFiles.ParentHistPath)
                    errCh <- err
                }()
                errChCount++
                go func() {
                    errCh <- f.saveOrder(record, metaFiles.ParentOrder)
                }()
            } else {
                errChCount += 2
                go func() {
                    hr := map[string]string{}
                    for k, v := range record {
                        hr[k] = v
                    }

                    theIdBefore := hr["id"]
                    hr["id"] += "/history/"
                    hr["non_history_id"] = theIdBefore
                    errCh <- f.saveInternal(false, hr)
                    historyId := hr["id"]
                    // note that saveInternal updates the id
                    // some of the processing could be improved by using localRecordId instead of trimming, splitting?
                    // save up one level only
                    parts := strings.Split(theIdBefore, "/"+recordsName+"/")
                    parts = parts[0:len(parts)-1]

                    hr = map[string]string{}
                    hr["id"] = strings.Join(parts, "/"+recordsName+"/") + "/history/"
                    // hr["override_symlink"] = historyId + "/record.txt"
                    hr["override_symlink"] = "../../../../" + strings.TrimPrefix(historyId, originalID) + "/record.txt"
                    errCh <- f.saveInternal(false, hr)
                    // errCh <- nil
                }()
            }
        }

        linkedList := false 
        if linkedList {
            lastPath := f.RootDir + "/" + originalID + "last"
            _, err = os.Stat(lastPath);
            if os.IsNotExist(err) {
                // err = os.Symlink(fullDir, lastPath)
                err = os.Symlink("./" + strings.TrimPrefix(record["id"], originalID), lastPath)
                if err != nil {
                    return err
                }
                firstPath := f.RootDir + "/" + originalID + "first"
                // err = os.Symlink(fullDir, firstPath)
                err = os.Symlink("./" + strings.TrimPrefix(record["id"], originalID), firstPath)
                if err != nil {
                    return err
                }
                
                // lengthPath := f.RootDir + "/" + originalID + "length"
                // err = os.WriteFile(lengthPath, []byte("1"), 0644)
                // if err != nil {
                //     return err
                // }
                // versionPath := f.RootDir + "/" + originalID + "version"
                // err = os.WriteFile(lengthPath, []byte("1"), 0644)
                // if err != nil {
                //     return err
                // }
            } else if err == nil {
                prevLastDir, err := os.Readlink(lastPath)
                // fmt.Println("reading link:", prevLastDir)
                if err != nil {
                    return err
                }
                // "next" part
                errChCount++
                go func() {
                    nextPath := f.RootDir + "/" + originalID + prevLastDir[2:] + "/next"
                    errCh <- os.Symlink("../../../" + strings.TrimPrefix(record["id"], originalID), nextPath)
                    // errCh <- os.Symlink(fullDir, nextPath)
                }()
                // "prev" part
                errChCount++
                go func() {
                    prevPath := fullDir + "/prev"
                    errCh <- os.Symlink("../../../" + strings.TrimPrefix(prevLastDir[2:], originalID), prevPath)
                }()
                // "last" part including removing and renaming
                errChCount += 2
                go func() {
                    if err := os.Remove(lastPath); err != nil && !os.IsNotExist(err) {
                        errCh <- err
                        errCh <- nil
                        return
                    }
                    errCh <- nil
                    errCh <- os.Symlink("./" + strings.TrimPrefix(record["id"], originalID), lastPath)
                }()
        
                // slower barely
                // newSymlink := lastPath + ".new"
                // if err := os.Symlink(fullDir, newSymlink); err != nil {
                //     return err
                // }
                // if err := os.Rename(newSymlink, lastPath); err != nil {
                //     return err
                // }
            } else {
                return err
            }
        }
        
        
        for i := 0; i < errChCount; i++ {
            if err := <-errCh; err != nil {
                return err
            }
        }
    } else {
        // update:

        // fmt.Println("updating", record["id"], "with", len(record), "fields")
        if doLog {
            record["updated_at"] = time.Now().Format(time.RFC3339Nano)
        }
        
        errCh := make(chan error, 2)
        var errChCount = 0
        if doLog {
            if singleFileHistory {
                serializedBytes := serializeRecordToBytes(record)
                metaFiles, err := f.MetaFilesForRecord(record, false)
                if err != nil {
                    return err
                }
                errChCount++
                go func() {
                    size, err := f.saveHistory(record, serializedBytes, metaFiles.RecordHist)
                    _ = size
                    f.BroadcastForFile(metaFiles.RecordHistPath)
                    errCh <- err
                }()
                errChCount++
                go func() {
                    size, err := f.saveHistory(record, serializedBytes, metaFiles.ParentHist)
                    _ = size
                    f.BroadcastForFile(metaFiles.ParentHistPath)
                    errCh <- err
                }()
            } else {
                errChCount += 2
                go func() {
                    // fmt.Println("history for update", "_coral")
                    hr := map[string]string{}
                    for k, v := range record {
                        hr[k] = v
                    }

                    theIdBefore := hr["id"]
                    hr["id"] += "/history/"
                    hr["non_history_id"] = theIdBefore
                    errCh <- f.saveInternal(false, hr)
                    historyId := hr["id"]
                    // note that saveInternal updates the id

                    // some of the processing could be improved by using localRecordId instead of trimming, splitting?
                    // save up one level only
                    parts := strings.Split(theIdBefore, "/"+recordsName+"/")
                    parts = parts[0:len(parts)-1]

                    hr = map[string]string{}
                    hr["id"] = strings.Join(parts, "/"+recordsName+"/") + "/history/"
                    // hr["override_symlink"] = historyId + "/record.txt"
                    // hr["override_symlink"] = "../../../../" + strings.TrimPrefix(historyId, originalID) + "/record.txt"
                    hr["override_symlink"] = "../../../../" + strings.TrimPrefix(historyId, strings.Join(parts, "/"+recordsName+"/") + "/") + "/record.txt"
                    // fmt.Println("saving update", hr["override_symlink"], "_coral")
                    errCh <- f.saveInternal(false, hr)
                    // errCh <- nil
                }()
            }
        }
        existingData, err := os.ReadFile(filePath)
        if err != nil {
            return err
        }
        existingRecord := deserializeRecordBytes(existingData)
        for k, v := range record {
            if len(k) >= 2 {
                if k[0] == '+' {
                    // existingRecord[k] = strconv.
                    continue
                }
                if k[0] == '.' {
                    existingRecord[k] += v
                    continue
                }
            }
            existingRecord[k] = v
        }
        serializedBytes := serializeRecordToBytes(existingRecord)
        err = os.WriteFile(filePath, []byte(serializedBytes), 0644)
        if err != nil {
            return err
        }
        for i := 0; i < errChCount; i++ {
            if err := <-errCh; err != nil {
                return err
            }
        }
    }
    return nil
}





// implement the load function that loads all to a map[string]string
// thePath will be the id prefix and it will start at "first"
// and go along the linked list until there is no "next"
func (f *Filecab) Load(thePath string) ([]map[string]string, error) {
    f.mu.RLock()
    defer f.mu.RUnlock()
    // lengthPath := f.RootDir + "/" + thePath + "/length"
    // existingLengthData, err := os.ReadFile(lengthPath)
    // if err != nil {
    //     return nil, err
    // }
    // existingLength, err := strconv.Atoi(string(existingLengthData))
    // if err != nil {
    //     return nil, err
    // }
    
    
    var records []map[string]string
    // var records = make([]map[string]string, existingLength)
    recordDir := f.RootDir + "/" + thePath + "/first"
    i := -1
    for {
        i++
        recordFile := recordDir + "/record.txt"
        data, err := os.ReadFile(recordFile)
        if err != nil {
            return nil, err
        }
        record := deserializeRecordBytes(data)
        records = append(records, record)
        // records[i] = record
        nextLink := recordDir + "/next"
        if _, err := os.Lstat(nextLink); os.IsNotExist(err) {
            break
        } else if err != nil {
            return nil, err
        } else {
            nextPath, err := os.Readlink(nextLink)
            if err != nil {
                return nil, err
            }
            recordDir = nextPath
        }
    }
    return records, nil
}

// Deprecated, uses symlink method
func (f *Filecab) Load3(thePath string) ([]map[string]string, error) {
    f.mu.RLock()
    defer f.mu.RUnlock()
    var paths []string
    recordDir := f.RootDir + "/" + thePath + "/first"
    for {
        paths = append(paths, recordDir)
        nextLink := recordDir + "/next"
        if _, err := os.Lstat(nextLink); os.IsNotExist(err) {
            break
        } else if err != nil {
            return nil, err
        } else {
            nextPath, err := os.Readlink(nextLink)
            if err != nil {
                return nil, err
            }
            // recordDir = nextPath
            recordDir = f.RootDir + "/" + thePath + "/" + nextPath[9:]
            // fmt.Println()
        }
    }
    
    var maxConcurrency = 100
    var ch = make(chan int, maxConcurrency)
    var records = make([]map[string]string, len(paths))
    errCh := make(chan error, len(paths))
    for i, path := range paths {
        i := i
        ch <- 1
        go func(path string) {
            defer func() {
                <- ch
            }()
            recordFile := path + "/record.txt"
            data, err := os.ReadFile(recordFile)
            if err != nil {
                errCh <- err
                return
            }
            record := deserializeRecordBytes(data)
            records[i] = record
        }(path)
    }
    for i := 0; i < maxConcurrency; i++ {
        ch <- 1
    }
    close(errCh)
    if len(errCh) > 0 {
        return nil, <-errCh
    }
    return records, nil
}

func (f *Filecab) InitWaitFile(absolutePath string) *sync.Cond {
    c, ok := f.conds[absolutePath]
    if !ok {
        c = sync.NewCond(&f.mu)
        f.conds[absolutePath] = c
        f.condCounts[absolutePath] = 0
    }
    f.condCounts[absolutePath] += 1
    return c
}

func (f *Filecab) BroadcastForFile(absolutePath string) {
    fmt.Println("broadcasting for:", absolutePath, "#lime")
    c, ok := f.conds[absolutePath]
    if !ok {
        return
    }
    c.Broadcast()
}

func (f *Filecab) DoneWaitingForFile(absolutePath string) {
    _, ok := f.conds[absolutePath]
    if ok {
        f.condCounts[absolutePath] -= 1
        if (f.condCounts[absolutePath] == 0) {
            delete(f.conds, absolutePath)
            delete(f.condCounts, absolutePath)
        }
    }
}


// TODO: max
func (f *Filecab) LoadHistorySince(ctx context.Context, thePath string, startOffset int, doWait bool) ([]map[string]string, int, error) {
    f.mu.Lock() // using lock and unlock cuz of Cond
    defer f.mu.Unlock()
    historyPath := f.RootDir + "/" + thePath + "/history.txt"
    var waitErr error
    var rawBytes []byte
    
    // see the pattern here: https://pkg.go.dev/context#example-AfterFunc-Cond
    stopF := context.AfterFunc(ctx, func () {
        // should I explicitly make it the mutex from c
        f.mu.Lock() // using lock and unlock cuz of Cond
        defer f.mu.Unlock()
        fmt.Println("stopped", historyPath, "#deepskyblue")
        f.BroadcastForFile(historyPath)
    })
    defer stopF()
    
    c := f.InitWaitFile(historyPath)
    for {
        file, err := os.Open(historyPath)
        if err != nil {
            return nil, 0, err
        }
        defer file.Close()
        _, err = file.Seek(int64(startOffset), 0)
        if err != nil {
            return nil, 0, err
        }
        rawBytes, err = io.ReadAll(file)
        if err != nil {
            return nil, 0, err
        }
        
        if len(rawBytes) > 0 {
            break
        }
        if !doWait {
            break
        } else {
            fmt.Println("waiting for:", historyPath, "#orangered")
            c.Wait()
            if ctx.Err() != nil {
                waitErr = ctx.Err()
                break
            }
        }
    }
    f.DoneWaitingForFile(historyPath)
    
    // just flow like normal
    _ = waitErr
    // if waitErr != nil {
    //     return []map[string]string{}, 0, waitErr
    // }
    
    rawRecords := bytes.Split(rawBytes, []byte("\n\n"))
    // remove trailing \n\n
    rawRecords = rawRecords[0:len(rawRecords)-1]
    records := make([]map[string]string, len(rawRecords))
    for i, rawRecord := range rawRecords {
        record := deserializeRecordBytes(rawRecord)
        records[i] = record
    }
    return records, startOffset + len(rawBytes), nil
}

func (f *Filecab) LoadRecord(thePath string) (map[string]string, error) {
    f.mu.RLock()
    defer f.mu.RUnlock()
    recordPath := f.RootDir + "/" + thePath + "/record.txt"
    data, err := os.ReadFile(recordPath)
    if err != nil {
        return nil, err
    }
    historyPath := f.RootDir + "/" + thePath + "/history.txt"
    fileInfo, err := os.Stat(historyPath)
    if err != nil {
        return nil, err
    }
    historySize := int(fileInfo.Size())
    record := deserializeRecordBytes(data)
    record["history_offset"] = strconv.Itoa(historySize)
    return deserializeRecordBytes(data), nil
}


func (f *Filecab) LoadAll(thePath string) ([]map[string]string, error) {
    f.mu.RLock()
    defer f.mu.RUnlock()
    
    // you could make this part concurrent (perf)
    hSizeChan := make(chan string, 1)
    go func() {
        historyPath := f.RootDir + "/" + thePath + "/history.txt"
        fileInfo, err := os.Stat(historyPath)
        if err != nil {
            hSizeChan <- "-1"
            // return nil, err
        } else {
            historySize := int(fileInfo.Size())
            historySizeString := strconv.Itoa(historySize)
            hSizeChan <- historySizeString
        }
    }()

    // fixed size local ids
    orderPath := f.RootDir + "/" + thePath + "/order.txt"
    data, err := os.ReadFile(orderPath)
    if err != nil {
        return nil, err
    }
    paths := strings.Split(string(data), "\n")
    paths = paths[0:len(paths) - 1] // trailing newline
    var maxConcurrency = 100
    var ch = make(chan int, maxConcurrency)
    var records = make([]map[string]string, len(paths))
    errCh := make(chan error, len(paths))
    for i, path := range paths {
        path := f.RootDir + "/" + thePath  + "/" + recordsName + "/" + strings.TrimRight(path, "_")
        i := i
        ch <- 1
        go func(path string) {
            defer func() {
                <- ch
            }()
            recordFile := path + "/record.txt"
            data, err := os.ReadFile(recordFile)
            if err != nil {
                panic(err)
                errCh <- err
                return
            }
            record := deserializeRecordBytes(data)
            records[i] = record
        }(path)
    }
    for i := 0; i < maxConcurrency; i++ {
        ch <- 1
    }
    close(errCh)
    if len(errCh) > 0 {
        return nil, <-errCh
    }
    historySizeString := <- hSizeChan
    if len(records) > 0 {
        records[0]["collection_offset"] = historySizeString
    }
    return records, nil
}

func (f *Filecab) LoadRange(thePath string, offset, limit int64) ([]map[string]string, error) {
    f.mu.RLock()
    defer f.mu.RUnlock()
    // you could make this part concurrent
    historyPath := f.RootDir + "/" + thePath + "/history.txt"
    fileInfo, err := os.Stat(historyPath)
    if err != nil {
        return nil, err
    }
    historySize := int(fileInfo.Size())
    historySizeString := strconv.Itoa(historySize)
    
    // fixed size local ids
    // each item in the file is 66 bytes separated by a new line
    // update this code to use the offset and limit args
    // so offset of 0 and limit of 1 will actually just grab 66 bytes from file
    // offset of 0 and limit of 2 will grab 66 + 1 + 66 bytes. (the 1 being newline)
    // offset of 0 and limit of 3 will grab 66 + 1 + 66 + 1 + 66 bytes. (the 1s being newline)
    // use the Seek and Read operations to read one large chunk
    // if offset is negative, start from the emd of the file
    // for example offset of -2 with limit 1 will give the second to last item in the file
    // don't read the whole file in to memory, just use Seek and Read to pull out a chunk
    // to memory, then split it on newline
    // Construct the path to order.txt
    // update this code to not get the lengt of the file, but use SEEK_END 
    // when dealing with negative offsets
    // but I only want one seek call at all
    // so either a SEEK_SET, or a SEEK_END depending in megative offset
    // I do not want a file.Stat call at all
    //
    orderPath := f.RootDir + "/" + thePath + "/order.txt"
    file, err := os.Open(orderPath)
    if err != nil {
        return nil, err
    }
    defer file.Close()
    const newlineSize = 1
    var startPos int64
    if offset < 0 {
        totalOffset := offset * (idSize + newlineSize)
        startPos, err = file.Seek(totalOffset, os.SEEK_END)
        if err != nil {
            return nil, err
        }
    } else {
        startPos = offset * (idSize + newlineSize)
        _, err = file.Seek(startPos, os.SEEK_SET)
        if err != nil {
            return nil, err
        }
    }

    chunkSize := limit * (idSize + newlineSize) - newlineSize
    buffer := make([]byte, chunkSize)
    n, err := file.Read(buffer)
    if err != nil && err != io.EOF {
        return nil, err
    }
    if n == 0 {
        return nil, nil // Handle case where no data was read
    }
    if n != len(buffer) { // adjust the buffer if we read less than expected
        buffer = buffer[:n]
    }
    paths := strings.Split(string(buffer), "\n")
    // for _, path := range paths {
    //     fmt.Println(path)
    // }
    // return nil, nil
    
    
    var maxConcurrency = 100
    var ch = make(chan int, maxConcurrency)
    var records = make([]map[string]string, len(paths))
    errCh := make(chan error, len(paths))
    for i, path := range paths {
        path := f.RootDir + "/" + thePath  + "/" + recordsName + "/" + strings.TrimRight(path, "_")
        i := i
        ch <- 1
        go func(path string) {
            defer func() {
                <- ch
            }()
            recordFile := path + "/record.txt"
            data, err := os.ReadFile(recordFile)
            if err != nil {
                panic(err)
                errCh <- err
                return
            }
            record := deserializeRecordBytes(data)
            records[i] = record
        }(path)
    }
    for i := 0; i < maxConcurrency; i++ {
        ch <- 1
    }
    close(errCh)
    if len(errCh) > 0 {
        return nil, <-errCh
    }
    if len(records) > 0 {
        records[0]["collection_offset"] = historySizeString
    }
    return records, nil
}





// todo: keep history file open...

func (f *Filecab) Load2(thePath string) ([]map[string]string, error) {
    f.mu.RLock()
    defer f.mu.RUnlock()
    var records []map[string]string
    theDir := f.RootDir + "/" + thePath
    fmt.Println("Loading:", theDir)
    
    var maxConcurrency = 1000
    ch := make(chan int, maxConcurrency)
    var mu sync.Mutex
    err := filepath.Walk(theDir, func(path string, info os.FileInfo, err error) error {
        if err != nil {
            return err
        }
        ch <- 1
        go func() {
            defer func() { <-ch }()
            // if info.IsDir() && strings.Count(path[len(theDir):], string(os.PathSeparator)) > 3 {
            //     return filepath.SkipDir
            // }
            if !info.IsDir() && info.Name() == "record.txt"  {
                data, err := os.ReadFile(path)
                if err != nil {
                    // return err
                }
                record := deserializeRecordBytes(data)
                mu.Lock()
                records = append(records, record)
                mu.Unlock()
            }
        }()
        return nil
    })
    if err != nil {
        return nil, err
    }
    for i := 0; i < maxConcurrency; i++ {
        ch <- 1
    }
    sort.Slice(records, func(i, j int) bool {
        return records[i]["id"] < records[j]["id"]
    })
    return records, nil
}


// function in Go to replace all non alphanumeric with underscore
// and then truncate to at most 32 chars
var nameRE *regexp.Regexp
func init() {
    _ = strconv.Itoa
	nameRE = regexp.MustCompile(`[^a-zA-Z0-9]`)
}

func nameize(s string) string {
    if s == "" {
        s = "r"
    }
    processed := nameRE.ReplaceAllString(s, "_")
    if len(processed) > 15 {
        processed = processed[:15]
    }
    processed = strings.ToLower(processed)
    processed = strings.TrimRight(processed, "_")
    return processed
}




func serializeRecord(obj map[string]string) string {
    keys := make([]string, 0, len(obj))
    for key := range obj {
        keys = append(keys, key)
    }
    sort.Strings(keys)
    var lines []string
    for _, key := range keys {
        value := obj[key]
        if !strings.Contains(value, "\n") {
            lines = append(lines, key+": "+value)
        } else {
            lines = append(lines, key+":")
            valueLines := strings.Split(value, "\n")
            for _, line := range valueLines {
                lines = append(lines, "    "+line)
            }
        }
    }
    return strings.Join(lines, "\n") + "\n\n"
}

func serializeRecordToBytes(obj map[string]string) []byte {
    if len(obj) == 0 {
        return []byte("\n\n")
    }
    keys := make([]string, 0, len(obj))
    for key := range obj {
        keys = append(keys, key)
    }
    sort.Strings(keys)
    var buffer strings.Builder
    for _, key := range keys {
        value := obj[key]
        buffer.WriteString(key)
        buffer.WriteString(": ")
        if !strings.Contains(value, "\n") {
            buffer.WriteString(value)
            buffer.WriteByte('\n')
        } else {
            buffer.WriteByte('\n')
            valueLines := strings.Split(value, "\n")
            for _, line := range valueLines {
                buffer.WriteString("    ")
                buffer.WriteString(line)
                buffer.WriteByte('\n')
            }
        }
    }
    buffer.WriteString("\n\n")
    if false {
        return gzipBytes([]byte(buffer.String()))
    } else {
        return []byte(buffer.String())
    }
}

func gzipBytes(data []byte) []byte {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	gz.Write(data)
	gz.Close()
	return buf.Bytes()
}
func ungzipBytes(data []byte) ([]byte, error) {
	buf := bytes.NewBuffer(data)
	gz, err := gzip.NewReader(buf)
	if err != nil {
		return nil, err
	}
	defer gz.Close()
	result, err := io.ReadAll(gz)
	if err != nil {
		return nil, err
	}
	return result, nil
}


// write the inverse of this function
// to turn the serialized value to a map[string]string
func deserializeRecord(data string) map[string]string {
    result := make(map[string]string)
    lines := strings.Split(data, "\n")
    var currentKey string
    var currentValue []string
    for _, line := range lines {
        if strings.HasPrefix(line, "    ") {
            currentValue = append(currentValue, strings.TrimPrefix(line, "    "))
        } else if strings.Contains(line, ":") {
            if currentKey != "" {
                result[currentKey] = strings.Join(currentValue, "\n")
            }
            parts := strings.SplitN(line, ": ", 2)
            currentKey = parts[0]
            if len(parts) == 2 {
                currentValue = []string{parts[1]}
            } else {
                currentValue = []string{}
            }
        }
    }
    if currentKey != "" {
        result[currentKey] = strings.Join(currentValue, "\n")
    }
    return result
}

// write a more efficient version of this function
// but take a []byte instead of a string
func deserializeRecordBytes(data []byte) map[string]string {
    result := make(map[string]string)
    // data, _ = ungzipBytes(data)
    lines := bytes.Split(data, []byte("\n"))
    var currentKey string
    var currentValue []byte
    for _, line := range lines {
        if bytes.HasPrefix(line, []byte("    ")) {
            currentValue = append(currentValue, bytes.TrimPrefix(line, []byte("    "))...)
            currentValue = append(currentValue, '\n')
        } else if idx := bytes.Index(line, []byte(": ")); idx != -1 {
            if currentKey != "" {
                result[currentKey] = string(bytes.TrimSuffix(currentValue, []byte("\n")))
            }
            currentKey = string(line[:idx])
            currentValue = append(line[idx+2:], '\n')
        }
    }
    if currentKey != "" {
        result[currentKey] = string(bytes.TrimSuffix(currentValue, []byte("\n")))
    }
    
    return result
}



func padString(s string, size int) string {
    for len(s) < size {
        s = "0" + s
    }
    return s
}

var counter int
func generateUniqueID() string {
    counter = (counter + 1) % 216000 // 60 ^ 3
    return padString(toBase60(counter), 3)
}

func generateUniqueID_old() string {
	randomBytes := make([]byte, 8)
	_, err := rand.Read(randomBytes)
	if err != nil {
		panic(err)
	}
	randomPart := hex.EncodeToString(randomBytes)
	return randomPart
}


// golang function to read a file in chuncks backwards up to a specific byte offset
func readFileInChunksBackwards(filePath string, offset int64, chunkSize int64) ([]byte, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var result []byte
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}
	fileSize := fileInfo.Size()
	readOffset := fileSize
	if offset < fileSize {
		readOffset = offset
	}
	for readOffset > 0 {
		chunkStart := readOffset - chunkSize
		if chunkStart < 0 {
			chunkStart = 0
		}
		chunk := make([]byte, readOffset-chunkStart)
		file.Seek(chunkStart, 0)
		file.Read(chunk)
		result = append(chunk, result...) // Prepend to maintain order
		readOffset = chunkStart
	}
	return result, nil
}

