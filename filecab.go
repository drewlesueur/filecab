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
}

// update this code to also add a prev symmlink to the previous record
// to make a doubly linked list basically
func (f *Filecab) Save(record map[string]string) error {
    f.mu.Lock()
    defer f.mu.Unlock()
    return f.saveInternal(true, record)
}

func (f *Filecab) saveHistory(level int, record map[string]string) error {
    // if level != 0 {
    //     return nil
    // }
    // return nil
    // open a file for appending that's record["id"] + "/history.txt"
    // serialize the record with the existing serializeRecordToBytes function
    // and append the bytes, followed by 2 new lines
    
    parts := strings.Split(record["id"], "/"+recordsName+"/")
    parts = parts[0:len(parts)-level]
    usedId := strings.Join(parts, "/"+recordsName+"/")
    // fmt.Println(level, "used:", usedId, "orig:", record["id"])
    
    // return nil
    filePath := f.RootDir + "/" + usedId + "/history.txt"
    file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
    if err != nil {
        return err
    }
    defer file.Close()
    data := serializeRecordToBytes(record) // TODO: reuse serialized
    if _, err := file.Write(data); err != nil {
        return err
    }
    return nil
}

const singleFileHistory = true
// const singleFileHistory = false


const recordsName = "records"
// const recordsName = "records"
func (f *Filecab) saveInternal(doLog bool, record map[string]string) error {
    // if !doLog {
    //     return nil
    // }
    isNew := false
    var originalID = ""
    if strings.HasSuffix(record["id"], "/") {
        originalID = record["id"]
        now := time.Now()
        record["id"] += recordsName + "/" + now.Format("2006_01_02/15_04_05_") + fmt.Sprintf("%03d", now.Nanosecond()/1e6) + "_" + generateUniqueID() + "_" + nameize(record["name"])
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
        var err error
        err = os.MkdirAll(fullDir, os.ModePerm)
        if err != nil {
            return err
        }
        errCh := make(chan error, 12)
        var errChCount = 0
        
        if record["override_symlink"] == "" {
            serializedBytes := serializeRecordToBytes(record)
            errChCount++
            go func() {
                errCh <- os.WriteFile(filePath, serializedBytes, 0644)
            }()
        } else {
            errChCount++
            go func() {
                // errCh <- os.MkdirAll(fullDir, os.ModePerm)
                errCh <- os.Symlink(record["override_symlink"], filePath)
                // Todo: make relative
                // errCh <- os.Symlink("./" + record["override_symlink"], filePath)
            }()
        }

        if doLog {
            if singleFileHistory {
                errChCount++
                go func() {
                    errCh <- f.saveHistory(0, record)
                }()
                errChCount++
                go func() {
                    errCh <- f.saveHistory(1, record)
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
                errChCount++
                go func() {
                    errCh <- f.saveHistory(0, record)
                }()
                errChCount++
                go func() {
                    errCh <- f.saveHistory(1, record)
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


func (f *Filecab) Load3(thePath string) ([]map[string]string, error) {
    f.mu.RLock()
    defer f.mu.RUnlock()
    var paths []string
    recordDir := f.RootDir + "/" + thePath + "/first"
    fmt.Println(recordDir, "_dodgerblue")
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
	if len(processed) > 32 {
		processed = processed[:32]
	}
	return strings.ToLower(processed)
}


func New(rootDir string) *Filecab {
    if rootDir == "/" {
        panic("Root directory cannot be '/'")
    }
    return &Filecab{
        RootDir: rootDir,
        // cachedDir: map[string]bool{},
    }
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




var counter int
func generateUniqueID() string {
    counter++
    return strconv.Itoa(counter)
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
