package filecab


import (
    "sync"
    "strings"
    "strconv"
    "sort"
    "time"
    "fmt"
    "math/rand"
    "encoding/hex"
    "regexp"
    "os"
    "bytes"
    "path/filepath"
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
// 2024/07_14

// TODO: add log implementedemented as regular flow
// log in another directory for grepping purposes


// special fields
func (f *Filecab) Save_old(record map[string]string) error {
    f.mu.Lock()
    defer f.mu.Unlock()
    
    isNew := false
    var originalID = ""
    if strings.HasSuffix(record["id"], "/") {
        originalID = record["id"]
        now := time.Now()
        record["id"] += now.Format("2006/01_02/15_04_05_") + fmt.Sprintf("%03d", now.Nanosecond()/1e6) + "_" + generateUniqueID() + "_" + nameize(record["name"])
        isNew = true
    }
    
    record["id"] = strings.ReplaceAll(record["id"], "..", "")
    fullDir := f.RootDir + "/" + record["id"]
    filePath := fullDir + "/" + "record.txt"
    if isNew {
        err := os.MkdirAll(fullDir, os.ModePerm)
        if err != nil {
            return err
        }


        serializedBytes := serializeRecordToBytes(record)
        err = os.WriteFile(filePath, serializedBytes, 0644)
        if err != nil {
            return err
        }
        
        lastPath := f.RootDir + "/" + originalID + "last"
        _, err = os.Stat(lastPath);
        if os.IsNotExist(err) {
            // fmt.Println(record["name"] + " made a new last")
            err = os.Symlink(fullDir, lastPath)
            if err != nil {
                return err
            }
            firstPath := f.RootDir + "/" + originalID + "first"
            err = os.Symlink(fullDir, firstPath)
            if err != nil {
                return err
            }
            lengthPath := f.RootDir + "/" + originalID + "length"
            err = os.WriteFile(lengthPath, []byte("1"), 0644)
            if err != nil {
                return err
            }
        } else if err == nil {
            if true {
                nextPath := f.RootDir + "/" + originalID + "last/next"
                // Attempt to create the first symlink
                if err := os.Symlink(fullDir, nextPath); err != nil {
                    return err
                }
                // Attempt to remove the existing 'last' symlink
                if err := os.Remove(lastPath); err != nil && !os.IsNotExist(err) {
                    return err
                }
                // Attempt to create the new 'last' symlink
                if err := os.Symlink(fullDir, lastPath); err != nil {
                    return err
                }

                // lengthPath := f.RootDir + "/" + originalID + "length"
                // existingLengthData, err := os.ReadFile(lengthPath)
                // if err != nil {
                //     return err
                // }
                // existingLength, err := strconv.Atoi(string(existingLengthData))
                // if err != nil {
                //     return err
                // }
                // newLength := existingLength + 1
                // err = os.WriteFile(lengthPath, []byte(strconv.Itoa(newLength)), 0644)
                // if err != nil {
                //     return err
                // }
            }
        } else {
            return err
        }
    } else {
        existingData, err := os.ReadFile(filePath)
        if err != nil {
            return err
        }
        existingRecord := deserializeRecordBytes(existingData)
        // Modify the existing record as needed
        for k, v := range record {
            existingRecord[k] = v
        }
        serializedBytes := serializeRecordToBytes(existingRecord)
        err = os.WriteFile(filePath, []byte(serializedBytes), 0644)
        if err != nil {
            return err
        }
    }
    return nil
}

// update this code to also add a prev symmlink to the previous record
// to make a doubly linked list basically
func (f *Filecab) Save(record map[string]string) error {
    f.mu.Lock()
    defer f.mu.Unlock()
    
    isNew := false
    var originalID = ""
    if strings.HasSuffix(record["id"], "/") {
        originalID = record["id"]
        now := time.Now()
        record["id"] += now.Format("2006/01_02/15_04_05_") + fmt.Sprintf("%03d", now.Nanosecond()/1e6) + "_" + generateUniqueID() + "_" + nameize(record["name"])
        // record["id"] += fmt.Sprintf("%03d", now.Nanosecond()/1e6) + "_" + generateUniqueID() + "_" + nameize(record["name"])
        isNew = true
    }
    
    record["id"] = strings.ReplaceAll(record["id"], "..", "")
    fullDir := f.RootDir + "/" + record["id"]
    filePath := fullDir + "/" + "record.txt"
    if isNew {
        var err error
        
        err = os.MkdirAll(fullDir, os.ModePerm)
        if err != nil {
            return err
        }
        errCh := make(chan error, 10)
        var errChCount = 0
        serializedBytes := serializeRecordToBytes(record)
        errChCount++
        go func() {
            errCh <- os.WriteFile(filePath, serializedBytes, 0644)
        }()
        
        lastPath := f.RootDir + "/" + originalID + "last"
        _, err = os.Stat(lastPath);
        if os.IsNotExist(err) {
            err = os.Symlink(fullDir, lastPath)
            if err != nil {
                return err
            }
            firstPath := f.RootDir + "/" + originalID + "first"
            err = os.Symlink(fullDir, firstPath)
            if err != nil {
                return err
            }
            lengthPath := f.RootDir + "/" + originalID + "length"
            err = os.WriteFile(lengthPath, []byte("1"), 0644)
            if err != nil {
                return err
            }
        } else if err == nil {
            prevLastDir, err := os.Readlink(lastPath)
            if err != nil {
                return err
            }
            // "next" part
            errChCount++
            go func() {
                nextPath := prevLastDir + "/next"
                errCh <- os.Symlink(fullDir, nextPath)
            }()
            // "prev" part
            errChCount++
            go func() {
                prevPath := fullDir + "/prev"
                errCh <- os.Symlink(prevLastDir, prevPath)
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
                errCh <- os.Symlink(fullDir, lastPath)
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
    }
    return nil
}

func (f *Filecab) Save_old2(record map[string]string) error {
    f.mu.Lock()
    defer f.mu.Unlock()
    
    isNew := false
    var originalID = ""
    if strings.HasSuffix(record["id"], "/") {
        originalID = record["id"]
        now := time.Now()
        record["id"] += now.Format("2006/01_02/15_04_05_") + fmt.Sprintf("%03d", now.Nanosecond()/1e6) + "_" + generateUniqueID() + "_" + nameize(record["name"])
        isNew = true
    }
    
    record["id"] = strings.ReplaceAll(record["id"], "..", "")
    fullDir := f.RootDir + "/" + record["id"]
    filePath := fullDir + "/" + "record.txt"
    if isNew {
        err := os.MkdirAll(fullDir, os.ModePerm)
        if err != nil {
            return err
        }
        serializedBytes := serializeRecordToBytes(record)
        err = os.WriteFile(filePath, serializedBytes, 0644)
        if err != nil {
            return err
        }
        
        lastPath := f.RootDir + "/" + originalID + "last"
        _, err = os.Stat(lastPath);
        if os.IsNotExist(err) {
            err = os.Symlink(fullDir, lastPath)
            if err != nil {
                return err
            }
            firstPath := f.RootDir + "/" + originalID + "first"
            err = os.Symlink(fullDir, firstPath)
            if err != nil {
                return err
            }
            lengthPath := f.RootDir + "/" + originalID + "length"
            err = os.WriteFile(lengthPath, []byte("1"), 0644)
            if err != nil {
                return err
            }
        } else if err == nil {
            prevLastDir, err := os.Readlink(lastPath)
            if err != nil {
                return err
            }
            nextPath := prevLastDir + "/next"
            if err := os.Symlink(fullDir, nextPath); err != nil {
                return err
            }
            prevPath := fullDir + "/prev"
            if err := os.Symlink(prevLastDir, prevPath); err != nil {
                return err
            }
            if err := os.Remove(lastPath); err != nil && !os.IsNotExist(err) {
                return err
            }
            if err := os.Symlink(fullDir, lastPath); err != nil {
                return err
            }
        } else {
            return err
        }
    } else {
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
            recordDir = nextPath
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
        s = "record"
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
    return []byte(buffer.String())
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


func generateUniqueID() string {
	randomBytes := make([]byte, 8)
	_, err := rand.Read(randomBytes)
	if err != nil {
		panic(err)
	}
	randomPart := hex.EncodeToString(randomBytes)
	return randomPart
}
