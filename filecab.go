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
}
// 2024/07_14

// TODO: add log implementedemented as regular flow
// log in another directory for grepping purposes


// special fields
func (f *Filecab) Save(record map[string]string) error {
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
    // fmt.Println("fullDir", fullDir)
    err := os.MkdirAll(fullDir, os.ModePerm)
    if err != nil {
        return err
    }
    serialized := serializeRecord(record)
    filePath := fullDir + "/" + "record.txt"
    err = os.WriteFile(filePath, []byte(serialized), 0644)
    if err != nil {
        return err
    }

    if isNew {
        lastPath := f.RootDir + "/" + originalID + "last"
        _, err := os.Stat(lastPath);
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
            // fmt.Println(record["name"] + " making a next")
            nextPath := f.RootDir + "/" + originalID + "last/next"
            err := os.Symlink(fullDir, nextPath)
            if err != nil {
                return err
            }

            if _, err := os.Lstat(lastPath); err == nil {
                if err := os.Remove(lastPath); err != nil {
                    return err
                }
            }
            // os.Remove(lastPath)
            
            
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
        } else {
            // fmt.Println(record["name"] + " actual error", err)
            return err
        }
    }

    // fmt.Println(serialized)
    return nil
}
// implement the load function that loads all to a map[string]string
// thePath will be the id prefix and it will start at "first"
// and go along the linked list until there is no "next"
func (f *Filecab) Load(thePath string) ([]map[string]string, error) {
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
        record := deserializeRecord(string(data))
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


func generateUniqueID() string {
	randomBytes := make([]byte, 8)
	_, err := rand.Read(randomBytes)
	if err != nil {
		panic(err)
	}
	randomPart := hex.EncodeToString(randomBytes)
	return randomPart
}
