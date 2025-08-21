package main

// go build -o functions.so -buildmode=c-shared functions/functions.go

import (
    "C"
    "compress/gzip"
    "bufio"
    "fmt"
    "os"
    "strings"
    "encoding/json"
    "strconv"
)


type VCFContainer struct {
    Pos    int `json:"POS"`
    Qual   int `json:"QUAL"`
    Chrom  string `json:"CHROM"`
    Id     string `json:"ID"`
    Ref    string `json:"REF"`
    Alt    string `json:"ALT"`
    Filter string `json:"FILTER"`
    Info   string `json:"INFO"`
}

// Тип коллекции строк VCF
type Rows []*VCFContainer


func extractRow(line string) *VCFContainer {
    fields := strings.Split(strings.TrimSpace(line), "\t")
    
    chrom := fields[0]
    pos, _ := strconv.Atoi(fields[1])
    id := fields[2]
    ref := fields[3]
    alt := fields[4]
    qual, _ := strconv.Atoi(fields[5])
    filter := fields[6]
    info := fields[7]

    return &VCFContainer{
        Chrom: chrom,
        Pos: pos,
        Id: id,
        Ref: ref,
        Alt: alt,
        Qual: qual,
        Filter: filter,
        Info: info,
    }
}


//export CollectAll
func CollectAll(vcfPathPointer *C.char, isGzip bool) *C.char {
    var reader *bufio.Reader

    vcfPath :=  C.GoString(vcfPathPointer)

    if isGzip {
        f, err := os.Open(vcfPath)
        if err != nil {
            fmt.Fprintf(os.Stderr, "Failed to open the file: %v", err)
        }
        defer f.Close()
        
        gr, err := gzip.NewReader(f)
        if err != nil {
            fmt.Fprintf(os.Stderr, "Error creating gzip reader: %v", err)
        }
        defer gr.Close()
        
        reader = bufio.NewReader(gr)
    } else {
        f, err := os.Open(vcfPath)
        if err != nil {
            fmt.Fprintf(os.Stderr, "Failed to open the file: %v", err)
        }
        defer f.Close()
        
        reader = bufio.NewReader(f)
    }

    rows := make([]*VCFContainer, 0)

    scanner := bufio.NewScanner(reader)
    const maxTokenSize = 1 << 20
    buf := make([]byte, maxTokenSize)
    scanner.Buffer(buf, maxTokenSize)
    for scanner.Scan() {
        line := scanner.Text()
        if strings.HasPrefix(line, "#") {
            continue
        }
        row := extractRow(line)
        rows = append(rows, row)
    }

    if err := scanner.Err(); err != nil {
        fmt.Fprintf(os.Stderr, "reading standard input: %v", err)
    }

    jsonBytes, err := json.MarshalIndent(rows, "", "  ")
    if err != nil {
        fmt.Fprintf(os.Stderr, "JSON conversion error: %v", err)
    }

    return C.CString(string(jsonBytes))
}


//export Collect
func Collect(num_rows int, start_row int, vcfPathPointer *C.char, isGzip bool) *C.char {
    var reader *bufio.Reader
    vcfPath :=  C.GoString(vcfPathPointer)

    if isGzip {
        f, err := os.Open(vcfPath)
        if err != nil {
            fmt.Fprintf(os.Stderr, "Failed to open the file: %v", err)
        }
        defer f.Close()
        
        gr, err := gzip.NewReader(f)
        if err != nil {
            fmt.Fprintf(os.Stderr, "Error creating gzip reader: %v", err)
        }
        defer gr.Close()
        
        reader = bufio.NewReader(gr)
    } else {
        f, err := os.Open(vcfPath)
        if err != nil {
            fmt.Fprintf(os.Stderr, "Failed to open the file: %v", err)
        }
        defer f.Close()
        
        reader = bufio.NewReader(f)
    }

    num := 0
    flag := false
    rows := make([]*VCFContainer, 0)

    scanner := bufio.NewScanner(reader)
    const maxTokenSize = 1 << 20
    buf := make([]byte, maxTokenSize)
    scanner.Buffer(buf, maxTokenSize)
    for scanner.Scan() {
        line := scanner.Text()

        if strings.HasPrefix(line, "#") {
            continue
        } else if (num - start_row >= num_rows) {
            break
        } else if flag {
            row := extractRow(line)
            rows = append(rows, row)
            num += 1
        } else if start_row == num {
            flag = true
            row := extractRow(line)
            rows = append(rows, row)
            num += 1
        } else {
            num += 1
        }
    }

    if err := scanner.Err(); err != nil {
        fmt.Fprintf(os.Stderr, "Reading standard input: %v", err)
    }

    jsonBytes, err := json.MarshalIndent(rows, "", "  ")
    if err != nil {
        fmt.Fprintf(os.Stderr, "JSON conversion error: %v", err)
    }

    return C.CString(string(jsonBytes))
}


//export Count
func Count(vcfPathPointer *C.char, isGzip bool) int {
    var reader *bufio.Reader

    vcfPath :=  C.GoString(vcfPathPointer)

    if isGzip {
        f, err := os.Open(vcfPath)
        if err != nil {
            fmt.Fprintf(os.Stderr, "Failed to open the file: %v", err)
        }
        defer f.Close()
        
        gr, err := gzip.NewReader(f)
        if err != nil {
            fmt.Fprintf(os.Stderr, "Error creating gzip reader: %v", err)
        }
        defer gr.Close()
        
        reader = bufio.NewReader(gr)
    } else {
        f, err := os.Open(vcfPath)
        if err != nil {
            fmt.Fprintf(os.Stderr, "Failed to open the file: %v", err)
        }
        defer f.Close()
        
        reader = bufio.NewReader(f)
    }

    rows_count := 0

    scanner := bufio.NewScanner(reader)
    const maxTokenSize = 1 << 20
    buf := make([]byte, maxTokenSize)
    scanner.Buffer(buf, maxTokenSize)
    for scanner.Scan() {
        line := scanner.Text()
        if strings.HasPrefix(line, "#") {
            continue
        }
        rows_count += 1
    }

    if err := scanner.Err(); err != nil {
        fmt.Fprintf(os.Stderr, "reading standard input: %v", err)
    }

    return rows_count
}


func main() {}
