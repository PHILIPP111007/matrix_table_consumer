package main

// go build -o functions.so -buildmode=c-shared functions/functions.go

import (
	"C"
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
)
import "time"

type VCFContainer struct {
	Qual   int8   `json:"QUAL"`
	Pos    int32  `json:"POS"`
	Chrom  string `json:"CHROM"`
	Id     string `json:"ID"`
	Ref    string `json:"REF"`
	Alt    string `json:"ALT"`
	Filter string `json:"FILTER"`
	Info   string `json:"INFO"`
}

// Тип коллекции строк VCF
type Rows []*VCFContainer

func getTime() string {
	return time.Now().Format("02-01-2006 15:04:05")
}

func loggerInfo(s string) {
	t := getTime()
	fmt.Printf("[%s] - INFO - %s", t, s)
}
func loggerError(s string) {
	t := getTime()
	fmt.Printf("[%s] - ERROR - %s", t, s)
}

func extractRow(line string) *VCFContainer {
	fields := strings.Split(strings.TrimSpace(line), "\t")

	chrom := fields[0]
	pos, _ := strconv.Atoi(fields[1])
	pos32 := int32(pos)
	id := fields[2]
	ref := fields[3]
	alt := fields[4]
	qual, _ := strconv.Atoi(fields[5])
	qual8 := int8(qual)
	filter := fields[6]
	info := fields[7]

	return &VCFContainer{
		Qual:   qual8,
		Pos:    pos32,
		Chrom:  chrom,
		Id:     id,
		Ref:    ref,
		Alt:    alt,
		Filter: filter,
		Info:   info,
	}
}

func parallelExtractRows(lines <-chan string, wg *sync.WaitGroup, output chan<- *VCFContainer) {
	defer wg.Done()
	for line := range lines {
		output <- extractRow(line)
	}
}

//export CollectAll
func CollectAll(vcf_path_pointer *C.char, is_gzip bool, num_cpu int) *C.char {
	var reader *bufio.Reader
	vcf_path := C.GoString(vcf_path_pointer)

	if is_gzip {
		f, err := os.Open(vcf_path)
		if err != nil {
			s := fmt.Sprintf("Failed to open the file: %v\n", err)
			loggerError(s)
		}
		defer f.Close()

		gr, err := gzip.NewReader(f)
		if err != nil {
			s := fmt.Sprintf("Error creating gzip reader: %v\n", err)
			loggerError(s)
		}
		defer gr.Close()

		reader = bufio.NewReader(gr)
	} else {
		f, err := os.Open(vcf_path)
		if err != nil {
			s := fmt.Sprintf("Failed to open the file: %v\n", err)
			loggerError(s)
		}
		defer f.Close()

		reader = bufio.NewReader(f)
	}

	// Каналы для передачи строк и результатов
	linesChan := make(chan string, 10_000)
	resultsChan := make(chan *VCFContainer, 10_000)

	rows_count := 0
	var rows []*VCFContainer

	wg := sync.WaitGroup{}
	wg.Add(num_cpu)

	// Запустить рабочие горутины
	for range num_cpu {
		go parallelExtractRows(linesChan, &wg, resultsChan)
	}

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

		// Проверяем, не заполнен ли канал результатов
		select {
		case row := <-resultsChan:
			rows = append(rows, row)
		default:
			// ничего не делаем, если канал пуст
		}

		linesChan <- line

		if rows_count%50_000 == 0 {
			s := fmt.Sprintf("%d lines read\n", rows_count)
			loggerInfo(s)
		}
	}

	loggerInfo("Waiting for channels...\n")

	close(linesChan)
	wg.Wait()
	close(resultsChan)

	loggerInfo("Extracting data...\n")

	// Собираем результаты
	for row := range resultsChan {
		rows = append(rows, row)
	}

	if err := scanner.Err(); err != nil {
		s := fmt.Sprintf("Reading standard input: %v\n", err)
		loggerError(s)
	}

	jsonBytes, err := json.MarshalIndent(rows, "", "  ")
	if err != nil {
		s := fmt.Sprintf("JSON conversion error: %v\n", err)
		loggerError(s)
	}

	return C.CString(string(jsonBytes))
}

//export Collect
func Collect(num_rows int, start_row int, vcf_path_pointer *C.char, is_gzip bool) *C.char {
	var reader *bufio.Reader
	vcf_path := C.GoString(vcf_path_pointer)

	if is_gzip {
		f, err := os.Open(vcf_path)
		if err != nil {
			s := fmt.Sprintf("Failed to open the file: %v\n", err)
			loggerError(s)
		}
		defer f.Close()

		gr, err := gzip.NewReader(f)
		if err != nil {
			s := fmt.Sprintf("Error creating gzip reader: %v\n", err)
			loggerError(s)
		}
		defer gr.Close()

		reader = bufio.NewReader(gr)
	} else {
		f, err := os.Open(vcf_path)
		if err != nil {
			s := fmt.Sprintf("Failed to open the file: %v\n", err)
			loggerError(s)
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
		} else if num-start_row >= num_rows {
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
		s := fmt.Sprintf("Reading standard input: %v\n", err)
		loggerError(s)
	}

	jsonBytes, err := json.MarshalIndent(rows, "", "  ")
	if err != nil {
		s := fmt.Sprintf("JSON conversion error: %v\n", err)
		loggerError(s)
	}

	return C.CString(string(jsonBytes))
}

//export Count
func Count(vcf_path_pointer *C.char, is_gzip bool) int {
	var reader *bufio.Reader

	vcf_path := C.GoString(vcf_path_pointer)

	if is_gzip {
		f, err := os.Open(vcf_path)
		if err != nil {
			s := fmt.Sprintf("Failed to open the file: %v\n", err)
			loggerError(s)
		}
		defer f.Close()

		gr, err := gzip.NewReader(f)
		if err != nil {
			s := fmt.Sprintf("Error creating gzip reader: %v\n", err)
			loggerError(s)
		}
		defer gr.Close()

		reader = bufio.NewReader(gr)
	} else {
		f, err := os.Open(vcf_path)
		if err != nil {
			s := fmt.Sprintf("Failed to open the file: %v\n", err)
			loggerError(s)
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
		s := fmt.Sprintf("Reading standard input: %v\n", err)
		loggerError(s)
	}

	return rows_count
}

func main() {}
