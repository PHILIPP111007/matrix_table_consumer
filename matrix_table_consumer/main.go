package main

import (
	"C"
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"

	functions "functions/functions"
)

//export CollectAll
func CollectAll(vcf_path_pointer *C.char, is_gzip bool, num_cpu int) *C.char {
	if num_cpu <= 0 {
		num_cpu = 1
	}

	var reader *bufio.Reader
	vcf_path := C.GoString(vcf_path_pointer)

	if is_gzip {
		f, err := os.Open(vcf_path)
		if err != nil {
			s := fmt.Sprintf("Failed to open the file: %v\n", err)
			functions.LoggerError(s)
		}
		defer f.Close()

		gr, err := gzip.NewReader(f)
		if err != nil {
			s := fmt.Sprintf("Error creating gzip reader: %v\n", err)
			functions.LoggerError(s)
		}
		defer gr.Close()

		reader = bufio.NewReader(gr)
	} else {
		f, err := os.Open(vcf_path)
		if err != nil {
			s := fmt.Sprintf("Failed to open the file: %v\n", err)
			functions.LoggerError(s)
		}
		defer f.Close()

		reader = bufio.NewReader(f)
	}

	// Каналы для передачи строк и результатов
	linesChan := make(chan string, 10_000)
	resultsChan := make(chan *functions.VCFContainer, 10_000)

	rows_count := 0
	var rows []*functions.VCFContainer

	wg := sync.WaitGroup{}
	wg.Add(num_cpu)

	for range num_cpu {
		go functions.ParallelExtractRows(linesChan, &wg, resultsChan)
	}

	scanner := bufio.NewScanner(reader)
	const maxTokenSize = 1 << 20
	buf := make([]byte, maxTokenSize)
	scanner.Buffer(buf, maxTokenSize)

	// Пропускаем строки с символами # (заголовки)
	for scanner.Scan() && strings.HasPrefix(scanner.Text(), "#") {
	}

	for scanner.Scan() {
		line := scanner.Text()

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
			functions.LoggerInfo(s)
		}

		rows_count += 1
	}

	functions.LoggerInfo("Waiting for channels...\n")

	close(linesChan)
	wg.Wait()
	close(resultsChan)

	functions.LoggerInfo("Extracting data...\n")

	// Собираем результаты
	for row := range resultsChan {
		rows = append(rows, row)
	}

	if err := scanner.Err(); err != nil {
		s := fmt.Sprintf("Reading standard input: %v\n", err)
		functions.LoggerError(s)
	}

	jsonBytes, err := json.MarshalIndent(rows, "", "  ")
	if err != nil {
		s := fmt.Sprintf("JSON conversion error: %v\n", err)
		functions.LoggerError(s)
	}

	return C.CString(string(jsonBytes))
}

//export Collect
func Collect(num_rows int, start_row int, vcf_path_pointer *C.char, is_gzip bool, num_cpu int) *C.char {
	if num_cpu <= 0 {
		num_cpu = 1
	}

	var reader *bufio.Reader
	vcf_path := C.GoString(vcf_path_pointer)

	if is_gzip {
		f, err := os.Open(vcf_path)
		if err != nil {
			s := fmt.Sprintf("Failed to open the file: %v\n", err)
			functions.LoggerError(s)
		}
		defer f.Close()

		gr, err := gzip.NewReader(f)
		if err != nil {
			s := fmt.Sprintf("Error creating gzip reader: %v\n", err)
			functions.LoggerError(s)
		}
		defer gr.Close()

		reader = bufio.NewReader(gr)
	} else {
		f, err := os.Open(vcf_path)
		if err != nil {
			s := fmt.Sprintf("Failed to open the file: %v\n", err)
			functions.LoggerError(s)
		}
		defer f.Close()

		reader = bufio.NewReader(f)
	}

	flag := false
	rows := make([]*functions.VCFContainer, 0)
	rows_count := 0

	// Каналы для передачи строк и результатов
	linesChan := make(chan string, 10_000)
	resultsChan := make(chan *functions.VCFContainer, 10_000)

	wg := sync.WaitGroup{}
	wg.Add(num_cpu)

	for range num_cpu {
		go functions.ParallelExtractRows(linesChan, &wg, resultsChan)
	}

	scanner := bufio.NewScanner(reader)
	const maxTokenSize = 1 << 20
	buf := make([]byte, maxTokenSize)
	scanner.Buffer(buf, maxTokenSize)

	// Пропускаем строки с символами # (заголовки)
	for scanner.Scan() && strings.HasPrefix(scanner.Text(), "#") {
	}

	for scanner.Scan() {
		if rows_count >= start_row+num_rows {
			flag = false
			break
		} else if flag {
			line := scanner.Text()
			linesChan <- line
		} else if start_row == rows_count {
			flag = true
			line := scanner.Text()
			linesChan <- line
		}

		select {
		case row := <-resultsChan:
			rows = append(rows, row)
		default:
			// ничего не делаем, если канал пуст
		}

		rows_count += 1
	}

	close(linesChan)
	wg.Wait()
	close(resultsChan)

	if err := scanner.Err(); err != nil {
		s := fmt.Sprintf("Reading standard input: %v\n", err)
		functions.LoggerError(s)
	}

	// Собираем результаты
	for row := range resultsChan {
		rows = append(rows, row)
	}

	jsonBytes, err := json.MarshalIndent(rows, "", "  ")
	if err != nil {
		s := fmt.Sprintf("JSON conversion error: %v\n", err)
		functions.LoggerError(s)
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
			functions.LoggerError(s)
		}
		defer f.Close()

		gr, err := gzip.NewReader(f)
		if err != nil {
			s := fmt.Sprintf("Error creating gzip reader: %v\n", err)
			functions.LoggerError(s)
		}
		defer gr.Close()

		reader = bufio.NewReader(gr)
	} else {
		f, err := os.Open(vcf_path)
		if err != nil {
			s := fmt.Sprintf("Failed to open the file: %v\n", err)
			functions.LoggerError(s)
		}
		defer f.Close()

		reader = bufio.NewReader(f)
	}

	rows_count := 0

	scanner := bufio.NewScanner(reader)
	const maxTokenSize = 1 << 20
	buf := make([]byte, maxTokenSize)
	scanner.Buffer(buf, maxTokenSize)

	for scanner.Scan() && strings.HasPrefix(scanner.Text(), "#") {
	}

	for scanner.Scan() {
		rows_count += 1
	}

	if err := scanner.Err(); err != nil {
		s := fmt.Sprintf("Reading standard input: %v\n", err)
		functions.LoggerError(s)
	}

	return rows_count
}

func main() {}
