package functions_go

import (
	"C"
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
)

func Collect(num_rows int, start_row int, vcf_path string, is_gzip bool, num_cpu int) string {
	if num_cpu <= 0 {
		num_cpu = 1
	}

	var reader *bufio.Reader

	if is_gzip {
		f, err := os.Open(vcf_path)
		if err != nil {
			s := fmt.Sprintf("Failed to open the file: %v\n", err)
			LoggerError(s)
		}
		defer f.Close()

		gr, err := gzip.NewReader(f)
		if err != nil {
			s := fmt.Sprintf("Error creating gzip reader: %v\n", err)
			LoggerError(s)
		}
		defer gr.Close()

		reader = bufio.NewReader(gr)
	} else {
		f, err := os.Open(vcf_path)
		if err != nil {
			s := fmt.Sprintf("Failed to open the file: %v\n", err)
			LoggerError(s)
		}
		defer f.Close()

		reader = bufio.NewReader(f)
	}

	flag := false
	rows := make([]*VCFRowJSON, 0)
	rows_count := 1
	num := 0

	linesChan := make(chan string, 100_000)
	resultsChan := make(chan *VCFRowJSON, 200_000)

	wg := sync.WaitGroup{}
	wg.Add(num_cpu)

	for range num_cpu {
		go ParallelExtractRows(linesChan, &wg, resultsChan)
	}

	scanner := bufio.NewScanner(reader)
	const maxTokenSize = 1 << 21
	buf := make([]byte, maxTokenSize)
	scanner.Buffer(buf, maxTokenSize)

	bar := NewTqdm(num_rows, WithDescription("Collecting data"))

	for scanner.Scan() {
		if strings.HasPrefix(scanner.Text(), "#") {
			continue
		}

		if rows_count >= start_row+num_rows {
			flag = false
			break
		} else if flag {
			line := scanner.Text()
			linesChan <- line
			bar.Increment()
		} else if start_row == rows_count {
			flag = true
			line := scanner.Text()
			linesChan <- line
			bar.Increment()
		}

		if num == 200_000 {
			num = 0
			len_chan := len(resultsChan)
			if len_chan != 0 {
				for range len_chan {
					row := <-resultsChan
					rows = append(rows, row)
				}
			}
		}

		rows_count += 1
		num += 1
	}
	bar.Close()

	close(linesChan)
	wg.Wait()
	close(resultsChan)

	if err := scanner.Err(); err != nil {
		s := fmt.Sprintf("Reading standard input: %v\n", err)
		LoggerError(s)
	}

	for row := range resultsChan {
		rows = append(rows, row)
	}

	jsonBytes, err := json.MarshalIndent(rows, "", "  ")
	if err != nil {
		s := fmt.Sprintf("JSON conversion error: %v\n", err)
		LoggerError(s)
	}

	return string(jsonBytes)
}

func CollectAll(vcf_path string, is_gzip bool, num_cpu int) string {
	if num_cpu <= 0 {
		num_cpu = 1
	}

	var reader *bufio.Reader

	if is_gzip {
		f, err := os.Open(vcf_path)
		if err != nil {
			s := fmt.Sprintf("Failed to open the file: %v\n", err)
			LoggerError(s)
		}
		defer f.Close()

		gr, err := gzip.NewReader(f)
		if err != nil {
			s := fmt.Sprintf("Error creating gzip reader: %v\n", err)
			LoggerError(s)
		}
		defer gr.Close()

		reader = bufio.NewReader(gr)
	} else {
		f, err := os.Open(vcf_path)
		if err != nil {
			s := fmt.Sprintf("Failed to open the file: %v\n", err)
			LoggerError(s)
		}
		defer f.Close()

		reader = bufio.NewReader(f)
	}

	// Channels for transmitting strings and results
	linesChan := make(chan string, 100_000)
	resultsChan := make(chan *VCFRowJSON, 200_000)

	rows_count := 0
	num := 0
	var rows []*VCFRowJSON

	wg := sync.WaitGroup{}
	wg.Add(num_cpu)

	for range num_cpu {
		go ParallelExtractRows(linesChan, &wg, resultsChan)
	}

	scanner := bufio.NewScanner(reader)
	const maxTokenSize = 1 << 21
	buf := make([]byte, maxTokenSize)
	scanner.Buffer(buf, maxTokenSize)

	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "#") {
			continue
		}

		if num == 200_000 {
			num = 0
			len_chan := len(resultsChan)
			if len_chan != 0 {
				for range len_chan {
					row := <-resultsChan
					rows = append(rows, row)
				}
			}
		}

		linesChan <- line

		if rows_count%50_000 == 0 && rows_count != 0 {
			s := fmt.Sprintf("%d lines read\n", rows_count)
			LoggerInfo(s)
		}

		num += 1
		rows_count += 1
	}

	LoggerInfo("Waiting for channels...\n")

	close(linesChan)
	wg.Wait()
	close(resultsChan)

	LoggerInfo("Extracting data...\n")

	for row := range resultsChan {
		rows = append(rows, row)
	}

	if err := scanner.Err(); err != nil {
		s := fmt.Sprintf("Reading standard input: %v\n", err)
		LoggerError(s)
	}

	jsonBytes, err := json.MarshalIndent(rows, "", "  ")
	if err != nil {
		s := fmt.Sprintf("JSON conversion error: %v\n", err)
		LoggerError(s)
	}

	return string(jsonBytes)
}

func Count(vcf_path string, is_gzip bool) int {
	var reader *bufio.Reader

	if is_gzip {
		f, err := os.Open(vcf_path)
		if err != nil {
			s := fmt.Sprintf("Failed to open the file: %v\n", err)
			LoggerError(s)
		}
		defer f.Close()

		gr, err := gzip.NewReader(f)
		if err != nil {
			s := fmt.Sprintf("Error creating gzip reader: %v\n", err)
			LoggerError(s)
		}
		defer gr.Close()

		reader = bufio.NewReader(gr)
	} else {
		f, err := os.Open(vcf_path)
		if err != nil {
			s := fmt.Sprintf("Failed to open the file: %v\n", err)
			LoggerError(s)
		}
		defer f.Close()

		reader = bufio.NewReader(f)
	}

	rows_count := 0

	scanner := bufio.NewScanner(reader)
	const maxTokenSize = 1 << 21
	buf := make([]byte, maxTokenSize)
	scanner.Buffer(buf, maxTokenSize)

	for scanner.Scan() && strings.HasPrefix(scanner.Text(), "#") {
	}

	for scanner.Scan() {
		rows_count += 1
	}

	if err := scanner.Err(); err != nil {
		s := fmt.Sprintf("Reading standard input: %v\n", err)
		LoggerError(s)
	}

	return rows_count + 1
}
