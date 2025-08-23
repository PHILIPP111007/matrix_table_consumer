package functions_go

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
)

func Filter(include string, input_vcf_path string, output_vcf_path string, is_gzip bool, num_cpu int) {
	if num_cpu <= 0 {
		num_cpu = 1
	}

	parts := strings.Split(include, " ")
	key := parts[0]
	expression := parts[1]
	filterNumberStr := parts[2]

	var reader *bufio.Reader
	if is_gzip {
		f, err := os.Open(input_vcf_path)
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
		f, err := os.Open(input_vcf_path)
		if err != nil {
			s := fmt.Sprintf("Failed to open the file: %v\n", err)
			LoggerError(s)
		}
		defer f.Close()

		reader = bufio.NewReader(f)
	}

	outputFile, err := os.Create(output_vcf_path)
	if err != nil {
		s := fmt.Sprintf("Error creating file: %v\n", err)
		LoggerError(s)
	}
	defer outputFile.Close()

	scanner := bufio.NewScanner(reader)
	writer := bufio.NewWriter(outputFile)
	defer writer.Flush()

	wg := sync.WaitGroup{}
	wg.Add(num_cpu)
	linesChan := make(chan string, 100_000)
	resultsChan := make(chan string, 500_000)
	num := 0

	switch key {
	case "QUAL":
		filterNumber, err := strconv.Atoi(filterNumberStr)
		if err != nil {
			s := fmt.Sprintf("Invalid number provided: %s\n", filterNumberStr)
			LoggerError(s)
			return
		}

		for range num_cpu {
			go ParallelFilterRowsByQUAL(linesChan, &wg, resultsChan, key, expression, filterNumber)
		}

	case "AF":
		filterNumber, err := strconv.ParseFloat(filterNumberStr, 64)
		if err != nil {
			s := fmt.Sprintf("Invalid number provided: %s\n", filterNumberStr)
			LoggerError(s)
			return
		}

		for range num_cpu {
			go ParallelFilterRowsByAF(linesChan, &wg, resultsChan, key, expression, filterNumber)
		}
	}

	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "#") {
			fmt.Fprintln(writer, line)
			continue
		}

		if num == 500_000 {
			num = 0
			len_chan := len(resultsChan)
			if len_chan != 0 {
				for range len_chan {
					row := <-resultsChan
					fmt.Fprintln(writer, row)
				}
			}
		}

		linesChan <- line
		num += 1
	}

	close(linesChan)
	wg.Wait()
	close(resultsChan)

	if err := scanner.Err(); err != nil {
		s := fmt.Sprintf("Reading standard input: %v\n", err)
		LoggerError(s)
	}

	for row := range resultsChan {
		fmt.Fprintln(writer, row)
	}
}
