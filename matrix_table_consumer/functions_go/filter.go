package functions_go

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"os"
	"strconv"
	"strings"
)

func Filter(include string, input_vcf_path string, output_vcf_path string, is_gzip bool) {
	parts := strings.Split(include, " ")
	key := parts[0]
	expression := parts[1]
	filterNumberStr := parts[2]

	filterNumber, err := strconv.Atoi(filterNumberStr)
	if err != nil {
		s := fmt.Sprintf("Invalid number provided: %s\n", filterNumberStr)
		LoggerError(s)
	}

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

	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "#") {
			fmt.Fprintln(writer, line)
			continue
		}

		lineList := strings.Split(line, "\t")

		if key == "QUAL" {
			itemValue := lineList[5]

			if itemValue == "." {
				continue
			}

			item, err := strconv.Atoi(itemValue)
			if err != nil {
				s := fmt.Sprintf("Skipping invalid value: %s\n", err)
				LoggerError(s)
				continue
			}

			switch expression {
			case ">":
				if item > filterNumber {
					fmt.Fprintln(writer, line)
				}
			case "<":
				if item < filterNumber {
					fmt.Fprintln(writer, line)
				}
			case ">=":
				if item >= filterNumber {
					fmt.Fprintln(writer, line)
				}
			case "<=":
				if item <= filterNumber {
					fmt.Fprintln(writer, line)
				}
			default:
				s := fmt.Sprintf("Unsupported comparison operator: %s\n", expression)
				LoggerError(s)
			}
		}
	}

	if err := scanner.Err(); err != nil {
		s := fmt.Sprintf("Reading standard input: %v\n", err)
		LoggerError(s)
	}
}
