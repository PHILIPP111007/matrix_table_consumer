package functions_go

import (
	"C"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

func getTime() string {
	return time.Now().Format("02-01-2006 15:04:05")
}

func LoggerInfo(s string) {
	t := getTime()
	fmt.Printf("[%s] - INFO - %s", t, s)
}
func LoggerError(s string) {
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

func ParallelExtractRows(lines <-chan string, wg *sync.WaitGroup, output chan<- *VCFContainer) {
	defer wg.Done()
	for line := range lines {
		output <- extractRow(line)
	}
}

func ParallelFilterRows(lines <-chan string, wg *sync.WaitGroup, output chan<- string, key string, expression string, filterNumber int) {
	defer wg.Done()

	for line := range lines {
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
					output <- line
				}
			case "<":
				if item < filterNumber {
					output <- line
				}
			case ">=":
				if item >= filterNumber {
					output <- line
				}
			case "<=":
				if item <= filterNumber {
					output <- line
				}
			default:
				s := fmt.Sprintf("Unsupported comparison operator: %s\n", expression)
				LoggerError(s)
			}
		}
	}
}
