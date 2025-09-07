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

func extractRow(line string) *VCFRowJSON {
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

	return &VCFRowJSON{
		Chrom:  chrom,
		Id:     id,
		Ref:    ref,
		Alt:    alt,
		Filter: filter,
		Info:   info,
		Pos:    pos32,
		Qual:   qual8,
	}
}

func ParallelExtractRows(lines <-chan string, wg *sync.WaitGroup, output chan<- *VCFRowJSON) {
	defer wg.Done()
	for line := range lines {
		output <- extractRow(line)
	}
}
