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

func ParallelExtractRows(lines <-chan string, wg *sync.WaitGroup, output chan<- *VCFContainer) {
	defer wg.Done()
	for line := range lines {
		output <- extractRow(line)
	}
}

func ParallelFilterRows(lines <-chan string, wg *sync.WaitGroup, output chan<- string, expressions []map[string]string) {
	defer wg.Done()

	for line := range lines {
		flag := true
		lineList := strings.Split(line, "\t")

		for i := range len(expressions) {
			if flag {
				part := expressions[i]

				key := part["key"]
				operator := part["operator"]
				value := part["value"]

				var linePart string
				if key == "QUAL" {
					linePart = lineList[5]
					if linePart == "." {
						flag = false
						break
					}

					numberFromLine, err := strconv.Atoi(linePart)
					if err != nil {
						s := fmt.Sprintf("Skipping invalid value: %s\n", err)
						LoggerError(s)
						continue
					}

					filterNumber, err := strconv.Atoi(value)
					if err != nil {
						s := fmt.Sprintf("Invalid number provided: %s\n", value)
						LoggerError(s)
						return
					}

					switch operator {
					case ">":
						if numberFromLine > filterNumber {
							flag = true
						} else {
							flag = false
							break
						}
					case "<":
						if numberFromLine < filterNumber {
							flag = true
						} else {
							flag = false
							break
						}
					case ">=":
						if numberFromLine >= filterNumber {
							flag = true
						} else {
							flag = false
							break
						}
					case "<=":
						if numberFromLine <= filterNumber {
							flag = true
						} else {
							flag = false
							break
						}
					case "==":
						if numberFromLine == filterNumber {
							flag = true
						} else {
							flag = false
							break
						}
					default:
						s := fmt.Sprintf("Unsupported comparison operator: %s\n", operator)
						LoggerError(s)
					}
				} else {
					linePart = lineList[7]
					itemValueList := strings.SplitSeq(linePart, ";")

					for itemValue := range itemValueList {
						if strings.HasPrefix(itemValue, key+"=") {
							numberStr := itemValue[len(key)+1:]
							numberList := strings.Split(numberStr, ",")
							numberStr = numberList[0]

							numberFromLine, err := strconv.ParseFloat(numberStr, 64)
							if err != nil {
								s := fmt.Sprintf("Skipping invalid value: %s\n", err)
								LoggerError(s)
								continue
							}

							filterNumber, err := strconv.ParseFloat(value, 64)
							if err != nil {
								s := fmt.Sprintf("Skipping invalid value: %s\n", err)
								LoggerError(s)
								continue
							}

							switch operator {
							case ">":
								if numberFromLine > filterNumber {
									flag = true
								} else {
									flag = false
									break
								}
							case "<":
								if numberFromLine < filterNumber {
									flag = true
								} else {
									flag = false
									break
								}
							case ">=":
								if numberFromLine >= filterNumber {
									flag = true
								} else {
									flag = false
									break
								}
							case "<=":
								if numberFromLine <= filterNumber {
									flag = true
								} else {
									flag = false
									break
								}
							case "==":
								if numberFromLine == filterNumber {
									flag = true
								} else {
									flag = false
									break
								}
							default:
								s := fmt.Sprintf("Unsupported comparison operator: %s\n", operator)
								LoggerError(s)
							}
						}
					}
				}
			}
		}
		if flag {
			output <- line
		}
	}
}
