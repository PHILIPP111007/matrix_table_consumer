package functions_go

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/PHILIPP111007/govaluate"
)

// ParseVCFRow парсит строку VCF
func ParseVCFRow(line string) *VCFRow {
	parts := strings.Split(line, "\t")
	if len(parts) < 8 {
		return nil
	}

	pos, _ := strconv.Atoi(parts[1])
	pos32 := int32(pos)

	var qual8 int8 = 0
	if parts[5] != "." {
		qual, _ := strconv.Atoi(parts[5])
		qual8 = int8(qual)
	}

	row := &VCFRow{
		Chrom:      parts[0],
		ID:         parts[2],
		Ref:        parts[3],
		Alt:        parts[4],
		Filter:     parts[6],
		Info:       parts[7],
		InfoFields: make(map[string]string),
		Pos:        pos32,
		Qual:       qual8,
	}

	if len(parts) > 8 {
		row.Format = parts[8]
	}
	if len(parts) > 9 {
		row.Samples = parts[9:]
	}

	// Парсим INFO поле
	infoParts := strings.SplitSeq(row.Info, ";")
	for part := range infoParts {
		if strings.Contains(part, "=") {
			kv := strings.SplitN(part, "=", 2)
			part := strings.SplitN(kv[1], ",", 2)[0]
			if len(kv) == 2 {
				row.InfoFields[kv[0]] = part
			}
		} else {
			row.InfoFields[part] = "true"
		}
	}

	return row
}

// GetValue возвращает значение поля по имени
func (r *VCFRow) GetValue(fieldName string) (any, error) {
	switch fieldName {
	case "QUAL":
		return r.Qual, nil
	case "CHROM":
		return r.Chrom, nil
	case "POS":
		return r.Pos, nil
	case "ID":
		return r.ID, nil
	case "REF":
		return r.Ref, nil
	case "ALT":
		return r.Alt, nil
	case "FILTER":
		return r.Filter, nil
	default:
		// INFO поля
		if value, exists := r.InfoFields[fieldName]; exists {
			if value == "true" {
				return true, nil
			}
			// Пробуем парсить как число

			valueParts := strings.Split(value, ",")
			value = valueParts[0]

			if num, err := strconv.ParseFloat(value, 64); err == nil {
				return num, nil
			}
			return value, nil
		}
		return nil, fmt.Errorf("field %s not found", fieldName)
	}
}

// FilterFunctions содержит функции для фильтрации
var FilterFunctions = map[string]govaluate.ExpressionFunction{
	"has": func(args ...any) (any, error) {
		if len(args) != 2 {
			return nil, fmt.Errorf("has() expects 2 arguments")
		}
		_, ok := args[0].(string)
		if !ok {
			return nil, fmt.Errorf("first argument must be string")
		}
		_, ok = args[1].(string)
		if !ok {
			return nil, fmt.Errorf("second argument must be string")
		}

		// Для простоты всегда возвращаем true, реальная реализация будет в EvaluateRow
		return true, nil
	},
}

// EvaluateRow оценивает строку VCF по выражению
func EvaluateRow(row *VCFRow, expression *govaluate.EvaluableExpression) (bool, error) {
	parameters := make(map[string]any)

	// Добавляем все поля VCF как параметры
	if qual, err := row.GetValue("QUAL"); err == nil {
		parameters["QUAL"] = qual
	}
	parameters["CHROM"] = row.Chrom
	parameters["POS"] = row.Pos
	parameters["ID"] = row.ID
	parameters["REF"] = row.Ref
	parameters["ALT"] = row.Alt
	parameters["FILTER"] = row.Filter

	// Добавляем INFO поля
	for key, value := range row.InfoFields {
		if num, err := strconv.ParseFloat(value, 64); err == nil {
			parameters[key] = num
		} else if value == "true" {
			parameters[key] = true
		} else if value == "false" {
			parameters[key] = false
		} else {
			parameters[key] = value
		}
	}

	result, err := expression.Evaluate(parameters)
	if err != nil {
		return false, err
	}

	if boolResult, ok := result.(bool); ok {
		return boolResult, nil
	}

	return false, fmt.Errorf("expression did not return boolean")
}

// ParallelFilterRows параллельно фильтрует строки
func ParallelFilterRows(lines <-chan string, wg *sync.WaitGroup, output chan<- string, expression *govaluate.EvaluableExpression) {
	defer wg.Done()

	for line := range lines {
		row := ParseVCFRow(line)
		if row == nil {
			continue
		}

		matches, err := EvaluateRow(row, expression)
		if err != nil {
			s := fmt.Sprintf("Error evaluating row: %v\n", err)
			LoggerError(s)
			continue
		}

		if matches {
			output <- line
		}
	}
}

func Filter(include string, input_vcf_path string, output_vcf_path string, is_gzip bool, num_cpu int) {
	if num_cpu <= 0 {
		num_cpu = 1
	}

	// Создаем выражение с помощью govaluate
	expression, err := govaluate.NewEvaluableExpressionWithFunctions(include, FilterFunctions)
	if err != nil {
		s := fmt.Sprintf("Failed to parse expression '%s': %v\n", include, err)
		LoggerError(s)
		return
	}

	var reader *bufio.Reader
	if is_gzip {
		f, err := os.Open(input_vcf_path)
		if err != nil {
			s := fmt.Sprintf("Failed to open the file: %v\n", err)
			LoggerError(s)
			return
		}
		defer f.Close()

		gr, err := gzip.NewReader(f)
		if err != nil {
			s := fmt.Sprintf("Error creating gzip reader: %v\n", err)
			LoggerError(s)
			return
		}
		defer gr.Close()

		reader = bufio.NewReader(gr)
	} else {
		f, err := os.Open(input_vcf_path)
		if err != nil {
			s := fmt.Sprintf("Failed to open the file: %v\n", err)
			LoggerError(s)
			return
		}
		defer f.Close()

		reader = bufio.NewReader(f)
	}

	outputFile, err := os.Create(output_vcf_path)
	if err != nil {
		s := fmt.Sprintf("Error creating file: %v\n", err)
		LoggerError(s)
		return
	}
	defer outputFile.Close()

	scanner := bufio.NewScanner(reader)
	const maxTokenSize = 1 << 21
	buf := make([]byte, maxTokenSize)
	scanner.Buffer(buf, maxTokenSize)
	writer := bufio.NewWriter(outputFile)
	defer writer.Flush()

	wg := sync.WaitGroup{}
	wg.Add(num_cpu)
	linesChan := make(chan string, 100000)
	resultsChan := make(chan string, 500000)

	// Запускаем worker'ов
	for i := 0; i < num_cpu; i++ {
		go ParallelFilterRows(linesChan, &wg, resultsChan, expression)
	}

	num := 0
	for scanner.Scan() {
		line := scanner.Text()

		// Заголовки пишем сразу
		if strings.HasPrefix(line, "#") {
			fmt.Fprintln(writer, line)
			continue
		}

		// Периодически сбрасываем результаты
		if num >= 500000 {
			num = 0
			for len(resultsChan) > 0 {
				row := <-resultsChan
				fmt.Fprintln(writer, row)
			}
			writer.Flush()
		}

		linesChan <- line
		num++
	}

	close(linesChan)
	wg.Wait()
	close(resultsChan)

	if err := scanner.Err(); err != nil {
		s := fmt.Sprintf("Reading input: %v\n", err)
		LoggerError(s)
	}

	// Записываем оставшиеся результаты
	for row := range resultsChan {
		fmt.Fprintln(writer, row)
	}
}
