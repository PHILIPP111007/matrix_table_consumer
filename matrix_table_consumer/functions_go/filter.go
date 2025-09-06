package functions_go

import (
    "bufio"
    "compress/gzip"
    "fmt"
    "os"
    "regexp"
    "strconv"
    "strings"
    "sync"
)

// Expression представляет одно условие фильтрации
type Expression struct {
    Key      string
    Operator string
    Value    string
}

// ConditionGroup представляет группу условий (AND или OR)
type ConditionGroup struct {
    Type       string       // "and" или "or"
    Conditions []Expression // Условия в группе
}

// ParseExpression разбирает строку с условиями фильтрации
func ParseExpression(include string) []ConditionGroup {
    var groups []ConditionGroup
    
    // Удаляем пробелы вокруг скобок для упрощения парсинга
    include = strings.ReplaceAll(include, "(", " ( ")
    include = strings.ReplaceAll(include, ")", " ) ")
    include = strings.ReplaceAll(include, "&&", " && ")
    include = strings.ReplaceAll(include, "||", " || ")
    
    tokens := strings.Fields(include)
    var currentGroup *ConditionGroup
    var stack []*ConditionGroup
    
    for i := 0; i < len(tokens); i++ {
        token := tokens[i]
        
        switch token {
        case "(":
            // Начало новой группы
            newGroup := &ConditionGroup{}
            stack = append(stack, currentGroup)
            currentGroup = newGroup
            
        case ")":
            // Конец текущей группы
            if len(stack) > 0 {
                parentGroup := stack[len(stack)-1]
                stack = stack[:len(stack)-1]
                
                if parentGroup != nil {
                    parentGroup.Conditions = append(parentGroup.Conditions, Expression{
                        Key:      "GROUP",
                        Operator: "GROUP",
                        Value:    "", // Группа будет храниться в специальном поле
                    })
                    // Здесь нужно хранить ссылку на группу, но для простоты используем Value
                }
                currentGroup = parentGroup
            }
            
        case "&&", "||":
            if currentGroup == nil {
                currentGroup = &ConditionGroup{Type: "and"}
            }
            currentGroup.Type = "and"
            if token == "||" {
                currentGroup.Type = "or"
            }
            
        default:
            // Это условие вида "KEY OPERATOR VALUE"
            if i+2 < len(tokens) {
                key := token
                operator := tokens[i+1]
                value := tokens[i+2]
                i += 2
                
                // Проверяем валидность оператора
                validOps := map[string]bool{">": true, "<": true, ">=": true, "<=": true, "==": true, "!=": true}
                if !validOps[operator] {
                    s := fmt.Sprintf("Invalid operator: %s\n", operator)
                    LoggerError(s)
                    continue
                }
                
                if currentGroup == nil {
                    currentGroup = &ConditionGroup{Type: "and"}
                }
                
                currentGroup.Conditions = append(currentGroup.Conditions, Expression{
                    Key:      key,
                    Operator: operator,
                    Value:    strings.Trim(value, `'"`),
                })
            }
        }
    }
    
    if currentGroup != nil {
        groups = append(groups, *currentGroup)
    }
    
    return groups
}

// EvaluateCondition оценивает одно условие для данной строки VCF
func EvaluateCondition(line string, expr Expression) bool {
    lineList := strings.Split(line, "\t")
    
    if expr.Key == "QUAL" {
        linePart := lineList[5]
        if linePart == "." {
            return false
        }
        
        numberFromLine, err := strconv.ParseFloat(linePart, 64)
        if err != nil {
            s := fmt.Sprintf("Skipping invalid QUAL value: %s\n", err)
            LoggerError(s)
            return false
        }
        
        filterNumber, err := strconv.ParseFloat(expr.Value, 64)
        if err != nil {
            s := fmt.Sprintf("Invalid filter number: %s\n", expr.Value)
            LoggerError(s)
            return false
        }
        
        return compareNumbers(numberFromLine, filterNumber, expr.Operator)
        
    } else {
        // Обработка INFO поля (например: AF=0.5)
        linePart := lineList[7]
        infoFields := strings.Split(linePart, ";")
        
        for _, field := range infoFields {
            if strings.HasPrefix(field, expr.Key+"=") {
                valueStr := field[len(expr.Key)+1:]
                values := strings.Split(valueStr, ",")
                firstValue := values[0]
                
                numberFromLine, err := strconv.ParseFloat(firstValue, 64)
                if err != nil {
                    s := fmt.Sprintf("Skipping invalid %s value: %s\n", expr.Key, err)
                    LoggerError(s)
                    return false
                }
                
                filterNumber, err := strconv.ParseFloat(expr.Value, 64)
                if err != nil {
                    s := fmt.Sprintf("Invalid filter number for %s: %s\n", expr.Key, expr.Value)
                    LoggerError(s)
                    return false
                }
                
                return compareNumbers(numberFromLine, filterNumber, expr.Operator)
            }
        }
        return false // Поле не найдено
    }
}

// compareNumbers выполняет сравнение чисел с заданным оператором
func compareNumbers(a, b float64, operator string) bool {
    switch operator {
    case ">":
        return a > b
    case "<":
        return a < b
    case ">=":
        return a >= b
    case "<=":
        return a <= b
    case "==":
        return a == b
    case "!=":
        return a != b
    default:
        s := fmt.Sprintf("Unsupported operator: %s\n", operator)
        LoggerError(s)
        return false
    }
}

// EvaluateGroup оценивает всю группу условий
func EvaluateGroup(line string, group ConditionGroup) bool {
    if len(group.Conditions) == 0 {
        return true
    }
    
    if group.Type == "and" {
        // Все условия должны быть истинны
        for _, condition := range group.Conditions {
            if !EvaluateCondition(line, condition) {
                return false
            }
        }
        return true
    } else {
        // Хотя бы одно условие должно быть истинно
        for _, condition := range group.Conditions {
            if EvaluateCondition(line, condition) {
                return true
            }
        }
        return false
    }
}

// ParallelFilterRows параллельно фильтрует строки
func ParallelFilterRows(lines <-chan string, wg *sync.WaitGroup, output chan<- string, groups []ConditionGroup) {
    defer wg.Done()
    
    for line := range lines {
        matchesAll := true
        
        for _, group := range groups {
            if !EvaluateGroup(line, group) {
                matchesAll = false
                break
            }
        }
        
        if matchesAll {
            output <- line
        }
    }
}

func Filter(include string, input_vcf_path string, output_vcf_path string, is_gzip bool, num_cpu int) {
    if num_cpu <= 0 {
        num_cpu = 1
    }

    // Парсим выражение
    groups := ParseExpression(include)
    if len(groups) == 0 {
        s := fmt.Sprintf("No valid expressions found in: %s\n", include)
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
    writer := bufio.NewWriter(outputFile)
    defer writer.Flush()

    wg := sync.WaitGroup{}
    wg.Add(num_cpu)
    linesChan := make(chan string, 100000)
    resultsChan := make(chan string, 500000)

    for i := 0; i < num_cpu; i++ {
        go ParallelFilterRows(linesChan, &wg, resultsChan, groups)
    }

    num := 0
    for scanner.Scan() {
        line := scanner.Text()
        if strings.HasPrefix(line, "#") {
            fmt.Fprintln(writer, line)
            continue
        }

        if num == 500000 {
            num = 0
            // Обрабатываем накопленные результаты
            for len(resultsChan) > 0 {
                row := <-resultsChan
                fmt.Fprintln(writer, row)
            }
        }
        
        linesChan <- line
        num++
    }

    close(linesChan)
    wg.Wait()
    close(resultsChan)

    if err := scanner.Err(); err != nil {
        s := fmt.Sprintf("Reading standard input: %v\n", err)
        LoggerError(s)
    }

    // Записываем оставшиеся результаты
    for row := range resultsChan {
        fmt.Fprintln(writer, row)
    }
}
