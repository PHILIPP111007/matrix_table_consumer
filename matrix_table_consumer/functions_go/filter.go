package functions_go

import (
    "bufio"
    "compress/gzip"
    "fmt"
    "os"
    "strconv"
    "regexp"
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
    
    // Упрощенный подход: разбиваем по внешним &&, учитывая скобки
    // Сначала находим все OR группы, разделенные &&
    
    // Удаляем внешние пробелы
    include = strings.TrimSpace(include)
    
    // Если выражение начинается и заканчивается скобками, убираем их
    if strings.HasPrefix(include, "(") && strings.HasSuffix(include, ")") {
        include = strings.TrimSpace(include[1 : len(include)-1])
    }
    
    // Разбиваем на группы, разделенные &&
    andGroups := splitByOperator(include, "&&")
    
    for _, andGroup := range andGroups {
        andGroup = strings.TrimSpace(andGroup)
        if andGroup == "" {
            continue
        }
        
        // Удаляем внешние скобки если они есть
        if strings.HasPrefix(andGroup, "(") && strings.HasSuffix(andGroup, ")") {
            andGroup = strings.TrimSpace(andGroup[1 : len(andGroup)-1])
        }
        
        // Разбиваем OR группу на отдельные условия
        orConditions := splitByOperator(andGroup, "||")
        var conditions []Expression
        
        for _, orCondition := range orConditions {
            orCondition = strings.TrimSpace(orCondition)
            if orCondition == "" {
                continue
            }
            
            // Удаляем внешние скобки если они есть
            if strings.HasPrefix(orCondition, "(") && strings.HasSuffix(orCondition, ")") {
                orCondition = strings.TrimSpace(orCondition[1 : len(orCondition)-1])
            }
            
            // Парсим отдельное условие
            expr, err := parseSingleExpression(orCondition)
            if err != nil {
                s := fmt.Sprintf("Error parsing expression '%s': %v\n", orCondition, err)
                LoggerError(s)
                continue
            }
            
            conditions = append(conditions, expr)
        }
        
        if len(conditions) > 0 {
            groupType := "and"
            if len(conditions) > 1 {
                groupType = "or" // Если в группе несколько условий, это OR группа
            }
            
            groups = append(groups, ConditionGroup{
                Type:       groupType,
                Conditions: conditions,
            })
        }
    }
    
    return groups
}

// splitByOperator разбивает строку по оператору, учитывая скобки
func splitByOperator(expr string, operator string) []string {
    var result []string
    var current strings.Builder
    parenDepth := 0
    
    for i := 0; i < len(expr); i++ {
        char := expr[i]
        
        if char == '(' {
            parenDepth++
        } else if char == ')' {
            parenDepth--
        }
        
        // Проверяем, не находимся ли мы на операторе вне скобок
        if parenDepth == 0 && i+len(operator) <= len(expr) {
            if expr[i:i+len(operator)] == operator {
                result = append(result, strings.TrimSpace(current.String()))
                current.Reset()
                i += len(operator) - 1 // Пропускаем оператор
                continue
            }
        }
        
        current.WriteByte(char)
    }
    
    if current.Len() > 0 {
        result = append(result, strings.TrimSpace(current.String()))
    }
    
    return result
}

// parseSingleExpression парсит одно условие вида "KEY OPERATOR VALUE"
func parseSingleExpression(expr string) (Expression, error) {
    matchExpr := regexp.MustCompile(`(\w+)\s*([=<>!]+)\s*(["']?[-\d\.]+["']?)`)
    matches := matchExpr.FindStringSubmatch(expr)
    
    if len(matches) != 4 {
        return Expression{}, fmt.Errorf("invalid expression format: %s", expr)
    }
    
    key := matches[1]
    operator := matches[2]
    value := strings.Trim(matches[3], `'"`)
    
    // Проверяем валидность оператора
    validOps := map[string]bool{">": true, "<": true, ">=": true, "<=": true, "==": true, "!=": true}
    if !validOps[operator] {
        return Expression{}, fmt.Errorf("invalid operator: %s", operator)
    }
    
    return Expression{
        Key:      key,
        Operator: operator,
        Value:    value,
    }, nil
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
        // Хотя бы одно условие должно быть истинно (OR группа)
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
        
        // Все группы должны быть истинны (группы соединены AND)
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
    const maxTokenSize = 1 << 21
    buf := make([]byte, maxTokenSize)
    scanner.Buffer(buf, maxTokenSize)   
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
