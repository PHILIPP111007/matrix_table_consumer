package functions_go

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"maps"
	"os"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
)

func newVCFRecordWithSamples(chrom, pos, id, ref, alt, qual, filter, info, format string) *VCFRecordWithSamples {
	return &VCFRecordWithSamples{
		Chrom:   chrom,
		Pos:     pos,
		ID:      id,
		Ref:     ref,
		Alt:     alt,
		Qual:    qual,
		Filter:  filter,
		Info:    info,
		Format:  format,
		Samples: make(map[string]string),
	}
}

func (r *VCFRecordWithSamples) addSample(sampleName, sampleValue string) {
	r.Samples[sampleName] = sampleValue
}

func (r *VCFRecordWithSamples) String() string {
	return fmt.Sprintf("%s:%s", r.Chrom, r.Pos)
}

// contains checks if a string is present in a slice
func contains(slice []string, item string) bool {
	return slices.Contains(slice, item)
}

// parseVCFLine parses a VCF string
func parseVCFLine(line string, sampleNames []string) *VCFRecordWithSamples {
	parts := strings.Split(strings.TrimSpace(line), "\t")

	record := newVCFRecordWithSamples(
		parts[0],
		parts[1],
		parts[2],
		parts[3],
		parts[4],
		parts[5],
		parts[6],
		parts[7],
		parts[8],
	)

	samplesValues := parts[9:]
	for i, sampleName := range sampleNames {
		if i < len(samplesValues) {
			record.addSample(sampleName, samplesValues[i])
		}
	}

	return record
}

// readVCFHeaders reads headers from two VCF files
func readVCFHeaders(vcf1, vcf2 string) ([]string, error) {
	headers := make([]string, 0)
	samplesNames := make([]string, 0)
	var endHeaderWithoutSamples []string

	// Reading the first file
	file1, err := os.Open(vcf1)
	if err != nil {
		return nil, err
	}
	defer file1.Close()

	var reader1 *bufio.Reader
	if strings.HasSuffix(vcf1, ".gz") {
		gr, err := gzip.NewReader(file1)
		if err != nil {
			s := fmt.Sprintf("Error creating gzip reader: %v\n", err)
			LoggerError(s)
		}
		defer gr.Close()
		reader1 = bufio.NewReader(gr)
	} else {
		reader1 = bufio.NewReader(file1)
	}

	scanner1 := GetScaner(reader1)
	for scanner1.Scan() {
		line := scanner1.Text()
		if strings.HasPrefix(line, "##") {
			if !contains(headers, line) {
				headers = append(headers, line)
			}
		} else if strings.HasPrefix(line, "#CHROM") {
			headerEnd := strings.Split(strings.TrimSpace(line), "\t")
			endHeaderWithoutSamples = headerEnd[:9]
			samplesNames = append(samplesNames, headerEnd[9:]...)
			break
		}
	}

	// Reading the second file
	file2, err := os.Open(vcf2)
	if err != nil {
		return nil, err
	}
	defer file2.Close()

	var reader2 *bufio.Reader
	if strings.HasSuffix(vcf2, ".gz") {
		gr, err := gzip.NewReader(file2)
		if err != nil {
			s := fmt.Sprintf("Error creating gzip reader: %v\n", err)
			LoggerError(s)
		}
		defer gr.Close()
		reader2 = bufio.NewReader(gr)
	} else {
		reader2 = bufio.NewReader(file2)
	}

	scanner2 := GetScaner(reader2)
	for scanner2.Scan() {
		line := scanner2.Text()
		if strings.HasPrefix(line, "##") {
			if !contains(headers, line) {
				headers = append(headers, line)
			}
		} else if strings.HasPrefix(line, "#CHROM") {
			headerEnd := strings.Split(strings.TrimSpace(line), "\t")
			samplesNames = append(samplesNames, headerEnd[9:]...)
			break
		}
	}

	// Sorting sample names
	sort.Strings(samplesNames)

	// Formation of final headings
	endHeaderStr := strings.Join(endHeaderWithoutSamples, "\t") + "\t"
	endHeaderStr += strings.Join(samplesNames, "\t") + "\n"
	headers = append(headers, endHeaderStr)

	return headers, nil
}

// readAndMergeVCFs reads and merges two VCF files with streaming processing for large files
func readVCFs(vcf1, vcf2 string) ([]*VCFRecordWithSamples, []string, error) {
	allSamples := make(map[string]bool)

	recordChan := make(chan *VCFRecordWithSamples, 5_000)
	errorChan := make(chan error, 2)
	doneChan := make(chan bool, 1)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		defer close(recordChan)

		processFile := func(file_path string) error {
			file, err := os.Open(file_path)
			if err != nil {
				return err
			}
			defer file.Close()

			var reader io.Reader = file
			if strings.HasSuffix(file_path, ".gz") {
				gr, err := gzip.NewReader(file)
				if err != nil {
					return fmt.Errorf("gzip error: %v", err)
				}
				defer gr.Close()
				reader = gr
			}

			scanner := bufio.NewScanner(reader)
			const maxTokenSize = 1 << 21
			buf := make([]byte, maxTokenSize)
			scanner.Buffer(buf, maxTokenSize)
			var sampleNames []string
			recordCount := 0

			for scanner.Scan() {
				line := scanner.Text()
				if strings.HasPrefix(line, "#CHROM") {
					sampleNames = strings.Split(strings.TrimSpace(line), "\t")[9:]
					continue
				} else if strings.HasPrefix(line, "#") {
					continue
				} else if strings.TrimSpace(line) == "" {
					break
				}

				record := parseVCFLine(line, sampleNames)
				recordChan <- record

				for sample := range record.Samples {
					allSamples[sample] = true
				}

				recordCount++
				if recordCount%10_000 == 0 {
					runtime.Gosched()
				}
			}
			return scanner.Err()
		}

		if err := processFile(vcf1); err != nil {
			errorChan <- err
			return
		}

		if err := processFile(vcf2); err != nil {
			errorChan <- err
			return
		}

		doneChan <- true
	}()

	records := make([]*VCFRecordWithSamples, 0)
	processing := true
	for processing {
		select {
		case record, ok := <-recordChan:
			if !ok {
				processing = false
				break
			}

			records = append(records, record)

		case err := <-errorChan:
			return nil, nil, err

		case <-doneChan:
			// We continue to process the remaining records in the channel
		}
	}

	wg.Wait()

	samplesList := make([]string, 0, len(allSamples))
	for sample := range allSamples {
		samplesList = append(samplesList, sample)
	}
	sort.Strings(samplesList)

	return records, samplesList, nil
}

func mergeRecords(records []*VCFRecordWithSamples) ([]*VCFRecordWithSamples, error) {
	if len(records) == 0 {
		return nil, nil
	}

	// Group records by key (Chrom, Pos)
	recordsByKey := make(map[[2]string][]*VCFRecordWithSamples)
	for _, record := range records {
		key := [2]string{record.Chrom, record.Pos}
		recordsByKey[key] = append(recordsByKey[key], record)
	}

	// Sorting keys for sequential processing
	keys := make([][2]string, 0, len(recordsByKey))
	for key := range recordsByKey {
		keys = append(keys, key)
	}

	// Sort keys by chromosome and position
	sort.Slice(keys, func(i, j int) bool {
		if keys[i][0] != keys[j][0] {
			return keys[i][0] < keys[j][0]
		}

		posI, errI := strconv.Atoi(keys[i][1])
		posJ, errJ := strconv.Atoi(keys[j][1])
		if errI != nil || errJ != nil {
			return keys[i][1] < keys[j][1] // lexicographic comparison
		}
		return posI < posJ
	})

	// Merge records for each key
	var mergedRecords []*VCFRecordWithSamples
	for _, key := range keys {
		recordsWithSameKey := recordsByKey[key]
		mergedRecord := mergeRecordsForKey(key, recordsWithSameKey)
		if mergedRecord != nil {
			mergedRecords = append(mergedRecords, mergedRecord)
		}
	}

	return mergedRecords, nil
}

// mergeRecordsForKey merges all records for a specific key
func mergeRecordsForKey(key [2]string, records []*VCFRecordWithSamples) *VCFRecordWithSamples {
	if len(records) == 0 {
		return nil
	}

	firstRecord := records[0]
	mergedRecord := newVCFRecordWithSamples(
		firstRecord.Chrom,
		key[1],
		firstRecord.ID,
		firstRecord.Ref,
		firstRecord.Alt,
		firstRecord.Qual,
		firstRecord.Filter,
		firstRecord.Info,
		firstRecord.Format,
	)

	for _, record := range records {
		maps.Copy(mergedRecord.Samples, record.Samples)
	}
	return mergedRecord
}

// writeHeaders writes headers to the output file
func writeHeaders(headerLines []string, outputFile string) error {
	file, err := os.Create(outputFile)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	headerLinesLen := len(headerLines)
	for i, line := range headerLines {
		if i == headerLinesLen-1 {
			_, err := writer.WriteString(line)
			if err != nil {
				return err
			}
		} else {
			if _, err := writer.WriteString(line + "\n"); err != nil {
				return err
			}
		}
	}
	return writer.Flush()
}

// writeMergedRecords writes merged records to a file
func writeMergedRecord(record *VCFRecordWithSamples, samplesOrdered []string, outputFile string) error {
	file, err := os.OpenFile(outputFile, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	columns := []string{
		record.Chrom,
		record.Pos,
		record.ID,
		record.Ref,
		record.Alt,
		record.Qual,
		record.Filter,
		record.Info,
		record.Format,
	}

	var sampleValues []string
	for _, sample := range samplesOrdered {
		sampleData := "./."
		if val, exists := record.Samples[sample]; exists {
			sampleData = val
		}
		sampleValues = append(sampleValues, sampleData)
	}

	columns = append(columns, sampleValues...)
	if _, err := writer.WriteString(strings.Join(columns, "\t") + "\n"); err != nil {
		return err
	}
	return writer.Flush()
}

// Merge combines two VCF files
func Merge(vcf1, vcf2, outputVCF string) {
	headers, err := readVCFHeaders(vcf1, vcf2)
	if err != nil {
		s := fmt.Sprintf("Error: %v\n", err)
		LoggerError(s)
	}

	LoggerInfo("Writing headers...\n")

	if err := writeHeaders(headers, outputVCF); err != nil {
		s := fmt.Sprintf("Error: %v\n", err)
		LoggerError(s)
	}

	LoggerInfo("Reading VCFs...\n")

	records, samplesList, err := readVCFs(vcf1, vcf2)
	if err != nil {
		s := fmt.Sprintf("Error: %v\n", err)
		LoggerError(s)
	}

	LoggerInfo("Merging records...\n")

	mergedRecords, err := mergeRecords(records)
	if err != nil {
		s := fmt.Sprintf("Error: %v\n", err)
		LoggerError(s)
	}

	bar := NewTqdm(len(mergedRecords), WithDescription("Write merged records"))
	defer bar.Close()

	for _, record := range mergedRecords {
		err := writeMergedRecord(record, samplesList, outputVCF)
		if err != nil {
			s := fmt.Sprintf("Error: %v\n", err)
			LoggerError(s)
		}
		bar.Increment()
	}
}
