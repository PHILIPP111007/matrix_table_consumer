package functions_go

import (
	"bufio"
	"compress/gzip"
	"encoding/gob"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
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

func remove_from_slice(slice []*VCFRecordWithSamples, s int) []*VCFRecordWithSamples {
	return append(slice[:s], slice[s+1:]...)
}

// readVCFHeaders reads headers from two VCF files
func readVCFHeaders(vcf1, vcf2 string, is_gzip, is_gzip2 bool) ([]string, error) {
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
	if is_gzip {
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

	scanner1 := bufio.NewScanner(reader1)
	const maxTokenSize = 1 << 21
	buf := make([]byte, maxTokenSize)
	scanner1.Buffer(buf, maxTokenSize)
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
	if is_gzip2 {
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

	scanner2 := bufio.NewScanner(reader2)
	buf = make([]byte, maxTokenSize)
	scanner2.Buffer(buf, maxTokenSize)
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
func readAndMergeVCFs(vcf1, vcf2, outputVCF string, is_gzip, is_gzip2 bool) error {
	bar := New(4, WithDescription("Merging data"))
	defer bar.Close()

	allSamples := make(map[string]bool)
	tempDir, err := os.MkdirTemp("", "vcf_merge_chunks")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tempDir)

	recordChan := make(chan *VCFRecordWithSamples, 5_000)
	errorChan := make(chan error, 2)
	doneChan := make(chan bool, 1)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		defer close(recordChan)

		processFile := func(filename string, isGzip bool) error {
			file, err := os.Open(filename)
			if err != nil {
				return err
			}
			defer file.Close()

			var reader io.Reader = file
			if isGzip {
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

		if err := processFile(vcf1, is_gzip); err != nil {
			errorChan <- err
			return
		}
		bar.Increment()

		if err := processFile(vcf2, is_gzip2); err != nil {
			errorChan <- err
			return
		}
		bar.Increment()

		doneChan <- true
	}()

	chunkSize := 50_000
	chunkRecords := make([]*VCFRecordWithSamples, 0, chunkSize)
	chunkCount := 0
	chunkFiles := []string{}

	processing := true
	for processing {
		select {
		case record, ok := <-recordChan:
			if !ok {
				processing = false
				break
			}

			chunkRecords = append(chunkRecords, record)
			if len(chunkRecords) >= chunkSize {
				chunkFile, err := writeChunkToDisk(chunkRecords, tempDir, chunkCount)
				if err != nil {
					return err
				}
				chunkFiles = append(chunkFiles, chunkFile)
				chunkRecords = make([]*VCFRecordWithSamples, 0, chunkSize)

				s := fmt.Sprintf("Chunk file saved (%s)\n", chunkFile)
				LoggerInfo(s)

				chunkCount++
			}

		case err := <-errorChan:
			return err

		case <-doneChan:
			// We continue to process the remaining records in the channel
		}
	}

	if len(chunkRecords) > 0 {
		chunkFile, err := writeChunkToDisk(chunkRecords, tempDir, chunkCount)
		if err != nil {
			return err
		}
		s := fmt.Sprintf("Chunk file saved (%s)\n", chunkFile)
		LoggerInfo(s)

		chunkFiles = append(chunkFiles, chunkFile)
	}

	wg.Wait()
	bar.Increment()

	samplesList := make([]string, 0, len(allSamples))
	for sample := range allSamples {
		samplesList = append(samplesList, sample)
	}
	sort.Strings(samplesList)

	err = mergeSortedChunksAndWrite(chunkFiles, samplesList, outputVCF)
	if err != nil {
		return err
	}
	bar.Increment()

	return nil
}

func mergeSortedChunksAndWrite(chunkFiles []string, samplesList []string, outputVCF string) error {
	if len(chunkFiles) == 0 {
		return nil
	}

	files := make([]*os.File, len(chunkFiles))
	decoders := make([]*gob.Decoder, len(chunkFiles))
	currentRecords := make(map[int][]*VCFRecordWithSamples)

	defer func() {
		for _, file := range files {
			if file != nil {
				file.Close()
				if err := os.Remove(file.Name()); err != nil && !os.IsNotExist(err) {
					s := fmt.Sprintf("Warning: failed to remove temporary file %s: %v\n", file.Name(), err)
					LoggerError(s)
				}
			}
		}
	}()

	// Initialize files and decoders
	for chunkIndex, chunkFile := range chunkFiles {
		file, err := os.Open(chunkFile)
		if err != nil {
			// Close any already opened files
			for j := range chunkIndex {
				files[j].Close()
			}
			return fmt.Errorf("opening chunk file %s: %v", chunkFile, err)
		}
		files[chunkIndex] = file
		decoders[chunkIndex] = gob.NewDecoder(file)

		var records []*VCFRecordWithSamples
		for {
			var record VCFRecordWithSamples
			err := decoders[chunkIndex].Decode(&record)
			if err == io.EOF {
				break
			}
			if err != nil {
				return fmt.Errorf("decoding record from chunk %d: %v", chunkIndex, err)
			}
			records = append(records, &record)
		}
		currentRecords[chunkIndex] = records

		s := fmt.Sprintf("Chunk file loaded (%s)\n", chunkFile)
		LoggerInfo(s)
	}

	activeChunks := len(currentRecords)
	for activeChunks > 0 {
		// Find the minimum record
		var minRecord *VCFRecordWithSamples
		var minChunkIndex int = -1

		for chunkIndex, records := range currentRecords {
			if len(records) == 0 {
				continue
			}

			currentRecord := records[0]
			if minRecord == nil {
				minRecord = currentRecord
				minChunkIndex = chunkIndex
				continue
			}

			// Compare chromosome and position
			if currentRecord.Chrom < minRecord.Chrom {
				minRecord = currentRecord
				minChunkIndex = chunkIndex
			} else if currentRecord.Chrom == minRecord.Chrom {
				pos1, err1 := strconv.Atoi(currentRecord.Pos)
				pos2, err2 := strconv.Atoi(minRecord.Pos)
				if err1 != nil || err2 != nil {
					continue
				}
				if pos1 < pos2 {
					minRecord = currentRecord
					minChunkIndex = chunkIndex
				}
			}
		}

		if minChunkIndex == -1 {
			break // All records processed
		}

		// Collect ALL records with the same key from ALL chunks
		key := [2]string{minRecord.Chrom, minRecord.Pos}
		var recordsWithSameKey []*VCFRecordWithSamples
		recordsWithSameKey = append(recordsWithSameKey, minRecord)

		currentRecords[minChunkIndex] = currentRecords[minChunkIndex][1:]

		if len(currentRecords[minChunkIndex]) == 0 {
			delete(currentRecords, minChunkIndex)
			activeChunks--
		}

		// Check other chunks for records with the same key
		for chunkIndex, records := range currentRecords {
			if len(records) == 0 {
				continue
			}

			for i, record := range records {
				if record.Chrom == key[0] && record.Pos == key[1] {
					recordsWithSameKey = append(recordsWithSameKey, record)
					currentRecords[chunkIndex] = remove_from_slice(currentRecords[chunkIndex], i)
				}

				if len(currentRecords[chunkIndex]) == 0 {
					delete(currentRecords, chunkIndex)
					activeChunks--
				}
			}
		}
		// Merge all records with the same key
		mergedRecord := mergeRecordsForKey(key, recordsWithSameKey)
		writeMergedRecord(mergedRecord, samplesList, outputVCF)
	}
	return nil
}

// writeChunkToDisk writes a chunk of records to disk and returns the filename
func writeChunkToDisk(records []*VCFRecordWithSamples, tempDir string, chunkCount int) (string, error) {
	// Sort records within chunk
	sort.Slice(records, func(i, j int) bool {
		if records[i].Chrom == records[j].Chrom {
			return records[i].Pos < records[j].Pos
		}
		return records[i].Chrom < records[j].Chrom
	})

	chunkFile := filepath.Join(tempDir, fmt.Sprintf("chunk_%d.gob", chunkCount))
	file, err := os.Create(chunkFile)
	if err != nil {
		return "", err
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	for _, record := range records {
		if err := encoder.Encode(record); err != nil {
			return "", err
		}
	}

	return chunkFile, nil
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
func Merge(vcf1, vcf2, outputVCF string, is_gzip, is_gzip2 bool) {
	headers, err := readVCFHeaders(vcf1, vcf2, is_gzip, is_gzip2)
	if err != nil {
		s := fmt.Sprintf("Error: %v\n", err)
		LoggerError(s)
	}

	if err := writeHeaders(headers, outputVCF); err != nil {
		s := fmt.Sprintf("Error: %v\n", err)
		LoggerError(s)
	}

	err = readAndMergeVCFs(vcf1, vcf2, outputVCF, is_gzip, is_gzip2)
	if err != nil {
		s := fmt.Sprintf("Error: %v\n", err)
		LoggerError(s)
	}
}
