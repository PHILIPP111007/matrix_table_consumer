package functions_go

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"maps"
	"os"
	"slices"
	"sort"
	"strings"
)

type VCFRecord struct {
	Chrom   string
	Pos     string
	ID      string
	Ref     string
	Alt     string
	Qual    string
	Filter  string
	Info    string
	Format  string
	Samples map[string]string
}

func newVCFRecord(chrom, pos, id, ref, alt, qual, filter, info, format string) *VCFRecord {
	return &VCFRecord{
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

func (r *VCFRecord) addSample(sampleName, sampleValue string) {
	r.Samples[sampleName] = sampleValue
}

func (r *VCFRecord) String() string {
	return fmt.Sprintf("%s:%s", r.Chrom, r.Pos)
}

// contains checks if a string is present in a slice
func contains(slice []string, item string) bool {
	return slices.Contains(slice, item)
}

// parseVCFLine parses a VCF string
func parseVCFLine(line string, sampleNames []string) *VCFRecord {
	parts := strings.Split(strings.TrimSpace(line), "\t")

	record := newVCFRecord(
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

// readAndMergeVCFs reads and merges two VCF files
func readAndMergeVCFs(vcf1, vcf2 string, is_gzip, is_gzip2 bool) ([]*VCFRecord, []string, error) {
	mergedRecords := make(map[[2]string][]*VCFRecord)
	allSamples := make(map[string]bool)

	processFile := func(filename string, is_gzip bool) error {
		file, err := os.Open(filename)
		if err != nil {
			return err
		}
		defer file.Close()

		var reader *bufio.Reader

		if is_gzip {
			gr, err := gzip.NewReader(file)
			if err != nil {
				s := fmt.Sprintf("Error creating gzip reader: %v\n", err)
				LoggerError(s)
			}
			defer gr.Close()

			reader = bufio.NewReader(gr)
		} else {
			reader = bufio.NewReader(file)
		}

		scanner := bufio.NewScanner(reader)
		var sampleNames []string

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
			key := [2]string{record.Chrom, record.Pos}
			mergedRecords[key] = append(mergedRecords[key], record)

			for sample := range record.Samples {
				allSamples[sample] = true
			}
		}

		return scanner.Err()
	}

	// Processing both files
	if err := processFile(vcf1, is_gzip); err != nil {
		return nil, nil, err
	}
	if err := processFile(vcf2, is_gzip2); err != nil {
		return nil, nil, err
	}

	// Sorting keys
	var keys [][2]string
	for key := range mergedRecords {
		keys = append(keys, key)
	}

	sort.Slice(keys, func(i, j int) bool {
		if keys[i][0] == keys[j][0] {
			return keys[i][1] < keys[j][1]
		}
		return keys[i][0] < keys[j][0]
	})

	// Creating merged records
	var mergedResults []*VCFRecord
	for _, key := range keys {
		entries := mergedRecords[key]
		firstEntry := entries[0]

		combinedRecord := newVCFRecord(
			firstEntry.Chrom,
			key[1],
			firstEntry.ID,
			firstEntry.Ref,
			firstEntry.Alt,
			firstEntry.Qual,
			firstEntry.Filter,
			firstEntry.Info,
			firstEntry.Format,
		)

		for _, entry := range entries {
			maps.Copy(combinedRecord.Samples, entry.Samples)
		}

		mergedResults = append(mergedResults, combinedRecord)
	}

	// Converting a set of samples into a sorted list
	samplesList := make([]string, 0, len(allSamples))
	for sample := range allSamples {
		samplesList = append(samplesList, sample)
	}
	sort.Strings(samplesList)

	return mergedResults, samplesList, nil
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
func writeMergedRecords(mergedRecords []*VCFRecord, samplesOrdered []string, outputFile string) error {
	file, err := os.OpenFile(outputFile, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	for _, rec := range mergedRecords {
		columns := []string{
			rec.Chrom,
			rec.Pos,
			rec.ID,
			rec.Ref,
			rec.Alt,
			rec.Qual,
			rec.Filter,
			rec.Info,
			rec.Format,
		}

		var sampleValues []string
		for _, sample := range samplesOrdered {
			sampleData := "."
			if val, exists := rec.Samples[sample]; exists {
				sampleData = val
			}
			sampleValues = append(sampleValues, sampleData)
		}

		columns = append(columns, sampleValues...)
		if _, err := writer.WriteString(strings.Join(columns, "\t") + "\n"); err != nil {
			return err
		}
	}

	return writer.Flush()
}

// Merge combines two VCF files
func Merge(vcf1, vcf2, outputVCF string, is_gzip, is_gzip2 bool) error {
	headers, err := readVCFHeaders(vcf1, vcf2, is_gzip, is_gzip2)
	if err != nil {
		return err
	}

	if err := writeHeaders(headers, outputVCF); err != nil {
		return err
	}

	mergedRecords, allSamples, err := readAndMergeVCFs(vcf1, vcf2, is_gzip, is_gzip2)
	if err != nil {
		return err
	}

	return writeMergedRecords(mergedRecords, allSamples, outputVCF)
}
