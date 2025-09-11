package functions_go

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// Logger functions (replace with your actual logger implementation)
func loggerInfo(format string, args ...interface{}) {
	log.Printf("[INFO] "+format, args...)
}

func loggerError(format string, args ...interface{}) {
	log.Printf("[ERROR] "+format, args...)
}

type VCFRecord struct {
	Chromosome string
	Position   int
	Line       string
}

type ByChromosomePos []VCFRecord

func (a ByChromosomePos) Len() int      { return len(a) }
func (a ByChromosomePos) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a ByChromosomePos) Less(i, j int) bool {
	keyI := chromosomeKey(a[i].Chromosome)
	keyJ := chromosomeKey(a[j].Chromosome)

	if keyI != keyJ {
		return keyI < keyJ
	}
	return a[i].Position < a[j].Position
}

func chromosomeKey(chrom string) int {
	chrom = strings.ToUpper(chrom)
	if strings.HasPrefix(chrom, "CHR") {
		chrom = chrom[3:]
	}

	// Try to parse as integer
	if num, err := strconv.Atoi(chrom); err == nil {
		return num
	}

	// Special chromosomes
	specialChroms := map[string]int{
		"X":  100,
		"Y":  101,
		"MT": 102,
		"M":  102,
	}

	if val, exists := specialChroms[chrom]; exists {
		return val
	}

	// Default for other chromosomes
	return 1000 + int(hashString(chrom))
}

func hashString(s string) uint32 {
	var h uint32 = 0
	for _, char := range s {
		h = h*31 + uint32(char)
	}
	return h
}

func readChunk(reader *bufio.Reader, size int) ([]VCFRecord, error) {
	chunk := make([]VCFRecord, 0, size)

	for i := 0; i < size; i++ {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		parts := strings.Split(line, "\t")
		if len(parts) < 2 {
			continue
		}

		pos, err := strconv.Atoi(parts[1])
		if err != nil {
			continue
		}

		chunk = append(chunk, VCFRecord{
			Chromosome: parts[0],
			Position:   pos,
			Line:       line + "\n",
		})
	}

	return chunk, nil
}

func writeChunk(chunk []VCFRecord, filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	defer writer.Flush()

	for _, record := range chunk {
		_, err := writer.WriteString(record.Line)
		if err != nil {
			return err
		}
	}

	return nil
}

func mergeSortedFiles(filePaths []string, outputFile string) error {
	// Open all files
	files := make([]*os.File, len(filePaths))
	readers := make([]*bufio.Reader, len(filePaths))
	currentLines := make([]string, len(filePaths))

	for i, path := range filePaths {
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		files[i] = file
		readers[i] = bufio.NewReader(file)

		line, err := readers[i].ReadString('\n')
		if err != nil && err != io.EOF {
			return err
		}
		currentLines[i] = line
	}
	defer func() {
		for _, file := range files {
			file.Close()
		}
	}()

	// Create output file
	outFile, err := os.Create(outputFile)
	if err != nil {
		return err
	}
	defer outFile.Close()

	writer := bufio.NewWriter(outFile)
	defer writer.Flush()

	for {
		// Find the minimum record
		minIndex := -1
		var minRecord string
		var minKey struct {
			chromKey int
			position int
		}

		for i, line := range currentLines {
			if line == "" {
				continue
			}

			parts := strings.Split(strings.TrimSpace(line), "\t")
			if len(parts) < 2 {
				continue
			}

			pos, err := strconv.Atoi(parts[1])
			if err != nil {
				continue
			}

			chromKey := chromosomeKey(parts[0])
			currentKey := struct {
				chromKey int
				position int
			}{chromKey, pos}

			if minIndex == -1 || currentKey.chromKey < minKey.chromKey ||
				(currentKey.chromKey == minKey.chromKey && currentKey.position < minKey.position) {
				minIndex = i
				minRecord = line
				minKey = currentKey
			}
		}

		if minIndex == -1 {
			break
		}

		// Write the minimum record
		_, err := writer.WriteString(minRecord)
		if err != nil {
			return err
		}

		// Read next line from the file that had the minimum record
		line, err := readers[minIndex].ReadString('\n')
		if err != nil {
			if err == io.EOF {
				currentLines[minIndex] = ""
			} else {
				return err
			}
		} else {
			currentLines[minIndex] = line
		}
	}

	return nil
}

func Sort(inputVCF, outputVCF string, chunkSize int) {
	tempDir, err := os.MkdirTemp("", "vcf_sort_")
	if err != nil {
		loggerError("Error creating temp directory: %v", err)
		return
	}
	defer os.RemoveAll(tempDir)

	loggerInfo("Temp dir: %s", tempDir)
	tempFiles := []string{}

	// Open input file
	var inputFile io.ReadCloser
	inputFile, err = os.Open(inputVCF)
	if err != nil {
		loggerError("Error opening input file: %v", err)
		return
	}
	defer inputFile.Close()

	// Handle gzip compression
	if strings.HasSuffix(inputVCF, ".gz") {
		gzReader, err := gzip.NewReader(inputFile)
		if err != nil {
			loggerError("Error creating gzip reader: %v", err)
			return
		}
		defer gzReader.Close()
		inputFile = gzReader
	}

	reader := bufio.NewReader(inputFile)

	// Read headers
	headerLines := []string{}
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			loggerError("Error reading headers: %v", err)
			return
		}

		if strings.HasPrefix(line, "#") {
			headerLines = append(headerLines, line)
		} else {
			// Put the line back for data processing
			reader = bufio.NewReader(io.MultiReader(
				strings.NewReader(line),
				reader,
			))
			break
		}
	}

	// Process file in chunks
	chunkCount := 0
	for {
		chunk, err := readChunk(reader, chunkSize)
		if err != nil {
			loggerError("Error reading chunk: %v", err)
			return
		}

		if len(chunk) == 0 {
			break
		}

		// Sort chunk
		sort.Sort(ByChromosomePos(chunk))

		// Write sorted chunk to temp file
		tempFile := filepath.Join(tempDir, fmt.Sprintf("chunk_%d.tmp", chunkCount))
		err = writeChunk(chunk, tempFile)
		if err != nil {
			loggerError("Error writing chunk: %v", err)
			return
		}

		loggerInfo("Saved chunk in %s", tempFile)
		tempFiles = append(tempFiles, tempFile)
		chunkCount++
	}

	// Create output file
	outputFile, err := os.Create(outputVCF)
	if err != nil {
		loggerError("Error creating output file: %v", err)
		return
	}
	defer outputFile.Close()

	writer := bufio.NewWriter(outputFile)
	defer writer.Flush()

	// Write headers
	for _, header := range headerLines {
		_, err := writer.WriteString(header)
		if err != nil {
			loggerError("Error writing headers: %v", err)
			return
		}
	}

	// Handle single chunk case
	if len(tempFiles) == 1 {
		tempFile, err := os.Open(tempFiles[0])
		if err != nil {
			loggerError("Error opening temp file: %v", err)
			return
		}
		defer tempFile.Close()

		_, err = io.Copy(writer, tempFile)
		if err != nil {
			loggerError("Error copying temp file: %v", err)
			return
		}
	} else {
		// Merge multiple chunks
		mergedTemp := filepath.Join(tempDir, "merged.tmp")
		err = mergeSortedFiles(tempFiles, mergedTemp)
		if err != nil {
			loggerError("Error merging files: %v", err)
			return
		}
		defer os.Remove(mergedTemp)

		// Copy merged data to output
		mergedFile, err := os.Open(mergedTemp)
		if err != nil {
			loggerError("Error opening merged file: %v", err)
			return
		}
		defer mergedFile.Close()

		_, err = io.Copy(writer, mergedFile)
		if err != nil {
			loggerError("Error copying merged file: %v", err)
			return
		}
	}

	loggerInfo("Successfully sorted %d chunks", chunkCount)
}
