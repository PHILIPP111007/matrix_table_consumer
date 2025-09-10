package functions_go

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/nsf/termbox-go"
)

func lazyRead(filePath string) <-chan string {
	out := make(chan string)
	go func() {
		defer close(out)
		var reader io.Reader
		if strings.HasSuffix(filePath, ".gz") {
			f, err := os.Open(filePath)
			if err != nil {
				panic(err)
			}
			gzReader, err := gzip.NewReader(f)
			if err != nil {
				panic(err)
			}
			reader = gzReader
		} else {
			file, err := os.Open(filePath)
			if err != nil {
				panic(err)
			}
			reader = bufio.NewReader(file)
		}

		scanner := bufio.NewScanner(reader)
		for scanner.Scan() {
			line := scanner.Text()
			out <- line
		}
	}()
	return out
}

func drawTextAt(x, y int, text string) {
	for _, r := range text {
		termbox.SetCell(x, y, r, termbox.ColorWhite, termbox.ColorBlack)
		x++
	}
}

func truncateString(s string, bias, maxWidth int) string {
	runes := []rune(s)
	if len(runes) > maxWidth {
		return string(runes[bias:maxWidth])
	}
	return s
}

func ViewVCF(vcfFile string) error {
	err := termbox.Init()
	if err != nil {
		return fmt.Errorf("cannot initialize termbox: %w", err)
	}
	defer termbox.Close()

	termbox.Clear(termbox.ColorDefault, termbox.ColorDefault)
	_, height := termbox.Size()

	bufferSize := height * 2
	lineBuffer := []string{}
	position := 0
	maxPosition := -1

	gen := lazyRead(vcfFile)
	termbox.HideCursor()

	flag := ""
	bias := 0
	for {

		termbox.Clear(termbox.ColorDefault, termbox.ColorDefault)

		for len(lineBuffer) <= bufferSize && maxPosition == -1 {
			select {
			case line, ok := <-gen:
				if !ok {
					maxPosition = len(lineBuffer) - height
				} else {
					lineBuffer = append(lineBuffer, line)
				}
			default:
			}
		}

		width, height := termbox.Size()
		bufferSize += height * 2

		if position < 0 {
			position = 0
		} else if maxPosition != -1 && position > maxPosition {
			position = maxPosition
		}

		switch flag {
		case "right":
			bias += 1
			flag = ""
		case "left":
			bias -= 1
			if bias < 0 {
				bias = 0
			}
			flag = ""
		}

		for i := range height {
			y := i
			x := 0

			idx := position + i
			if idx < len(lineBuffer) {
				biasLocal := bias
				maxWidth := width + biasLocal

				if biasLocal >= len(lineBuffer[idx]) {
					biasLocal = len(lineBuffer[idx]) - 1
					maxWidth = width + biasLocal
				}
				textToDisplay := truncateString(lineBuffer[idx], biasLocal, maxWidth)
				drawTextAt(x, y, textToDisplay)
			}
		}

		termbox.Flush()

		event := termbox.PollEvent()
		switch event.Type {
		case termbox.EventKey:
			keyCode := event.Key
			switch keyCode {
			case termbox.KeyArrowUp:
				position--
				flag = ""
			case termbox.KeyArrowDown:
				position++
				flag = ""
			case termbox.KeyArrowRight:
				flag = "right"
			case termbox.KeyArrowLeft:
				flag = "left"
			case termbox.KeyEnter:
				flag = ""
				position += height
			case termbox.KeyCtrlC, termbox.KeyEsc:
				flag = ""
				return nil
			case termbox.KeySpace:
				flag = ""
				termbox.Close()
				fmt.Print("Go to line: ")
				var inputStr string
				fmt.Scanln(&inputStr)
				newPos, err := strconv.Atoi(inputStr)
				if err == nil && newPos >= 0 {
					position = newPos
					bufferSize = position + height*2
				}
				termbox.Init()
				termbox.HideCursor()
			}
		}
	}
}
