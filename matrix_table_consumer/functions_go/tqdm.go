package functions_go

import (
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"time"
	"unicode/utf8"
)

// Tqdm introduces progress bar
type Tqdm struct {
	total       int
	current     int
	startTime   time.Time
	lastUpdate  time.Time
	description string
	writer      io.Writer
	barFormat   string
	width       int
	showBar     bool
	showRate    bool
	showETA     bool
	unit        string
	unitScale   bool
	minInterval time.Duration
}

// Option defines a function to configure Tqdm
type Option func(*Tqdm)

func New(total int, options ...Option) *Tqdm {
	t := &Tqdm{
		total:       total,
		current:     0,
		startTime:   time.Now(),
		lastUpdate:  time.Now(),
		description: "",
		writer:      os.Stderr,
		barFormat:   "{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]",
		width:       80,
		showBar:     true,
		showRate:    true,
		showETA:     true,
		unit:        "it",
		unitScale:   false,
		minInterval: 100 * time.Millisecond,
	}

	for _, option := range options {
		option(t)
	}

	return t
}

// WithDescription sets the description
func WithDescription(desc string) Option {
	return func(t *Tqdm) {
		t.description = desc
	}
}

// WithWriter sets the writer to output
func WithWriter(w io.Writer) Option {
	return func(t *Tqdm) {
		t.writer = w
	}
}

// WithBarFormat sets the format of the progress bar
func WithBarFormat(format string) Option {
	return func(t *Tqdm) {
		t.barFormat = format
	}
}

// WithWidth sets the width of the progress bar
func WithWidth(width int) Option {
	return func(t *Tqdm) {
		t.width = width
	}
}

// WithUnit sets the unit of measurement
func WithUnit(unit string) Option {
	return func(t *Tqdm) {
		t.unit = unit
	}
}

// WithUnitScale enables/disables unit scaling
func WithUnitScale(scale bool) Option {
	return func(t *Tqdm) {
		t.unitScale = scale
	}
}

// Update updates the progress by n steps
func (t *Tqdm) Update(n int) {
	t.current += n
	t.refresh()
}

// Increment increases progress by 1 step
func (t *Tqdm) Increment() {
	t.Update(1)
}

// SetDescription sets the description
func (t *Tqdm) SetDescription(desc string) {
	t.description = desc
	t.refresh()
}

// Close completes the progress bar
func (t *Tqdm) Close() {
	t.refresh()
	fmt.Fprintln(t.writer)
}

// refreshes the progress bar display
func (t *Tqdm) refresh() {
	now := time.Now()
	if now.Sub(t.lastUpdate) < t.minInterval && t.current < t.total {
		return
	}
	t.lastUpdate = now

	fmt.Fprint(t.writer, "\r"+t.render())
}

// render generates a progress bar line
func (t *Tqdm) render() string {
	elapsed := time.Since(t.startTime)
	rate := t.calculateRate(elapsed)
	remaining := t.calculateRemaining(rate)

	replacements := map[string]string{
		"{desc}":       t.description,
		"{n_fmt}":      t.formatNumber(t.current),
		"{total_fmt}":  t.formatNumber(t.total),
		"{elapsed}":    t.formatDuration(elapsed),
		"{remaining}":  t.formatDuration(remaining),
		"{rate}":       t.formatRate(rate),
		"{bar}":        t.renderBar(),
		"{l_bar}":      t.renderLeftBar(),
		"{percentage}": fmt.Sprintf("%.1f%%", t.percentage()),
	}

	result := t.barFormat
	for key, value := range replacements {
		result = strings.ReplaceAll(result, key, value)
	}

	return t.truncateToWidth(result)
}

// renderBar draws the progress bar itself
func (t *Tqdm) renderBar() string {
	if !t.showBar || t.width < 10 {
		return ""
	}

	width := t.width - 50 // Место для текстовой информации
	if width < 10 {
		width = 10
	}

	progress := float64(t.current) / float64(t.total)
	completedWidth := int(progress * float64(width))

	bar := strings.Repeat("=", completedWidth)
	if completedWidth < width {
		bar += ">" + strings.Repeat(" ", width-completedWidth-1)
	} else {
		bar += "="
	}

	return "[" + bar + "]"
}

// renderLeftBar draws the left part with the description
func (t *Tqdm) renderLeftBar() string {
	if t.description == "" {
		return ""
	}
	return t.description + ": "
}

// calculateRate calculates the processing speed
func (t *Tqdm) calculateRate(elapsed time.Duration) float64 {
	if elapsed.Seconds() == 0 {
		return 0
	}
	return float64(t.current) / elapsed.Seconds()
}

// calculateRemaining calculates the remaining time
func (t *Tqdm) calculateRemaining(rate float64) time.Duration {
	if rate == 0 || t.current == 0 {
		return 0
	}
	remainingItems := t.total - t.current
	return time.Duration(float64(remainingItems)/rate) * time.Second
}

// percentage calculates the percentage of completion
func (t *Tqdm) percentage() float64 {
	if t.total == 0 {
		return 0
	}
	return float64(t.current) / float64(t.total) * 100
}

// formatNumber formats numbers with scaling
func (t *Tqdm) formatNumber(n int) string {
	if !t.unitScale {
		return fmt.Sprintf("%d", n)
	}

	sizes := []string{"", "K", "M", "G", "T"}
	if n == 0 {
		return "0"
	}

	size := math.Floor(math.Log10(float64(n)) / 3)
	value := float64(n) / math.Pow(1000, size)

	return fmt.Sprintf("%.1f%s", value, sizes[int(size)])
}

// formatDuration formats the duration
func (t *Tqdm) formatDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%.1fs", d.Seconds())
	}

	hours := int(d.Hours())
	minutes := int(d.Minutes()) % 60
	seconds := int(d.Seconds()) % 60

	if hours > 0 {
		return fmt.Sprintf("%02d:%02d:%02d", hours, minutes, seconds)
	}
	return fmt.Sprintf("%02d:%02d", minutes, seconds)
}

// formatRate formats the speed
func (t *Tqdm) formatRate(rate float64) string {
	if !t.unitScale {
		return fmt.Sprintf("%.1f %s/s", rate, t.unit)
	}

	sizes := []string{"", "K", "M", "G", "T"}
	if rate == 0 {
		return fmt.Sprintf("0 %s/s", t.unit)
	}

	size := math.Floor(math.Log10(rate) / 3)
	value := rate / math.Pow(1000, size)

	return fmt.Sprintf("%.1f%s %s/s", value, sizes[int(size)], t.unit)
}

// truncateToWidth truncates a string to the desired width
func (t *Tqdm) truncateToWidth(s string) string {
	// Calculate the real length of a string taking into account UTF-8
	currentWidth := utf8.RuneCountInString(s)
	if currentWidth <= t.width {
		return s
	}

	// Cut to the desired width
	runes := []rune(s)
	if len(runes) > t.width {
		return string(runes[:t.width])
	}
	return s
}
