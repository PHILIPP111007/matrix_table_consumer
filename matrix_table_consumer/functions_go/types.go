package functions_go

import (
	"io"
	"time"
)

type VCFContainer struct {
	Qual   int8   `json:"QUAL"`
	Pos    int32  `json:"POS"`
	Chrom  string `json:"CHROM"`
	Id     string `json:"ID"`
	Ref    string `json:"REF"`
	Alt    string `json:"ALT"`
	Filter string `json:"FILTER"`
	Info   string `json:"INFO"`
}

// Тип коллекции строк VCF
type Rows []*VCFContainer

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
