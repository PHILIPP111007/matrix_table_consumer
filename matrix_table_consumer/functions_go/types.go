package functions_go

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
