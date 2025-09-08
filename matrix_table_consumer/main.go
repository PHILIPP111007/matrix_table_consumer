package main

import (
	"C"

	functions "functions_go/functions_go"
)

import (
	"functions_go/functions_go"
)

//export CollectAll
func CollectAll(vcf_path_pointer *C.char, num_cpu int) *C.char {
	vcf_path := C.GoString(vcf_path_pointer)

	return C.CString(functions.CollectAll(vcf_path, num_cpu))
}

//export Collect
func Collect(num_rows int, start_row int, vcf_path_pointer *C.char, num_cpu int) *C.char {
	vcf_path := C.GoString(vcf_path_pointer)

	// return functions_go.Collect(num_rows, start_row, vcf_path, is_gzip, num_cpu)
	return C.CString(functions_go.Collect(num_rows, start_row, vcf_path, num_cpu))
}

//export Count
func Count(vcf_path_pointer *C.char) int {
	vcf_path := C.GoString(vcf_path_pointer)

	return functions_go.Count(vcf_path)
}

//export Filter
func Filter(include_pointer *C.char, input_vcf_path_pointer *C.char, output_vcf_path_pointer *C.char, num_cpu int) {
	include := C.GoString(include_pointer)
	input_vcf_path := C.GoString(input_vcf_path_pointer)
	output_vcf_path := C.GoString(output_vcf_path_pointer)

	functions_go.Filter(include, input_vcf_path, output_vcf_path, num_cpu)
}

//export Merge
func Merge(vcf1_pointer *C.char, vcf2_pointer *C.char, output_vcf_pointer *C.char) {
	vcf1 := C.GoString(vcf1_pointer)
	vcf2 := C.GoString(vcf2_pointer)
	output_vcf := C.GoString(output_vcf_pointer)

	functions_go.Merge(vcf1, vcf2, output_vcf)
}

func main() {}
