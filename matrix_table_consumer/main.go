package main

import (
	"C"

	functions "functions_go/functions_go"
)

import (
	"functions_go/functions_go"
)

//export CollectAll
func CollectAll(vcf_path_pointer *C.char, is_gzip bool, num_cpu int) *C.char {
	vcf_path := C.GoString(vcf_path_pointer)

	return C.CString(functions.CollectAll(vcf_path, is_gzip, num_cpu))
}

//export Collect
func Collect(num_rows int, start_row int, vcf_path_pointer *C.char, is_gzip bool, num_cpu int) *C.char {
	vcf_path := C.GoString(vcf_path_pointer)

	// return functions_go.Collect(num_rows, start_row, vcf_path, is_gzip, num_cpu)
	return C.CString(functions_go.Collect(num_rows, start_row, vcf_path, is_gzip, num_cpu))
}

//export Count
func Count(vcf_path_pointer *C.char, is_gzip bool) int {
	vcf_path := C.GoString(vcf_path_pointer)

	return functions_go.Count(vcf_path, is_gzip)
}

//export Filter
func Filter(include_pointer *C.char, input_vcf_path_pointer *C.char, output_vcf_path_pointer *C.char, is_gzip bool, num_cpu int) {
	include := C.GoString(include_pointer)
	input_vcf_path := C.GoString(input_vcf_path_pointer)
	output_vcf_path := C.GoString(output_vcf_path_pointer)

	functions_go.Filter(include, input_vcf_path, output_vcf_path, is_gzip, num_cpu)
}

func main() {}
