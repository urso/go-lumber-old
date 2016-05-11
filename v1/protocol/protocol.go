package protocol

const (
	Version = 1

	CodeVersion byte = '1'

	CodeWindowSize byte = 'W'
	CodeDataFrame  byte = 'D'
	CodeCompressed byte = 'C'
	CodeACK        byte = 'A'
)
