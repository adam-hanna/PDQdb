package globals

type CliFlagsStruct struct {
	ConfigFilePath string
	FilePath       string
	ServerHostname string
	ServerPort     uint16
}

var DataSet map[string][]byte

func InitializeDataset() {
	// make the dataset map
	DataSet = make(map[string][]byte)
}
