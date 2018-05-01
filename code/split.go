package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"strconv"
)

func main() {
	var fileName string
	var totalChunks int
	var outDir string

	flag.StringVar(&fileName, "fileName", "../data/passwords.txt", "filename to"+
		" make chunks of")
	flag.IntVar(&totalChunks, "totalChunks", 4, "number of equal chunks to divide "+
		"the file in")
	flag.StringVar(&outDir, "outDir", "../data/chunks/", "directory to save chunks in")
	file, err := os.Open(fileName)

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	defer file.Close()

	fileInfo, _ := file.Stat()
	var fileSize int64 = fileInfo.Size()
	var fileChunk uint64 = uint64((716 / totalChunks) * (1 << 20))
	// 1 MB, change this to your requirement

	// calculate total number of parts the file will be chunked into
	totalPartsNum := uint64(math.Ceil(float64(fileSize) / float64(fileChunk)))
	fmt.Printf("Splitting to %d pieces.\n", totalPartsNum)

	for i := uint64(0); i < totalPartsNum; i++ {

		partSize := int(math.Min(float64(fileChunk),
			float64(fileSize-int64(i*fileChunk))))
		partBuffer := make([]byte, partSize)
		file.Read(partBuffer)

		// write to disk
		fileName := outDir + strconv.FormatUint(i+1, 10) + ".txt"
		_, err := os.Create(fileName)

		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		// write/save buffer to disk
		ioutil.WriteFile(fileName, partBuffer, os.ModeAppend)

		fmt.Println("Split to : ", fileName)
	}
}
