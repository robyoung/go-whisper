/*
	Package whisper implements the timeseries database backend
	for the Graphite project.

	You can find more information at http://graphite.wikidot.com/
	You can find the Whisper Python code at https://github.com/graphite-project/whisper
*/
package whisper

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
)

var retentionRegexp *regexp.Regexp

func init() {
	retentionRegexp = regexp.MustCompile("^(\\d+)([smhdwy]+)$")
}

const (
	Seconds = 1
	Minutes = 60
	Hours   = 3600
	Days    = 86400
	Weeks   = 86400 * 7
	Years   = 86400 * 365
)

type AggregationMethod int

const (
	Average AggregationMethod = iota + 1
	Sum
	Last
	Max
	Min
)

func unitMultiplier(s string) (int64, error) {
	switch {
	case strings.HasPrefix(s, "s"):
		return Seconds, nil
	case strings.HasPrefix(s, "m"):
		return Minutes, nil
	case strings.HasPrefix(s, "h"):
		return Hours, nil
	case strings.HasPrefix(s, "d"):
		return Days, nil
	case strings.HasPrefix(s, "w"):
		return Weeks, nil
	case strings.HasPrefix(s, "y"):
		return Years, nil
	}
	return 0, fmt.Errorf("Invalid unit multiplier [%v]", s)
}

func parseRetentionPart(retentionPart string) (int64, error) {
	part, err := strconv.ParseInt(retentionPart, 10, 64)
	if err == nil {
		return part, nil
	}
	if !retentionRegexp.MatchString(retentionPart) {
		return 0, fmt.Errorf("%v", retentionPart)
	}
	matches := retentionRegexp.FindStringSubmatch(retentionPart)
	value, err := strconv.ParseInt(matches[1], 10, 32)
	if err != nil {
		panic(fmt.Sprintf("Regex on %v is borked, %v cannot be parsed as int", retentionPart, matches[1]))
	}
	multiplier, err := unitMultiplier(matches[2])
	return multiplier * value, err
}

// TODO: should return an ArchiveInfo object
func parseRetentionDef(retentionDef string) (precision, points int64, err error) {
	parts := strings.Split(retentionDef, ":")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("Not enough parts in retentionDef [%v]", retentionDef)
	}
	precision, err = parseRetentionPart(parts[0])
	if err != nil {
		return 0, 0, fmt.Errorf("Failed to parse precision: %v", err)
	}

	points, err = parseRetentionPart(parts[1])
	if err != nil {
		return 0, 0, fmt.Errorf("Failed to parse points: %v", err)
	}
	points /= precision

	return precision, points, err
}

type Metadata struct {
	aggregationMethod AggregationMethod
	maxRetention      int
	xFilesFactor      float32
	archives          []ArchiveInfo
}

// TODO: need two different type for different cases
type ArchiveInfo struct {
	secondsPerPoint int
	numberOfPoints  int
}

// Create a new whisper database file
func Create(path string, archiveList []ArchiveInfo, aggregationMethod AggregationMethod, xFilesFactor float32) error {
	// validate archive list
	// open file
	_, err := os.Stat(path)
	if err == nil {
		return os.ErrExist
	}
	file, _ := os.Create(path) // test for error
	defer func() { file.Close() }()
	// lock file ?

	// create metadata
	// write metadata
	file.Write(metadataToBytes(archiveList, aggregationMethod, xFilesFactor))

	// write archive info
	headerSize := 0x10 + (0x0c * len(archiveList)) // TODO: calculate this
	archiveOffset := headerSize
	for _, archive := range archiveList {
		file.Write(archiveInfoToBytes(archiveOffset, archive))
		archiveOffset += (archive.numberOfPoints * 12)
	}

	// allocate file size
	// fallocate proved slower
	remaining := archiveOffset - headerSize
	chunkSize := 16384
	zeros := make([]byte, chunkSize)
	for remaining > chunkSize {
		file.Write(zeros)
		remaining -= chunkSize
	}
	file.Write(zeros[:remaining])
	// sync
	file.Sync()

	return nil
}

func metadataToBytes(archiveList []ArchiveInfo, aggregationMethod AggregationMethod, xFilesFactor float32) []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, int32(aggregationMethod))
	var oldest int
	for _, archive := range archiveList {
		if (archive.secondsPerPoint * archive.numberOfPoints) > oldest {
			oldest = archive.secondsPerPoint * archive.numberOfPoints
		}
	}
	binary.Write(buffer, binary.BigEndian, int32(oldest))
	binary.Write(buffer, binary.BigEndian, xFilesFactor)
	binary.Write(buffer, binary.BigEndian, int32(len(archiveList)))
	return buffer.Bytes()
}

func archiveInfoToBytes(archiveOffset int, archive ArchiveInfo) []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, int32(archiveOffset))
	binary.Write(buffer, binary.BigEndian, int32(archive.secondsPerPoint))
	binary.Write(buffer, binary.BigEndian, int32(archive.numberOfPoints))
	return buffer.Bytes()
}

func sum(values []int) int {
	result := 0
	for _, value := range values {
		result += value
	}
	return result
}
func Aggregate(method AggregationMethod, knownValues []int) float64 {
	switch method {
	case Average:
		return float64(sum(knownValues)) / float64(len(knownValues))
	case Sum:
		return float64(sum(knownValues))
	case Last:
		return float64(knownValues[len(knownValues)-1])
	case Max:
		max := knownValues[0]
		for _, value := range knownValues {
			if value > max {
				max = value
			}
		}
		return float64(max)
	case Min:
		min := knownValues[0]
		for _, value := range knownValues {
			if value < min {
				min = value
			}
		}
		return float64(min)
	}
	panic("Invalid aggregation method")
}

func readHeader(file *os.File) {

}
