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
	"time"
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

type Metadata struct {
	aggregationMethod AggregationMethod
	maxRetention      int
	xFilesFactor      float32
	archives          []ArchiveInfo
}

/*
	Describes how an archive will be stored in terms of the number of seconds each point
	represents and the number of points the archive will retain. This is calculated
	from a RetentionDef.
*/
type Retention struct {
	secondsPerPoint int
	numberOfPoints  int
}

/*
	Describes an archive in a whisper file.
*/
type ArchiveInfo struct {
	offset          int
	secondsPerPoint int
	numberOfPoints  int
	secondsRetained int
	size            int
}

type DataPoint struct {
	interval int32
	value    float64
}

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

func parseRetentionPart(retentionPart string) (int, error) {
	part, err := strconv.ParseInt(retentionPart, 10, 64)
	if err == nil {
		return int(part), nil
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
	return int(multiplier * value), err
}

func parseRetentionDef(retentionDef string) (*Retention, error) {
	parts := strings.Split(retentionDef, ":")
	if len(parts) != 2 {
		return nil, fmt.Errorf("Not enough parts in retentionDef [%v]", retentionDef)
	}
	precision, err := parseRetentionPart(parts[0])
	if err != nil {
		return nil, fmt.Errorf("Failed to parse precision: %v", err)
	}

	points, err := parseRetentionPart(parts[1])
	if err != nil {
		return nil, fmt.Errorf("Failed to parse points: %v", err)
	}
	points /= precision

	return &Retention{precision, points}, err
}

// Create a new whisper database file
func Create(path string, archiveList []Retention, aggregationMethod AggregationMethod, xFilesFactor float32) error {
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

func metadataToBytes(archiveList []Retention, aggregationMethod AggregationMethod, xFilesFactor float32) []byte {
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

func archiveInfoToBytes(archiveOffset int, archive Retention) []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, int32(archiveOffset))
	binary.Write(buffer, binary.BigEndian, int32(archive.secondsPerPoint))
	binary.Write(buffer, binary.BigEndian, int32(archive.numberOfPoints))
	return buffer.Bytes()
}

func sum(values []float64) float64 {
	result := 0.0
	for _, value := range values {
		result += value
	}
	return result
}
func Aggregate(method AggregationMethod, knownValues []float64) float64 {
	switch method {
	case Average:
		return sum(knownValues) / float64(len(knownValues))
	case Sum:
		return sum(knownValues)
	case Last:
		return knownValues[len(knownValues)-1]
	case Max:
		max := knownValues[0]
		for _, value := range knownValues {
			if value > max {
				max = value
			}
		}
		return max
	case Min:
		min := knownValues[0]
		for _, value := range knownValues {
			if value < min {
				min = value
			}
		}
		return min
	}
	panic("Invalid aggregation method")
}

func unpackInt(byteArray []byte, name string) (int, error) {
	buffer := bytes.NewBuffer(byteArray)
	var value int32
	err := binary.Read(buffer, binary.BigEndian, &value)
	if err != nil {
		return 0, fmt.Errorf("Failed to read %v: %v", name, err)
	}
	return int(value), nil
}

func unpackFloat(byteArray []byte, name string) (float32, error) {
	buffer := bytes.NewBuffer(byteArray)
	var value float32
	err := binary.Read(buffer, binary.BigEndian, &value)
	if err != nil {
		return 0, fmt.Errorf("Failed to read %v: %v", name, err)
	}
	return value, nil
}

func unpackDecimal(byteArray []byte, name string) (value float64, err error) {
	buffer := bytes.NewBuffer(byteArray)
	err = binary.Read(buffer, binary.BigEndian, &value)
	if err != nil {
		return 0, fmt.Errorf("Failed to read %v: %v", name, err)
	}
	return value, nil
}

func readHeader(file *os.File) (metadata *Metadata, err error) {
	// return from cache

	// store original position
	// TODO: figure out originalOffset := file.Tell() ... file.Seek(0, 1)
	// seek to start of file
	_, err = file.Seek(0, 0)
	if err != nil {
		return nil, fmt.Errorf("Failed to read header: %v", err)
	}
	// read metadata
	bytes := make([]byte, 16) // TODO: extract magic number
	file.Read(bytes)
	// unpack Metadata
	// TODO: extract magic numbers
	aggregationMethod, err := unpackInt(bytes[0:4], "aggregationMethod")
	if err != nil {
		return nil, err
	}
	maxRetention, err := unpackInt(bytes[4:8], "maxRetention")
	if err != nil {
		return nil, err
	}
	xFilesFactor, err := unpackFloat(bytes[8:12], "xFilesFactor")
	if err != nil {
		return nil, err
	}
	archiveCount, err := unpackInt(bytes[12:16], "archiveCount")
	if err != nil {
		return nil, err
	}

	// unpack ArchiveInfo array
	archives := make([]ArchiveInfo, 0, archiveCount)

	bytes = make([]byte, 12) // TODO: extract magic number
	var offset, secondsPerPoint, numberOfPoints int
	for i := 0; i < archiveCount; i++ {
		file.Read(bytes)
		offset, err = unpackInt(bytes[0:4], fmt.Sprintf("offset[%v]", i))
		if err != nil {
			return nil, err
		}
		secondsPerPoint, err = unpackInt(bytes[4:8], fmt.Sprintf("secondsPerPoint[%v]", i))
		if err != nil {
			return nil, err
		}
		numberOfPoints, err = unpackInt(bytes[8:12], fmt.Sprintf("numberOfPoints[%v]", i))
		if err != nil {
			return nil, err
		}
		archives = append(archives, ArchiveInfo{offset, secondsPerPoint, numberOfPoints, secondsPerPoint * numberOfPoints, numberOfPoints * 12})
	}

	// restore original position
	// store to cache

	metadata = &Metadata{AggregationMethod(aggregationMethod), maxRetention, xFilesFactor, archives}
	return metadata, err
}

func FileUpdate(file *os.File, value float64, timestamp int64) (err error) {
	// NOTE: this does not lock, it's up to the caller to ensure thread safety

	header, err := readHeader(file)
	if err != nil {
		return err
	}
	now := time.Now()
	// find time position and confirm it's within maxRetention
	// find the highest precision archive that covers the timestamp
	// as a consequence find all archives that must be updated
	fmt.Println(header, now)

	return nil
}

func propagate(file *os.File, metadata *Metadata, timestamp int64, higher, lower *ArchiveInfo) (bool, error) {
	lowerIntervalStart := timestamp - (timestamp % int64(lower.secondsPerPoint))
	lowerIntervalEnd := lowerIntervalStart + int64(lower.secondsPerPoint)

	fmt.Printf("lowerIntervalStart: %v\n", lowerIntervalStart)
	fmt.Printf("lowerIntervalEnd:   %v\n", lowerIntervalEnd)

	// TODO: look at extracting into a method on ArchiveInfo
	_, err := file.Seek(int64(higher.offset), 0)
	if err != nil {
		return false, fmt.Errorf("Failed to read point: %v", err)
	}
	packedPoint := make([]byte, 12) // TODO: extract magic number
	file.Read(packedPoint)
	higherBaseInterval, err := unpackInt(packedPoint[0:4], "higherBaseInterval")
	if err != nil {
		return false, err
	}
	// Only the first number is needed TODO: address this
	//higherBaseValue, err := unpackDecimal(bytes[4:12], "higherBaseValue")
	//if err != nil { return false, err }

	var higherFirstOffset int
	if higherBaseInterval == 0 {
		higherFirstOffset = higher.offset
	} else {
		timeDistance := int(lowerIntervalStart) - higherBaseInterval
		pointDistance := timeDistance / higher.secondsPerPoint
		byteDistance := pointDistance * 12 // TODO: extract magic number
		higherFirstOffset = higher.offset + (byteDistance % higher.size)
	}

	// TODO: todo all this series extraction stuff
	higherPoints := lower.secondsPerPoint / higher.secondsPerPoint
	higherSize := higherPoints * 12
	relativeFirstOffset := higherFirstOffset - higher.offset
	relativeLastOffset := (relativeFirstOffset + higherSize) % higher.size
	higherLastOffset := relativeLastOffset + higher.offset
	file.Seek(int64(higherFirstOffset), 0)

	var seriesBytes []byte
	if higherFirstOffset < higherLastOffset { // we don't wrap the archive
		seriesBytes = make([]byte, higherLastOffset-higherFirstOffset)
		file.Read(seriesBytes)
	} else { // we do wrap the archive
		seriesBytes = make([]byte, higher.offset+higher.size-higherFirstOffset)
		file.Read(seriesBytes)
		file.Seek(int64(higher.offset), 0)
		chunk := make([]byte, higherLastOffset-higher.offset)
		file.Read(chunk)
		seriesBytes = append(seriesBytes, chunk...)
	}

	// now we unpack the series data we just read
	series := make([]DataPoint, 0, len(seriesBytes)/12)
	var interval int
	var value float64
	for i := 0; i < len(seriesBytes); i += 12 {
		interval, err = unpackInt(seriesBytes[i:i+4], fmt.Sprintf("series interval [%v]", i/12))
		if err != nil {
			return false, err
		}
		value, err = unpackDecimal(seriesBytes[i+4:i+12], fmt.Sprintf("series value [%v]", i/12))
		if err != nil {
			return false, err
		}
		series = append(series, DataPoint{int32(interval), value})
	}

	// and finally we construct a list of values
	knownValues := make([]float64, 0, len(series))
	currentInterval := lowerIntervalStart
	step := higher.secondsPerPoint

	for _, dataPoint := range series {
		if int64(dataPoint.interval) == currentInterval {
			knownValues = append(knownValues, dataPoint.value)
		}
		currentInterval += int64(step)
	}

	// propagate aggregateValue to propagate from neighborValues if we have enough known points
	if len(knownValues) == 0 {
		return false, nil
	}
	knownPercent := float32(len(knownValues)) / float32(len(series))
	if knownPercent < metadata.xFilesFactor { // we have enough data points to propagate a value
		return false, nil
	} else {
		aggregateValue := Aggregate(metadata.aggregationMethod, knownValues)
		// TODO: move this to DataPoint.Pack()
		packedPoint := new(bytes.Buffer)
		binary.Write(packedPoint, binary.BigEndian, int32(lowerIntervalStart))
		binary.Write(packedPoint, binary.BigEndian, float64(aggregateValue))
		file.Seek(int64(lower.offset), 0) // TODO: get rid of cast
		// TODO: move this into DataPoint.Unpack()
		pointBytes := make([]byte, 12) // TODO: extract magic number
		file.Read(pointBytes)
		lowerBaseInterval, err := unpackInt(pointBytes[0:4], "lowerBaseInterval")
		if err != nil {
			return false, err
		}
		// only the interval is needed TODO: address this
		//lowerBaseValue, err := unpackDecimal(pointBytes[4:12], "lowerBaseValue")
		//if err != nil { return false, err }
		if lowerBaseInterval == 0 {
			file.Seek(int64(lower.offset), 0) // TODO: get rid of cast
			file.Write(packedPoint.Bytes())
		} else {
			timeDistance := lowerIntervalStart - int64(lowerBaseInterval)           // TODO: get rid of cast
			pointDistance := timeDistance / int64(lower.secondsPerPoint)            // TODO: get rid of cast
			byteDistance := pointDistance * 12                                      // TODO: extract magic number
			lowerOffset := int64(lower.offset) + (byteDistance % int64(lower.size)) // TODO: get rid of cast
			file.Seek(lowerOffset, 0)
			file.Write(packedPoint.Bytes())
		}
	}
	return true, nil
}
