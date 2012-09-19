package whisper

import (
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	IntSize         = 4
	FloatSize       = 4
	Float64Size     = 8
	PointSize       = 12
	MetadataSize    = 16
	ArchiveInfoSize = 12
)

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

func unitMultiplier(s string) (int, error) {
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

var retentionRegexp *regexp.Regexp = regexp.MustCompile("^(\\d+)([smhdwy]+)$")

func parseRetentionPart(retentionPart string) (int, error) {
	part, err := strconv.ParseInt(retentionPart, 10, 32)
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
	return multiplier * int(value), err
}

/*
  Parse a retention definition as you would find in the storage-schemas.conf of a Carbon install.
  Note that this only parses a single retention definition, if you have multiple definitions (separated by a comma)
  you will have to split them yourself.

  ParseRetentionDef("10s:14d") Retention{10, 120960}

  See: http://graphite.readthedocs.org/en/1.0/config-carbon.html#storage-schemas-conf
*/
func ParseRetentionDef(retentionDef string) (*Retention, error) {
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

func ParseRetentionDefs(retentionDefs string) (Retentions, error) {
	retentions := make(Retentions, 0)
	for _, retentionDef := range strings.Split(retentionDefs, ",") {
		retention, err := ParseRetentionDef(retentionDef)
		if err != nil {
			return nil, err
		}
		retentions = append(retentions, retention)
	}
	return retentions, nil
}

/*
	Represents a Whisper database file.
*/
type Whisper struct {
	file *os.File

	// Metadata
	aggregationMethod AggregationMethod
	maxRetention      int
	xFilesFactor      float32
	archives          []ArchiveInfo
}

/*
	Create a new Whisper database file and write it's header.
*/
func Create(path string, retentions Retentions, aggregationMethod AggregationMethod, xFilesFactor float32) (whisper *Whisper, err error) {
	sort.Sort(ByPrecision{retentions})
	if err = validateRetentions(retentions); err != nil {
		return nil, err
	}
	_, err = os.Stat(path)
	if err == nil {
		return nil, os.ErrExist
	}
	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	whisper = new(Whisper)

	// Set the metadata
	whisper.file = file
	whisper.aggregationMethod = aggregationMethod
	whisper.xFilesFactor = xFilesFactor
	for _, retention := range retentions {
		if retention.MaxRetention() > whisper.maxRetention {
			whisper.maxRetention = retention.MaxRetention()
		}
	}

	// Set the archive info
	offset := MetadataSize + (ArchiveInfoSize * len(retentions))
	whisper.archives = make([]ArchiveInfo, 0, len(retentions))
	for _, retention := range retentions {
		whisper.archives = append(whisper.archives, ArchiveInfo{*retention, offset})
		offset += retention.Size()
	}

	err = whisper.writeHeader()
	if err != nil {
		return nil, err
	}

	// pre-allocate file size, fallocate proved slower
	remaining := whisper.Size() - whisper.MetadataSize()
	chunkSize := 16384
	zeros := make([]byte, chunkSize)
	for remaining > chunkSize {
		whisper.file.Write(zeros)
		remaining -= chunkSize
	}
	whisper.file.Write(zeros[:remaining])
	whisper.file.Sync()

	return whisper, nil
}

func validateRetentions(retentions Retentions) error {
	if len(retentions) == 0 {
		return fmt.Errorf("No retentions")
	}
	for i, retention := range retentions {
		if i == len(retentions)-1 {
			break
		}

		nextRetention := retentions[i+1]
		if !(retention.secondsPerPoint < nextRetention.secondsPerPoint) {
			return fmt.Errorf("A Whisper database may not be configured having two archives with the same precision (archive%v: %v, archive%v: %v)", i, retention, i+1, nextRetention)
		}

		if nextRetention.secondsPerPoint%retention.secondsPerPoint != 0 {
			return fmt.Errorf("Higher precision archives' precision must evenly divide all lower precision archives' precision (archive%v: %v, archive%v: %v)", i, retention.secondsPerPoint, i+1, nextRetention.secondsPerPoint)
		}

		if retention.MaxRetention() >= nextRetention.MaxRetention() {
			return fmt.Errorf("Lower precision archives must cover larger time intervals than higher precision archives (archive%v: %v seconds, archive%v: %v seconds)", i, retention.MaxRetention(), i+1, nextRetention.MaxRetention())
		}

		if retention.numberOfPoints < (nextRetention.secondsPerPoint / retention.secondsPerPoint) {
			return fmt.Errorf("Each archive must have at least enough points to consolidate to the next archive (archive%v consolidates %v of archive%v's points but it has only %v total points)", i+1, nextRetention.secondsPerPoint/retention.secondsPerPoint, i, retention.numberOfPoints)
		}
	}
	return nil
}

/*
  Open an existing Whisper database and read it's header
*/
func Open(path string) (whisper *Whisper, err error) {
	file, err := os.OpenFile(path, os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	whisper = new(Whisper)
	whisper.file = file

	// read the metadata
	b := make([]byte, MetadataSize)
	offset := 0
	file.Read(b)
	whisper.aggregationMethod = AggregationMethod(unpackInt(b[offset : offset+IntSize]))
	offset += IntSize
	whisper.maxRetention = unpackInt(b[offset : offset+IntSize])
	offset += IntSize
	whisper.xFilesFactor = unpackFloat32(b[offset : offset+FloatSize])
	offset += FloatSize
	archiveCount := unpackInt(b[offset : offset+IntSize])
	offset += IntSize

	// read the archive info
	b = make([]byte, ArchiveInfoSize*archiveCount)
	file.Read(b)
	whisper.archives = make([]ArchiveInfo, archiveCount)
	for i := 0; i < archiveCount; i++ {
		whisper.archives[i] = unpackArchiveInfo(b[i*ArchiveInfoSize : (i+1)*ArchiveInfoSize])
	}

	return whisper, nil
}

func (whisper *Whisper) writeHeader() (err error) {
	// TODO: consider optimizing this in the same way as unpack / pack int / float
	//       we know the size and the types so it should be effective.
	if err = binary.Write(whisper.file, binary.BigEndian, int32(whisper.aggregationMethod)); err != nil {
		return err
	}

	if err = binary.Write(whisper.file, binary.BigEndian, int32(whisper.maxRetention)); err != nil {
		return err
	}
	if err = binary.Write(whisper.file, binary.BigEndian, whisper.xFilesFactor); err != nil {
		return err
	}
	if err = binary.Write(whisper.file, binary.BigEndian, int32(len(whisper.archives))); err != nil {
		return err
	}
	for _, archive := range whisper.archives {
		if err = binary.Write(whisper.file, binary.BigEndian, int32(archive.offset)); err != nil {
			return err
		}
		if err = binary.Write(whisper.file, binary.BigEndian, int32(archive.secondsPerPoint)); err != nil {
			return err
		}
		if err = binary.Write(whisper.file, binary.BigEndian, int32(archive.numberOfPoints)); err != nil {
			return err
		}
	}
	return nil
}

/*
  Close the whisper file
*/
func (whisper *Whisper) Close() {
	whisper.file.Close()
}

/*
  Calculate the total number of bytes the Whisper file should be according to the metadata.
*/
func (whisper *Whisper) Size() int {
	size := whisper.MetadataSize()
	for _, archive := range whisper.archives {
		size += archive.Size()
	}
	return size
}

/*
  Calculate the number of bytes the metadata section will be.  
*/
func (whisper *Whisper) MetadataSize() int {
	return MetadataSize + (ArchiveInfoSize * len(whisper.archives))
}

/*
  Update a value in the database.

  If the timestamp is in the future or outside of the maximum retention it will
  fail immediately.
*/
func (whisper *Whisper) Update(value float64, timestamp int) (err error) {
	diff := int(time.Now().Unix()) - timestamp
	if !(diff < whisper.maxRetention && diff >= 0) {
		return fmt.Errorf("Timestamp not covered by any archives in this database")
	}
	var archive ArchiveInfo
	var lowerArchives []ArchiveInfo
	var i int
	for i, archive = range whisper.archives {
		if archive.MaxRetention() < diff {
			continue
		}
		lowerArchives = whisper.archives[i+1:] // TODO: investigate just returning the positions
		break
	}

	myInterval := timestamp - (timestamp % archive.secondsPerPoint)
	point := DataPoint{myInterval, value}

	_, err = whisper.file.WriteAt(point.Bytes(), int64(whisper.getPointOffset(myInterval, &archive)))
	if err != nil {
		return err
	}

	higher := archive
	for _, lower := range lowerArchives {
		propagated, err := whisper.propagate(myInterval, &higher, &lower)
		if err != nil {
			return err
		} else if !propagated {
			break
		}
		higher = lower
	}

	return nil
}

func (whisper *Whisper) getPointOffset(start int, archive *ArchiveInfo) int {
	base, _ := whisper.readInt(archive.Offset())
	if base == 0 {
		return int(archive.Offset())
	}
	return int(archive.Offset() + int64(((start-base)/archive.secondsPerPoint)*PointSize%archive.Size()))
}

func (whisper *Whisper) propagate(timestamp int, higher, lower *ArchiveInfo) (bool, error) {
	lowerIntervalStart := timestamp - (timestamp % lower.secondsPerPoint)

	higherFirstOffset := whisper.getPointOffset(lowerIntervalStart, higher)

	// TODO: extract all this series extraction stuff
	higherPoints := lower.secondsPerPoint / higher.secondsPerPoint
	higherSize := higherPoints * PointSize
	relativeFirstOffset := higherFirstOffset - higher.offset
	relativeLastOffset := (relativeFirstOffset + higherSize) % higher.Size()
	higherLastOffset := relativeLastOffset + higher.offset

	var seriesBytes []byte
	if higherFirstOffset < higherLastOffset {
		seriesBytes = make([]byte, higherLastOffset-higherFirstOffset)
		whisper.file.ReadAt(seriesBytes, int64(higherFirstOffset))
	} else {
		seriesBytes = make([]byte, higher.offset+higher.Size()-higherFirstOffset)
		whisper.file.ReadAt(seriesBytes, int64(higherFirstOffset))
		chunk := make([]byte, higherLastOffset-higher.offset)
		whisper.file.ReadAt(chunk, higher.Offset())
		seriesBytes = append(seriesBytes, chunk...)
	}

	// now we unpack the series data we just read
	series := make([]DataPoint, 0, len(seriesBytes)/PointSize)
	for i := 0; i < len(seriesBytes); i += PointSize {
		interval := unpackInt(seriesBytes[i : i+IntSize])
		value := unpackFloat64(seriesBytes[i+IntSize : i+PointSize])
		series = append(series, DataPoint{interval, value})
	}

	// and finally we construct a list of values
	knownValues := make([]float64, 0, len(series))
	currentInterval := lowerIntervalStart

	for _, dataPoint := range series {
		if dataPoint.interval == currentInterval {
			knownValues = append(knownValues, dataPoint.value)
		}
		currentInterval += higher.secondsPerPoint
	}

	// propagate aggregateValue to propagate from neighborValues if we have enough known points        
	if len(knownValues) == 0 {
		return false, nil
	}
	knownPercent := float32(len(knownValues)) / float32(len(series))
	if knownPercent < whisper.xFilesFactor { // we have enough data points to propagate a value       
		return false, nil
	} else {
		aggregateValue := Aggregate(whisper.aggregationMethod, knownValues)
		point := DataPoint{lowerIntervalStart, aggregateValue}
		whisper.file.WriteAt(point.Bytes(), int64(whisper.getPointOffset(lowerIntervalStart, lower)))
	}
	return true, nil
}

/*
  Fetch a TimeSeries for a given time span from the file.
*/
func (whisper *Whisper) Fetch(fromTime, untilTime int) (timeSeries *TimeSeries, err error) {
	now := int(time.Now().Unix()) // TODO: danger of 2030 something overflow
	if fromTime > untilTime {
		return nil, fmt.Errorf("Invalid time interval: from time '%s' is after until time '%s'", fromTime, untilTime)
	}
	oldestTime := now - whisper.maxRetention
	// range is in the future
	if fromTime > now {
		return nil, nil
	}
	// range is beyond retention
	if untilTime < oldestTime {
		return nil, nil
	}
	if fromTime < oldestTime {
		fromTime = oldestTime
	}
	if untilTime > now {
		untilTime = now
	}

	// TODO: improve this algorithm it's ugly
	diff := now - fromTime
	var archive ArchiveInfo
	for _, archive = range whisper.archives {
		if archive.MaxRetention() >= diff {
			break
		}
	}

	// TODO: should be an integer
	fromInterval := fromTime - (fromTime % archive.secondsPerPoint) + archive.secondsPerPoint
	untilInterval := untilTime - (untilTime % archive.secondsPerPoint) + archive.secondsPerPoint

	baseInterval, err := whisper.readInt(archive.Offset())
	if err != nil {
		return nil, err
	}

	if baseInterval == 0 {
		step := archive.secondsPerPoint
		points := (untilInterval - fromInterval) / step
		values := make([]float64, points)
		// TODO: this is wrong, zeros for nil values is wrong
		return &TimeSeries{fromInterval, untilInterval, step, values}, nil
	}

	// TODO: extract these two offset calcs (also done when writing)
	timeDistance := fromInterval - baseInterval
	pointDistance := timeDistance / archive.secondsPerPoint
	byteDistance := pointDistance * PointSize
	fromOffset := archive.offset + (byteDistance % archive.Size())

	timeDistance = untilInterval - baseInterval
	pointDistance = timeDistance / archive.secondsPerPoint
	byteDistance = pointDistance * PointSize
	untilOffset := archive.offset + (byteDistance % archive.Size())

	// Read all the points in the interval
	var seriesBytes []byte
	if fromOffset < untilOffset {
		seriesBytes = make([]byte, untilOffset-fromOffset)
		_, err = whisper.file.ReadAt(seriesBytes, int64(fromOffset))
		if err != nil {
			return nil, err
		}
	} else {
		archiveEnd := archive.offset + archive.Size()
		seriesBytes = make([]byte, archiveEnd-fromOffset)
		_, err = whisper.file.ReadAt(seriesBytes, int64(fromOffset))
		if err != nil {
			return nil, err
		}
		chunk := make([]byte, untilOffset-archive.offset)
		_, err = whisper.file.ReadAt(chunk, archive.Offset())
		if err != nil {
			return nil, err
		}
		seriesBytes = append(seriesBytes, chunk...)
	}

	// Unpack the series data we just read
	// TODO: extract this into a common method (also used when writing)
	series := make([]DataPoint, 0, len(seriesBytes)/PointSize)
	for i := 0; i < len(seriesBytes); i += PointSize {
		interval := unpackInt(seriesBytes[i : i+IntSize])
		value := unpackFloat64(seriesBytes[i+IntSize : i+PointSize])
		series = append(series, DataPoint{interval, value})
	}

	values := make([]float64, len(series))
	currentInterval := fromInterval
	step := archive.secondsPerPoint

	for i, dataPoint := range series {
		if dataPoint.interval == currentInterval { // TODO: get rid of cast
			values[i] = dataPoint.value
		}
		currentInterval += step
	}

	return &TimeSeries{fromInterval, untilInterval, step, values}, nil
}

func (whisper *Whisper) readInt(offset int64) (int, error) {
	// TODO: make errors better
	b := make([]byte, IntSize)
	_, err := whisper.file.ReadAt(b, offset)
	if err != nil {
		return 0, err
	}

	return unpackInt(b), nil
}

/*
  A retention level.

  Retention levels describe a given archive in the database. How detailed it is and how far back
  it records.
*/
type Retention struct {
	secondsPerPoint int
	numberOfPoints  int
}

func (retention *Retention) MaxRetention() int {
	return retention.secondsPerPoint * retention.numberOfPoints
}

func (retention *Retention) Size() int {
	return retention.numberOfPoints * PointSize
}

type Retentions []*Retention

func (r Retentions) Len() int {
	return len(r)
}

func (r Retentions) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}

type ByPrecision struct{ Retentions }

func (r ByPrecision) Less(i, j int) bool {
	return r.Retentions[i].secondsPerPoint < r.Retentions[j].secondsPerPoint
}

/*
  Describes an archive in terms of a file.

  The only addition this type has over a Retention is the offset at which it exists within the
  whisper file.
*/
type ArchiveInfo struct {
	Retention
	offset int
}

func unpackArchiveInfo(b []byte) ArchiveInfo {
	return ArchiveInfo{Retention{unpackInt(b[:IntSize]), unpackInt(b[IntSize : IntSize*2])}, unpackInt(b[IntSize*2 : IntSize*3])}
}

func (archive *ArchiveInfo) Offset() int64 {
	return int64(archive.offset)
}

type TimeSeries struct {
	fromTime  int
	untilTime int
	step      int
	values    []float64
}

func (ts *TimeSeries) Points() []TimeSeriesPoint {
	points := make([]TimeSeriesPoint, len(ts.values))
	for i, value := range ts.values {
		points[i] = TimeSeriesPoint{Time: time.Unix(int64(ts.fromTime+ts.step*i), 0), Value: value}
	}
	return points
}

type TimeSeriesPoint struct {
	Time  time.Time
	Value float64
}

type DataPoint struct {
	interval int
	value    float64
}

func (point *DataPoint) Bytes() []byte {
	b := make([]byte, PointSize)
	binary.BigEndian.PutUint32(b[:IntSize], uint32(point.interval))
	binary.BigEndian.PutUint64(b[IntSize:PointSize], math.Float64bits(point.value))
	return b
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

func unpackInt(b []byte) int {
	return int(binary.BigEndian.Uint32(b))
}

func unpackFloat32(b []byte) float32 {
	return math.Float32frombits(binary.BigEndian.Uint32(b))
}

func unpackFloat64(b []byte) float64 {
	return math.Float64frombits(binary.BigEndian.Uint64(b))
}
