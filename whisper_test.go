package whisper

import (
	"fmt"
	"os"
	"testing"
	"time"
)

func testRetentionDef(t *testing.T, retentionDef string, expectedPrecision, expectedPoints int, hasError bool) {
	errTpl := fmt.Sprintf("Expected %%v to be %%v but received %%v for retentionDef %v", retentionDef)

	retention, err := parseRetentionDef(retentionDef)

	if (err == nil && hasError) || (err != nil && !hasError) {
		if hasError {
			t.Fatalf("Expected error but received none for retentionDef %v", retentionDef)
		} else {
			t.Fatalf("Expected no error but received %v for retentionDef %v", err, retentionDef)
		}
	}
	if err == nil {
		if retention.secondsPerPoint != expectedPrecision {
			t.Fatalf(errTpl, "precision", expectedPrecision, retention.secondsPerPoint)
		}
		if retention.numberOfPoints != expectedPoints {
			t.Fatalf(errTpl, "points", expectedPoints, retention.numberOfPoints)
		}
	}
}

func TestRetentionDef(t *testing.T) {
	testRetentionDef(t, "1s:5m", 1, 300, false)
	testRetentionDef(t, "1m:30m", 60, 30, false)
	testRetentionDef(t, "1m", 0, 0, true)
	testRetentionDef(t, "1m:30m:20s", 0, 0, true)
	testRetentionDef(t, "1f:30s", 0, 0, true)
	testRetentionDef(t, "1m:30f", 0, 0, true)
}

func setUpCreate() (path string, fileExists func(string) bool, archiveList []Retention, tearDown func()) {
	path = "/tmp/whisper-testing.wsp"
	os.Remove(path)
	fileExists = func(path string) bool {
		fi, _ := os.Lstat(path)
		return fi != nil
	}
	archiveList = []Retention{{1, 300}, {60, 30}, {300, 12}}
	tearDown = func() {
		os.Remove(path)
	}
	return path, fileExists, archiveList, tearDown
}

func TestCreateCreatesFile(t *testing.T) {
	path, fileExists, archiveList, tearDown := setUpCreate()
	expected := []byte{
		// Metadata
		0x00, 0x00, 0x00, 0x01, // Aggregation type
		0x00, 0x00, 0x0e, 0x10, // Max retention
		0x3f, 0x00, 0x00, 0x00, // xFilesFactor
		0x00, 0x00, 0x00, 0x03, // Retention count
		// Archive Info
		// Retention 1 (1, 300)
		0x00, 0x00, 0x00, 0x34, // offset
		0x00, 0x00, 0x00, 0x01, // secondsPerPoint
		0x00, 0x00, 0x01, 0x2c, // numberOfPoints
		// Retention 2 (60, 30)
		0x00, 0x00, 0x0e, 0x44, // offset
		0x00, 0x00, 0x00, 0x3c, // secondsPerPoint
		0x00, 0x00, 0x00, 0x1e, // numberOfPoints
		// Retention 3 (300, 12)
		0x00, 0x00, 0x0f, 0xac, // offset
		0x00, 0x00, 0x01, 0x2c, // secondsPerPoint
		0x00, 0x00, 0x00, 0x0c} // numberOfPoints
	Create(path, archiveList, Average, 0.5)
	if !fileExists(path) {
		t.Fatalf("File does not exist after create")
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("Failed to open whisper file")
	}
	contents := make([]byte, len(expected))
	file.Read(contents)
	//fmt.Printf("%x\n", expected)
	//fmt.Printf("%x\n", contents)

	for i := 0; i < len(contents); i++ {
		if expected[i] != contents[i] {
			t.Fatalf("File is incorrect at character %v, expected %x got %x", i, expected[i], contents[i])
		}
	}

	// test size
	info, err := os.Stat(path)
	if info.Size() != 4156 {
		t.Fatalf("File size is incorrect, expected %v got %v", 4156, info.Size())
	}
	tearDown()
}

func TestCreateFileAlreadyExists(t *testing.T) {
	path, _, _, tearDown := setUpCreate()
	os.Create(path)
	err := Create(path, make([]Retention, 0), Average, 0.5)
	if err == nil {
		t.Fatalf("Existing file should cause create to fail.")
	}
	tearDown()
}

func BenchmarkCreate(b *testing.B) {
	path, _, archiveList, tearDown := setUpCreate()
	for i := 0; i < b.N; i++ {
		Create(path, archiveList, Average, 0.5)
		tearDown()
	}
	Create(path, archiveList, Average, 0.5)
}

func Test_metadataToBytes(t *testing.T) {
	expected := []byte{0, 0, 0, 1, 0, 0, 0xe, 0x10, 0x3f, 0, 0, 0, 0, 0, 0, 3}
	received := metadataToBytes([]Retention{{1, 300}, {60, 30}, {300, 12}}, Average, 0.5)
	if len(expected) != len(received) {
		t.Fatalf("Metadata is no the same length [%v] - [%v]", expected, received)
	}
	for i := 0; i < len(received); i++ {
		if expected[i] != received[i] {
			t.Fatalf("Metadata is incorrect at byte %v, expected %x got %x", i, expected[i], received[i])
		}
	}
}

func Test_archiveInfoToBytes(t *testing.T) {
	archiveOffset := 292
	expected := []byte{
		0x00, 0x00, 0x01, 0x24, // offset
		0x00, 0x00, 0x00, 0x01, // secondsPerPointer
		0x00, 0x00, 0x01, 0x2c} // numberOfPoints
	received := archiveInfoToBytes(archiveOffset, Retention{1, 300})
	if len(expected) != len(received) {
		t.Fatalf("Received %x is not the same length as %x", expected, received)
	}
	for i := 0; i < len(received); i++ {
		if expected[i] != received[i] {
			t.Fatalf("Archive info is incorrect at byte %v, expected %x got %x", i, expected[i], received[i])
		}
	}
}

func Test_readHeader(t *testing.T) {
	path, _, archiveList, tearDown := setUpCreate()
	Create(path, archiveList, Average, 0.5)

	file, _ := os.Open(path)
	metadata, err := readHeader(file)
	if err != nil {
		t.Fatalf("Error received %v", err)
	}
	if metadata.aggregationMethod != Average {
		t.Fatalf("Unexpected aggregationMethod %v, expected %v", metadata.aggregationMethod, Average)
	}
	if metadata.maxRetention != 3600 {
		t.Fatalf("Unexpected maxRetention %v, expected 3600", metadata.maxRetention)
	}
	if metadata.xFilesFactor != 0.5 {
		t.Fatalf("Unexpected xFilesFactor %v, expected 0.5", metadata.xFilesFactor)
	}
	if len(metadata.archives) != 3 {
		t.Fatalf("Unexpected archive count %v, expected 3", len(metadata.archives))
	}
	tearDown()
}

func testAggregate(t *testing.T, method AggregationMethod, expected float64) {
	received := Aggregate(method, []float64{1.0, 2.0, 3.0, 5.0, 4.0})
	if expected != received {
		t.Fatalf("Expected %v, received %v", expected, received)
	}
}
func TestAggregateAverage(t *testing.T) {
	testAggregate(t, Average, 3.0)
}

func TestAggregateSum(t *testing.T) {
	testAggregate(t, Sum, 15.0)
}

func TestAggregateLast(t *testing.T) {
	testAggregate(t, Last, 4.0)
}

func TestAggregateMax(t *testing.T) {
	testAggregate(t, Max, 5.0)
}

func TestAggregateMin(t *testing.T) {
	testAggregate(t, Min, 1.0)
}

func TestFileUpdate(t *testing.T) {
	path, _, archiveList, tearDown := setUpCreate()
	Create(path, archiveList, Average, 0.5)
	file, _ := os.Open(path)
	err := FileUpdate(file, 120.5, time.Now().Unix())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	tearDown()
}

func Test_proagate(t *testing.T) {
	path, _, archiveList, tearDown := setUpCreate()
	Create(path, archiveList, Average, 0.5)
	file, _ := os.Open(path)
	header, _ := readHeader(file)
	propagate(file, header, time.Now().Unix(), &header.archives[0], &header.archives[1])
	tearDown()
}
