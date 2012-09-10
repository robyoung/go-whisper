package whisper

import (
	"fmt"
	"os"
	"testing"
)

func testRetentionDef(t *testing.T, retentionDef string, expectedPrecision, expectedPoints int64, hasError bool) {
	errTpl := fmt.Sprintf("Expected %%v to be %%v but received %%v for retentionDef %v", retentionDef)

	precision, points, err := parseRetentionDef(retentionDef)

	if (err == nil && hasError) || (err != nil && !hasError) {
		if hasError {
			t.Fatalf("Expected error but received none for retentionDef %v", retentionDef)
		} else {
			t.Fatalf("Expected no error but received %v for retentionDef %v", err, retentionDef)
		}
	}
	if precision != expectedPrecision {
		t.Fatalf(errTpl, "precision", expectedPrecision, precision)
	}
	if points != expectedPoints {
		t.Fatalf(errTpl, "points", expectedPoints, points)
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

func setUpCreate() (path string, fileExists func(string) bool, archiveList []ArchiveInfo, tearDown func()) {
	path = "/tmp/whisper-testing.wsp"
	os.Remove(path)
	fileExists = func(path string) bool {
		fi, _ := os.Lstat(path)
		return fi != nil
	}
	archiveList = []ArchiveInfo{{1, 300}, {60, 30}, {300, 12}}
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
		0x00, 0x00, 0x00, 0x03, // ArchiveInfo count
		// ArchiveInfo
		// ArchiveInfo 1 (1, 300)
		0x00, 0x00, 0x00, 0x34, // offset
		0x00, 0x00, 0x00, 0x01, // secondsPerPoint
		0x00, 0x00, 0x01, 0x2c, // numberOfPoints
		// ArchiveInfo 2 (60, 30)
		0x00, 0x00, 0x0e, 0x44, // offset
		0x00, 0x00, 0x00, 0x3c, // secondsPerPoint
		0x00, 0x00, 0x00, 0x1e, // numberOfPoints
		// ArchiveInfo 3 (300, 12)
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
	err := Create(path, make([]ArchiveInfo, 0), Average, 0.5)
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
	received := metadataToBytes([]ArchiveInfo{{1, 300}, {60, 30}, {300, 12}}, Average, 0.5)
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
	received := archiveInfoToBytes(archiveOffset, ArchiveInfo{1, 300})
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
	readHeader(file)
	tearDown()
}

func testAggregate(t *testing.T, method AggregationMethod, expected float64) {
	received := Aggregate(method, []int{1, 2, 3, 5, 4})
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
