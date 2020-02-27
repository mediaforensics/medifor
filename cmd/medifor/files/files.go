// Package files contains file format parsing and production routines for the medifor command.
package files

import (
	"fmt"
	"log"
	"strconv"
	"strings"

	"gitlab.mediforprogram.com/medifor/medifor-proto/pkg/fileutil"
)

// DetectionType holds the kind of thing we're doing, e.g., image manipulation.
type DetectionType string

// Detection types, split out by endpoint.
const (
	ImageManipulation DetectionType = "imgmanip"
	VideoManipulation DetectionType = "vidmanip"
	ImageSplice       DetectionType = "imgsplice"
	ImageCameraMatch  DetectionType = "imgcameramatch"
)

func manipType(fname string) DetectionType {
	if fileutil.IsVideo(fname) {
		return VideoManipulation
	}
	return ImageManipulation
}

// NISTRowType finds the detection type from the information in a CSV row (not a header).
func NISTRowType(row map[string]string) (DetectionType, error) {
	// TODO Check row size and return error instead of panicing
	switch id, fname := strings.ToLower(row["TaskID"]), row["ProbeFileName"]; id {
	case "manipulation":
		return manipType(fname), nil
	case "splice":
		return ImageSplice, nil
	case "cameraverification":
		return ImageCameraMatch, nil
	default:
		return "", fmt.Errorf("unrecognized TaskID: %q", id)
	}
}

// IsNISTVideoManipulationRow indicates whether the given row represents a video manipulation.
func IsNISTVideoManipulationRow(row map[string]string) bool {
	recType, err := NISTRowType(row)
	return err == nil && recType == VideoManipulation
}

// NISTManipulation holds information from a row representing an image or video manipulation.
type NISTManipulation struct {
	TaskID        string
	ProbeFileID   string
	ProbeFileName string
	ProbeWidth    int
	ProbeHeight   int
	HPDeviceID    string
	HPSensorID    string
}

// NISTCameraVerification holds information from a row representing a camera verification task.
type NISTCameraVerification struct {
	TaskID        string
	ProbeFileID   string
	ProbeFileName string
	ProbeWidth    int
	ProbeHeight   int
	ProbeFileSize int
	TrainCamID    string
}

// NISTRowToCameraVerification converts a CSV index row into a camera verification struct.
func NISTRowToCameraVerification(row map[string]string) *NISTCameraVerification {
	return &NISTCameraVerification{
		TaskID:        colStr(row, "TaskID"),
		ProbeFileID:   colStr(row, "ProbeFileID"),
		ProbeFileName: colStr(row, "ProbeFileName"),
		ProbeWidth:    colInt(row, "ProbeWidth"),
		ProbeHeight:   colInt(row, "ProbeHeight"),
		ProbeFileSize: colInt(row, "ProbeFileSize"),
		TrainCamID:    colStr(row, "TrainCamID"),
	}
}

// NISTRowToManipulation converts a CSV index row into a manipulation struct.
func NISTRowToManipulation(row map[string]string) *NISTManipulation {
	return &NISTManipulation{
		TaskID:        colStr(row, "TaskID"),
		ProbeFileID:   colStr(row, "ProbeFileID"),
		ProbeFileName: colStr(row, "ProbeFileName"),
		ProbeWidth:    colInt(row, "ProbeWidth"),
		ProbeHeight:   colInt(row, "ProbeHeight"),
		HPDeviceID:    colStr(row, "HPDeviceID"),
		HPSensorID:    colStr(row, "HPSensorID"),
	}
}

// DetectionType returns the detection type for this manipulation.
func (m *NISTManipulation) DetectionType() DetectionType {
	return manipType(m.ProbeFileName)
}

// DeviceID returns the HPDeviceID, but favors HPSensorID if present.
func (m *NISTManipulation) DeviceID() string {
	if m.HPSensorID != "" {
		return m.HPSensorID
	}
	return m.HPDeviceID
}

// MIMETypeByExtension returns the mime type by extension for this file, or octet-stream.
func (m *NISTManipulation) MIMETypeByExtension() string {
	return fileutil.MimeTypeOrGeneric(m.ProbeFileName)
}

// NISTSplice holds information from a row representing an image splice.
type NISTSplice struct {
	TaskID        string
	ProbeFileID   string
	ProbeFileName string
	ProbeWidth    int
	ProbeHeight   int
	DonorFileID   string
	DonorFileName string
	DonorWidth    int
	DonorHeight   int
}

// NISTRowToSplice converts a CSV index row into a manipulation struct.
func NISTRowToSplice(row map[string]string) *NISTSplice {
	return &NISTSplice{
		TaskID:        colStr(row, "TaskID"),
		ProbeFileID:   colStr(row, "ProbeFileID"),
		ProbeFileName: colStr(row, "ProbeFileName"),
		ProbeWidth:    colInt(row, "ProbeWidth"),
		ProbeHeight:   colInt(row, "ProbeHeight"),
		DonorFileID:   colStr(row, "DonorFileID"),
		DonorFileName: colStr(row, "DonorFileName"),
		DonorWidth:    colInt(row, "DonorWidth"),
		DonorHeight:   colInt(row, "DonorHeight"),
	}
}

// MIMETypesByExtension returns the probe and donor mime types, respectively.
func (m *NISTSplice) MIMETypesByExtension() (probeType, donorType string) {
	return fileutil.MimeTypeOrGeneric(m.ProbeFileName), fileutil.MimeTypeOrGeneric(m.DonorFileName)
}

func colInt(row map[string]string, c string) int {
	v, ok := row[c]
	if !ok {
		return -1
	}
	val, err := strconv.Atoi(v)
	if err != nil {
		log.Printf("Error converting %q to int: %v", v, err)
		return -1
	}
	return val
}

func colStr(row map[string]string, c string) string {
	return row[c]
}
