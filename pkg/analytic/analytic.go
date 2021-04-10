// Package analytic holds basic functions for working with analytics.
package analytic

import (
	"encoding/json"
	"fmt"
	"net/url"
	"reflect"
	"sort"

	"github.com/golang/protobuf/jsonpb"
	"github.com/mediaforensics/medifor/pkg/medifor"
	pb "github.com/mediaforensics/medifor/pkg/mediforproto"
	"github.com/pkg/errors"
)

// AnalyticInboxes returns a slice of inbox queue names for the given formats and analytic ID.
func AnalyticInboxes(formats []string, analyticID string) []string {
	seen := make(map[string]bool)
	for _, f := range formats {
		seen[AnalyticInbox(f, analyticID)] = true
	}
	var names []string
	for name := range seen {
		names = append(names, name)
	}
	return names
}

// AnalyticInbox returns the inbox name for a given analytic ID.
func AnalyticInbox(format, analyticID string) string {
	return fmt.Sprintf(format, url.PathEscape(analyticID))
}

// ResourceHandler is called when a Resource type is found in a proto.
type ResourceHandler func(r *pb.Resource)

// reflectResources recursively descends into the given proto, looking for
// Resource types, calling a given function when it finds one.
func reflectResources(v reflect.Value, handler ResourceHandler, seenPtrs map[uintptr]bool) error {
	resourceType := reflect.TypeOf(pb.Resource{})

	switch v.Kind() {
	case reflect.Interface:
		if v.IsNil() {
			return nil
		}
		return errors.Wrap(reflectResources(v.Elem(), handler, seenPtrs), "reflect interface")
	case reflect.Ptr:
		if v.IsNil() {
			return nil
		}
		if seenPtrs[v.Pointer()] {
			return nil
		}
		seenPtrs[v.Pointer()] = true
		return errors.Wrap(reflectResources(v.Elem(), handler, seenPtrs), "reflect pointer")
	case reflect.Array, reflect.Slice:
		for i := 0; i < v.Len(); i++ {
			if err := reflectResources(v.Index(i), handler, seenPtrs); err != nil {
				return errors.Wrap(err, "reflect array/slice")
			}
		}
		return nil
	case reflect.Struct:
		if v.Type() == resourceType {
			res := v.Interface().(pb.Resource)
			handler(&res)
			return nil
		}
		for i := 0; i < v.NumField(); i++ {
			if err := reflectResources(v.Field(i), handler, seenPtrs); err != nil {
				return errors.Wrap(err, "reflect struct")
			}
		}
		return nil
	default:
		return nil
	}
}

// FindDetectionRequestResources finds all Resource types in a detection proto's request field.
func FindDetectionRequestResources(det *pb.Detection) ([]*pb.Resource, error) {
	return findResourcesImpl(det.GetRequest())
}

// FindDetectionResources finds all Resource types in a detection proto.
func FindDetectionResources(det *pb.Detection) ([]*pb.Resource, error) {
	return findResourcesImpl(det)
}

// findResourcesImpl finds all Resource types in a proto and calls the given handler for each.
func findResourcesImpl(val interface{}) ([]*pb.Resource, error) {
	var resources []*pb.Resource
	handler := func(r *pb.Resource) {
		resources = append(resources, r)
	}
	seen := make(map[uintptr]bool)
	if err := reflectResources(reflect.ValueOf(val), handler, seen); err != nil {
		return nil, errors.Wrap(err, "search for Resource elements in proto")
	}
	return resources, nil
}

// FusionOutDir gets the output directory from the fusion request
func FusionOutDir(fus *pb.Fusion) (string, error) {
	switch fus.GetRequest().(type) {
	case *pb.Fusion_ImgManipReq:
		return fus.GetImgManipReq().GetOutDir(), nil
	case *pb.Fusion_VidManipReq:
		return fus.GetVidManipReq().GetOutDir(), nil
	case *pb.Fusion_ImgSpliceReq:
		return fus.GetImgSpliceReq().GetOutDir(), nil
	case *pb.Fusion_ImgCamMatchReq:
		return fus.GetImgCamMatchReq().GetOutDir(), nil
	default:
		return "", errors.Errorf("unknown request type: %T", fus.GetRequest())
	}
}

// SetFusionOutDir
func SetFusionOutDir(fus *pb.Fusion, dir string) error {
	switch fus.GetRequest().(type) {
	case *pb.Fusion_ImgManipReq:
		fus.GetImgManipReq().OutDir = dir
	case *pb.Fusion_VidManipReq:
		fus.GetVidManipReq().OutDir = dir
	case *pb.Fusion_ImgSpliceReq:
		fus.GetImgSpliceReq().OutDir = dir
	case *pb.Fusion_ImgCamMatchReq:
		fus.GetImgCamMatchReq().OutDir = dir
	default:
		return errors.Errorf("unknown request type: %T", fus.GetRequest())
	}
	return nil
}

// FusionFromDetections takes a slice of AnnotatedDetection of the same type
// (i.e. imfManip, vidManip, imgSplice, etc.) and returns a Fusion object.
func FusionFromDetections(ds []*pb.AnnotatedDetection, outDir string) (fusion *pb.Fusion, err error) {
	if len(ds) == 0 {
		return nil, errors.New("no detections given for fusion")
	}

	fusion = new(pb.Fusion)

	firstDet := ds[0].GetDetection()
	switch val := firstDet.GetRequest().(type) {
	case *pb.Detection_ImgManipReq:
		fusion.Request = &pb.Fusion_ImgManipReq{
			ImgManipReq: &pb.FuseImageManipulationRequest{
				ImgManipReq: firstDet.GetImgManipReq(),
			},
		}
		freq := fusion.GetImgManipReq()
		freq.OutDir = outDir
		for _, d := range ds {
			// TODO: check that none of the requests differ materially from the first one.
			id, ver, err := medifor.IDVersionFromDetection(d)
			// If using combined ID_Version format
			// id = fmt.Sprintf("%v_%v", id, ver)
			if err != nil {
				return nil, errors.Wrap(err, "detection id/version")
			}
			freq.ImgManip = append(freq.ImgManip, &pb.AnnotatedImageManipulation{
				Id:      id,
				Version: ver,
				Data:    d.GetDetection().GetImgManip(),
			})
		}
		//TODO Splice and Video
	case *pb.Detection_VidManipReq:
		fusion.Request = &pb.Fusion_VidManipReq{
			VidManipReq: &pb.FuseVideoManipulationRequest{
				VidManipReq: firstDet.GetVidManipReq(),
			},
		}
		freq := fusion.GetVidManipReq()
		freq.OutDir = outDir
		for _, d := range ds {
			id, ver, err := medifor.IDVersionFromDetection(d)
			if err != nil {
				return nil, errors.Wrap(err, "detection id/version for vid fusion")
			}
			freq.VidManip = append(freq.VidManip, &pb.AnnotatedVideoManipulation{
				Id:      id,
				Version: ver,
				Data:    d.GetDetection().GetVidManip(),
			})
		}
	default:
		return nil, errors.Errorf("unknown fusion request type %T", val)
	}

	return fusion, nil
}

// OutDir tries to find the output directory in a detection's request.
func OutDir(det *pb.Detection) (string, error) {
	switch det.GetRequest().(type) {
	case *pb.Detection_ImgManipReq:
		return det.GetImgManipReq().GetOutDir(), nil
	case *pb.Detection_VidManipReq:
		return det.GetVidManipReq().GetOutDir(), nil
	case *pb.Detection_ImgSpliceReq:
		return det.GetImgSpliceReq().GetOutDir(), nil
	case *pb.Detection_ImgCamMatchReq:
		return det.GetImgCamMatchReq().GetOutDir(), nil
	default:
		return "", errors.Errorf("unknown request type: %T", det.GetRequest())
	}
}

// SetOutDir attempts to set the output directory in a detection's request.
func SetOutDir(det *pb.Detection, dir string) error {
	switch det.GetRequest().(type) {
	case *pb.Detection_ImgManipReq:
		det.GetImgManipReq().OutDir = dir
	case *pb.Detection_VidManipReq:
		det.GetVidManipReq().OutDir = dir
	case *pb.Detection_ImgSpliceReq:
		det.GetImgSpliceReq().OutDir = dir
	case *pb.Detection_ImgCamMatchReq:
		det.GetImgCamMatchReq().OutDir = dir
	default:
		return errors.Errorf("unknown request type: %T", det.GetRequest())
	}
	return nil
}

// UnmarshalDetectionJSON is a silly workaround for unmarshaling JSON where
// some oneof fields are nil (even though the type is inferable).
func UnmarshalDetectionJSON(s string) (*pb.Detection, error) {
	// Unmarshal to a nested map, find missing response element and set it to empty instead of null.
	val := make(map[string]interface{})
	if err := json.Unmarshal([]byte(s), &val); err != nil {
		return nil, errors.Wrap(err, "unmarshal detection JSON (unmarshal)")
	}
	switch {
	case val["imgManipReq"] != nil && val["imgManip"] == nil:
		val["imgManip"] = make(map[string]interface{})
	case val["vidManipReq"] != nil && val["vidManip"] == nil:
		val["vidManip"] = make(map[string]interface{})
	case val["imgSpliceReq"] != nil && val["imgSplice"] == nil:
		val["imgSplice"] = make(map[string]interface{})
	case val["imgMetaReq"] != nil && val["imgMetaReq"] == nil:
		val["imgMeta"] = make(map[string]interface{})
	}

	b, err := json.Marshal(val)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshal detection json (marshal)")
	}
	det := new(pb.Detection)
	if err := jsonpb.UnmarshalString(string(b), det); err != nil {
		return nil, errors.Wrap(err, "unmarshal detection json")
	}
	return det, nil
}

func UnmarshalFusionJSON(s string) (*pb.Fusion, error) {
	// Unmarshal to a nested map, find missing response element and set it to empty instead of null.
	val := make(map[string]interface{})
	if err := json.Unmarshal([]byte(s), &val); err != nil {
		return nil, errors.Wrap(err, "unmarshal fusion JSON (unmarshal)")
	}
	switch {
	case val["imgManipReq"] != nil && val["imgManip"] == nil:
		val["imgManip"] = make(map[string]interface{})
	case val["vidManipReq"] != nil && val["vidManip"] == nil:
		val["vidManip"] = make(map[string]interface{})
	case val["imgSpliceReq"] != nil && val["imgSplice"] == nil:
		val["imgSplice"] = make(map[string]interface{})
	case val["imgMetaReq"] != nil && val["imgMetaReq"] == nil:
		val["imgMeta"] = make(map[string]interface{})
	}

	b, err := json.Marshal(val)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshal fusion json (marshal)")
	}
	fus := new(pb.Fusion)
	if err := jsonpb.UnmarshalString(string(b), fus); err != nil {
		return nil, errors.Wrap(err, "unmarshal fusion json")
	}
	return fus, nil
}

// AnalyticIDsFromFusion gets all the analytic IDs which produced the analytic
// output contained in the fusion proto.
func AnalyticIDsFromFusion(fusion *pb.Fusion) ([]string, error) {
	var analyticIDs []string
	switch val := fusion.GetRequest().(type) {
	case *pb.Fusion_ImgManipReq:
		fReq := fusion.GetImgManipReq()
		for _, imgManip := range fReq.ImgManip {
			analyticIDs = append(analyticIDs, imgManip.Id)
		}
	case *pb.Fusion_VidManipReq:
		fReq := fusion.GetVidManipReq()
		for _, vidManip := range fReq.VidManip {
			analyticIDs = append(analyticIDs, vidManip.Id)
		}
	default:
		return nil, errors.Errorf("unknown fusion request type %T", val)
	}
	sort.Strings(analyticIDs)
	return analyticIDs, nil
}
