// Package protoutil contains utilities for manipulation of MediFor protocol buffers.
package protoutil

import (
	"fmt"
	"log"
	"reflect"

	"github.com/golang/protobuf/proto"
	pb "gitlab.mediforprogram.com/medifor/medifor-proto/pkg/mediforproto"
)

// URIRewriter class to rewrite UR as proper resolved local or remote file paths
type URIRewriter func(src string) (string, error)

// FieldNotFound error thrown when field not found
var FieldNotFound = fmt.Errorf("field not found")

// reflectURIs is a recursive helper for RewriteURIs, operating on
// the reflected values instead of directly on an interface.
func reflectURIs(v reflect.Value, resourceType reflect.Type, rewriteURI URIRewriter) error {
	switch v.Kind() {
	case reflect.Ptr:
		if v.IsNil() {
			return nil
		}
		return reflectURIs(v.Elem(), resourceType, rewriteURI)
	case reflect.Array, reflect.Slice:
		for i := 0; i < v.Len(); i++ {
			if err := reflectURIs(v.Index(i), resourceType, rewriteURI); err != nil {
				return fmt.Errorf("reflect array/slice: %v", err)
			}
		}
		return nil
	case reflect.Map:
		for _, k := range v.MapKeys() {
			if err := reflectURIs(v.MapIndex(k), resourceType, rewriteURI); err != nil {
				return fmt.Errorf("reflect map: %v", err)
			}
		}
		return nil
	case reflect.Struct:
		if v.Type() == resourceType {
			val := v.FieldByName("Uri").String()
			if val == "" {
				log.Printf("Empty source in resource: %+v", v)
				return nil
			}
			rewritten, err := rewriteURI(v.FieldByName("Uri").String())
			if err != nil {
				return fmt.Errorf("reflect uri: %v", err)
			}
			v.FieldByName("Uri").SetString(rewritten)
			return nil
		}

		for i := 0; i < v.NumField(); i++ {
			if err := reflectURIs(v.Field(i), resourceType, rewriteURI); err != nil {
				return fmt.Errorf("reflect struct: %v", err)
			}
		}
		return nil
	default:
		return nil
	}
}

// MessageStringField gets the string value of a particular field in a message.
func MessageStringField(msg proto.Message, fieldName string) (string, error) {
	v := reflect.ValueOf(msg)
	fv := v.Elem().FieldByName(fieldName)
	if fv.Kind() != reflect.String {
		return "", FieldNotFound
	}
	return fv.String(), nil
}

// SetMessageStringField sets the value of a string field in a given message.
func SetMessageStringField(msg proto.Message, fieldName, value string) error {
	v := reflect.ValueOf(msg)
	fv := v.Elem().FieldByName(fieldName)
	if fv.Kind() != reflect.String {
		return FieldNotFound
	}
	fv.SetString(value)
	return nil
}

// RewriteURIs recursively looks through all fields of the given
// message, and attempts to rewrite URI fields in any Resource types it
// encounters, using the given URI rewrite function to apply the transformation.
func RewriteURIs(val interface{}, f URIRewriter) error {
	return reflectURIs(reflect.ValueOf(val), reflect.TypeOf(pb.Resource{}), f)
}

// RewriteOutDir looks for an OutDir in the top level of the message and rewrites it.
func RewriteOutDir(val proto.Message, f URIRewriter) error {
	out, err := MessageStringField(val, "OutDir")
	if err == FieldNotFound {
		return nil // It's okay if it isn't there.
	}
	if err != nil {
		return fmt.Errorf("get out dir: %v", err)
	}
	newOut, err := f(out)
	if err != nil {
		return fmt.Errorf("rewrite out: %v", err)
	}
	return SetMessageStringField(val, "OutDir", newOut)
}
