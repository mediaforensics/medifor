// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.26.0
// 	protoc        v3.6.1
// source: medifor/v1/kill.proto

package mediforproto

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

var File_medifor_v1_kill_proto protoreflect.FileDescriptor

var file_medifor_v1_kill_proto_rawDesc = []byte{
	0x0a, 0x15, 0x6d, 0x65, 0x64, 0x69, 0x66, 0x6f, 0x72, 0x2f, 0x76, 0x31, 0x2f, 0x6b, 0x69, 0x6c,
	0x6c, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x0c, 0x6d, 0x65, 0x64, 0x69, 0x66, 0x6f, 0x72,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x19, 0x6d, 0x65, 0x64, 0x69, 0x66, 0x6f, 0x72, 0x2f, 0x76,
	0x31, 0x2f, 0x61, 0x6e, 0x61, 0x6c, 0x79, 0x74, 0x69, 0x63, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x32, 0x3c, 0x0a, 0x08, 0x4b, 0x69, 0x6c, 0x6c, 0x61, 0x62, 0x6c, 0x65, 0x12, 0x30, 0x0a, 0x04,
	0x4b, 0x69, 0x6c, 0x6c, 0x12, 0x13, 0x2e, 0x6d, 0x65, 0x64, 0x69, 0x66, 0x6f, 0x72, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x2e, 0x45, 0x6d, 0x70, 0x74, 0x79, 0x1a, 0x13, 0x2e, 0x6d, 0x65, 0x64, 0x69,
	0x66, 0x6f, 0x72, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x45, 0x6d, 0x70, 0x74, 0x79, 0x42, 0x59,
	0x0a, 0x18, 0x63, 0x6f, 0x6d, 0x2e, 0x6d, 0x65, 0x64, 0x69, 0x66, 0x6f, 0x72, 0x70, 0x72, 0x6f,
	0x67, 0x72, 0x61, 0x6d, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x42, 0x09, 0x4b, 0x69, 0x6c, 0x6c,
	0x50, 0x72, 0x6f, 0x74, 0x6f, 0x5a, 0x32, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f,
	0x6d, 0x2f, 0x6d, 0x65, 0x64, 0x69, 0x61, 0x66, 0x6f, 0x72, 0x65, 0x6e, 0x73, 0x69, 0x63, 0x73,
	0x2f, 0x6d, 0x65, 0x64, 0x69, 0x66, 0x6f, 0x72, 0x2f, 0x70, 0x6b, 0x67, 0x2f, 0x6d, 0x65, 0x64,
	0x69, 0x66, 0x6f, 0x72, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x33,
}

var file_medifor_v1_kill_proto_goTypes = []interface{}{
	(*Empty)(nil), // 0: mediforproto.Empty
}
var file_medifor_v1_kill_proto_depIdxs = []int32{
	0, // 0: mediforproto.Killable.Kill:input_type -> mediforproto.Empty
	0, // 1: mediforproto.Killable.Kill:output_type -> mediforproto.Empty
	1, // [1:2] is the sub-list for method output_type
	0, // [0:1] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_medifor_v1_kill_proto_init() }
func file_medifor_v1_kill_proto_init() {
	if File_medifor_v1_kill_proto != nil {
		return
	}
	file_medifor_v1_analytic_proto_init()
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_medifor_v1_kill_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   0,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_medifor_v1_kill_proto_goTypes,
		DependencyIndexes: file_medifor_v1_kill_proto_depIdxs,
	}.Build()
	File_medifor_v1_kill_proto = out.File
	file_medifor_v1_kill_proto_rawDesc = nil
	file_medifor_v1_kill_proto_goTypes = nil
	file_medifor_v1_kill_proto_depIdxs = nil
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConnInterface

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion6

// KillableClient is the client API for Killable service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type KillableClient interface {
	Kill(ctx context.Context, in *Empty, opts ...grpc.CallOption) (*Empty, error)
}

type killableClient struct {
	cc grpc.ClientConnInterface
}

func NewKillableClient(cc grpc.ClientConnInterface) KillableClient {
	return &killableClient{cc}
}

func (c *killableClient) Kill(ctx context.Context, in *Empty, opts ...grpc.CallOption) (*Empty, error) {
	out := new(Empty)
	err := c.cc.Invoke(ctx, "/mediforproto.Killable/Kill", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// KillableServer is the server API for Killable service.
type KillableServer interface {
	Kill(context.Context, *Empty) (*Empty, error)
}

// UnimplementedKillableServer can be embedded to have forward compatible implementations.
type UnimplementedKillableServer struct {
}

func (*UnimplementedKillableServer) Kill(context.Context, *Empty) (*Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Kill not implemented")
}

func RegisterKillableServer(s *grpc.Server, srv KillableServer) {
	s.RegisterService(&_Killable_serviceDesc, srv)
}

func _Killable_Kill_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(KillableServer).Kill(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/mediforproto.Killable/Kill",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(KillableServer).Kill(ctx, req.(*Empty))
	}
	return interceptor(ctx, in, info, handler)
}

var _Killable_serviceDesc = grpc.ServiceDesc{
	ServiceName: "mediforproto.Killable",
	HandlerType: (*KillableServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Kill",
			Handler:    _Killable_Kill_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "medifor/v1/kill.proto",
}
