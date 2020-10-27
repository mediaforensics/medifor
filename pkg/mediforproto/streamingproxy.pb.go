// Code generated by protoc-gen-go. DO NOT EDIT.
// source: medifor/v1/streamingproxy.proto

package mediforproto

import (
	context "context"
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	math "math"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion3 // please upgrade the proto package

// FileChunk contains a single chunk of a file. It is intended to be part of a
// streaming protocol, so size and offset are implied
type FileChunk struct {
	// The name of the file. Should be sent with every chunk. Works best when every file sent is uniquely named.
	Name string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	// The (optional) MIME type of the file.
	MimeType string `protobuf:"bytes,2,opt,name=mime_type,json=mimeType,proto3" json:"mime_type,omitempty"`
	// This chunk's offset into the file specified by the name above.
	Offset int64 `protobuf:"varint,3,opt,name=offset,proto3" json:"offset,omitempty"`
	// The total size of the file.
	TotalBytes int64 `protobuf:"varint,4,opt,name=total_bytes,json=totalBytes,proto3" json:"total_bytes,omitempty"`
	// The value of this chunk.
	Value                []byte   `protobuf:"bytes,5,opt,name=value,proto3" json:"value,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *FileChunk) Reset()         { *m = FileChunk{} }
func (m *FileChunk) String() string { return proto.CompactTextString(m) }
func (*FileChunk) ProtoMessage()    {}
func (*FileChunk) Descriptor() ([]byte, []int) {
	return fileDescriptor_1c83d807c84235b2, []int{0}
}

func (m *FileChunk) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_FileChunk.Unmarshal(m, b)
}
func (m *FileChunk) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_FileChunk.Marshal(b, m, deterministic)
}
func (m *FileChunk) XXX_Merge(src proto.Message) {
	xxx_messageInfo_FileChunk.Merge(m, src)
}
func (m *FileChunk) XXX_Size() int {
	return xxx_messageInfo_FileChunk.Size(m)
}
func (m *FileChunk) XXX_DiscardUnknown() {
	xxx_messageInfo_FileChunk.DiscardUnknown(m)
}

var xxx_messageInfo_FileChunk proto.InternalMessageInfo

func (m *FileChunk) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

func (m *FileChunk) GetMimeType() string {
	if m != nil {
		return m.MimeType
	}
	return ""
}

func (m *FileChunk) GetOffset() int64 {
	if m != nil {
		return m.Offset
	}
	return 0
}

func (m *FileChunk) GetTotalBytes() int64 {
	if m != nil {
		return m.TotalBytes
	}
	return 0
}

func (m *FileChunk) GetValue() []byte {
	if m != nil {
		return m.Value
	}
	return nil
}

// DetectionChunk is part of the streaming detection protocol, where detection
// objects are sent (both as request and response) along with the files that go
// with them.
type DetectionChunk struct {
	// Types that are valid to be assigned to Value:
	//	*DetectionChunk_Detection
	//	*DetectionChunk_FileChunk
	Value                isDetectionChunk_Value `protobuf_oneof:"value"`
	XXX_NoUnkeyedLiteral struct{}               `json:"-"`
	XXX_unrecognized     []byte                 `json:"-"`
	XXX_sizecache        int32                  `json:"-"`
}

func (m *DetectionChunk) Reset()         { *m = DetectionChunk{} }
func (m *DetectionChunk) String() string { return proto.CompactTextString(m) }
func (*DetectionChunk) ProtoMessage()    {}
func (*DetectionChunk) Descriptor() ([]byte, []int) {
	return fileDescriptor_1c83d807c84235b2, []int{1}
}

func (m *DetectionChunk) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_DetectionChunk.Unmarshal(m, b)
}
func (m *DetectionChunk) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_DetectionChunk.Marshal(b, m, deterministic)
}
func (m *DetectionChunk) XXX_Merge(src proto.Message) {
	xxx_messageInfo_DetectionChunk.Merge(m, src)
}
func (m *DetectionChunk) XXX_Size() int {
	return xxx_messageInfo_DetectionChunk.Size(m)
}
func (m *DetectionChunk) XXX_DiscardUnknown() {
	xxx_messageInfo_DetectionChunk.DiscardUnknown(m)
}

var xxx_messageInfo_DetectionChunk proto.InternalMessageInfo

type isDetectionChunk_Value interface {
	isDetectionChunk_Value()
}

type DetectionChunk_Detection struct {
	Detection *Detection `protobuf:"bytes,1,opt,name=detection,proto3,oneof"`
}

type DetectionChunk_FileChunk struct {
	FileChunk *FileChunk `protobuf:"bytes,2,opt,name=file_chunk,json=fileChunk,proto3,oneof"`
}

func (*DetectionChunk_Detection) isDetectionChunk_Value() {}

func (*DetectionChunk_FileChunk) isDetectionChunk_Value() {}

func (m *DetectionChunk) GetValue() isDetectionChunk_Value {
	if m != nil {
		return m.Value
	}
	return nil
}

func (m *DetectionChunk) GetDetection() *Detection {
	if x, ok := m.GetValue().(*DetectionChunk_Detection); ok {
		return x.Detection
	}
	return nil
}

func (m *DetectionChunk) GetFileChunk() *FileChunk {
	if x, ok := m.GetValue().(*DetectionChunk_FileChunk); ok {
		return x.FileChunk
	}
	return nil
}

// XXX_OneofWrappers is for the internal use of the proto package.
func (*DetectionChunk) XXX_OneofWrappers() []interface{} {
	return []interface{}{
		(*DetectionChunk_Detection)(nil),
		(*DetectionChunk_FileChunk)(nil),
	}
}

func init() {
	proto.RegisterType((*FileChunk)(nil), "mediforproto.FileChunk")
	proto.RegisterType((*DetectionChunk)(nil), "mediforproto.DetectionChunk")
}

func init() { proto.RegisterFile("medifor/v1/streamingproxy.proto", fileDescriptor_1c83d807c84235b2) }

var fileDescriptor_1c83d807c84235b2 = []byte{
	// 285 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x7c, 0x50, 0xcd, 0x4e, 0x83, 0x40,
	0x10, 0x76, 0xed, 0x8f, 0x32, 0x25, 0x3d, 0x6c, 0x8c, 0x62, 0x35, 0x29, 0xe9, 0x89, 0x13, 0xd5,
	0x7a, 0xd0, 0x73, 0x35, 0xc6, 0x93, 0x31, 0xe8, 0x1d, 0xb7, 0x74, 0xd0, 0x8d, 0xc0, 0x12, 0x98,
	0x36, 0xf2, 0x0c, 0xfa, 0xd0, 0x86, 0x5d, 0xa8, 0x4d, 0x8c, 0xbd, 0xed, 0xf7, 0x37, 0xfb, 0xcd,
	0xc0, 0x38, 0xc5, 0xa5, 0x8c, 0x55, 0x31, 0x5d, 0x5f, 0x4e, 0x4b, 0x2a, 0x50, 0xa4, 0x32, 0x7b,
	0xcb, 0x0b, 0xf5, 0x59, 0xf9, 0x79, 0xa1, 0x48, 0x71, 0xbb, 0x31, 0x68, 0x34, 0x3a, 0xdd, 0xb2,
	0x8b, 0x4c, 0x24, 0x15, 0xc9, 0xc8, 0x18, 0x27, 0x5f, 0x0c, 0xac, 0x7b, 0x99, 0xe0, 0xed, 0xfb,
	0x2a, 0xfb, 0xe0, 0x1c, 0xba, 0x99, 0x48, 0xd1, 0x61, 0x2e, 0xf3, 0xac, 0x40, 0xbf, 0xf9, 0x19,
	0x58, 0xa9, 0x4c, 0x31, 0xa4, 0x2a, 0x47, 0x67, 0x5f, 0x0b, 0x87, 0x35, 0xf1, 0x52, 0xe5, 0xc8,
	0x8f, 0xa1, 0xaf, 0xe2, 0xb8, 0x44, 0x72, 0x3a, 0x2e, 0xf3, 0x3a, 0x41, 0x83, 0xf8, 0x18, 0x06,
	0xa4, 0x48, 0x24, 0xe1, 0xa2, 0x22, 0x2c, 0x9d, 0xae, 0x16, 0x41, 0x53, 0xf3, 0x9a, 0xe1, 0x47,
	0xd0, 0x5b, 0x8b, 0x64, 0x85, 0x4e, 0xcf, 0x65, 0x9e, 0x1d, 0x18, 0x30, 0xf9, 0x66, 0x30, 0xbc,
	0x43, 0xc2, 0x88, 0xa4, 0xca, 0x4c, 0xa5, 0x6b, 0xb0, 0x96, 0x2d, 0xa3, 0x7b, 0x0d, 0x66, 0x27,
	0xfe, 0xf6, 0x76, 0xfe, 0x26, 0xf0, 0xb0, 0x17, 0xfc, 0x7a, 0xf9, 0x0d, 0x40, 0x2c, 0x13, 0x0c,
	0xa3, 0x7a, 0x8c, 0x2e, 0xfe, 0x27, 0xb9, 0x59, 0xbc, 0x4e, 0xc6, 0x2d, 0x98, 0x1f, 0x34, 0xdd,
	0x66, 0xaf, 0x30, 0x7c, 0x6e, 0xaf, 0xfb, 0x54, 0x5f, 0x97, 0x3f, 0x82, 0x6d, 0xbe, 0x33, 0x3c,
	0x3f, 0xff, 0xa7, 0x8a, 0x1e, 0x34, 0xda, 0xa9, 0x7a, 0xec, 0x82, 0x2d, 0xfa, 0x5a, 0xb9, 0xfa,
	0x09, 0x00, 0x00, 0xff, 0xff, 0xea, 0x5d, 0xc7, 0x32, 0xd1, 0x01, 0x00, 0x00,
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// StreamingProxyClient is the client API for StreamingProxy service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type StreamingProxyClient interface {
	DetectStream(ctx context.Context, opts ...grpc.CallOption) (StreamingProxy_DetectStreamClient, error)
}

type streamingProxyClient struct {
	cc *grpc.ClientConn
}

func NewStreamingProxyClient(cc *grpc.ClientConn) StreamingProxyClient {
	return &streamingProxyClient{cc}
}

func (c *streamingProxyClient) DetectStream(ctx context.Context, opts ...grpc.CallOption) (StreamingProxy_DetectStreamClient, error) {
	stream, err := c.cc.NewStream(ctx, &_StreamingProxy_serviceDesc.Streams[0], "/mediforproto.StreamingProxy/DetectStream", opts...)
	if err != nil {
		return nil, err
	}
	x := &streamingProxyDetectStreamClient{stream}
	return x, nil
}

type StreamingProxy_DetectStreamClient interface {
	Send(*DetectionChunk) error
	Recv() (*DetectionChunk, error)
	grpc.ClientStream
}

type streamingProxyDetectStreamClient struct {
	grpc.ClientStream
}

func (x *streamingProxyDetectStreamClient) Send(m *DetectionChunk) error {
	return x.ClientStream.SendMsg(m)
}

func (x *streamingProxyDetectStreamClient) Recv() (*DetectionChunk, error) {
	m := new(DetectionChunk)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// StreamingProxyServer is the server API for StreamingProxy service.
type StreamingProxyServer interface {
	DetectStream(StreamingProxy_DetectStreamServer) error
}

// UnimplementedStreamingProxyServer can be embedded to have forward compatible implementations.
type UnimplementedStreamingProxyServer struct {
}

func (*UnimplementedStreamingProxyServer) DetectStream(srv StreamingProxy_DetectStreamServer) error {
	return status.Errorf(codes.Unimplemented, "method DetectStream not implemented")
}

func RegisterStreamingProxyServer(s *grpc.Server, srv StreamingProxyServer) {
	s.RegisterService(&_StreamingProxy_serviceDesc, srv)
}

func _StreamingProxy_DetectStream_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(StreamingProxyServer).DetectStream(&streamingProxyDetectStreamServer{stream})
}

type StreamingProxy_DetectStreamServer interface {
	Send(*DetectionChunk) error
	Recv() (*DetectionChunk, error)
	grpc.ServerStream
}

type streamingProxyDetectStreamServer struct {
	grpc.ServerStream
}

func (x *streamingProxyDetectStreamServer) Send(m *DetectionChunk) error {
	return x.ServerStream.SendMsg(m)
}

func (x *streamingProxyDetectStreamServer) Recv() (*DetectionChunk, error) {
	m := new(DetectionChunk)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

var _StreamingProxy_serviceDesc = grpc.ServiceDesc{
	ServiceName: "mediforproto.StreamingProxy",
	HandlerType: (*StreamingProxyServer)(nil),
	Methods:     []grpc.MethodDesc{},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "DetectStream",
			Handler:       _StreamingProxy_DetectStream_Handler,
			ServerStreams: true,
			ClientStreams: true,
		},
	},
	Metadata: "medifor/v1/streamingproxy.proto",
}
