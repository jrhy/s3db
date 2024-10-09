// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.35.1
// 	protoc        (unknown)
// source: v1/node.proto

package v1

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	_ "google.golang.org/protobuf/types/known/anypb"
	durationpb "google.golang.org/protobuf/types/known/durationpb"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type Type int32

const (
	Type_NULL Type = 0
	Type_INT  Type = 1
	Type_REAL Type = 2
	Type_TEXT Type = 3
	Type_BLOB Type = 4
)

// Enum value maps for Type.
var (
	Type_name = map[int32]string{
		0: "NULL",
		1: "INT",
		2: "REAL",
		3: "TEXT",
		4: "BLOB",
	}
	Type_value = map[string]int32{
		"NULL": 0,
		"INT":  1,
		"REAL": 2,
		"TEXT": 3,
		"BLOB": 4,
	}
)

func (x Type) Enum() *Type {
	p := new(Type)
	*p = x
	return p
}

func (x Type) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (Type) Descriptor() protoreflect.EnumDescriptor {
	return file_v1_node_proto_enumTypes[0].Descriptor()
}

func (Type) Type() protoreflect.EnumType {
	return &file_v1_node_proto_enumTypes[0]
}

func (x Type) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use Type.Descriptor instead.
func (Type) EnumDescriptor() ([]byte, []int) {
	return file_v1_node_proto_rawDescGZIP(), []int{0}
}

type Node struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Key   []*SQLiteValue `protobuf:"bytes,1,rep,name=key,proto3" json:"key,omitempty"`
	Value []*CRDTValue   `protobuf:"bytes,2,rep,name=value,proto3" json:"value,omitempty"`
	Link  []string       `protobuf:"bytes,3,rep,name=link,proto3" json:"link,omitempty"`
}

func (x *Node) Reset() {
	*x = Node{}
	mi := &file_v1_node_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Node) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Node) ProtoMessage() {}

func (x *Node) ProtoReflect() protoreflect.Message {
	mi := &file_v1_node_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Node.ProtoReflect.Descriptor instead.
func (*Node) Descriptor() ([]byte, []int) {
	return file_v1_node_proto_rawDescGZIP(), []int{0}
}

func (x *Node) GetKey() []*SQLiteValue {
	if x != nil {
		return x.Key
	}
	return nil
}

func (x *Node) GetValue() []*CRDTValue {
	if x != nil {
		return x.Value
	}
	return nil
}

func (x *Node) GetLink() []string {
	if x != nil {
		return x.Link
	}
	return nil
}

type SQLiteValue struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Type Type    `protobuf:"varint,1,opt,name=Type,proto3,enum=jrhy.s3db.v1.Type" json:"Type,omitempty"`
	Int  int64   `protobuf:"varint,2,opt,name=Int,proto3" json:"Int,omitempty"`
	Real float64 `protobuf:"fixed64,3,opt,name=Real,proto3" json:"Real,omitempty"`
	Text string  `protobuf:"bytes,4,opt,name=Text,proto3" json:"Text,omitempty"`
	Blob []byte  `protobuf:"bytes,5,opt,name=Blob,proto3" json:"Blob,omitempty"`
}

func (x *SQLiteValue) Reset() {
	*x = SQLiteValue{}
	mi := &file_v1_node_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *SQLiteValue) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SQLiteValue) ProtoMessage() {}

func (x *SQLiteValue) ProtoReflect() protoreflect.Message {
	mi := &file_v1_node_proto_msgTypes[1]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SQLiteValue.ProtoReflect.Descriptor instead.
func (*SQLiteValue) Descriptor() ([]byte, []int) {
	return file_v1_node_proto_rawDescGZIP(), []int{1}
}

func (x *SQLiteValue) GetType() Type {
	if x != nil {
		return x.Type
	}
	return Type_NULL
}

func (x *SQLiteValue) GetInt() int64 {
	if x != nil {
		return x.Int
	}
	return 0
}

func (x *SQLiteValue) GetReal() float64 {
	if x != nil {
		return x.Real
	}
	return 0
}

func (x *SQLiteValue) GetText() string {
	if x != nil {
		return x.Text
	}
	return ""
}

func (x *SQLiteValue) GetBlob() []byte {
	if x != nil {
		return x.Blob
	}
	return nil
}

type CRDTValue struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	ModEpochNanos            int64  `protobuf:"varint,1,opt,name=ModEpochNanos,proto3" json:"ModEpochNanos,omitempty"`
	PreviousRoot             string `protobuf:"bytes,2,opt,name=PreviousRoot,proto3" json:"PreviousRoot,omitempty"`
	TombstoneSinceEpochNanos int64  `protobuf:"varint,3,opt,name=TombstoneSinceEpochNanos,proto3" json:"TombstoneSinceEpochNanos,omitempty"`
	Value                    *Row   `protobuf:"bytes,4,opt,name=Value,proto3" json:"Value,omitempty"`
}

func (x *CRDTValue) Reset() {
	*x = CRDTValue{}
	mi := &file_v1_node_proto_msgTypes[2]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *CRDTValue) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CRDTValue) ProtoMessage() {}

func (x *CRDTValue) ProtoReflect() protoreflect.Message {
	mi := &file_v1_node_proto_msgTypes[2]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use CRDTValue.ProtoReflect.Descriptor instead.
func (*CRDTValue) Descriptor() ([]byte, []int) {
	return file_v1_node_proto_rawDescGZIP(), []int{2}
}

func (x *CRDTValue) GetModEpochNanos() int64 {
	if x != nil {
		return x.ModEpochNanos
	}
	return 0
}

func (x *CRDTValue) GetPreviousRoot() string {
	if x != nil {
		return x.PreviousRoot
	}
	return ""
}

func (x *CRDTValue) GetTombstoneSinceEpochNanos() int64 {
	if x != nil {
		return x.TombstoneSinceEpochNanos
	}
	return 0
}

func (x *CRDTValue) GetValue() *Row {
	if x != nil {
		return x.Value
	}
	return nil
}

type Row struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	ColumnValues       map[string]*ColumnValue `protobuf:"bytes,1,rep,name=ColumnValues,proto3" json:"ColumnValues,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
	Deleted            bool                    `protobuf:"varint,2,opt,name=Deleted,proto3" json:"Deleted,omitempty"`
	DeleteUpdateOffset *durationpb.Duration    `protobuf:"bytes,3,opt,name=DeleteUpdateOffset,proto3" json:"DeleteUpdateOffset,omitempty"`
}

func (x *Row) Reset() {
	*x = Row{}
	mi := &file_v1_node_proto_msgTypes[3]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Row) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Row) ProtoMessage() {}

func (x *Row) ProtoReflect() protoreflect.Message {
	mi := &file_v1_node_proto_msgTypes[3]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Row.ProtoReflect.Descriptor instead.
func (*Row) Descriptor() ([]byte, []int) {
	return file_v1_node_proto_rawDescGZIP(), []int{3}
}

func (x *Row) GetColumnValues() map[string]*ColumnValue {
	if x != nil {
		return x.ColumnValues
	}
	return nil
}

func (x *Row) GetDeleted() bool {
	if x != nil {
		return x.Deleted
	}
	return false
}

func (x *Row) GetDeleteUpdateOffset() *durationpb.Duration {
	if x != nil {
		return x.DeleteUpdateOffset
	}
	return nil
}

type ColumnValue struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	UpdateOffset *durationpb.Duration `protobuf:"bytes,1,opt,name=UpdateOffset,proto3" json:"UpdateOffset,omitempty"`
	Value        *SQLiteValue         `protobuf:"bytes,2,opt,name=Value,proto3" json:"Value,omitempty"`
}

func (x *ColumnValue) Reset() {
	*x = ColumnValue{}
	mi := &file_v1_node_proto_msgTypes[4]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ColumnValue) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ColumnValue) ProtoMessage() {}

func (x *ColumnValue) ProtoReflect() protoreflect.Message {
	mi := &file_v1_node_proto_msgTypes[4]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ColumnValue.ProtoReflect.Descriptor instead.
func (*ColumnValue) Descriptor() ([]byte, []int) {
	return file_v1_node_proto_rawDescGZIP(), []int{4}
}

func (x *ColumnValue) GetUpdateOffset() *durationpb.Duration {
	if x != nil {
		return x.UpdateOffset
	}
	return nil
}

func (x *ColumnValue) GetValue() *SQLiteValue {
	if x != nil {
		return x.Value
	}
	return nil
}

var File_v1_node_proto protoreflect.FileDescriptor

var file_v1_node_proto_rawDesc = []byte{
	0x0a, 0x0d, 0x76, 0x31, 0x2f, 0x6e, 0x6f, 0x64, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12,
	0x0c, 0x6a, 0x72, 0x68, 0x79, 0x2e, 0x73, 0x33, 0x64, 0x62, 0x2e, 0x76, 0x31, 0x1a, 0x19, 0x67,
	0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2f, 0x61,
	0x6e, 0x79, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x1e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65,
	0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2f, 0x64, 0x75, 0x72, 0x61, 0x74, 0x69,
	0x6f, 0x6e, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0x76, 0x0a, 0x04, 0x4e, 0x6f, 0x64, 0x65,
	0x12, 0x2b, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x19, 0x2e,
	0x6a, 0x72, 0x68, 0x79, 0x2e, 0x73, 0x33, 0x64, 0x62, 0x2e, 0x76, 0x31, 0x2e, 0x53, 0x51, 0x4c,
	0x69, 0x74, 0x65, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x2d, 0x0a,
	0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x02, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x17, 0x2e, 0x6a,
	0x72, 0x68, 0x79, 0x2e, 0x73, 0x33, 0x64, 0x62, 0x2e, 0x76, 0x31, 0x2e, 0x43, 0x52, 0x44, 0x54,
	0x56, 0x61, 0x6c, 0x75, 0x65, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x12, 0x12, 0x0a, 0x04,
	0x6c, 0x69, 0x6e, 0x6b, 0x18, 0x03, 0x20, 0x03, 0x28, 0x09, 0x52, 0x04, 0x6c, 0x69, 0x6e, 0x6b,
	0x22, 0x83, 0x01, 0x0a, 0x0b, 0x53, 0x51, 0x4c, 0x69, 0x74, 0x65, 0x56, 0x61, 0x6c, 0x75, 0x65,
	0x12, 0x26, 0x0a, 0x04, 0x54, 0x79, 0x70, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x12,
	0x2e, 0x6a, 0x72, 0x68, 0x79, 0x2e, 0x73, 0x33, 0x64, 0x62, 0x2e, 0x76, 0x31, 0x2e, 0x54, 0x79,
	0x70, 0x65, 0x52, 0x04, 0x54, 0x79, 0x70, 0x65, 0x12, 0x10, 0x0a, 0x03, 0x49, 0x6e, 0x74, 0x18,
	0x02, 0x20, 0x01, 0x28, 0x03, 0x52, 0x03, 0x49, 0x6e, 0x74, 0x12, 0x12, 0x0a, 0x04, 0x52, 0x65,
	0x61, 0x6c, 0x18, 0x03, 0x20, 0x01, 0x28, 0x01, 0x52, 0x04, 0x52, 0x65, 0x61, 0x6c, 0x12, 0x12,
	0x0a, 0x04, 0x54, 0x65, 0x78, 0x74, 0x18, 0x04, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x54, 0x65,
	0x78, 0x74, 0x12, 0x12, 0x0a, 0x04, 0x42, 0x6c, 0x6f, 0x62, 0x18, 0x05, 0x20, 0x01, 0x28, 0x0c,
	0x52, 0x04, 0x42, 0x6c, 0x6f, 0x62, 0x22, 0xba, 0x01, 0x0a, 0x09, 0x43, 0x52, 0x44, 0x54, 0x56,
	0x61, 0x6c, 0x75, 0x65, 0x12, 0x24, 0x0a, 0x0d, 0x4d, 0x6f, 0x64, 0x45, 0x70, 0x6f, 0x63, 0x68,
	0x4e, 0x61, 0x6e, 0x6f, 0x73, 0x18, 0x01, 0x20, 0x01, 0x28, 0x03, 0x52, 0x0d, 0x4d, 0x6f, 0x64,
	0x45, 0x70, 0x6f, 0x63, 0x68, 0x4e, 0x61, 0x6e, 0x6f, 0x73, 0x12, 0x22, 0x0a, 0x0c, 0x50, 0x72,
	0x65, 0x76, 0x69, 0x6f, 0x75, 0x73, 0x52, 0x6f, 0x6f, 0x74, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09,
	0x52, 0x0c, 0x50, 0x72, 0x65, 0x76, 0x69, 0x6f, 0x75, 0x73, 0x52, 0x6f, 0x6f, 0x74, 0x12, 0x3a,
	0x0a, 0x18, 0x54, 0x6f, 0x6d, 0x62, 0x73, 0x74, 0x6f, 0x6e, 0x65, 0x53, 0x69, 0x6e, 0x63, 0x65,
	0x45, 0x70, 0x6f, 0x63, 0x68, 0x4e, 0x61, 0x6e, 0x6f, 0x73, 0x18, 0x03, 0x20, 0x01, 0x28, 0x03,
	0x52, 0x18, 0x54, 0x6f, 0x6d, 0x62, 0x73, 0x74, 0x6f, 0x6e, 0x65, 0x53, 0x69, 0x6e, 0x63, 0x65,
	0x45, 0x70, 0x6f, 0x63, 0x68, 0x4e, 0x61, 0x6e, 0x6f, 0x73, 0x12, 0x27, 0x0a, 0x05, 0x56, 0x61,
	0x6c, 0x75, 0x65, 0x18, 0x04, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x11, 0x2e, 0x6a, 0x72, 0x68, 0x79,
	0x2e, 0x73, 0x33, 0x64, 0x62, 0x2e, 0x76, 0x31, 0x2e, 0x52, 0x6f, 0x77, 0x52, 0x05, 0x56, 0x61,
	0x6c, 0x75, 0x65, 0x22, 0x8f, 0x02, 0x0a, 0x03, 0x52, 0x6f, 0x77, 0x12, 0x47, 0x0a, 0x0c, 0x43,
	0x6f, 0x6c, 0x75, 0x6d, 0x6e, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28,
	0x0b, 0x32, 0x23, 0x2e, 0x6a, 0x72, 0x68, 0x79, 0x2e, 0x73, 0x33, 0x64, 0x62, 0x2e, 0x76, 0x31,
	0x2e, 0x52, 0x6f, 0x77, 0x2e, 0x43, 0x6f, 0x6c, 0x75, 0x6d, 0x6e, 0x56, 0x61, 0x6c, 0x75, 0x65,
	0x73, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x52, 0x0c, 0x43, 0x6f, 0x6c, 0x75, 0x6d, 0x6e, 0x56, 0x61,
	0x6c, 0x75, 0x65, 0x73, 0x12, 0x18, 0x0a, 0x07, 0x44, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x64, 0x18,
	0x02, 0x20, 0x01, 0x28, 0x08, 0x52, 0x07, 0x44, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x64, 0x12, 0x49,
	0x0a, 0x12, 0x44, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x55, 0x70, 0x64, 0x61, 0x74, 0x65, 0x4f, 0x66,
	0x66, 0x73, 0x65, 0x74, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x19, 0x2e, 0x67, 0x6f, 0x6f,
	0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x44, 0x75, 0x72,
	0x61, 0x74, 0x69, 0x6f, 0x6e, 0x52, 0x12, 0x44, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x55, 0x70, 0x64,
	0x61, 0x74, 0x65, 0x4f, 0x66, 0x66, 0x73, 0x65, 0x74, 0x1a, 0x5a, 0x0a, 0x11, 0x43, 0x6f, 0x6c,
	0x75, 0x6d, 0x6e, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x73, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x12, 0x10,
	0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x6b, 0x65, 0x79,
	0x12, 0x2f, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32,
	0x19, 0x2e, 0x6a, 0x72, 0x68, 0x79, 0x2e, 0x73, 0x33, 0x64, 0x62, 0x2e, 0x76, 0x31, 0x2e, 0x43,
	0x6f, 0x6c, 0x75, 0x6d, 0x6e, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75,
	0x65, 0x3a, 0x02, 0x38, 0x01, 0x22, 0x7d, 0x0a, 0x0b, 0x43, 0x6f, 0x6c, 0x75, 0x6d, 0x6e, 0x56,
	0x61, 0x6c, 0x75, 0x65, 0x12, 0x3d, 0x0a, 0x0c, 0x55, 0x70, 0x64, 0x61, 0x74, 0x65, 0x4f, 0x66,
	0x66, 0x73, 0x65, 0x74, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x19, 0x2e, 0x67, 0x6f, 0x6f,
	0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x44, 0x75, 0x72,
	0x61, 0x74, 0x69, 0x6f, 0x6e, 0x52, 0x0c, 0x55, 0x70, 0x64, 0x61, 0x74, 0x65, 0x4f, 0x66, 0x66,
	0x73, 0x65, 0x74, 0x12, 0x2f, 0x0a, 0x05, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x02, 0x20, 0x01,
	0x28, 0x0b, 0x32, 0x19, 0x2e, 0x6a, 0x72, 0x68, 0x79, 0x2e, 0x73, 0x33, 0x64, 0x62, 0x2e, 0x76,
	0x31, 0x2e, 0x53, 0x51, 0x4c, 0x69, 0x74, 0x65, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x52, 0x05, 0x56,
	0x61, 0x6c, 0x75, 0x65, 0x2a, 0x37, 0x0a, 0x04, 0x54, 0x79, 0x70, 0x65, 0x12, 0x08, 0x0a, 0x04,
	0x4e, 0x55, 0x4c, 0x4c, 0x10, 0x00, 0x12, 0x07, 0x0a, 0x03, 0x49, 0x4e, 0x54, 0x10, 0x01, 0x12,
	0x08, 0x0a, 0x04, 0x52, 0x45, 0x41, 0x4c, 0x10, 0x02, 0x12, 0x08, 0x0a, 0x04, 0x54, 0x45, 0x58,
	0x54, 0x10, 0x03, 0x12, 0x08, 0x0a, 0x04, 0x42, 0x4c, 0x4f, 0x42, 0x10, 0x04, 0x42, 0x1f, 0x5a,
	0x1d, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x6a, 0x72, 0x68, 0x79,
	0x2f, 0x73, 0x33, 0x64, 0x62, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x76, 0x31, 0x62, 0x06,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_v1_node_proto_rawDescOnce sync.Once
	file_v1_node_proto_rawDescData = file_v1_node_proto_rawDesc
)

func file_v1_node_proto_rawDescGZIP() []byte {
	file_v1_node_proto_rawDescOnce.Do(func() {
		file_v1_node_proto_rawDescData = protoimpl.X.CompressGZIP(file_v1_node_proto_rawDescData)
	})
	return file_v1_node_proto_rawDescData
}

var file_v1_node_proto_enumTypes = make([]protoimpl.EnumInfo, 1)
var file_v1_node_proto_msgTypes = make([]protoimpl.MessageInfo, 6)
var file_v1_node_proto_goTypes = []any{
	(Type)(0),                   // 0: jrhy.s3db.v1.Type
	(*Node)(nil),                // 1: jrhy.s3db.v1.Node
	(*SQLiteValue)(nil),         // 2: jrhy.s3db.v1.SQLiteValue
	(*CRDTValue)(nil),           // 3: jrhy.s3db.v1.CRDTValue
	(*Row)(nil),                 // 4: jrhy.s3db.v1.Row
	(*ColumnValue)(nil),         // 5: jrhy.s3db.v1.ColumnValue
	nil,                         // 6: jrhy.s3db.v1.Row.ColumnValuesEntry
	(*durationpb.Duration)(nil), // 7: google.protobuf.Duration
}
var file_v1_node_proto_depIdxs = []int32{
	2, // 0: jrhy.s3db.v1.Node.key:type_name -> jrhy.s3db.v1.SQLiteValue
	3, // 1: jrhy.s3db.v1.Node.value:type_name -> jrhy.s3db.v1.CRDTValue
	0, // 2: jrhy.s3db.v1.SQLiteValue.Type:type_name -> jrhy.s3db.v1.Type
	4, // 3: jrhy.s3db.v1.CRDTValue.Value:type_name -> jrhy.s3db.v1.Row
	6, // 4: jrhy.s3db.v1.Row.ColumnValues:type_name -> jrhy.s3db.v1.Row.ColumnValuesEntry
	7, // 5: jrhy.s3db.v1.Row.DeleteUpdateOffset:type_name -> google.protobuf.Duration
	7, // 6: jrhy.s3db.v1.ColumnValue.UpdateOffset:type_name -> google.protobuf.Duration
	2, // 7: jrhy.s3db.v1.ColumnValue.Value:type_name -> jrhy.s3db.v1.SQLiteValue
	5, // 8: jrhy.s3db.v1.Row.ColumnValuesEntry.value:type_name -> jrhy.s3db.v1.ColumnValue
	9, // [9:9] is the sub-list for method output_type
	9, // [9:9] is the sub-list for method input_type
	9, // [9:9] is the sub-list for extension type_name
	9, // [9:9] is the sub-list for extension extendee
	0, // [0:9] is the sub-list for field type_name
}

func init() { file_v1_node_proto_init() }
func file_v1_node_proto_init() {
	if File_v1_node_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_v1_node_proto_rawDesc,
			NumEnums:      1,
			NumMessages:   6,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_v1_node_proto_goTypes,
		DependencyIndexes: file_v1_node_proto_depIdxs,
		EnumInfos:         file_v1_node_proto_enumTypes,
		MessageInfos:      file_v1_node_proto_msgTypes,
	}.Build()
	File_v1_node_proto = out.File
	file_v1_node_proto_rawDesc = nil
	file_v1_node_proto_goTypes = nil
	file_v1_node_proto_depIdxs = nil
}
