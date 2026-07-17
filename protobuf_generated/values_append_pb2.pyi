from . import mdm_pb2 as _mdm_pb2
from . import ods_external_data_pb2 as _ods_external_data_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class AppendRequest(_message.Message):
    __slots__ = ("handle", "values")
    HANDLE_FIELD_NUMBER: _ClassVar[int]
    VALUES_FIELD_NUMBER: _ClassVar[int]
    handle: _ods_external_data_pb2.Handle
    values: _mdm_pb2.WriteRequestList
    def __init__(self, handle: _Optional[_Union[_ods_external_data_pb2.Handle, _Mapping]] = ..., values: _Optional[_Union[_mdm_pb2.WriteRequestList, _Mapping]] = ...) -> None: ...

class AppendResult(_message.Message):
    __slots__ = ("row_count_by_group",)
    class RowCountByGroupEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: int
        def __init__(self, key: _Optional[str] = ..., value: _Optional[int] = ...) -> None: ...
    ROW_COUNT_BY_GROUP_FIELD_NUMBER: _ClassVar[int]
    row_count_by_group: _containers.ScalarMap[str, int]
    def __init__(self, row_count_by_group: _Optional[_Mapping[str, int]] = ...) -> None: ...

class InitRequest(_message.Message):
    __slots__ = ("group_ids",)
    GROUP_IDS_FIELD_NUMBER: _ClassVar[int]
    group_ids: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, group_ids: _Optional[_Iterable[int]] = ...) -> None: ...

class InitResult(_message.Message):
    __slots__ = ("duckdb_file_content",)
    DUCKDB_FILE_CONTENT_FIELD_NUMBER: _ClassVar[int]
    duckdb_file_content: bytes
    def __init__(self, duckdb_file_content: _Optional[bytes] = ...) -> None: ...
