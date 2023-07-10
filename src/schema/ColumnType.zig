const Self = @This();

data_type: DataType,
nullable: bool,

pub const DataType = enum {
    Boolean,
    Integer,
    Float,
    Text,
    Blob,
};

