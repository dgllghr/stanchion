//! pub const Validator = interface {
//!     const Self = @This();
//!
//!     const Value;
//!
//!     /// Type of the encoder that is created when validation completes successfully
//!     const Encoder;
//!
//!     /// Provide the next value in the sequence to the validator. If the validator
//!     /// determines that the sequence of values is not encodable while executing
//!     /// `next`, it must continue to accept calls to next but they should be a noop.
//!     fn next(self: *Self, value: Value) void;
//!
//!     /// Returns the `byte_len`, `encoding`, and `encoder` as part of the `Valid`
//!     /// struct if validation succeeds. If validation fails, returns `.NotEncodable`.
//!     fn end(self: Self) error{NotEncodable}!Valid(Encoder);
//! };
//!
//! pub const Encoder = interface {
//!     const Self = @This();
//!
//!     const Value;
//!
//!     /// Destroy the encoder. Do not do any IO. Any remaining IO should have been done
//!     /// in `end`, which is called before `deinit`.
//!     fn deinit(self: *Self) void;
//!
//!     /// Write initial data to the blob, such as data that is provided by the
//!     /// validator. Return whether the encoding process should continue (true) or
//!     /// whether encoding is complete (false). If `false` is returned, `encode` must
//!     /// be a noop. In either case, the caller must call `end`.
//!     ///
//!     /// For example, the constant validator provides the single value present in the
//!     /// stripe to the encoder, which writes it in `begin`. No other writes are
//!     /// necessary so `begin` returns `false` and the caller can skip calling `encode`
//!     /// but must call `end`.
//!     fn begin(self: *Self, blob: anytype) !bool;
//!
//!     /// Encode a single value at the end of the encoded values
//!     fn write(self: *Self, blob: anytype, value: Value) !void;
//!
//!     /// Finish the encoding by writing any buffered data or other data required for
//!     /// decoding
//!     fn end(self: *Self, blob: anytype) !void;
//! };
//!
//! /// Decoder does not do bounds testing. Attempting to access a value out of bounds of
//! /// the slice is undefined behavior.
//! pub const Decoder = interface {
//!     const Self = @This();
//!
//!     const Value;
//!
//!     /// Initialize the decoder by reading any necessary data from the provided blob.
//!     /// Calling read immediately after calling begin should read the first value in
//!     /// the stripe.
//!     fn begin(self: *Self, blob: anytype) !void;
//!
//!     /// Advance the position of the decoder by `n`
//!     fn next(self: *Self, n: u32) void;
//!
//!     /// Read the value at the current position of the decoder
//!     fn read(self: *Self, blob: anytype) !Value;
//!
//!     /// Reads the next `dst.len` values into `dst` without advancing the position of
//!     /// the decoder
//!     fn readAll(self: *Self, dst: []Value, blob: anytype) !void;
//! };

pub const Encoding = @import("stripe/encoding.zig").Encoding;
const validator = @import("stripe/validator.zig");
pub const Meta = validator.Meta;
pub const Valid = validator.Valid;

pub const Bool = @import("stripe/logical_type/Bool.zig");
pub const Byte = @import("stripe/logical_type/Byte.zig");
pub const Int = @import("stripe/logical_type/Int.zig");
pub const Float = @import("stripe/logical_type/Float.zig");
