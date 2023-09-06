//! pub const Validator = struct {
//!     const Self = @This();
//!
//!     const Value;
//!
//!     /// Type of the encoder that is created when validation completes successfully
//!     const Encoder;
//!
//!     /// Returns true if `next` has not been called, false otherwise.
//!     fn unused(self: Self) bool;
//!
//!     /// Provide the next value in the sequence to the validator. If the validator
//!     /// determines that the sequence of values is not encodable while executing
//!     /// `next`, it must continue to accept calls to next but they may be a noop.
//!     fn next(self: *Self, value: Value) void;
//!
//!     /// Returns the `byte_len`, `encoding`, and `encoder` as part of the `Valid`
//!     /// struct if validation succeeds. If validation fails, returns `.NotEncodable`.
//!     fn end(self: Self) error{NotEncodable}!Valid(Encoder);
//! };
//!
//! pub const Encoder = struct {
//!     const Self = @This();
//!
//!     const Value;
//!
//!     /// Destroy the encoder. Do not do any IO
//!     fn deinit(self: *Self) void;
//!
//!     /// Write any data to the blob that is provided by the validator that is needed
//!     /// for decoding. Return whether the encoding process should continue (true) or
//!     /// whether encoding is complete (false). If `false` is returned, `encode` must
//!     /// be a noop. In either case, the caller must call `end`.
//!     ///
//!     /// For example, the constant encoder receives the single value present in the
//!     /// stripe from the validator and writes it in `begin`. No other writes are
//!     /// necessary so the caller can skip calling `encode` and must call `end`.
//!     fn begin(self: *Self, blob: anytype) !bool;
//!
//!     /// Encode a single value at the end of the encoded values
//!     pub fn encode(self: *Self, blob: anytype, value: Value) !void;
//!
//!     /// Finish the encoding by writing any buffered data or other data required for
//!     /// decoding
//!     pub fn end(self: *Self, blob: anytype) !void;
//! };

pub const Encoding = @import("stripe/encoding.zig").Encoding;
const validator = @import("stripe/validator.zig");
pub const Meta = validator.Meta;
pub const Valid = validator.Valid;

pub const Bool = @import("stripe/logical_type/Bool.zig");
pub const Byte = @import("stripe/logical_type/Byte.zig");
pub const Int = @import("stripe/logical_type/Int.zig");
