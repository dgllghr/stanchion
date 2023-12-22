const std = @import("std");
const rand = std.rand;

const schema_mod = @import("../schema.zig");
const Column = schema_mod.Column;
const Schema = schema_mod.Schema;
const SchemaDef = schema_mod.SchemaDef;

const MemoryValue = @import("../value.zig").MemoryValue;

pub const planets = struct {
    pub const name = "planets";

    pub const schema = Schema{
        .columns = &[_]Column{
            Column{
                .rank = 0,
                .name = "quadrant",
                .column_type = .{ .data_type = .Text, .nullable = false },
                .sk_rank = 0,
            },
            Column{
                .rank = 1,
                .name = "sector",
                .column_type = .{ .data_type = .Integer, .nullable = false },
                .sk_rank = 1,
            },
            Column{
                .rank = 2,
                .name = "size",
                .column_type = .{ .data_type = .Integer, .nullable = true },
                .sk_rank = null,
            },
            Column{
                .rank = 3,
                .name = "gravity",
                .column_type = .{ .data_type = .Float, .nullable = true },
                .sk_rank = null,
            },
            Column{
                .rank = 4,
                .name = "name",
                .column_type = .{ .data_type = .Text, .nullable = true },
                .sk_rank = null,
            },
        },
        .sort_key = &[_]usize{ 0, 1 },
    };

    pub const fixed_data = [_][5]MemoryValue{
        .{
            .{ .Text = "Alpha" }, .{ .Integer = 7 }, .{ .Integer = 100 }, .{ .Float = 1.5 },
            .{ .Text = "Andor" },
        },
        .{
            .{ .Text = "Alpha" }, .{ .Integer = 9 }, .{ .Integer = 50 }, .Null,
            .{ .Text = "Bajor" },
        },
        .{
            .{ .Text = "Gamma" },   .{ .Integer = 3 }, .{ .Integer = 75 }, .{ .Float = 2 },
            .{ .Text = "Karemma" },
        },
        .{
            .{ .Text = "Beta" },   .{ .Integer = 17 }, .{ .Integer = 105 }, .{ .Float = 0.75 },
            .{ .Text = "Vulkan" },
        },
    };

    const quadrants = [_][]const u8{ "Alpha", "Beta", "Gamma", "Delta" };

    pub fn randomRecord(prng: *rand.DefaultPrng) [5]MemoryValue {
        const quadrant = quadrants[prng.random().intRangeLessThan(usize, 0, quadrants.len)];

        return [5]MemoryValue{
            .{ .Text = quadrant },
            .{ .Integer = prng.random().int(i64) },
            .{ .Integer = prng.random().int(i64) },
            .{ .Float = prng.random().float(f64) },
            .{ .Text = "Veridian 3" },
        };
    }
};

pub const all_column_types = struct {
    pub const name = "all_column_types";

    pub const schema = Schema{
        .columns = &[_]Column{
            Column{
                .rank = 0,
                .name = "first_col",
                .column_type = .{ .data_type = .Integer, .nullable = false },
                .sk_rank = 0,
            },
            Column{
                .rank = 1,
                .name = "second_col",
                .column_type = .{ .data_type = .Float, .nullable = false },
                .sk_rank = null,
            },
            Column{
                .rank = 2,
                .name = "third_col",
                .column_type = .{ .data_type = .Text, .nullable = false },
                .sk_rank = null,
            },
            Column{
                .rank = 3,
                .name = "fourth_col",
                .column_type = .{ .data_type = .Boolean, .nullable = false },
                .sk_rank = null,
            },
            Column{
                .rank = 4,
                .name = "fifth_col",
                .column_type = .{ .data_type = .Blob, .nullable = false },
                .sk_rank = null,
            },
            Column{
                .rank = 5,
                .name = "sixth_col",
                .column_type = .{ .data_type = .Integer, .nullable = true },
                .sk_rank = 0,
            },
            Column{
                .rank = 6,
                .name = "seventh_col",
                .column_type = .{ .data_type = .Float, .nullable = true },
                .sk_rank = 0,
            },
            Column{
                .rank = 7,
                .name = "eighth_col",
                .column_type = .{ .data_type = .Text, .nullable = true },
                .sk_rank = 0,
            },
            Column{
                .rank = 8,
                .name = "ninth_col",
                .column_type = .{ .data_type = .Boolean, .nullable = true },
                .sk_rank = 0,
            },
            Column{
                .rank = 9,
                .name = "tenth_col",
                .column_type = .{ .data_type = .Blob, .nullable = true },
                .sk_rank = 0,
            },
        },
        .sort_key = &[_]usize{0},
    };

    pub const fixed_data = [_][]MemoryValue{};
};
