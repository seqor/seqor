const std = @import("std");
const zeit = @import("zeit");

const ColumnType = @import("block_header.zig").ColumnType;

/// ValuesDecoder decodes values encoded by ValuesEncoder back to string representations.
pub const ValuesDecoder = struct {
    dictStrings: []const []const u8,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) !*ValuesDecoder {
        const vd = try allocator.create(ValuesDecoder);
        vd.* = .{
            .dictStrings = &[_][]const u8{},
            .allocator = allocator,
        };
        return vd;
    }

    pub fn deinit(self: *ValuesDecoder) void {
        if (self.dictStrings.len > 0) {
            self.allocator.free(self.dictStrings);
        }
        self.allocator.destroy(self);
    }

    /// Decode values encoded with the given vt and dictValues.
    /// Values slice is modified to decoded string representations.
    pub fn decode(
        self: *ValuesDecoder,
        values: [][]u8,
        vt: ColumnType,
        dictValues: []const []const u8,
    ) !void {
        switch (vt) {
            .string => {
                // values are already decoded
            },
            .dict => {
                // Store dict strings as a field for reuse
                if (self.dictStrings.len > 0) {
                    self.allocator.free(self.dictStrings);
                }
                self.dictStrings = try self.allocator.dupe([]const u8, dictValues);

                // Map dictionary IDs to actual values
                for (values, 0..) |v, i| {
                    if (v.len < 1) {
                        return error.InvalidDictValue;
                    }
                    const id: usize = @intCast(v[0]);
                    if (id >= self.dictStrings.len) {
                        return error.DictIdOutOfRange;
                    }
                    // Cast away const since we're just updating the pointer
                    values[i] = @constCast(self.dictStrings[id]);
                }
            },
            .uint8 => {
                for (values, 0..) |v, i| {
                    if (v.len < 1) {
                        return error.InvalidValueLength;
                    }
                    const n = decodeUint8(v);
                    const len = decodeUint8String(values[i], n);
                    values[i] = values[i][0..len];
                }
            },
            .uint16 => {
                for (values, 0..) |v, i| {
                    if (v.len < 2) {
                        return error.InvalidValueLength;
                    }
                    const n = decodeUint16(v);
                    const len = decodeUint64String(values[i], n);
                    values[i] = values[i][0..len];
                }
            },
            .uint32 => {
                for (values, 0..) |v, i| {
                    if (v.len < 4) {
                        return error.InvalidValueLength;
                    }
                    const n = decodeUint32(v);
                    const len = decodeUint64String(values[i], n);
                    values[i] = values[i][0..len];
                }
            },
            .uint64 => {
                for (values, 0..) |v, i| {
                    if (v.len < 8) {
                        return error.InvalidValueLength;
                    }
                    const n = decodeUint64(v);
                    const len = decodeUint64String(values[i], n);
                    values[i] = values[i][0..len];
                }
            },
            .int64 => {
                for (values, 0..) |v, i| {
                    if (v.len < 8) {
                        return error.InvalidValueLength;
                    }
                    const n = decodeInt64(v);
                    const len = decodeInt64String(values[i], n);
                    values[i] = values[i][0..len];
                }
            },
            .float64 => {
                for (values, 0..) |v, i| {
                    if (v.len < 8) {
                        return error.InvalidValueLength;
                    }
                    const f = decodeFloat64(v);
                    const len = decodeFloat64String(values[i], f);
                    values[i] = values[i][0..len];
                }
            },
            .ipv4 => {
                for (values, 0..) |v, i| {
                    if (v.len < 4) {
                        return error.InvalidValueLength;
                    }
                    const ip = decodeIPv4(v);
                    const len = decodeIPv4String(values[i], ip);
                    values[i] = values[i][0..len];
                }
            },
            .timestampIso8601 => {
                for (values, 0..) |v, i| {
                    if (v.len < 8) {
                        return error.InvalidValueLength;
                    }
                    const timestamp = decodeTimestampISO8601(v);
                    const len = try decodeTimestampISO8601String(values[i], timestamp);
                    values[i] = values[i][0..len];
                }
            },
            else => {
                return error.UnknownValueType;
            },
        }
    }
};

// Decode binary values to native types

fn decodeUint8(v: []const u8) u8 {
    return v[0];
}

fn decodeUint16(v: []const u8) u16 {
    return std.mem.bytesToValue(u16, v[0..2]);
}

fn decodeUint32(v: []const u8) u32 {
    return std.mem.bytesToValue(u32, v[0..4]);
}

fn decodeUint64(v: []const u8) u64 {
    return std.mem.bytesToValue(u64, v[0..8]);
}

fn decodeInt64(v: []const u8) i64 {
    return std.mem.bytesToValue(i64, v[0..8]);
}

fn decodeFloat64(v: []const u8) f64 {
    const n = decodeUint64(v);
    return @bitCast(n);
}

fn decodeIPv4(v: []const u8) u32 {
    return decodeUint32(v);
}

fn decodeTimestampISO8601(v: []const u8) i64 {
    const n = decodeUint64(v);
    return @bitCast(n);
}

// Decode native types to string representations
// All functions return the length of the written string

fn decodeUint8String(dst: []u8, n: u8) usize {
    if (n < 10) {
        dst[0] = '0' + n;
        return 1;
    }
    if (n < 100) {
        dst[0] = '0' + n / 10;
        dst[1] = '0' + n % 10;
        return 2;
    }

    if (n < 200) {
        dst[0] = '1';
        const rem = n - 100;
        if (rem < 10) {
            dst[1] = '0';
            dst[2] = '0' + rem;
        } else {
            dst[1] = '0' + rem / 10;
            dst[2] = '0' + rem % 10;
        }
    } else {
        dst[0] = '2';
        const rem = n - 200;
        if (rem < 10) {
            dst[1] = '0';
            dst[2] = '0' + rem;
        } else {
            dst[1] = '0' + rem / 10;
            dst[2] = '0' + rem % 10;
        }
    }
    return 3;
}

fn decodeUint64String(dst: []u8, n: u64) usize {
    const str = std.fmt.bufPrint(dst, "{d}", .{n}) catch unreachable;
    return str.len;
}

fn decodeInt64String(dst: []u8, n: i64) usize {
    const str = std.fmt.bufPrint(dst, "{d}", .{n}) catch unreachable;
    return str.len;
}

fn decodeFloat64String(dst: []u8, f: f64) usize {
    const str = std.fmt.bufPrint(dst, "{d}", .{f}) catch unreachable;
    return str.len;
}

fn decodeIPv4String(dst: []u8, n: u32) usize {
    const len1 = decodeUint8String(dst[0..], @intCast((n >> 24) & 0xFF));
    dst[len1] = '.';
    const len2 = decodeUint8String(dst[len1 + 1 ..], @intCast((n >> 16) & 0xFF));
    dst[len1 + 1 + len2] = '.';
    const len3 = decodeUint8String(dst[len1 + 1 + len2 + 1 ..], @intCast((n >> 8) & 0xFF));
    dst[len1 + 1 + len2 + 1 + len3] = '.';
    const len4 = decodeUint8String(dst[len1 + 1 + len2 + 1 + len3 + 1 ..], @intCast(n & 0xFF));
    return len1 + 1 + len2 + 1 + len3 + 1 + len4;
}

fn decodeTimestampISO8601String(dst: []u8, nsecs: i64) !usize {
    const instant = try zeit.instant(.{ .source = .{ .unix_nano = nsecs } });
    const time = instant.time();

    // Calculate milliseconds within the second from total nanoseconds
    const nsecs_in_second = @mod(nsecs, 1_000_000_000);
    const millis = @divTrunc(@abs(nsecs_in_second), 1_000_000);

    // Cast year to unsigned to avoid '+' prefix in formatting
    const year: u16 = if (time.year >= 0) @intCast(time.year) else 0;
    const str = std.fmt.bufPrint(dst, "{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}.{d:0>3}Z", .{
        year,
        time.month,
        time.day,
        time.hour,
        time.minute,
        time.second,
        millis,
    }) catch unreachable;
    return str.len;
}

test "decodeUint8String" {
    var buf: [16]u8 = undefined;

    var len = decodeUint8String(&buf, 0);
    try std.testing.expectEqualStrings("0", buf[0..len]);

    len = decodeUint8String(&buf, 9);
    try std.testing.expectEqualStrings("9", buf[0..len]);

    len = decodeUint8String(&buf, 42);
    try std.testing.expectEqualStrings("42", buf[0..len]);

    len = decodeUint8String(&buf, 99);
    try std.testing.expectEqualStrings("99", buf[0..len]);

    len = decodeUint8String(&buf, 100);
    try std.testing.expectEqualStrings("100", buf[0..len]);

    len = decodeUint8String(&buf, 199);
    try std.testing.expectEqualStrings("199", buf[0..len]);

    len = decodeUint8String(&buf, 200);
    try std.testing.expectEqualStrings("200", buf[0..len]);

    len = decodeUint8String(&buf, 255);
    try std.testing.expectEqualStrings("255", buf[0..len]);
}

test "decodeIPv4String" {
    var buf: [16]u8 = undefined;

    // 1.2.3.4 = (1 << 24) | (2 << 16) | (3 << 8) | 4
    const ip: u32 = (1 << 24) | (2 << 16) | (3 << 8) | 4;
    var len = decodeIPv4String(&buf, ip);
    try std.testing.expectEqualStrings("1.2.3.4", buf[0..len]);

    const ip2: u32 = (192 << 24) | (168 << 16) | (1 << 8) | 1;
    len = decodeIPv4String(&buf, ip2);
    try std.testing.expectEqualStrings("192.168.1.1", buf[0..len]);
}
