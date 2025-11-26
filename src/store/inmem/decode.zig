const std = @import("std");
const zeit = @import("zeit");

const ColumnType = @import("block_header.zig").ColumnType;

/// ValuesDecoder decodes values encoded by ValuesEncoder back to string representations.
/// The decoded values remain valid until reset() is called.
pub const ValuesDecoder = struct {
    buf: std.ArrayList(u8),
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) !*ValuesDecoder {
        const buf = try std.ArrayList(u8).initCapacity(allocator, 512);
        const vd = try allocator.create(ValuesDecoder);
        vd.* = .{
            .buf = buf,
            .allocator = allocator,
        };
        return vd;
    }

    pub fn deinit(self: *ValuesDecoder) void {
        self.buf.deinit(self.allocator);
        self.allocator.destroy(self);
    }

    pub fn reset(self: *ValuesDecoder) void {
        self.buf.clearRetainingCapacity();
    }

    /// Decode values encoded with the given vt and dictValues inplace.
    /// The decoded values remain valid until reset() is called.
    /// Values slice is modified in-place to point to decoded string representations.
    pub fn decodeInplace(
        self: *ValuesDecoder,
        values: [][]const u8,
        vt: ColumnType,
        dictValues: []const []const u8,
    ) !void {
        // Do not reset buf, since it may contain previously decoded data,
        // which must be preserved until reset() call.

        switch (vt) {
            .string => {
                // Nothing to do - values are already decoded
            },
            .dict => {
                // Build temporary dict strings buffer
                var dictStrings = try std.ArrayList([]const u8).initCapacity(self.allocator, dictValues.len);
                defer dictStrings.deinit(self.allocator);

                for (dictValues) |v| {
                    const dstLen = self.buf.items.len;
                    try self.buf.appendSlice(self.allocator, v);
                    dictStrings.appendAssumeCapacity(self.buf.items[dstLen..]);
                }

                // Map dictionary IDs to actual values
                for (values, 0..) |v, i| {
                    if (v.len != 1) {
                        return error.InvalidDictValue;
                    }
                    const id: usize = @intCast(v[0]);
                    if (id >= dictValues.len) {
                        return error.DictIdOutOfRange;
                    }
                    values[i] = dictStrings.items[id];
                }
            },
            .uint8 => {
                for (values, 0..) |v, i| {
                    if (v.len != 1) {
                        return error.InvalidValueLength;
                    }
                    const n = unmarshalUint8(v);
                    const dstLen = self.buf.items.len;
                    try marshalUint8String(&self.buf, self.allocator, n);
                    values[i] = self.buf.items[dstLen..];
                }
            },
            .uint16 => {
                for (values, 0..) |v, i| {
                    if (v.len != 2) {
                        return error.InvalidValueLength;
                    }
                    const n = unmarshalUint16(v);
                    const dstLen = self.buf.items.len;
                    try marshalUint64String(&self.buf, self.allocator, n);
                    values[i] = self.buf.items[dstLen..];
                }
            },
            .uint32 => {
                for (values, 0..) |v, i| {
                    if (v.len != 4) {
                        return error.InvalidValueLength;
                    }
                    const n = unmarshalUint32(v);
                    const dstLen = self.buf.items.len;
                    try marshalUint64String(&self.buf, self.allocator, n);
                    values[i] = self.buf.items[dstLen..];
                }
            },
            .uint64 => {
                for (values, 0..) |v, i| {
                    if (v.len != 8) {
                        return error.InvalidValueLength;
                    }
                    const n = unmarshalUint64(v);
                    const dstLen = self.buf.items.len;
                    try marshalUint64String(&self.buf, self.allocator, n);
                    values[i] = self.buf.items[dstLen..];
                }
            },
            .int64 => {
                for (values, 0..) |v, i| {
                    if (v.len != 8) {
                        return error.InvalidValueLength;
                    }
                    const n = unmarshalInt64(v);
                    const dstLen = self.buf.items.len;
                    try marshalInt64String(&self.buf, self.allocator, n);
                    values[i] = self.buf.items[dstLen..];
                }
            },
            .float64 => {
                for (values, 0..) |v, i| {
                    if (v.len != 8) {
                        return error.InvalidValueLength;
                    }
                    const f = unmarshalFloat64(v);
                    const dstLen = self.buf.items.len;
                    try marshalFloat64String(&self.buf, self.allocator, f);
                    values[i] = self.buf.items[dstLen..];
                }
            },
            .ipv4 => {
                for (values, 0..) |v, i| {
                    if (v.len != 4) {
                        return error.InvalidValueLength;
                    }
                    const ip = unmarshalIPv4(v);
                    const dstLen = self.buf.items.len;
                    try marshalIPv4String(&self.buf, self.allocator, ip);
                    values[i] = self.buf.items[dstLen..];
                }
            },
            .timestampIso8601 => {
                for (values, 0..) |v, i| {
                    if (v.len != 8) {
                        return error.InvalidValueLength;
                    }
                    const timestamp = unmarshalTimestampISO8601(v);
                    const dstLen = self.buf.items.len;
                    try marshalTimestampISO8601String(&self.buf, self.allocator, timestamp);
                    values[i] = self.buf.items[dstLen..];
                }
            },
            else => {
                return error.UnknownValueType;
            },
        }
    }
};

// Unmarshal functions

fn unmarshalUint8(v: []const u8) u8 {
    return v[0];
}

fn unmarshalUint16(v: []const u8) u16 {
    return std.mem.bytesToValue(u16, v[0..2]);
}

fn unmarshalUint32(v: []const u8) u32 {
    return std.mem.bytesToValue(u32, v[0..4]);
}

fn unmarshalUint64(v: []const u8) u64 {
    return std.mem.bytesToValue(u64, v[0..8]);
}

fn unmarshalInt64(v: []const u8) i64 {
    return std.mem.bytesToValue(i64, v[0..8]);
}

fn unmarshalFloat64(v: []const u8) f64 {
    const n = unmarshalUint64(v);
    return @bitCast(n);
}

fn unmarshalIPv4(v: []const u8) u32 {
    return unmarshalUint32(v);
}

fn unmarshalTimestampISO8601(v: []const u8) i64 {
    const n = unmarshalUint64(v);
    return @bitCast(n);
}

// Marshal to string functions

fn marshalUint8String(dst: *std.ArrayList(u8), allocator: std.mem.Allocator, n: u8) !void {
    if (n < 10) {
        try dst.append(allocator, '0' + n);
        return;
    }
    if (n < 100) {
        try dst.append(allocator, '0' + n / 10);
        try dst.append(allocator, '0' + n % 10);
        return;
    }

    if (n < 200) {
        try dst.append(allocator, '1');
        const rem = n - 100;
        if (rem < 10) {
            try dst.append(allocator, '0');
            try dst.append(allocator, '0' + rem);
        } else {
            try dst.append(allocator, '0' + rem / 10);
            try dst.append(allocator, '0' + rem % 10);
        }
    } else {
        try dst.append(allocator, '2');
        const rem = n - 200;
        if (rem < 10) {
            try dst.append(allocator, '0');
            try dst.append(allocator, '0' + rem);
        } else {
            try dst.append(allocator, '0' + rem / 10);
            try dst.append(allocator, '0' + rem % 10);
        }
    }
}

fn marshalUint64String(dst: *std.ArrayList(u8), allocator: std.mem.Allocator, n: u64) !void {
    var buf: [20]u8 = undefined;
    const str = std.fmt.bufPrint(&buf, "{d}", .{n}) catch unreachable;
    try dst.appendSlice(allocator, str);
}

fn marshalInt64String(dst: *std.ArrayList(u8), allocator: std.mem.Allocator, n: i64) !void {
    var buf: [21]u8 = undefined;
    const str = std.fmt.bufPrint(&buf, "{d}", .{n}) catch unreachable;
    try dst.appendSlice(allocator, str);
}

fn marshalFloat64String(dst: *std.ArrayList(u8), allocator: std.mem.Allocator, f: f64) !void {
    var buf: [64]u8 = undefined;
    const str = std.fmt.bufPrint(&buf, "{d}", .{f}) catch unreachable;
    try dst.appendSlice(allocator, str);
}

fn marshalIPv4String(dst: *std.ArrayList(u8), allocator: std.mem.Allocator, n: u32) !void {
    try marshalUint8String(dst, allocator, @intCast((n >> 24) & 0xFF));
    try dst.append(allocator, '.');
    try marshalUint8String(dst, allocator, @intCast((n >> 16) & 0xFF));
    try dst.append(allocator, '.');
    try marshalUint8String(dst, allocator, @intCast((n >> 8) & 0xFF));
    try dst.append(allocator, '.');
    try marshalUint8String(dst, allocator, @intCast(n & 0xFF));
}

fn marshalTimestampISO8601String(dst: *std.ArrayList(u8), allocator: std.mem.Allocator, nsecs: i64) !void {
    const instant = try zeit.instant(.{ .source = .{ .unix_nano = nsecs } });
    const time = instant.time();

    // Format: "2006-01-02T15:04:05.000Z" (24 chars) but year can be longer
    var buf: [64]u8 = undefined;

    // Calculate milliseconds within the second from total nanoseconds
    // Get the nanoseconds within the current second, then convert to millis
    const nsecs_in_second = @mod(nsecs, 1_000_000_000);
    const millis = @divTrunc(@abs(nsecs_in_second), 1_000_000);

    // Cast year to unsigned to avoid '+' prefix in formatting
    const year: u16 = if (time.year >= 0) @intCast(time.year) else 0;
    const str = std.fmt.bufPrint(&buf, "{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}.{d:0>3}Z", .{
        year,
        time.month,
        time.day,
        time.hour,
        time.minute,
        time.second,
        millis,
    }) catch unreachable;
    try dst.appendSlice(allocator, str);
}

test "marshalUint8String" {
    const allocator = std.testing.allocator;
    var buf = try std.ArrayList(u8).initCapacity(allocator, 16);
    defer buf.deinit(allocator);

    try marshalUint8String(&buf, allocator, 0);
    try std.testing.expectEqualStrings("0", buf.items);

    buf.clearRetainingCapacity();
    try marshalUint8String(&buf, allocator, 9);
    try std.testing.expectEqualStrings("9", buf.items);

    buf.clearRetainingCapacity();
    try marshalUint8String(&buf, allocator, 42);
    try std.testing.expectEqualStrings("42", buf.items);

    buf.clearRetainingCapacity();
    try marshalUint8String(&buf, allocator, 99);
    try std.testing.expectEqualStrings("99", buf.items);

    buf.clearRetainingCapacity();
    try marshalUint8String(&buf, allocator, 100);
    try std.testing.expectEqualStrings("100", buf.items);

    buf.clearRetainingCapacity();
    try marshalUint8String(&buf, allocator, 199);
    try std.testing.expectEqualStrings("199", buf.items);

    buf.clearRetainingCapacity();
    try marshalUint8String(&buf, allocator, 200);
    try std.testing.expectEqualStrings("200", buf.items);

    buf.clearRetainingCapacity();
    try marshalUint8String(&buf, allocator, 255);
    try std.testing.expectEqualStrings("255", buf.items);
}

test "marshalIPv4String" {
    const allocator = std.testing.allocator;
    var buf = try std.ArrayList(u8).initCapacity(allocator, 16);
    defer buf.deinit(allocator);

    // 1.2.3.4 = (1 << 24) | (2 << 16) | (3 << 8) | 4
    const ip: u32 = (1 << 24) | (2 << 16) | (3 << 8) | 4;
    try marshalIPv4String(&buf, allocator, ip);
    try std.testing.expectEqualStrings("1.2.3.4", buf.items);

    buf.clearRetainingCapacity();
    const ip2: u32 = (192 << 24) | (168 << 16) | (1 << 8) | 1;
    try marshalIPv4String(&buf, allocator, ip2);
    try std.testing.expectEqualStrings("192.168.1.1", buf.items);
}
