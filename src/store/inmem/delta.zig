const std = @import("std");
const testing = std.testing;

// Helper for tests - in production this would use the actual Packer
const TestPacker = struct {
    buf: []u8,
    offset: usize = 0,

    const Self = @This();

    /// writeVarInt uses leb128 to encode a u64 into a variable-length byte sequence.
    pub fn writeVarInt(self: *Self, value: u64) void {
        const slice = self.buf[self.offset .. self.offset + 10];

        var i: u8 = 0;
        var v = value;
        while (v >= 0x80) {
            slice[i] = @as(u8, @truncate(v)) | 0x80;
            v >>= 7;
            i += 1;
        }
        slice[i] = @as(u8, @truncate(v));

        self.offset += i + 1;
    }
};

/// marshalInt64NearestDelta encodes src using `nearest delta` encoding
/// with the given precisionBits and writes to packer.
///
/// precisionBits must be in the range [1...64], where 1 means 50% precision,
/// while 64 means 100% precision, i.e. lossless encoding.
///
/// Returns the first value from src.
pub fn marshalInt64NearestDelta(packer: anytype, src: []const u64, precisionBits: u8) i64 {
    const firstValue = src[0];
    var v = src[0];

    if (precisionBits == 64) {
        // Fast path.
        for (src[1..]) |next| {
            const d = next - v;
            v += d;
            packer.writeVarInt(d);
        }
    } else {
        // Slower path.
        var trailingZeros = getTrailingZeros(v, precisionBits);
        for (src[1..]) |next| {
            const result = nearestDelta(next, v, precisionBits, trailingZeros);
            const d = result[0];
            trailingZeros = result[1];
            v += d;
            packer.writeVarInt(@as(u64, @bitCast(d)));
        }
    }

    return firstValue;
}

fn getTrailingZeros(val: i64, precisionBits: u8) u8 {
    var v = val;
    if (v < 0) {
        v = -v;
        // There is no need in special case handling for v = -1<<63
    }
    const uv: u64 = @bitCast(v);
    const vBits: u8 = if (uv == 0) 0 else @intCast(64 - @clz(uv));
    if (vBits <= precisionBits) {
        return 0;
    }
    return vBits - precisionBits;
}

fn nearestDelta(next: i64, prev: i64, precisionBits: u8, prevTrailingZeros: u8) struct { i64, u8 } {
    const d = next - prev;
    if (d == 0) {
        // Fast path.
        return .{ 0, decIfNonZero(prevTrailingZeros) };
    }

    var origin = next;
    if (origin < 0) {
        origin = -origin;
        // There is no need in handling special case origin = -1<<63.
    }

    const uorigin: u64 = @bitCast(origin);
    const originBits: u8 = if (uorigin == 0) 0 else @intCast(64 - @clz(uorigin));
    if (originBits <= precisionBits) {
        // Cannot zero trailing bits for the given precisionBits.
        return .{ d, decIfNonZero(prevTrailingZeros) };
    }

    // originBits > precisionBits. May zero trailing bits in d.
    const trailingZeros = originBits - precisionBits;
    if (trailingZeros > prevTrailingZeros + 4) {
        // Probably counter reset. Return d with full precision.
        return .{ d, prevTrailingZeros + 2 };
    }
    if (trailingZeros + 4 < prevTrailingZeros) {
        // Probably counter reset. Return d with full precision.
        return .{ d, prevTrailingZeros - 2 };
    }

    // Zero trailing bits in d.
    const minus = d < 0;
    var delta = if (minus) -d else d;
    // There is no need in handling special case d = -1<<63.

    const shift_amount: u6 = @intCast(trailingZeros);
    const mask: u64 = ~@as(u64, 0) << shift_amount;
    const ud: u64 = @bitCast(delta);
    delta = @bitCast(ud & mask);

    if (minus) {
        delta = -delta;
    }

    return .{ delta, trailingZeros };
}

fn decIfNonZero(n: u8) u8 {
    if (n == 0) {
        return 0;
    }
    return n - 1;
}

test "marshalInt64NearestDelta basic lossless" {
    var buf: [1024]u8 = undefined;
    var packer = TestPacker{ .buf = &buf };

    const values = [_]u64{ 100, 102, 105, 103, 110 };
    const firstValue = marshalInt64NearestDelta(&packer, &values, 64);

    try testing.expectEqual(100, firstValue);
    try testing.expect(packer.offset > 0);
    try testing.expect(packer.offset < 50);
}
