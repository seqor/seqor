const std = @import("std");

const Encoder = @import("encoding").Encoder;
const Decoder = @import("encoding").Decoder;

const sizing = @import("inmem/sizing.zig");

pub const maxTenantIDLen = 16;

pub const SID = struct {
    tenantID: []const u8,
    id: u128,

    pub fn eql(self: *const SID, another: *const SID) bool {
        return std.mem.eql(u8, self.tenantID, another.tenantID) and
            self.id == another.id;
    }

    pub fn lessThan(self: *const SID, another: *const SID) bool {
        // tenant is less
        return std.mem.lessThan(u8, self.tenantID, another.tenantID) or
            // or if tenant is eq than id is less
            (std.mem.eql(u8, self.tenantID, another.tenantID) and
                self.id < another.id);
    }

    pub const encodeBound = 32;
    pub fn encode(self: *const SID, enc: *Encoder) void {
        enc.writePadded(self.tenantID, maxTenantIDLen);
        enc.writeInt(u128, self.id);
    }

    pub fn decode(buf: []const u8) SID {
        var decoder = Decoder.init(buf);
        const tenantID = decoder.readPadded(maxTenantIDLen);
        const id = decoder.readInt(u128);
        return .{
            .tenantID = tenantID,
            .id = id,
        };
    }
};

const ControlChar = enum(u8) {
    escape = 0,
    tagTerminator = 1,
};

pub const Field = struct {
    key: []const u8,
    value: []const u8,

    pub fn encodeIndexTagBound(self: Field) usize {
        var res = self.key.len + self.value.len;
        res += count(u8, self.key, @intFromEnum(ControlChar.escape));
        res += count(u8, self.key, @intFromEnum(ControlChar.tagTerminator));
        res += count(u8, self.value, @intFromEnum(ControlChar.escape));
        res += count(u8, self.value, @intFromEnum(ControlChar.tagTerminator));
        return res;
    }
    pub fn count(comptime T: type, haystack: []const T, needle: T) usize {
        var i: usize = 0;
        var found: usize = 0;

        while (std.mem.indexOfScalarPos(T, haystack, i, needle)) |idx| {
            i = idx + 1;
            found += 1;
        }

        return found;
    }

    pub fn encodeIndexTag(self: Field, dst: []u8) usize {
        const offset = escapeTag(dst, self.key);
        return offset + escapeTag(dst[offset..], self.value);
    }

    fn escapeTag(dst: []u8, src: []const u8) usize {
        var offset: usize = 0;
        var last: usize = 0;
        for (0..src.len) |i| {
            switch (src[i]) {
                @intFromEnum(ControlChar.escape) => {
                    const len = i + 1 - last;
                    @memcpy(dst[offset..][0..len], src[last .. i + 1]);
                    offset += len;
                    dst[offset] = '0';
                    offset += 1;
                    last = i + 1;
                },
                @intFromEnum(ControlChar.tagTerminator) => {
                    const len = i + 1 - last;
                    @memcpy(dst[offset..][0..len], src[last .. i + 1]);
                    offset += len;
                    dst[offset] = '1';
                    offset += 1;
                    last = i + 1;
                },
                else => {},
            }
        }

        if (last < src.len) {
            const remaining = src.len - last;
            @memcpy(dst[offset..][0..remaining], src[last..]);
            offset += remaining;
        }

        return offset;
    }
};

// Line is an internal representation of a log line,
pub const Line = struct {
    timestampNs: u64,
    sid: SID,
    // field.key can be empty meaning it's a message field (_msg by fefault in the API)
    // can't be const because we reorder fields
    fields: []Field,

    pub fn fieldsSize(self: *const Line) u32 {
        return sizing.fieldsJsonSize(self);
    }
};

pub fn lineLessThan(_: void, one: *const Line, another: *const Line) bool {
    // sid is less
    return one.sid.lessThan(&another.sid) or
        // or sid is eq, but timestamp is less
        (one.sid.eql(&another.sid) and one.timestampNs < another.timestampNs);
}

pub fn fieldLessThan(_: void, one: Field, another: Field) bool {
    return std.mem.lessThan(u8, one.key, another.key);
}

const testing = std.testing;

test "Field.encodeIndexTag" {
    const alloc = testing.allocator;
    const Case = struct {
        key: []const u8,
        value: []const u8,
        expected: []const u8,
    };

    const cases = [_]Case{
        .{
            .key = "key",
            .value = "value",
            .expected = "keyvalue",
        },
        .{
            .key = "ke\x00y",
            .value = "value",
            .expected = "ke\x000yvalue",
        },
        .{
            .key = "key",
            .value = "val\x01ue",
            .expected = "keyval\x011ue",
        },
        .{
            .key = "k\x00e\x01y",
            .value = "v\x01al\x00ue",
            .expected = "k\x000e\x011yv\x011al\x000ue",
        },
    };
    for (cases) |case| {
        const f = Field{ .key = case.key, .value = case.value };
        const bound = f.encodeIndexTagBound();
        const buf = try alloc.alloc(u8, bound);
        defer alloc.free(buf);

        _ = f.encodeIndexTag(buf);
        try testing.expectEqualSlices(u8, case.expected, buf);
    }
}
