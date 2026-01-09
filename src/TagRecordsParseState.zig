const std = @import("std");
const Allocator = std.mem.Allocator;

const encoding = @import("encoding");
const Encoder = encoding.Encoder;

const IndexKind = @import("Index.zig").IndexKind;

const maxTenantIDLen = @import("store/lines.zig").maxTenantIDLen;
const Field = @import("store/lines.zig").Field;

const Self = @This();

streamIDs: std.ArrayList(u128) = .empty,
tenantID: []const u8,
tag: Field,

pub fn init(alloc: Allocator) !*Self {
    const s = try alloc.create(Self);
    return s;
}
pub fn deinit(self: *Self, alloc: Allocator) void {
    alloc.destroy(self);
}
pub fn setup(self: *const Self, item: []const u8) !void {
    _ = self;
    _ = item;
    unreachable;
}

pub fn streamsLen(self: *const Self) usize {
    _ = self;
    unreachable;
}

pub fn parseStreamIDs(self: *const Self) void {
    _ = self;
    _ = unreachable;
}

pub fn encodePrefixBound(self: *const Self) usize {
    return 1 + maxTenantIDLen + self.tag.encodeIndexTagBound();
}
pub fn encodePrefix(self: *const Self, dst: []u8) void {
    dst[0] = @intFromEnum(IndexKind.tagToSids);
    var enc = Encoder.init(dst[1..]);
    enc.writePadded(self.tenantID, maxTenantIDLen);
    _ = self.tag.encodeIndexTag(enc.buf[enc.offset..]);
}
