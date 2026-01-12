const std = @import("std");
const Allocator = std.mem.Allocator;

const encoding = @import("encoding");
const Encoder = encoding.Encoder;

const IndexKind = @import("Index.zig").IndexKind;

const maxTenantIDLen = @import("store/lines.zig").maxTenantIDLen;
const Field = @import("store/lines.zig").Field;

const Self = @This();

streamIDs: std.ArrayList(u128) = .empty,
tenantID: []const u8 = undefined,
tag: Field = undefined,
streamsRaw: []const u8 = undefined,

pub fn init(alloc: Allocator) !*Self {
    const s = try alloc.create(Self);
    s.* = .{};
    return s;
}
pub fn deinit(self: *Self, alloc: Allocator) void {
    alloc.destroy(self);
}
pub fn setup(self: *Self, item: []const u8) !void {
    const kind = item[0];
    const tenantOffset = 1 + maxTenantIDLen;
    self.tenantID = item[1..tenantOffset];

    std.debug.assert(kind == @intFromEnum(IndexKind.tagToSids));

    // We need to modify the buffer in-place for unescaping
    // This is safe because we're only unescaping (making it shorter)
    const tag_portion = @constCast(item[tenantOffset..]);
    const offset = self.tag.decodeIndexTag(tag_portion);

    self.streamsRaw = item[tenantOffset + offset ..];
}

pub fn streamsLen(self: *const Self) usize {
    return self.streamsRaw.len / 16;
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
