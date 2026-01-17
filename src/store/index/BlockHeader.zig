const std = @import("std");
const Allocator = std.mem.Allocator;

const encoding = @import("encoding");
const Encoder = encoding.Encoder;

pub const EncodingType = enum(u8) {
    plain = 0,
    zstd = 1,
};

const BlockHeader = @This();

firstItem: []const u8,
prefix: []const u8,
encodingType: EncodingType,
itemsCount: u32 = 0,
itemsBlockOffset: u64 = 0,
lensBlockOffset: u64 = 0,
itemsBlockSize: u32 = 0,
lensBlockSize: u32 = 0,

pub fn reset(self: *BlockHeader) void {
    self.* = .{ .firstItem = undefined, .prefix = undefined, .encodingType = undefined };
}

// [len:n][firstItem:len][len:n][prefix:len][count:4][type:1][offset:8][size:4][offset:8][size:4] = bound + len + 29
pub fn bound(self: *const BlockHeader) usize {
    const firstItemLenBound = Encoder.varIntBound(self.firstItem.len);
    const prefixLenBound = Encoder.varIntBound(self.prefix.len);
    return firstItemLenBound + prefixLenBound + self.firstItem.len + self.prefix.len + 29;
}

pub fn encode(self: *const BlockHeader, buf: []u8) void {
    var enc = Encoder.init(buf);

    enc.writeString(self.firstItem);
    enc.writeString(self.prefix);
    enc.writeInt(u8, @intFromEnum(self.encodingType));
    enc.writeInt(u32, self.itemsCount);
    enc.writeInt(u64, self.itemsBlockOffset);
    enc.writeInt(u64, self.lensBlockOffset);
    enc.writeInt(u32, self.itemsBlockSize);
    enc.writeInt(u32, self.lensBlockSize);
}

pub fn encodeAlloc(self: *const BlockHeader, alloc: Allocator) ![]u8 {
    const size = self.bound();
    const buf = try alloc.alloc(u8, size);
    self.encode(buf);

    return buf;
}
