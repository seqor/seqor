const std = @import("std");
const Allocator = std.mem.Allocator;

const encoding = @import("encoding");
const Decoder = encoding.Decoder;
const Encoder = encoding.Encoder;

const MetaIndex = @This();

pub const DecodedMetaIndex = struct {
    buf: []u8,
    records: []MetaIndex,
};

firstItem: []const u8 = "",
blockHeadersCount: u32 = 0,
indexBlockOffset: u64 = 0,
indexBlockSize: u32 = 0,

pub fn reset(self: *MetaIndex) void {
    self.* = .{};
}

// [firstItem.len:firstItem][4:count][8:offset][4:size] = firstItem.len + lenBound + 16
pub fn bound(self: *const MetaIndex) usize {
    const firstItemBound = Encoder.varIntBound(self.firstItem.len);
    return firstItemBound + self.firstItem.len + 16;
}

pub fn encode(self: *const MetaIndex, buf: []u8) void {
    var enc = Encoder.init(buf);

    enc.writeString(self.firstItem);
    enc.writeInt(u32, self.blockHeadersCount);
    enc.writeInt(u64, self.indexBlockOffset);
    enc.writeInt(u32, self.indexBlockSize);
}

pub fn encodeAlloc(self: *const MetaIndex, alloc: Allocator) ![]u8 {
    const buf = try alloc.alloc(u8, self.bound());

    self.encode(buf);

    return buf;
}

pub fn decode(self: *MetaIndex, buf: []u8) usize {
    var dec = Decoder.init(buf);
    self.firstItem = dec.readString();
    self.blockHeadersCount = dec.readInt(u32);
    self.indexBlockOffset = dec.readInt(u64);
    self.indexBlockSize = dec.readInt(u32);
    std.debug.assert(self.blockHeadersCount > 0);
    return dec.offset;
}

pub fn decodeDecompress(alloc: Allocator, compressed: []const u8, blocksCount: u64) !DecodedMetaIndex {
    const metaindexBufSize = try encoding.getFrameContentSize(compressed);
    const metaindexBuf = try alloc.alloc(u8, metaindexBufSize);
    errdefer alloc.free(metaindexBuf);
    const metaindexLen = try encoding.decompress(metaindexBuf, compressed);

    var records = try std.ArrayList(MetaIndex).initCapacity(alloc, @intCast(blocksCount));
    errdefer records.deinit(alloc);

    var slice = metaindexBuf[0..metaindexLen];
    var totalBlockHeaders: u64 = 0;
    while (slice.len > 0) {
        var rec: MetaIndex = undefined;
        const n = rec.decode(slice);
        slice = slice[n..];
        totalBlockHeaders += rec.blockHeadersCount;
        try records.append(alloc, rec);
    }
    std.debug.assert(totalBlockHeaders == blocksCount);

    const recordsOwned = try records.toOwnedSlice(alloc);
    return .{
        .buf = metaindexBuf,
        .records = recordsOwned,
    };
}

pub fn lessThan(_: void, one: MetaIndex, another: MetaIndex) bool {
    return std.mem.lessThan(u8, one.firstItem, another.firstItem);
}

const testing = std.testing;

test "MetaIndex decodeDecompress roundtrip" {
    const alloc = testing.allocator;

    const rec1 = MetaIndex{
        .firstItem = "alpha",
        .blockHeadersCount = 2,
        .indexBlockOffset = 10,
        .indexBlockSize = 64,
    };
    const rec2 = MetaIndex{
        .firstItem = "omega",
        .blockHeadersCount = 3,
        .indexBlockOffset = 74,
        .indexBlockSize = 128,
    };

    var uncompressed = std.ArrayList(u8).empty;
    defer uncompressed.deinit(alloc);

    var recordBound = rec1.bound();
    try uncompressed.ensureUnusedCapacity(alloc, recordBound);
    rec1.encode(uncompressed.unusedCapacitySlice());
    uncompressed.items.len += recordBound;

    recordBound = rec2.bound();
    try uncompressed.ensureUnusedCapacity(alloc, recordBound);
    rec2.encode(uncompressed.unusedCapacitySlice());
    uncompressed.items.len += recordBound;

    const compressedBound = try encoding.compressBound(uncompressed.items.len);
    const compressed = try alloc.alloc(u8, compressedBound);
    defer alloc.free(compressed);
    const compressedLen = try encoding.compressAuto(compressed, uncompressed.items);

    const decoded = try MetaIndex.decodeDecompress(
        alloc,
        compressed[0..compressedLen],
        rec1.blockHeadersCount + rec2.blockHeadersCount,
    );
    defer {
        if (decoded.records.len > 0) alloc.free(decoded.records);
        if (decoded.buf.len > 0) alloc.free(decoded.buf);
    }

    try testing.expectEqualDeep(&[_]MetaIndex{ rec1, rec2 }, decoded.records);
}
