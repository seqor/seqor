const std = @import("std");

const SID = @import("../lines.zig").SID;
const StreamWriter = @import("StreamWriter.zig");
const encoding = @import("encoding");
const Encoder = encoding.Encoder;
const Decoder = encoding.Decoder;

const Self = @This();

// Maximum size for an index block (8MB)
pub const maxIndexBlockSize: u64 = 8 * 1024 * 1024;

sid: SID,
minTs: u64,
maxTs: u64,

offset: u64,
size: u64,

pub fn init(allocator: std.mem.Allocator) !*Self {
    // TODO: test if it can be used by value, doesn't seem it needs an allocator
    const bh = try allocator.create(Self);
    bh.* = .{
        .sid = .{ .tenantID = "", .id = 0 },
        .minTs = 0,
        .maxTs = 0,
        .offset = 0,
        .size = 0,
    };
    return bh;
}

pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
    allocator.destroy(self);
}

pub fn deinitSIDAlloc(self: *Self, allocator: std.mem.Allocator) void {
    allocator.destroy(self);
    self.sid.deinit(allocator);
}

pub fn writeIndexBlock(
    self: *Self,
    allocator: std.mem.Allocator,
    indexBlockBuf: *std.ArrayList(u8),
    sid: SID,
    minTs: u64,
    maxTs: u64,
    streamWriter: *StreamWriter,
) !void {
    if (indexBlockBuf.items.len == 0) {
        return;
    }

    self.sid = sid;
    self.minTs = minTs;
    self.maxTs = maxTs;

    const compressBound = try encoding.compressBound(indexBlockBuf.items.len);
    const buf = try allocator.alloc(u8, compressBound);
    defer allocator.free(buf);
    const offset = try encoding.compressAuto(buf, indexBlockBuf.items);
    self.offset = streamWriter.indexBuf.items.len;
    self.size = offset;

    try streamWriter.indexBuf.appendSlice(allocator, buf[0..offset]);
}

// sid 32 + self 32 = 64
pub const encodeExpectedSize = 64;
pub fn encode(self: *const Self, buf: []u8) usize {
    var enc = Encoder.init(buf);
    self.sid.encode(&enc);
    enc.writeInt(u64, self.minTs);
    enc.writeInt(u64, self.maxTs);
    enc.writeInt(u64, self.offset);
    enc.writeInt(u64, self.size);
    return enc.offset;
}

pub fn decode(buf: []const u8) Self {
    var decoder = Decoder.init(buf);
    const sid = SID.decode(buf);
    decoder.offset = 32; // SID is 32 bytes
    const minTs = decoder.readInt(u64);
    const maxTs = decoder.readInt(u64);
    const offset = decoder.readInt(u64);
    const size = decoder.readInt(u64);
    return .{
        .sid = sid,
        .minTs = minTs,
        .maxTs = maxTs,
        .offset = offset,
        .size = size,
    };
}

pub fn decodeAlloc(allocator: std.mem.Allocator, buf: []const u8) Self {
    var decoder = Decoder.init(buf);
    const sid = SID.decodeAlloc(allocator, buf);
    decoder.offset = 32; // SID is 32 bytes
    const minTs = decoder.readInt(u64);
    const maxTs = decoder.readInt(u64);
    const offset = decoder.readInt(u64);
    const size = decoder.readInt(u64);
    return .{
        .sid = sid,
        .minTs = minTs,
        .maxTs = maxTs,
        .offset = offset,
        .size = size,
    };
}

pub fn ReadIndexBlockHeaders(
    allocator: std.mem.Allocator,
    compressed: []const u8,
) ![]Self {
    const decompressedSize = try encoding.getFrameContentSize(compressed);

    // potential problem
    var decompressed = try allocator.alloc(u8, decompressedSize);
    defer allocator.free(decompressed);

    try encoding.decompress(
        decompressed,
        compressed,
    );

    if (decompressed.len % encodeExpectedSize != 0) {
        return error.InvalidIndexBlockHeadersSize;
    }

    const count = decompressed.len / encodeExpectedSize;

    var dst = try allocator.alloc(Self, count);
    var i: usize = 0;
    errdefer {
        for (dst[0..i]) |reader| reader.deinitSIDAlloc(allocator);
        allocator.free(dst);
    }

    var off: usize = 0;
    while (off < decompressed.len) : ({
        off += encodeExpectedSize;
        i += 1;
    }) {
        dst[i] = decodeAlloc(allocator, decompressed[off .. off + encodeExpectedSize]);
    }

    try validateIndexBlockHeaders(dst);

    return dst;
}

fn validateIndexBlockHeaders(headers: []const Self) !void {
    _ = headers;
}

pub fn mustReadNextIndexBlock(
    self: *const Self,
    allocator: std.mem.Allocator,
    dst: *std.ArrayList(u8),
    indexBuf: []const u8,
) !void {
    const indexBlockSize = self.size;

    // Validate indexBlockSize
    if (indexBlockSize > maxIndexBlockSize) {
        std.log.err("FATAL: indexBlockHeader.indexBlockSize={} cannot exceed {} bytes", .{ indexBlockSize, maxIndexBlockSize });
        return error.InvalidIndexBlockSize;
    }

    // Validate that offset matches current read position (sequential reading)
    // if (self.offset != streamReader.indexBytesRead.*) {
    //     std.log.err("FATAL: indexBlockHeader.offset={} must equal to {}", .{ self.offset, streamReader.indexBytesRead.* });
    //     return error.InvalidIndexBlockOffset;
    // }

    // Bounds checking
    if (self.offset > indexBuf.len) {
        return error.InvalidIndexBlockOffset;
    }
    if (self.offset + indexBlockSize > indexBuf.len) {
        return error.InvalidIndexBlockData;
    }

    // Read compressed data
    const compressed = indexBuf[self.offset..][0..indexBlockSize];

    // Get decompressed size
    const decompressedSize = try encoding.getFrameContentSize(compressed);

    // Ensure dst has enough capacity
    try dst.ensureTotalCapacity(allocator, decompressedSize);
    dst.items.len = decompressedSize;

    // Decompress
    const actualDecompressedSize = try encoding.decompress(dst.items, compressed);
    if (actualDecompressedSize != decompressedSize) {
        std.log.err("FATAL: cannot decompress indexBlock read at offset {} with size {}: decompressed size mismatch", .{ self.offset, indexBlockSize });
        return error.DecompressionSizeMismatch;
    }

    // Update bytes read position
    // streamReader.indexBytesRead.* += indexBlockSize;
}

pub const Error = error{
    InvalidIndexBlockSize,
    InvalidIndexBlockOffset,
    InvalidIndexBlockData,
    DecompressionSizeMismatch,
};

test "IndexBlockHeaderEncode" {
    const Case = struct {
        header: Self,
        expectedLen: usize,
    };

    const cases = &[_]Case{
        .{
            .header = .{
                .sid = .{
                    .tenantID = "tenant",
                    .id = 42,
                },
                .minTs = 100,
                .maxTs = 200,
                .offset = 1,
                .size = 1234,
            },
            .expectedLen = encodeExpectedSize,
        },
        .{
            .header = std.mem.zeroInit(Self, .{}),
            .expectedLen = encodeExpectedSize,
        },
        .{
            .header = .{
                .sid = .{
                    .tenantID = "tenant",
                    .id = std.math.maxInt(u128),
                },
                .minTs = std.math.maxInt(u64),
                .maxTs = std.math.maxInt(u64),
                .offset = std.math.maxInt(u64),
                .size = std.math.maxInt(u64),
            },
            .expectedLen = encodeExpectedSize,
        },
    };

    for (cases) |case| {
        var encodeBuf: [encodeExpectedSize]u8 = undefined;
        const offset = case.header.encode(&encodeBuf);
        try std.testing.expectEqual(case.expectedLen, offset);

        const h = Self.decode(encodeBuf[0..offset]);
        try std.testing.expectEqualDeep(case.header, h);
    }
}
