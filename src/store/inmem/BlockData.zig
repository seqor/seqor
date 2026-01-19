const std = @import("std");

const SID = @import("../lines.zig").SID;
const Column = @import("Column.zig");
const BlockHeader = @import("block_header.zig").BlockHeader;
const TimestampsHeader = @import("block_header.zig").TimestampsHeader;
const ColumnHeader = @import("block_header.zig").ColumnHeader;
const ColumnsHeader = @import("block_header.zig").ColumnsHeader;
const ColumnsHeaderIndex = @import("ColumnsHeaderIndex.zig");
const ColumnDict = @import("ColumnDict.zig");
const ColumnType = @import("block_header.zig").ColumnType;
const ColumnIDGen = @import("ColumnIDGen.zig");
const EncodingType = @import("TimestampsEncoder.zig").EncodingType;
const StreamWriter = @import("StreamWriter.zig");
const StreamReader = @import("reader.zig").StreamReader;

const maxTimestampsBlockSize = 8 * 1024 * 1024; // 64MB
const maxValuesBlockSize = 8 * 1024 * 1024; // 64MB
const maxBloomFilterBlockSize = 8 * 1024 * 1024; // 64MB
const maxColumnsHeaderSize = 8 * 1024 * 1024; // 64MB
const maxColumnsHeaderIndexSize = 8 * 1024 * 1024; // 64MB

pub const BlockData = struct {
    sid: SID,

    uncompressedSizeBytes: u64,

    rowsCount: u64,

    timestampsData: TimestampsData,

    columnsData: std.ArrayList(ColumnData),

    celledColumns: std.ArrayList(Column),

    allocator: std.mem.Allocator,

    pub fn deinit(self: *BlockData) void {
        self.reset();
        self.columnsData.deinit();
        self.celledColumns.deinit();
        self.allocator.destroy(self);
    }

    pub fn readFrom(
        allocator: std.mem.Allocator,
        bh: *const BlockHeader,
        sr: *StreamReader,
    ) !void {
        self.sid = bh.sid;
        self.uncompressedSizeBytes = bh.size;
        self.rowsCount = bh.len;

        // Read timestamps
        try self.timestampsData.mustReadFrom(&arena, &bh.timestampsHeader, sr);

        // Read columns header
        const columnsHeaderSize = bh.columnsHeaderSize;
        if (columnsHeaderSize > maxColumnsHeaderSize) {
            std.log.err(
                "BUG: too big columnsHeaderSize: {} bytes; mustn't exceed {} bytes",
                .{ columnsHeaderSize, maxColumnsHeaderSize },
            );
            return error.InvalidColumnsHeaderSize;
        }

        if (bh.columnsHeaderOffset + columnsHeaderSize > sr.columnsHeaderBuf.len) {
            std.log.err(
                "FATAL: columnsHeaderOffset={} + columnsHeaderSize={} exceeds buffer size: {}",
                .{ bh.columnsHeaderOffset, columnsHeaderSize, sr.columnsHeaderBuf.len },
            );
            return error.InvalidColumnsHeaderOffset;
        }

        const columnsHeaderBuf = sr.columnsHeaderBuf[bh.columnsHeaderOffset..][0..columnsHeaderSize];

        // Read columns header index
        const columnsHeaderIndexSize = bh.columnsHeaderIndexSize;
        if (columnsHeaderIndexSize > maxColumnsHeaderIndexSize) {
            std.log.err(
                "BUG: too big columnsHeaderIndexSize: {} bytes; mustn't exceed {} bytes",
                .{ columnsHeaderIndexSize, maxColumnsHeaderIndexSize },
            );
            return error.InvalidColumnsHeaderIndexSize;
        }

        if (bh.columnsHeaderIndexOffset + columnsHeaderIndexSize > sr.columnsHeaderIndexBuf.len) {
            std.log.err(
                "FATAL: columnsHeaderIndexOffset={} + columnsHeaderIndexSize={} exceeds buffer size: {}",
                .{ bh.columnsHeaderIndexOffset, columnsHeaderIndexSize, sr.columnsHeaderIndexBuf.len },
            );
            return error.InvalidColumnsHeaderIndexOffset;
        }

        const columnsHeaderIndexBuf = sr.columnsHeaderIndexBuf[bh.columnsHeaderIndexOffset..][0..columnsHeaderIndexSize];

        // Decode columns header index
        const cshIdx = try decodeColumnsHeaderIndex(allocator, columnsHeaderIndexBuf);
        defer cshIdx.deinit(allocator);

        // Decode ColumnIDGen from compressed buffers
        // Note: columnsKeysBuf and columnIdxsBuf contain compressed ColumnIDGen data
        // For now, we'll need to decode it properly. This might need to be passed separately.
        // TODO: Fix this - ColumnIDGen should be decoded from the table metadata, not per block
        const columnIDGen = try ColumnIDGen.decode(allocator, sr.columnsKeysBuf);
        defer columnIDGen.deinit(allocator);

        const csh = try ColumnsHeader.decode(allocator, columnsHeaderBuf, cshIdx, columnIDGen);
        defer csh.deinit(allocator);

        // Read column data
        try self.resizeColumnsData(csh.headers.len);
        for (csh.headers, 0..) |*ch, i| {
            try self.columnsData.items[i].mustReadFrom(&arena, ch, sr, columnIDGen);
        }

        // Read celled columns
        try self.celledColumns.resize(arena_allocator, csh.celledColumns.len);
        for (csh.celledColumns, 0..) |*srcCol, i| {
            self.celledColumns.items[i] = try copyColumn(&arena, srcCol);
        }
    }
};

pub const TimestampsData = struct {
    data: []const u8,

    encodingType: EncodingType,

    minTimestamp: u64,

    maxTimestamp: u64,

    pub fn mustReadFrom(
        th: *const TimestampsHeader,
        sr: *StreamReader,
    ) TimestampsData {
        const timestampsBlockSize = th.size;
        if (timestampsBlockSize > maxTimestampsBlockSize) {
            std.log.err(
                "FATAL: too big timestamps block with {} bytes; the maximum supported block size is {} bytes",
                .{ timestampsBlockSize, maxTimestampsBlockSize },
            );
            return error.InvalidTimestampsSize;
        }

        if (th.offset + timestampsBlockSize > sr.timestampsBuf.len) {
            std.log.err(
                "FATAL: timestampsHeader.offset={} + size={} exceeds buffer size: {}",
                .{ th.offset, timestampsBlockSize, sr.timestampsBuf.len },
            );
            return error.InvalidTimestampsOffset;
        }

        return .{
            .data = sr.timestampsBuf[th.offset..][0..timestampsBlockSize],
            .encodingType = th.encodingType,
            .minTimestamp = th.min,
            .maxTimestamp = th.max,
        };
    }
};

pub const ColumnData = struct {
    name: []const u8,
    valueType: ColumnType,

    minValue: u64,
    maxValue: u64,

    valuesDict: ColumnDict,
    valuesData: []const u8,

    bloomFilterData: []const u8,

    pub fn mustReadFrom(
        arena: *std.heap.ArenaAllocator,
        ch: *const ColumnHeader,
        sr: *StreamReader,
        columnIDGen: *ColumnIDGen,
    ) !void {

        self.name = try arena.allocator().dupe(u8, ch.key);
        self.valueType = ch.type;
        self.minValue = ch.min;
        self.maxValue = ch.max;
        try copyColumnDictFrom(&self.valuesDict, arena, &ch.dict);

        // Get bloom buffer for this column
        // First, get column ID from ColumnIDGen, then get bloom buffer index from colIdx
        const colID = columnIDGen.keyIDs.get(ch.key) orelse {
            // Use message bloom buffers
            if (ch.offset + ch.size > sr.messageBloomValuesBuf.len) {
                std.log.err(
                    "FATAL: columnHeader.offset={} + size={} exceeds messageBloomValuesBuf size: {}",
                    .{ ch.offset, ch.size, sr.messageBloomValuesBuf.len },
                );
                return error.InvalidColumnOffset;
            }

            const valuesSize = ch.size;
            if (valuesSize > maxValuesBlockSize) {
                std.log.err(
                    "FATAL: values block size cannot exceed {} bytes; got {} bytes",
                    .{ maxValuesBlockSize, valuesSize },
                );
                return error.InvalidValuesSize;
            }

            self.valuesData = try arena.allocator().dupe(u8, sr.messageBloomValuesBuf[ch.offset..][0..valuesSize]);

            // read bloom filter
            if (ch.type != .dict) {
                if (ch.bloomFilterOffset + ch.bloomFilterSize > sr.messageBloomTokensBuf.len) {
                    std.log.err(
                        "FATAL: columnHeader.bloomFilterOffset={} + size={} exceeds messageBloomTokensBuf size: {}",
                        .{ ch.bloomFilterOffset, ch.bloomFilterSize, sr.messageBloomTokensBuf.len },
                    );
                    return error.InvalidBloomFilterOffset;
                }

                const bloomFilterSize = ch.bloomFilterSize;
                if (bloomFilterSize > maxBloomFilterBlockSize) {
                    std.log.err(
                        "FATAL: bloom filter block size cannot exceed {} bytes; got {} bytes",
                        .{ maxBloomFilterBlockSize, bloomFilterSize },
                    );
                    return error.InvalidBloomFilterSize;
                }

                self.bloomFilterData = try arena.allocator().dupe(u8, sr.messageBloomTokensBuf[ch.bloomFilterOffset..][0..bloomFilterSize]);
            }
            return;
        };

        const bloomBufI = sr.colIdx.get(colID) orelse {
            // Column not found in colIdx, use message bloom buffers
            if (ch.offset + ch.size > sr.messageBloomValuesBuf.len) {
                std.log.err(
                    "FATAL: columnHeader.offset={} + size={} exceeds messageBloomValuesBuf size: {}",
                    .{ ch.offset, ch.size, sr.messageBloomValuesBuf.len },
                );
                return error.InvalidColumnOffset;
            }

            const valuesSize = ch.size;
            if (valuesSize > maxValuesBlockSize) {
                std.log.err(
                    "FATAL: values block size cannot exceed {} bytes; got {} bytes",
                    .{ maxValuesBlockSize, valuesSize },
                );
                return error.InvalidValuesSize;
            }

            self.valuesData = try arena.allocator().dupe(u8, sr.messageBloomValuesBuf[ch.offset..][0..valuesSize]);

            // read bloom filter
            if (ch.type != .dict) {
                if (ch.bloomFilterOffset + ch.bloomFilterSize > sr.messageBloomTokensBuf.len) {
                    std.log.err(
                        "FATAL: columnHeader.bloomFilterOffset={} + size={} exceeds messageBloomTokensBuf size: {}",
                        .{ ch.bloomFilterOffset, ch.bloomFilterSize, sr.messageBloomTokensBuf.len },
                    );
                    return error.InvalidBloomFilterOffset;
                }

                const bloomFilterSize = ch.bloomFilterSize;
                if (bloomFilterSize > maxBloomFilterBlockSize) {
                    std.log.err(
                        "FATAL: bloom filter block size cannot exceed {} bytes; got {} bytes",
                        .{ maxBloomFilterBlockSize, bloomFilterSize },
                    );
                    return error.InvalidBloomFilterSize;
                }

                self.bloomFilterData = try arena.allocator().dupe(u8, sr.messageBloomTokensBuf[ch.bloomFilterOffset..][0..bloomFilterSize]);
            }
            return;
        };
        if (bloomBufI >= sr.bloomValuesList.len) {
            std.log.err(
                "FATAL: bloomBufI={} exceeds bloomValuesList length: {}",
                .{ bloomBufI, sr.bloomValuesList.len },
            );
            return error.InvalidColumnOffset;
        }

        const bloomValuesBuf = sr.bloomValuesList[bloomBufI];
        const bloomTokensBuf = sr.bloomTokensList[bloomBufI];

        // read values
        if (ch.offset + ch.size > bloomValuesBuf.len) {
            std.log.err(
                "FATAL: columnHeader.offset={} + size={} exceeds bloomValuesBuf size: {}",
                .{ ch.offset, ch.size, bloomValuesBuf.len },
            );
            return error.InvalidColumnOffset;
        }

        const valuesSize = ch.size;
        if (valuesSize > maxValuesBlockSize) {
            std.log.err(
                "FATAL: values block size cannot exceed {} bytes; got {} bytes",
                .{ maxValuesBlockSize, valuesSize },
            );
            return error.InvalidValuesSize;
        }

        self.valuesData = try arena.allocator().dupe(u8, bloomValuesBuf[ch.offset..][0..valuesSize]);

        // read bloom filter
        // bloom filter is missing in valueTypeDict.
        if (ch.type != .dict) {
            if (ch.bloomFilterOffset + ch.bloomFilterSize > bloomTokensBuf.len) {
                std.log.err(
                    "FATAL: columnHeader.bloomFilterOffset={} + size={} exceeds bloomTokensBuf size: {}",
                    .{ ch.bloomFilterOffset, ch.bloomFilterSize, bloomTokensBuf.len },
                );
                return error.InvalidBloomFilterOffset;
            }

            const bloomFilterSize = ch.bloomFilterSize;
            if (bloomFilterSize > maxBloomFilterBlockSize) {
                std.log.err(
                    "FATAL: bloom filter block size cannot exceed {} bytes; got {} bytes",
                    .{ maxBloomFilterBlockSize, bloomFilterSize },
                );
                return error.InvalidBloomFilterSize;
            }

            self.bloomFilterData = try arena.allocator().dupe(u8, bloomTokensBuf[ch.bloomFilterOffset..][0..bloomFilterSize]);
        }
    }
};

// Helper functions

fn copyColumn(arena: *std.heap.ArenaAllocator, src: *Column) !Column {
    const values = try arena.allocator().alloc([]const u8, src.values.len);
    for (src.values, 0..) |val, i| {
        values[i] = try arena.allocator().dupe(u8, val);
    }
    return .{
        .key = try arena.allocator().dupe(u8, src.key),
        .values = values,
    };
}

// Error types
pub const Error = error{
    InvalidColumnsHeaderOffset,
    InvalidColumnsHeaderIndexOffset,
    InvalidColumnsHeaderSize,
    InvalidColumnsHeaderIndexSize,
    InvalidTimestampsSize,
    InvalidTimestampsOffset,
    InvalidValuesSize,
    InvalidBloomFilterSize,
    InvalidColumnOffset,
    InvalidBloomFilterOffset,
};

// Helper function to decode ColumnsHeaderIndex
fn decodeColumnsHeaderIndex(allocator: std.mem.Allocator, buf: []const u8) !*ColumnsHeaderIndex {
    const Decoder = @import("encoding").Decoder;
    var dec = Decoder.init(buf);

    const cshIdx = try ColumnsHeaderIndex.init(allocator);

    // Decode columns
    const columnsLen = dec.readVarInt();
    try cshIdx.columns.resize(allocator, columnsLen);
    for (0..columnsLen) |i| {
        const colID = dec.readVarInt();
        const offset = dec.readVarInt();
        cshIdx.columns.items[i] = .{
            .columndID = @intCast(colID),
            .offset = offset,
        };
    }

    // Decode celled columns
    const celledLen = dec.readVarInt();
    try cshIdx.celledColumns.resize(allocator, celledLen);
    for (0..celledLen) |i| {
        const colID = dec.readVarInt();
        const offset = dec.readVarInt();
        cshIdx.celledColumns.items[i] = .{
            .columndID = @intCast(colID),
            .offset = offset,
        };
    }

    return cshIdx;
}

// Extension methods for ColumnDict
fn copyColumnDictFrom(self: *ColumnDict, arena: *std.heap.ArenaAllocator, src: *ColumnDict) !void {
    self.reset();
    try self.values.resize(arena.allocator(), src.values.items.len);
    for (src.values.items, 0..) |val, i| {
        self.values.items[i] = try arena.allocator().dupe(u8, val);
    }
}
