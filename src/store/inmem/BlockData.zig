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

const maxTimestampsBlockSize = 8 * 1024 * 1024;
const maxValuesBlockSize = 8 * 1024 * 1024;
const maxBloomFilterBlockSize = 8 * 1024 * 1024;
const maxColumnsHeaderSize = 8 * 1024 * 1024;
const maxColumnsHeaderIndexSize = 8 * 1024 * 1024;

pub const BlockData = struct {
    sid: SID,
    uncompressedSizeBytes: u64,
    rowsCount: u32,

    timestampsData: TimestampsData,
    columnsData: std.ArrayList(ColumnData),
    celledColumns: *const []Column,

    pub fn deinit(self: *BlockData, allocator: std.mem.Allocator) void {
        self.columnsData.deinit(allocator);
    }

    pub fn readFrom(
        allocator: std.mem.Allocator,
        bh: *const BlockHeader,
        sr: *StreamReader,
    ) !BlockData {
        const sid = bh.sid;
        const uncompressedSizeBytes = bh.size;
        const rowsCount = bh.len;

        const timestampsData = TimestampsData.mustReadFrom(&bh.timestampsHeader, &sr);

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

        const cshIdx = try ColumnsHeaderIndex.decode(allocator, columnsHeaderIndexBuf);
        defer cshIdx.deinit(allocator);

        const csh = try ColumnsHeader.decode(allocator, columnsHeaderBuf, &cshIdx, &sr.columnIDGen);
        defer csh.deinit(allocator);

        const columnsData = try std.ArrayList(ColumnData).initCapacity(allocator, csh.headers.len);

        for (csh.headers) |*ch| {
            const col = try ColumnData.mustReadFrom(ch, &sr);
            columnsData.appendAssumeCapacity(col);
        }

        return .{
            .sid = sid,
            .uncompressedSizeBytes = uncompressedSizeBytes,
            .rowsCount = rowsCount,

            .timestampsData = timestampsData,
            .columnsData = columnsData,
            .celledColumns = &csh.celledColumns,
        };
    }
};

pub const TimestampsData = struct {
    data: []const u8,

    encodingType: EncodingType,

    minTimestamp: u64,

    maxTimestamp: u64,

    pub fn mustReadFrom(
        th: *const TimestampsHeader,
        sr: *const StreamReader,
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
    valueType: *ColumnType,

    minValue: u64,
    maxValue: u64,

    valuesDict: *ColumnDict,
    valuesData: []const u8,

    bloomFilterData: ?[]const u8,

    pub fn mustReadFrom(
        ch: *const ColumnHeader,
        sr: *const StreamReader,
    ) !ColumnData {
        const colID = try sr.columnIDGen.keyIDs.get(ch.key);
        const bloomBufI = try sr.colIdx.get(colID);

        if (bloomBufI >= sr.bloomValuesList.len) {
            std.log.err(
                "FATAL: bloomBufI={} exceeds bloomValuesList length: {}",
                .{ bloomBufI, sr.bloomValuesList.len },
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

        const bloomValuesBuf = sr.bloomValuesList[bloomBufI];
        const valuesData = bloomValuesBuf[ch.offset..][0..valuesSize];

        var bloomFilterData: ?[]const u8 = null;

        if (ch.type != .dict) {
            const bloomTokensBuf = sr.bloomTokensList[bloomBufI];
            bloomFilterData = bloomTokensBuf[ch.bloomFilterOffset][0..ch.bloomFilterSize];
        }

        return .{
            .name = ch.key,
            .valueType = &ch.type,

            .minValue = ch.min,
            .maxValue = ch.max,

            .valuesDict = &ch.dict,
            .valuesData = valuesData,

            .bloomFilterData = bloomFilterData,
        };
    }
};

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
