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
const EncodingType = @import("TimestampsEncoder.zig").EncodingType;
const StreamReader = @import("reader.zig").StreamReader;

const maxTimestampsBlockSize = 8 * 1024 * 1024;
const maxValuesBlockSize = 8 * 1024 * 1024;
const maxColumnsHeaderSize = 8 * 1024 * 1024;
const maxColumnsHeaderIndexSize = 8 * 1024 * 1024;

pub const BlockData = struct {
    sid: SID = undefined,
    uncompressedSizeBytes: u64 = 0,
    rowsCount: u32 = 0,

    timestampsData: TimestampsData,
    columnsData: std.ArrayList(ColumnData),
    celledColumns: ?*const []Column = null,

    pub fn initEmpty() BlockData {
        return .{ .columnsData = std.ArrayList(ColumnData).empty, .timestampsData = .{} };
    }

    pub fn reset(self: *BlockData) void {
        self.sid = undefined;
        self.uncompressedSizeBytes = 0;
        self.rowsCount = 0;

        self.timestampsData = .{};
        self.columnsData.clearRetainingCapacity();
        self.celledColumns = null;
    }

    pub fn deinit(self: *BlockData, allocator: std.mem.Allocator) void {
        self.columnsData.deinit(allocator);
    }

    pub fn readFrom(
        self: *BlockData,
        allocator: std.mem.Allocator,
        bh: *const BlockHeader,
        sr: *StreamReader,
    ) !void {
        self.reset();

        self.sid = bh.sid;
        self.uncompressedSizeBytes = bh.size;
        self.rowsCount = bh.len;

        self.timestampsData = try TimestampsData.readFrom(&bh.timestampsHeader, sr);

        const columnsHeaderSize = bh.columnsHeaderSize;
        std.debug.assert(columnsHeaderSize <= maxColumnsHeaderSize);

        const columnsHeaderBuf =
            sr.columnsHeaderBuf[bh.columnsHeaderOffset..][0..columnsHeaderSize];

        // --- index ---
        const columnsHeaderIndexSize = bh.columnsHeaderIndexSize;
        std.debug.assert(columnsHeaderIndexSize <= maxColumnsHeaderIndexSize);

        const columnsHeaderIndexBuf = sr.columnsHeaderIndexBuf[bh.columnsHeaderIndexOffset..][0..columnsHeaderIndexSize];

        const cshIdx = try ColumnsHeaderIndex.decode(
            allocator,
            columnsHeaderIndexBuf,
        );
        defer cshIdx.deinit(allocator);

        const csh = try ColumnsHeader.decode(
            allocator,
            columnsHeaderBuf,
            cshIdx,
            sr.columnIDGen,
        );
        defer csh.deinit(allocator);

        try self.columnsData.ensureTotalCapacity(allocator, csh.headers.len);

        for (csh.headers) |*ch| {
            const col = try ColumnData.readFrom(ch, sr);
            self.columnsData.appendAssumeCapacity(col);
        }

        self.celledColumns = &csh.celledColumns;
    }
};

pub const TimestampsData = struct {
    data: []const u8 = undefined,

    encodingType: EncodingType = .Undefined,

    minTimestamp: u64 = 0,

    maxTimestamp: u64 = 0,

    pub fn readFrom(
        th: *const TimestampsHeader,
        sr: *const StreamReader,
    ) !TimestampsData {
        const timestampsBlockSize = th.size;
        std.debug.assert(timestampsBlockSize <= maxTimestampsBlockSize);

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
    valueType: *const ColumnType,

    minValue: u64,
    maxValue: u64,

    valuesDict: *const ColumnDict,
    valuesData: []const u8,

    bloomFilterData: ?[]const u8,

    pub fn readFrom(
        ch: *const ColumnHeader,
        sr: *const StreamReader,
    ) !ColumnData {
        const colID = sr.columnIDGen.keyIDs.get(ch.key).?;
        const bloomBufI = sr.colIdx.get(colID).?;

        const valuesSize = ch.size;
        std.debug.assert(valuesSize <= maxValuesBlockSize);

        const bloomValuesBuf = sr.bloomValuesList[bloomBufI];
        const valuesData = bloomValuesBuf[ch.offset..][0..valuesSize];

        var bloomFilterData: ?[]const u8 = null;

        if (ch.type != .dict) {
            const bloomTokensBuf = sr.bloomTokensList[bloomBufI];
            bloomFilterData = bloomTokensBuf[ch.bloomFilterOffset..][0..ch.bloomFilterSize];
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
