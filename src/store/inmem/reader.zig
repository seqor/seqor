const std = @import("std");

const encoding = @import("encoding");

const SID = @import("../lines.zig").SID;
const IndexBlockHeader = @import("IndexBlockHeader.zig");
const BlockHeader = @import("block_header.zig").BlockHeader;
const TableHeader = @import("TableHeader.zig");
const TableMem = @import("TableMem.zig");
const BlockData = @import("BlockData.zig").BlockData;
const ColumnIDGen = @import("ColumnIDGen.zig");

pub const Error = error{
    InvalidBlockOrder,
    InvalidTimestampOrder,
    InvalidTimestampRange,
    InvalidSize,
    InvalidRowCount,
    InvalidBlockCount,
    InvalidCompressedSize,
    InvalidUncompressedSize,
    InvalidIndexBlockData,
};

// TODO: check maybe i don't need allocator.create
pub const StreamReader = struct {
    timestampsBuf: []const u8,
    indexBuf: []const u8,
    metaIndexBuf: []const u8,

    columnsHeaderBuf: []const u8,
    columnsHeaderIndexBuf: []const u8,

    messageBloomValuesBuf: []const u8,
    messageBloomTokensBuf: []const u8,
    bloomValuesList: [][]const u8,
    bloomTokensList: [][]const u8,

    columnIDGen: *const ColumnIDGen,
    colIdx: *const std.AutoHashMap(u16, u16),
    columnsKeysBuf: []const u8,
    columnIdxsBuf: []const u8,

    fn asReadonly2D(
        allocator: std.mem.Allocator,
        list: *const std.ArrayList(std.ArrayList(u8)),
    ) ![][]const u8 {
        var out = try allocator.alloc([]const u8, list.items.len);
        for (list.items, 0..) |inner, i| {
            out[i] = inner.items;
        }
        return out;
    }

    pub fn init(allocator: std.mem.Allocator, tableMem: *TableMem) !*StreamReader {
        const r = try allocator.create(StreamReader);
        r.* = StreamReader{
            .timestampsBuf = tableMem.streamWriter.timestampsBuf.items,
            .indexBuf = tableMem.streamWriter.indexBuf.items,
            .metaIndexBuf = tableMem.streamWriter.metaIndexBuf.items,
            .columnsHeaderBuf = tableMem.streamWriter.columnsHeaderBuf.items,
            .columnsHeaderIndexBuf = tableMem.streamWriter.columnsHeaderIndexBuf.items,

            .messageBloomValuesBuf = tableMem.streamWriter.messageBloomValuesBuf.items,
            .messageBloomTokensBuf = tableMem.streamWriter.messageBloomTokensBuf.items,
            .bloomValuesList = try asReadonly2D(allocator, &tableMem.streamWriter.bloomValuesList),
            .bloomTokensList = try asReadonly2D(allocator, &tableMem.streamWriter.bloomTokensList),

            .columnIDGen = tableMem.streamWriter.columnIDGen,
            .colIdx = &tableMem.streamWriter.colIdx,
            .columnsKeysBuf = tableMem.streamWriter.columnKeysBuf.items,
            .columnIdxsBuf = tableMem.streamWriter.columnIdxsBuf.items,
        };
        return r;
    }

    pub fn deinit(self: *StreamReader, allocator: std.mem.Allocator) void {
        allocator.free(self.bloomTokensList);
        allocator.free(self.bloomValuesList);

        allocator.destroy(self);
    }

    /// totalBytesRead returns the total number of bytes read from all buffers.
    pub fn totalBytesRead(self: *const StreamReader) u64 {
        var total: u64 = 0;
        total += self.timestampsBuf.len;
        total += self.indexBuf.len;
        total += self.metaIndexBuf.len;
        total += self.columnsHeaderBuf.len;
        total += self.columnsHeaderIndexBuf.len;
        total += self.messageBloomValuesBuf.len;
        total += self.messageBloomTokensBuf.len;
        for (self.bloomValuesList) |buf| {
            total += buf.len;
        }
        for (self.bloomTokensList) |buf| {
            total += buf.len;
        }
        total += self.columnsKeysBuf.len;
        total += self.columnIdxsBuf.len;
        return total;
    }
};

pub const BlockReader = struct {
    blocksCount: u32,
    len: u32,
    size: u32,

    sidLast: ?SID,
    minTimestampLast: u64,

    blockHeaders: std.ArrayList(BlockHeader),
    indexBlockHeaders: []IndexBlockHeader,

    nextBlockIdx: u32,
    nextIndexBlockIdx: u32,

    tableHeader: *TableHeader,
    streamReader: *StreamReader,

    // Global stats for validation
    globalUncompressedSizeBytes: u64,
    globalRowsCount: u64,
    globalBlocksCount: u64,

    // Block data
    blockData: BlockData,

    pub fn initFromTableMem(allocator: std.mem.Allocator, tableMem: *TableMem) !*BlockReader {
        const indexBlockHeaders = try IndexBlockHeader.ReadIndexBlockHeaders(allocator, tableMem.streamWriter.metaIndexBuf.items);
        errdefer {
            for (indexBlockHeaders) |*h| h.deinit(allocator);
            allocator.free(indexBlockHeaders);
        }

        var blockHeaders = try std.ArrayList(BlockHeader).initCapacity(allocator, 64);
        errdefer blockHeaders.deinit(allocator);

        const tableHeader = tableMem.tableHeader;
        const streamReader = try StreamReader.init(allocator, tableMem);
        errdefer streamReader.deinit(allocator);

        const br = try allocator.create(BlockReader);
        errdefer allocator.destroy(br);

        br.* = .{
            .blocksCount = 0,
            .len = 0,
            .size = 0,

            .sidLast = null,
            .minTimestampLast = 0,

            .blockHeaders = blockHeaders,
            .indexBlockHeaders = indexBlockHeaders,

            .nextBlockIdx = 0,
            .nextIndexBlockIdx = 0,

            .tableHeader = tableHeader,
            .streamReader = streamReader,

            .globalUncompressedSizeBytes = 0,
            .globalRowsCount = 0,
            .globalBlocksCount = 0,

            .blockData = BlockData.initEmpty(),
        };
        return br;
    }

    pub fn deinit(self: *BlockReader, allocator: std.mem.Allocator) void {
        self.blockHeaders.deinit(allocator);
        self.streamReader.deinit(allocator);
        self.blockData.deinit(allocator);

        for (self.indexBlockHeaders) |*bh| {
            bh.deinit(allocator);
        }
        allocator.free(self.indexBlockHeaders);

        allocator.destroy(self);
    }

    /// NextBlock reads the next block from the reader and puts it into blockData.
    /// Returns false if there are no more blocks.
    /// blockData is valid until the next call to NextBlock().
    pub fn NextBlock(self: *BlockReader, allocator: std.mem.Allocator) !bool {
        // Load more blocks if needed
        while (self.nextBlockIdx >= self.blockHeaders.items.len) {
            if (!try self.nextIndexBlock(allocator)) {
                return false;
            }
        }

        const ih = &self.indexBlockHeaders[self.nextIndexBlockIdx - 1];
        const bh = &self.blockHeaders.items[self.nextBlockIdx];
        const th = &bh.timestampsHeader;

        // Validate bh
        if (self.sidLast) |sidLast| {
            if (bh.sid.lessThan(&sidLast)) {
                std.log.err("FATAL: blockHeader.streamID cannot be smaller than the streamID from the previously read block", .{});
                return error.InvalidBlockOrder;
            }
            if (bh.sid.eql(&sidLast) and th.min < self.minTimestampLast) {
                std.log.err("FATAL: timestamps.minTimestamp={} cannot be smaller than the minTimestamp for the previously read block for the same streamID: {}", .{ th.min, self.minTimestampLast });
                return error.InvalidTimestampOrder;
            }
        }
        self.minTimestampLast = th.min;
        self.sidLast = bh.sid;

        if (th.min < ih.minTs) {
            std.log.err("FATAL: timestampsHeader.minTimestamp={} cannot be smaller than indexBlockHeader.minTimestamp={}", .{ th.min, ih.minTs });
            return error.InvalidTimestampRange;
        }
        if (th.max > ih.maxTs) {
            std.log.err("FATAL: timestampsHeader.maxTimestamp={} cannot be bigger than indexBlockHeader.maxTimestamp={}", .{ th.max, ih.maxTs });
            return error.InvalidTimestampRange;
        }

        try self.blockData.readFrom(allocator, bh, self.streamReader);

        self.globalUncompressedSizeBytes += bh.size;
        self.globalRowsCount += bh.len;
        self.globalBlocksCount += 1;

        // Validate against tableHeader
        if (self.globalUncompressedSizeBytes > self.tableHeader.uncompressedSize) {
            std.log.err("FATAL: too big size of entries read: {}; mustn't exceed tableHeader.uncompressedSize={}", .{ self.globalUncompressedSizeBytes, self.tableHeader.uncompressedSize });
            return error.InvalidSize;
        }
        if (self.globalRowsCount > self.tableHeader.len) {
            std.log.err("FATAL: too many log entries read so far: {}; mustn't exceed tableHeader.len={}", .{ self.globalRowsCount, self.tableHeader.len });
            return error.InvalidRowCount;
        }
        if (self.globalBlocksCount > self.tableHeader.blocksCount) {
            std.log.err("FATAL: too many blocks read so far: {}; mustn't exceed tableHeader.blocksCount={}", .{ self.globalBlocksCount, self.tableHeader.blocksCount });
            return error.InvalidBlockCount;
        }

        // The block has been successfully read
        self.nextBlockIdx += 1;
        return true;
    }

    fn nextIndexBlock(self: *BlockReader, allocator: std.mem.Allocator) !bool {
        if (self.nextIndexBlockIdx >= self.indexBlockHeaders.len) {
            // No more blocks left
            // Validate tableHeader
            const totalBytesRead = self.streamReader.totalBytesRead();
            if (self.tableHeader.compressedSize != totalBytesRead) {
                std.log.err("FATAL: tableHeader.compressedSize={} must match the size of data read: {}", .{ self.tableHeader.compressedSize, totalBytesRead });
                return error.InvalidCompressedSize;
            }
            if (self.tableHeader.uncompressedSize != self.globalUncompressedSizeBytes) {
                std.log.err("FATAL: tableHeader.uncompressedSize={} must match the size of entries read: {}", .{ self.tableHeader.uncompressedSize, self.globalUncompressedSizeBytes });
                return error.InvalidUncompressedSize;
            }
            if (self.tableHeader.len != self.globalRowsCount) {
                std.log.err("FATAL: tableHeader.len={} must match the number of log entries read: {}", .{ self.tableHeader.len, self.globalRowsCount });
                return error.InvalidRowCount;
            }
            if (self.tableHeader.blocksCount != self.globalBlocksCount) {
                std.log.err("FATAL: tableHeader.blocksCount={} must match the number of blocks read: {}", .{ self.tableHeader.blocksCount, self.globalBlocksCount });
                return error.InvalidBlockCount;
            }
            return false;
        }

        const ih = &self.indexBlockHeaders[self.nextIndexBlockIdx];

        // Validate ih
        if (ih.minTs < self.tableHeader.minTimestamp) {
            std.log.err("FATAL: indexBlockHeader.minTimestamp={} cannot be smaller than tableHeader.minTimestamp={}", .{ ih.minTs, self.tableHeader.minTimestamp });
            return error.InvalidTimestampRange;
        }
        if (ih.maxTs > self.tableHeader.maxTimestamp) {
            std.log.err("FATAL: indexBlockHeader.maxTimestamp={} cannot be bigger than tableHeader.maxTimestamp={}", .{ ih.maxTs, self.tableHeader.maxTimestamp });
            return error.InvalidTimestampRange;
        }

        const indexBlockData = try readIndexBlock(allocator, ih, self.streamReader);
        defer allocator.free(indexBlockData);

        self.blockHeaders.clearRetainingCapacity();
        try BlockHeader.decodeFew(allocator, &self.blockHeaders, indexBlockData);

        self.nextIndexBlockIdx += 1;
        self.nextBlockIdx = 0;
        return true;
    }
};

fn readIndexBlock(
    allocator: std.mem.Allocator,
    ih: *const IndexBlockHeader,
    streamReader: *StreamReader,
) ![]u8 {
    // Bounds checking
    if (ih.offset > streamReader.indexBuf.len) {
        return error.InvalidIndexBlockData;
    }
    if (ih.offset + ih.size > streamReader.indexBuf.len) {
        return error.InvalidIndexBlockData;
    }

    const compressed = streamReader.indexBuf[ih.offset..][0..ih.size];
    const decompressedSize = try encoding.getFrameContentSize(compressed);
    const decompressed = try allocator.alloc(u8, decompressedSize);
    errdefer allocator.free(decompressed);

    _ = try encoding.decompress(decompressed, compressed);
    return decompressed;
}

const Line = @import("../lines.zig").Line;
const Field = @import("../lines.zig").Field;

const SampleLines = struct {
    fields1: [2]Field,
    fields2: [2]Field,
    fields3: [2]Field,
    lines: [3]Line,
};

fn populateSampleLines(sample: *SampleLines) void {
    sample.fields1 = .{
        .{ .key = "level", .value = "info" },
        .{ .key = "app", .value = "seq" },
    };
    sample.fields2 = .{
        .{ .key = "level", .value = "warn" },
        .{ .key = "app", .value = "seq" },
    };
    sample.fields3 = .{
        .{ .key = "level", .value = "warn" },
        .{ .key = "app", .value = "seq" },
    };
    sample.lines = .{
        .{
            .timestampNs = 1,
            .sid = .{ .id = 2, .tenantID = "2222" },
            .fields = sample.fields1[0..],
        },
        .{
            .timestampNs = 2,
            .sid = .{ .id = 1, .tenantID = "1111" },
            .fields = sample.fields2[0..],
        },
        .{
            .timestampNs = 3,
            .sid = .{ .id = 1, .tenantID = "1111" },
            .fields = sample.fields3[0..],
        },
    };
}

test "readBlock reads buffers" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, testReadBlock, .{});
}

fn testReadBlock(allocator: std.mem.Allocator) !void {
    var sample: SampleLines = SampleLines{
        .fields1 = undefined,
        .fields2 = undefined,
        .fields3 = undefined,
        .lines = undefined,
    };
    populateSampleLines(&sample);

    // Unordered timestamps in lines so that it tests sorting.
    // line[0]: ts=1, sid=(2,"2222"); line[1]: ts=2, sid=(1,"1111"); line[2]: ts=3, sid=(1,"1111")
    // After sort by (sid, ts): first block (1111,1) 2 rows (ts 2,3), second block (2222,2) 1 row (ts 1).
    var lines = [3]*const Line{
        &sample.lines[0],
        &sample.lines[1],
        &sample.lines[2],
    };

    const memTable = try TableMem.init(allocator);
    defer memTable.deinit(allocator);
    try memTable.addLines(allocator, lines[0..]);

    const th = memTable.tableHeader;
    try std.testing.expectEqual(@as(u32, 3), th.len);
    try std.testing.expect(th.minTimestamp <= 1);
    try std.testing.expect(th.maxTimestamp >= 3);
    try std.testing.expect(th.blocksCount >= 1);
    try std.testing.expect(th.uncompressedSize > 0);
    try std.testing.expect(th.compressedSize > 0);

    const blockReader = try BlockReader.initFromTableMem(allocator, memTable);
    defer blockReader.deinit(allocator);

    var blocksRead: u32 = 0;
    while (try blockReader.NextBlock(allocator)) {
        blocksRead += 1;
    }

    try std.testing.expectEqual(@as(u32, 2), blocksRead);
    try std.testing.expectEqual(@as(u64, 2), blockReader.globalBlocksCount);
    try std.testing.expectEqual(@as(u64, 3), blockReader.globalRowsCount);
    try std.testing.expectEqual(th.uncompressedSize, blockReader.globalUncompressedSizeBytes);
    try std.testing.expectEqual(th.len, blockReader.globalRowsCount);
    try std.testing.expectEqual(th.blocksCount, blockReader.globalBlocksCount);
    try std.testing.expectEqual(th.compressedSize, blockReader.streamReader.totalBytesRead());

    // Second pass: check each block's blockData (sid, rowsCount, timestamps range)
    const blockReader2 = try BlockReader.initFromTableMem(allocator, memTable);
    defer blockReader2.deinit(allocator);

    var block1Sid1111 = false;
    var block2Sid2222 = false;
    var blocksWithFullData: u32 = 0;
    while (try blockReader2.NextBlock(allocator)) {
        const bd = &blockReader2.blockData;
        try std.testing.expect(bd.rowsCount >= 1);
        try std.testing.expect(bd.uncompressedSizeBytes > 0);
        try std.testing.expect(bd.timestampsData.minTimestamp <= bd.timestampsData.maxTimestamp);
        // columnsData may be empty in allocation-failure runs from checkAllocationFailures
        if (bd.columnsData.items.len >= 2) {
            blocksWithFullData += 1;
            if (std.mem.eql(u8, bd.sid.tenantID, "1111") and bd.sid.id == 1) {
                try std.testing.expectEqual(@as(u32, 2), bd.rowsCount);
                try std.testing.expectEqual(@as(u64, 2), bd.timestampsData.minTimestamp);
                try std.testing.expectEqual(@as(u64, 3), bd.timestampsData.maxTimestamp);
                block1Sid1111 = true;
            } else if (std.mem.eql(u8, bd.sid.tenantID, "2222") and bd.sid.id == 2) {
                try std.testing.expectEqual(@as(u32, 1), bd.rowsCount);
                try std.testing.expectEqual(@as(u64, 1), bd.timestampsData.minTimestamp);
                try std.testing.expectEqual(@as(u64, 1), bd.timestampsData.maxTimestamp);
                block2Sid2222 = true;
            }
        }
    }
    // When both blocks were read with full data (no alloc failure), both sids must be present
    if (blocksWithFullData == 2) {
        try std.testing.expect(block1Sid1111);
        try std.testing.expect(block2Sid2222);
    }
}
