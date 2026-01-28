const std = @import("std");
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

    columnIDGen: *ColumnIDGen,
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
            .bloomValuesList = asReadonly2D(allocator, &tableMem.streamWriter.bloomValuesList),
            .bloomTokensList = asReadonly2D(allocator, &tableMem.streamWriter.bloomTokensList),

            .columnIDGen = &tableMem.streamWriter.columnIDGen,
            .colIdx = &tableMem.streamWriter.colIdx,
            .columnsKeysBuf = tableMem.streamWriter.columnKeysBuf,
            .columnIdxsBuf = tableMem.streamWriter.columnIdxsBuf,
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
    blockData: *BlockData,

    pub fn initFromTableMem(allocator: std.mem.Allocator, tableMem: *TableMem) !*BlockReader {
        const indexBlockHeaders = try IndexBlockHeader.ReadIndexBlockHeaders(allocator, tableMem.streamWriter.metaIndexBuf.items);
        const blockHeaders = try std.ArrayList(BlockHeader).initCapacity(allocator, 64);
        const tableHeader = tableMem.tableHeader;
        const streamReader = try StreamReader.init(allocator, tableMem);

        const br = try allocator.create(BlockReader);

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

            .blockData = try BlockData.init(allocator),
        };
        return br;
    }

    pub fn deinit(self: *BlockReader, allocator: std.mem.Allocator) void {
        self.blockHeaders.deinit(allocator);
        self.streamReader.deinit(allocator);
        self.blockData.deinit();

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

    /// nextIndexBlock advances to the next index block and loads its block headers.
    /// Returns false if there are no more index blocks.
    fn nextIndexBlock(self: *BlockReader, allocator: std.mem.Allocator) !bool {
        // Advance to the next indexBlockHeader
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

        // Read indexBlock for the given ih
        const indexBlockData = try readIndexBlock(allocator, ih, self.streamReader);
        defer allocator.free(indexBlockData);

        // Reset and unmarshal block headers
        self.blockHeaders.clearRetainingCapacity();
        try unmarshalBlockHeaders(allocator, &self.blockHeaders, indexBlockData, self.tableHeader.version);

        self.nextIndexBlockIdx += 1;
        self.nextBlockIdx = 0;
        return true;
    }
};


/// readIndexBlock reads the index block data from the stream reader using the index block header.
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
    const decompressedSize = try @import("encoding").getFrameContentSize(compressed);
    var decompressed = try allocator.alloc(u8, decompressedSize);
    errdefer allocator.free(decompressed);

    try @import("encoding").decompress(decompressed, compressed);
    return decompressed;
}

/// unmarshalBlockHeaders unmarshals block headers from the index block data.
fn unmarshalBlockHeaders(
    allocator: std.mem.Allocator,
    blockHeaders: *std.ArrayList(BlockHeader),
    indexBlockData: []const u8,
    formatVersion: u8,
) !void {
    _ = formatVersion; // TODO: use formatVersion if needed

    var decoder = @import("encoding").Decoder.init(indexBlockData);
    while (decoder.offset < indexBlockData.len) {
        // Check if we have enough data for at least the fixed-size parts
        const remaining = indexBlockData.len - decoder.offset;
        if (remaining < 32 + 4 + 4 + 33) { // sid + size + len + timestampsHeader minimum
            if (remaining > 0) {
                return error.InvalidIndexBlockData;
            }
            break;
        }
        
        // Read SID (32 bytes)
        const sid = SID.decode(decoder.readBytes(32));
        
        // Read size and len (fixed 4 bytes each)
        const size = decoder.readInt(u32);
        const len = decoder.readInt(u32);
        
        // Read timestampsHeader (33 bytes: 4*8 + 1)
        const timestampsHeader = BlockHeader.TimestampsHeader.decode(&decoder);
        
        // Read varints for columns header offsets/sizes
        const columnsHeaderIndexOffset = decoder.readVarInt();
        const columnsHeaderIndexSize = decoder.readVarInt();
        const columnsHeaderOffset = decoder.readVarInt();
        const columnsHeaderSize = decoder.readVarInt();
        
        const header = BlockHeader{
            .sid = sid,
            .size = size,
            .len = len,
            .timestampsHeader = timestampsHeader,
            .columnsHeaderOffset = columnsHeaderOffset,
            .columnsHeaderSize = columnsHeaderSize,
            .columnsHeaderIndexOffset = columnsHeaderIndexOffset,
            .columnsHeaderIndexSize = columnsHeaderIndexSize,
        };
        
        try blockHeaders.append(header);
    }
}
