const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;

const Heap = @import("stds/Heap.zig").Heap;
const MemOrder = @import("stds/sort.zig").MemOrder;

const encoding = @import("encoding");
const Encoder = encoding.Encoder;

const fs = @import("fs.zig");

const IndexKind = @import("Index.zig").IndexKind;

const TagRecordsMerger = @import("TagRecordsMerger.zig");
const Entries = @import("Entries.zig");
const MemBlock = @import("MemBlock.zig");
const BlockHeader = @import("BlockHeader.zig");
const StorageBlock = @import("StorageBlock.zig");

const maxBlocksPerShard = 256;

// TODO: worth tuning on practice
const blocksInMemTable = 15;

const maxStreamsPerRecord = 32;

const maxIndexBlockSize = 64 * 1024;

const filenameMeta = "metadata.json";

const Self = @This();

flushInterval: u64,
entries: *Entries,

blocks: std.ArrayList(*MemBlock) = .empty,
mxBlocks: std.Thread.Mutex = .{},
flushAtUs: ?i64 = null,

pub fn init(alloc: Allocator, flushInterval: u64) !*Self {
    const entries = try Entries.init(alloc);
    errdefer entries.deinit(alloc);

    const t = try alloc.create(Self);
    t.* = .{
        .flushInterval = flushInterval,
        .entries = entries,
    };
    return t;
}

pub fn add(self: *Self, alloc: Allocator, entries: [][]const u8) !void {
    const shard = self.entries.next();
    const blocks = try shard.add(alloc, entries);
    if (blocks.len == 0) return;
    try self.flushBlocks(alloc, blocks);
}

fn flushBlocks(self: *Self, alloc: Allocator, blocks: []*MemBlock) !void {
    self.mxBlocks.lock();
    defer self.mxBlocks.unlock();

    if (self.blocks.items.len == 0) {
        self.flushAtUs = std.time.microTimestamp() + std.time.us_per_s;
    }

    try self.blocks.appendSlice(alloc, blocks);
    if (self.blocks.items.len >= maxBlocksPerShard * self.entries.shards.len) {
        try self.flush(alloc, self.blocks.items, false);
        self.blocks.clearRetainingCapacity();
    }
}

fn flush(self: *Self, alloc: Allocator, blocks: []*MemBlock, force: bool) !void {
    const tablesSize = (blocks.len + blocksInMemTable - 1) / blocksInMemTable;
    var memTables = try std.ArrayList(*MemTable).initCapacity(alloc, tablesSize);
    errdefer {
        for (memTables.items) |memTable| memTable.deinit(alloc);
        memTables.deinit(alloc);
    }

    var tail = blocks[0..];
    // TODO: benchmark parallel mem table creation
    while (tail.len > 0) {
        const offset = @min(blocksInMemTable, tail.len);
        const head = tail[0..offset];
        tail = tail[offset..];

        const memTable = try MemTable.init(alloc, head);
        memTables.appendAssumeCapacity(memTable);
    }

    _ = self;
    _ = force;
    // TODO: merge tables merfe adding them
    // var n: usize = 0;
    // var memTableSlice = memTables.items[n..];
    // while (memTableSlice.len > 1) {
    //     n = self.mergeMemTables(alloc, memTableSlice);
    //     memTableSlice = memTableSlice[n..];
    // }
    // if (memTableSlice.len == 1) {
    //     self.addToMemTables(alloc, memTableSlice[0], force);
    // }

    // for (memTables.items) |memTable| {
    //     self.addToMemTables(alloc, memTable, force);
    // }
}

const TableHeader = struct {
    itemsCount: u64,
    blocksCount: u64,
    firstItem: []const u8,
    lastItem: []const u8,

    pub fn writeMeta(self: *const TableHeader, alloc: Allocator, tablePath: []const u8) !void {
        const json = try std.json.Stringify.valueAlloc(alloc, .{
            .itemsCount = self.itemsCount,
            .blocksCount = self.blocksCount,
            .firstItem = self.firstItem,
            .lastItem = self.lastItem,
        }, .{ .whitespace = .minified });
        defer alloc.free(json);

        const metadataPath = try std.fs.path.join(alloc, &[_][]const u8{ tablePath, filenameMeta });
        defer alloc.free(metadataPath);

        try fs.writeBufferValToFile(metadataPath, json);
    }
};

const MetaIndex = struct {
    firstItem: []const u8 = "",
    blockHeadersCount: u32 = 0,
    indexBlockOffset: u64 = 0,
    indexBlockSize: u32 = 0,

    fn reset(self: *MetaIndex) void {
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

    fn encodeAlloc(self: *const MetaIndex, alloc: Allocator) ![]u8 {
        const buf = try alloc.alloc(u8, self.bound());

        self.encode(buf);

        return buf;
    }
};

const MemTable = struct {
    blockHeader: BlockHeader,
    tableHeader: TableHeader,
    metaIndex: MetaIndex,
    dataBuf: std.ArrayList(u8) = .empty,
    lensBuf: std.ArrayList(u8) = .empty,
    indexBuf: std.ArrayList(u8) = .empty,
    metaindexBuf: std.ArrayList(u8) = .empty,

    flushAtUs: ?i64 = null,

    pub fn init(alloc: Allocator, blocks: []*MemBlock) !*MemTable {
        var readers = try std.ArrayList(*BlockReader).initCapacity(alloc, blocks.len);
        defer {
            for (readers.items) |reader| reader.deinit(alloc);
            readers.deinit(alloc);
        }
        const t = try alloc.create(MemTable);
        errdefer alloc.destroy(t);

        if (blocks.len == 1) {
            // nothing to merge
            const b = blocks[0];

            const flushAtUs = std.time.microTimestamp() + std.time.us_per_s;
            try t.setup(alloc, b, flushAtUs);
            return t;
        }

        for (0..blocks.len) |i| {
            const reader = try BlockReader.initFromMemBlock(alloc, blocks[i]);
            readers.appendAssumeCapacity(reader);
        }

        const flushAtUs = std.time.microTimestamp() + std.time.us_per_s;
        try t.mergeIntoMemTable(alloc, &readers, flushAtUs);
        return t;
    }

    pub fn deinit(self: *MemTable, alloc: Allocator) void {
        self.dataBuf.deinit(alloc);
        self.lensBuf.deinit(alloc);
        self.indexBuf.deinit(alloc);
        self.metaindexBuf.deinit(alloc);
        alloc.destroy(self);
    }

    fn setup(self: *MemTable, alloc: Allocator, block: *MemBlock, flushAtUs: i64) !void {
        block.sortData();
        self.flushAtUs = flushAtUs;

        var sb = StorageBlock{};
        const encodedBlock = try block.encode(alloc, &sb);
        self.blockHeader.firstItem = encodedBlock.firstItem;
        self.blockHeader.prefix = encodedBlock.prefix;
        self.blockHeader.itemsCount = encodedBlock.itemsCount;
        self.blockHeader.encodingType = encodedBlock.encodingType;

        self.tableHeader = .{
            .itemsCount = @intCast(block.data.items.len),
            .blocksCount = 1,
            .firstItem = block.data.items[0],
            .lastItem = block.data.items[block.data.items.len - 1],
        };

        try self.dataBuf.appendSlice(alloc, sb.itemsData.items);
        self.blockHeader.itemsBlockOffset = 0;
        self.blockHeader.itemsBlockSize = @intCast(sb.itemsData.items.len);

        try self.lensBuf.appendSlice(alloc, sb.lensData.items);
        self.blockHeader.lensBlockOffset = 0;
        self.blockHeader.lensBlockSize = @intCast(sb.lensData.items.len);

        const encodedBlockHeader = try self.blockHeader.encodeAlloc(alloc);
        defer alloc.free(encodedBlockHeader);

        var bound = try encoding.compressBound(encodedBlockHeader.len);
        const compressed = try alloc.alloc(u8, bound);
        var n = try encoding.compressAuto(compressed, encodedBlockHeader);
        try self.indexBuf.appendSlice(alloc, compressed[0..n]);

        self.metaIndex.firstItem = self.blockHeader.firstItem;
        self.metaIndex.blockHeadersCount = 1;
        self.metaIndex.indexBlockOffset = 0;
        self.metaIndex.indexBlockSize = @intCast(n);

        var fbaFallback = std.heap.stackFallback(128, alloc);
        var fba = fbaFallback.get();
        const encodedMetaIndex = try self.metaIndex.encodeAlloc(fba);
        defer fba.free(encodedMetaIndex);

        bound = try encoding.compressBound(encodedMetaIndex.len);
        const compressedMr = try alloc.alloc(u8, bound);
        defer alloc.free(compressedMr);
        n = try encoding.compressAuto(compressedMr, encodedMetaIndex);

        try self.metaindexBuf.appendSlice(alloc, compressedMr[0..n]);
    }

    fn mergeIntoMemTable(
        self: *MemTable,
        alloc: Allocator,
        readers: *std.ArrayList(*BlockReader),
        flushAtUs: i64,
    ) !void {
        self.flushAtUs = flushAtUs;

        var outItemsCount: u64 = 0;
        for (readers.items) |reader| outItemsCount += reader.tableHeader.itemsCount;

        // TODO: init it inside mergeBlocks
        var blockWriter = BlockWriter.initFromMemTable(self);
        try self.mergeTables(alloc, "", &blockWriter, readers);
    }

    // FIXME: make it just mergeBlocks
    fn mergeTables(
        self: *MemTable,
        alloc: Allocator,
        tablePath: []const u8,
        writer: *BlockWriter,
        readers: *std.ArrayList(*BlockReader),
    ) !void {
        try self.mergeBlocks(alloc, writer, readers, null);
        if (tablePath.len != 0) {
            var fbaFallback = std.heap.stackFallback(512, alloc);
            const fba = fbaFallback.get();
            try self.tableHeader.writeMeta(fba, tablePath);
        }
    }

    fn mergeBlocks(
        self: *MemTable,
        alloc: Allocator,
        writer: *BlockWriter,
        readers: *std.ArrayList(*BlockReader),
        stopped: ?*std.atomic.Value(bool),
    ) !void {
        var merger = try BlockMerger.init(alloc, readers);

        // TODO: perhaps easier making it return TableHeader value and assign to MemTable,
        // make sure there are no accumulations in the table header
        try merger.merge(alloc, writer, &self.tableHeader, stopped);
        try writer.close(alloc);
    }
};

const BlockReader = struct {
    block: *MemBlock,
    tableHeader: TableHeader,

    // state

    // currentI defines a current item of the block
    currentI: usize,
    // read defines if the block has been read
    read: bool,

    pub fn initFromMemBlock(alloc: Allocator, block: *MemBlock) !*BlockReader {
        block.sortData();

        const r = try alloc.create(BlockReader);
        r.* = .{
            .block = block,
            .tableHeader = .{
                .blocksCount = undefined,
                .firstItem = undefined,
                .itemsCount = undefined,
                .lastItem = undefined,
            },
            .currentI = undefined,
            .read = false,
        };
        return r;
    }

    pub fn deinit(self: *BlockReader, alloc: Allocator) void {
        self.block.deinit(alloc);
        alloc.destroy(self);
    }

    pub fn blockReaderLessThan(one: *BlockReader, another: *BlockReader) bool {
        const first = one.current();
        const second = another.current();
        return std.mem.lessThan(u8, first, second);
    }

    pub inline fn current(self: *BlockReader) []const u8 {
        return self.block.data.items[self.currentI];
    }

    pub fn next(self: *BlockReader) !bool {
        // TODO: implement disk block reading

        if (self.read) return false;

        self.read = true;
        return true;
    }
};

const BlockWriter = struct {
    dataBuf: *std.ArrayList(u8),
    lensBuf: *std.ArrayList(u8),
    indexBuf: *std.ArrayList(u8),
    metaindexBuf: *std.ArrayList(u8),

    bh: BlockHeader = .{ .firstItem = undefined, .prefix = undefined, .encodingType = undefined },
    mr: MetaIndex = .{},

    itemsBlockOffset: u64 = 0,
    lensBlockOffset: u64 = 0,

    sb: StorageBlock = .{},
    uncompressedIndexBlockBuf: std.ArrayList(u8) = .empty,
    uncompressedMetaindexBuf: std.ArrayList(u8) = .empty,

    indexBlockOffset: u64 = 0,

    fn initFromMemTable(memTable: *MemTable) BlockWriter {
        return .{
            .dataBuf = &memTable.dataBuf,
            .lensBuf = &memTable.lensBuf,
            .indexBuf = &memTable.indexBuf,
            .metaindexBuf = &memTable.metaindexBuf,
        };
    }

    fn writeBlock(self: *BlockWriter, alloc: Allocator, block: *MemBlock) !void {
        const encoded = try block.encode(alloc, &self.sb);
        self.bh.firstItem = encoded.firstItem;
        self.bh.prefix = encoded.prefix;
        self.bh.itemsCount = encoded.itemsCount;
        self.bh.encodingType = encoded.encodingType;

        // Write data
        try self.dataBuf.appendSlice(alloc, self.sb.itemsData.items);
        self.bh.itemsBlockSize = @intCast(self.sb.itemsData.items.len);
        self.bh.itemsBlockOffset = self.itemsBlockOffset;
        self.itemsBlockOffset += self.bh.itemsBlockSize;

        // Write lens
        try self.lensBuf.appendSlice(alloc, self.sb.lensData.items);
        self.bh.lensBlockSize = @intCast(self.sb.lensData.items.len);
        self.bh.lensBlockOffset = self.lensBlockOffset;
        self.lensBlockOffset += self.bh.lensBlockSize;

        // Write block header
        const bhEncodeBound = self.bh.bound();
        if (self.uncompressedIndexBlockBuf.items.len + bhEncodeBound > maxIndexBlockSize) {
            try self.flushIndexData(alloc);
        }
        try self.uncompressedIndexBlockBuf.ensureUnusedCapacity(alloc, bhEncodeBound);
        self.bh.encode(self.uncompressedIndexBlockBuf.unusedCapacitySlice());
        self.uncompressedIndexBlockBuf.items.len += bhEncodeBound;

        // Write block header
        if (self.mr.firstItem.len == 0) {
            self.mr.firstItem = self.bh.firstItem;
        }
        self.bh.reset();
        self.mr.blockHeadersCount += 1;
    }

    fn flushIndexData(self: *BlockWriter, alloc: Allocator) !void {
        if (self.uncompressedIndexBlockBuf.items.len == 0) {
            // Nothing to flush.
            return;
        }

        // Write indexBlock
        const bound = try encoding.compressBound(self.uncompressedIndexBlockBuf.items.len);
        try self.indexBuf.ensureUnusedCapacity(alloc, bound);
        const n = try encoding.compressAuto(
            self.indexBuf.unusedCapacitySlice(),
            self.uncompressedIndexBlockBuf.items,
        );
        self.indexBuf.items.len += n;

        self.mr.indexBlockSize = @intCast(n);
        self.mr.indexBlockOffset = self.indexBlockOffset;
        self.indexBlockOffset += self.mr.indexBlockSize;
        self.uncompressedIndexBlockBuf.clearRetainingCapacity();

        // Write metaindex
        const mrBound = self.mr.bound();
        try self.uncompressedMetaindexBuf.ensureUnusedCapacity(alloc, mrBound);
        self.mr.encode(self.uncompressedMetaindexBuf.unusedCapacitySlice());
        self.uncompressedMetaindexBuf.items.len += mrBound;

        self.mr.reset();
    }

    fn close(self: *BlockWriter, alloc: Allocator) !void {
        try self.flushIndexData(alloc);

        const bound = try encoding.compressBound(self.uncompressedMetaindexBuf.items.len);
        try self.metaindexBuf.ensureUnusedCapacity(alloc, bound);
        const n = try encoding.compressAuto(
            self.metaindexBuf.unusedCapacitySlice(),
            self.uncompressedMetaindexBuf.items,
        );
        self.metaindexBuf.items.len += n;
    }
};

const BlockMerger = struct {
    heap: Heap(*BlockReader, BlockReader.blockReaderLessThan),
    block: *MemBlock,
    firstItem: []const u8 = &[_]u8{},
    lastItem: []const u8 = &[_]u8{},

    fn init(alloc: Allocator, readers: *std.ArrayList(*BlockReader)) !BlockMerger {
        // TODO: collect metrics and experiment with flat array on 1-3 elements

        // TODO: experiment with Loser tree intead of heap:
        // https://grafana.com/blog/the-loser-tree-data-structure-how-to-optimize-merges-and-make-your-programs-run-faster/

        for (readers.items) |reader| {
            const next = try reader.next();
            // TODO: identify if a read block may come here,
            // either skip this validation or create another readers array
            std.debug.assert(next);
        }

        var heap = Heap(*BlockReader, BlockReader.blockReaderLessThan).init(alloc, readers);
        heap.heapify();

        return .{
            .heap = heap,
            .block = undefined,
        };
    }

    fn nextReader(self: *BlockMerger) *BlockReader {
        // TODO: test just  return self.heap.peekNext().?;

        const len = self.heap.len();

        if (len < 3) {
            return self.heap.array.items[1];
        }

        const one = self.heap.array.items[1];
        const another = self.heap.array.items[2];
        const oneItem = one.current();
        const anotherItem = another.current();
        if (std.mem.lessThan(u8, oneItem, anotherItem)) {
            return one;
        }
        return another;
    }

    fn merge(
        self: *BlockMerger,
        alloc: Allocator,
        writer: *BlockWriter,
        tableHeader: *TableHeader,
        stopped: ?*std.atomic.Value(bool),
    ) !void {
        while (true) {
            if (self.heap.len() == 0) {
                try self.flush(alloc, writer, tableHeader);
                return;
            }

            if (stopped) |s| {
                // TODO: move the error to a generic workers error,
                // it must be handled to stop all the mergers
                if (s.load(.acquire)) return error.Stopped;
            }

            const reader = self.heap.array.items[0];
            var nextItem: []const u8 = undefined;
            var hasNextItem = false;

            if (self.heap.len() > 1) {
                const nReader = self.nextReader();
                nextItem = nReader.current();
                hasNextItem = true;
            }

            const items = reader.block.data.items;
            var compareEveryItem = true;
            if (reader.currentI < items.len) {
                const lastItem = items[items.len - 1];
                compareEveryItem = hasNextItem and std.mem.lessThan(u8, nextItem, lastItem);
            }

            while (reader.currentI < items.len) {
                const item = reader.current();
                if (compareEveryItem and std.mem.lessThan(u8, nextItem, item)) {
                    break;
                }

                if (!try self.block.add(alloc, item)) {
                    try self.flush(alloc, writer, tableHeader);
                    continue;
                }
                reader.currentI += 1;
            }

            if (reader.currentI == items.len) {
                if (try reader.next()) {
                    self.heap.fix(0);
                    continue;
                }

                _ = self.heap.pop();
                continue;
            }

            self.heap.fix(0);
        }
    }

    fn flush(
        self: *BlockMerger,
        alloc: Allocator,
        writer: *BlockWriter,
        tableHeader: *TableHeader,
    ) !void {
        const items = self.block.data.items;
        if (items.len == 0) {
            return;
        }

        self.firstItem = items[0];
        self.lastItem = items[items.len - 1];
        try self.mergeTagsRecords(alloc);

        if (self.block.data.items.len == 0) {
            // nothing to flush
            return;
        }

        const blockLastItem = self.block.data.items[self.block.data.items.len - 1];

        // TODO: move this validation to tests and test the block is sorted
        std.debug.assert(!std.mem.lessThan(u8, self.block.data.items[0], self.firstItem));
        std.debug.assert(std.mem.lessThan(u8, blockLastItem, self.lastItem) or
            std.mem.eql(u8, blockLastItem, self.lastItem));
        if (builtin.is_test) {
            std.debug.assert(std.sort.isSorted([]const u8, self.block.data.items, {}, MemOrder(u8).lessThanConst));
        }

        tableHeader.itemsCount += self.block.data.items.len;
        if (tableHeader.firstItem.len == 0) {
            tableHeader.firstItem = self.block.data.items[0];
        }
        tableHeader.lastItem = blockLastItem;
        try writer.writeBlock(alloc, self.block);
        self.block.reset();
    }

    fn mergeTagsRecords(self: *BlockMerger, alloc: Allocator) !void {
        const items = self.block.data.items;

        if (items.len <= 2) {
            return;
        }

        const firstItem = items[0];
        if (firstItem.len > 0 and firstItem[0] > @intFromEnum(IndexKind.tagToSids)) {
            return;
        }

        const lastItem = items[items.len - 1];
        if (lastItem.len > 0 and lastItem[0] < @intFromEnum(IndexKind.tagToSids)) {
            // nothing to merge, there are no tags -> stream records
            return;
        }

        // TODO: review concurrent writing model to make sure it actually can happen
        var blockCopy = try std.ArrayList([]const u8).initCapacity(alloc, items.len);
        defer blockCopy.deinit(alloc);
        blockCopy.appendSliceAssumeCapacity(items);
        // can start mutating the original array after copying
        self.block.data.clearRetainingCapacity();

        var tagRecordsMerger = try TagRecordsMerger.init(alloc);

        for (0..items.len) |i| {
            if (items[i].len == 0 or items[i][0] != @intFromEnum(IndexKind.tagToSids) or i == 0 or i == items.len - 1) {
                try tagRecordsMerger.writeState(alloc, &self.block.data);
                continue;
            }

            try tagRecordsMerger.state.setup(items[i]);
            if (tagRecordsMerger.state.streamsLen() > maxStreamsPerRecord) {
                try tagRecordsMerger.writeState(alloc, &self.block.data);
                continue;
            }

            if (!tagRecordsMerger.statesPrefixEqual()) {
                try tagRecordsMerger.writeState(alloc, &self.block.data);
            }

            try tagRecordsMerger.state.parseStreamIDs(alloc);
            try tagRecordsMerger.moveParsedState(alloc);

            if (tagRecordsMerger.streamIDs.items.len >= maxStreamsPerRecord) {
                try tagRecordsMerger.writeState(alloc, &self.block.data);
            }
        }

        std.debug.assert(tagRecordsMerger.streamIDs.items.len == 0);
        const isSorted = std.sort.isSorted([]const u8, self.block.data.items, {}, MemOrder(u8).lessThanConst);
        if (!isSorted) {
            // defend against parallel writing leaving the state unmerged,
            // fallback to the original data
            self.block.data.clearRetainingCapacity();
            self.block.data.appendSliceAssumeCapacity(blockCopy.items);
        }

        tagRecordsMerger.deinit(alloc);
    }
};
