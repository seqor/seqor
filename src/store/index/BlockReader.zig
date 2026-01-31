const std = @import("std");
const Allocator = std.mem.Allocator;
const builtin = @import("builtin");

const encoding = @import("encoding");

const MemBlock = @import("MemBlock.zig");
const MemTable = @import("MemTable.zig");
const MetaIndexRecord = @import("MetaIndexRecord.zig");
const StorageBlock = @import("StorageBlock.zig");
const TableHeader = @import("TableHeader.zig");
const BlockHeader = @import("BlockHeader.zig");

const BlockReader = @This();

// TODO: make it non optional, it was historically optional
// to show mem block is not owned on decoding it from mem table,
// but apparently it's owning
block: ?*MemBlock,
tableHeader: TableHeader,

// TODO: these buffers could be files, on zig 0.16 implement a reader API,
// this change will require implementing a proper close method
indexBuf: std.ArrayList(u8) = .empty,
dataBuf: std.ArrayList(u8) = .empty,
lensBuf: std.ArrayList(u8) = .empty,

// state

// currentI defines a current item of the block
currentI: usize,
// read defines if the block has been read
isRead: bool,

// metaindex state
metaIndexI: usize = 0,
// metaindex passed on init
metaIndexRecords: []MetaIndexRecord = &.{},
// compressed buf
compressedBuf: std.ArrayList(u8) = .empty,
// uncompressed buf
uncompressedBuf: std.ArrayList(u8) = .empty,

// current block header index
blockHeaderI: usize = 0,
// all block headers read from the buffers
blockHeaders: []BlockHeader = &.{},
// current block header
// TODO: perhaps remove it in order not to hold the pointer
blockHeader: *BlockHeader = undefined,
// current storage block
sb: StorageBlock = .{},
// number of blocks read
blocksRead: usize = 0,
// number of items read
itemsRead: usize = 0,

firstItemChecked: if (builtin.is_test) bool else void = if (builtin.is_test) false else {},

pub fn initFromMemBlock(alloc: Allocator, block: *MemBlock) !*BlockReader {
    std.debug.assert(block.items.items.len > 0);
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
        .currentI = 0,
        .isRead = false,
    };
    return r;
}

pub fn initFromMemTable(alloc: Allocator, memTable: *MemTable) !*BlockReader {
    std.debug.assert(memTable.tableHeader.blocksCount > 0);
    const metaIndexRecords = try decodeMetaIndexRecords(
        alloc,
        memTable.metaindexBuf,
        memTable.tableHeader.blocksCount,
    );
    errdefer alloc.free(metaIndexRecords);

    const block = try MemBlock.init(alloc, @intCast(memTable.tableHeader.itemsCount));
    errdefer block.deinit(alloc);

    const r = try alloc.create(BlockReader);
    errdefer alloc.destroy(r);
    r.* = .{
        .block = block,
        .metaIndexRecords = metaIndexRecords,
        .tableHeader = memTable.tableHeader,
        .indexBuf = memTable.indexBuf,
        .dataBuf = memTable.dataBuf,
        .lensBuf = memTable.lensBuf,
        .currentI = 0,
        .isRead = false,
    };

    std.debug.assert(r.tableHeader.blocksCount != 0);
    std.debug.assert(r.tableHeader.itemsCount != 0);
    return r;
}

pub fn deinit(self: *BlockReader, alloc: Allocator) void {
    if (self.block) |block| block.deinit(alloc);

    if (self.metaIndexRecords.len > 0) alloc.free(self.metaIndexRecords);
    if (self.blockHeaders.len > 0) alloc.free(self.blockHeaders);
    self.sb.deinit(alloc);
    self.compressedBuf.deinit(alloc);
    self.uncompressedBuf.deinit(alloc);

    alloc.destroy(self);
}

pub fn blockReaderLessThan(one: *BlockReader, another: *BlockReader) bool {
    const first = one.current();
    const second = another.current();
    return std.mem.lessThan(u8, first, second);
}

pub inline fn current(self: *BlockReader) []const u8 {
    return self.block.?.items.items[self.currentI];
}

pub fn next(self: *BlockReader, alloc: Allocator) !bool {
    if (self.isRead) return false;

    // TODO: perhaps it's worth adding read mode enum to show
    // we either read from mem block or decoding from mem table
    if (self.metaIndexRecords.len == 0) {
        self.isRead = true;
        return true;
    }

    if (self.blockHeaders.len == 0 or self.blockHeaderI >= self.blockHeaders.len) {
        const ok = try self.readNextBlockHeaders(alloc);
        if (!ok) {
            const lastItem = self.block.?.items.items[self.block.?.items.items.len - 1];
            std.debug.assert(std.mem.eql(u8, self.tableHeader.lastItem, lastItem));
            self.isRead = true;
            return ok;
        }
    }

    self.blockHeader = &self.blockHeaders[self.blockHeaderI];
    self.blockHeaderI += 1;

    // TODO: for chunked buffer find a way just to  transfer a chunk ownership, perhaps via std.mem.swap,
    // for a file reader we must just read the content
    self.sb.itemsData.clearRetainingCapacity();
    try self.sb.itemsData.ensureUnusedCapacity(alloc, self.blockHeader.itemsBlockSize);
    const itemsDest = self.sb.itemsData.unusedCapacitySlice()[0..self.blockHeader.itemsBlockSize];
    @memmove(itemsDest, self.dataBuf.items);
    self.sb.itemsData.items.len = self.dataBuf.items.len;

    self.sb.lensData.clearRetainingCapacity();
    try self.sb.lensData.ensureUnusedCapacity(alloc, self.blockHeader.lensBlockSize);
    const lensDest = self.sb.lensData.unusedCapacitySlice()[0..self.blockHeader.lensBlockSize];
    @memmove(lensDest, self.lensBuf.items);
    self.sb.lensData.items.len = self.lensBuf.items.len;

    try self.block.?.decode(
        alloc,
        &self.sb,
        self.blockHeader.firstItem,
        self.blockHeader.prefix,
        self.blockHeader.itemsCount,
        self.blockHeader.encodingType,
    );
    self.blocksRead += 1;
    std.debug.assert(self.blocksRead <= self.tableHeader.blocksCount);
    self.currentI = 0;
    self.itemsRead += self.block.?.items.items.len;
    std.debug.assert(self.itemsRead <= self.tableHeader.itemsCount);

    if (builtin.is_test and !self.firstItemChecked) {
        self.firstItemChecked = true;
        const firstItem = self.block.?.items.items[0];
        std.debug.assert(std.mem.eql(u8, self.tableHeader.firstItem, firstItem));
    }
    return true;
}

fn readNextBlockHeaders(self: *BlockReader, alloc: Allocator) !bool {
    if (self.metaIndexI >= self.metaIndexRecords.len) {
        return false;
    }

    const mi = &self.metaIndexRecords[self.metaIndexI];
    self.metaIndexI += 1;

    self.compressedBuf.clearRetainingCapacity();
    try self.compressedBuf.ensureUnusedCapacity(alloc, mi.indexBlockSize);
    const indexDest = self.compressedBuf.unusedCapacitySlice()[0..mi.indexBlockSize];
    @memmove(indexDest, self.indexBuf.items);
    self.compressedBuf.items.len = self.indexBuf.items.len;

    self.uncompressedBuf.clearRetainingCapacity();
    const uncompressedSize = try encoding.getFrameContentSize(self.compressedBuf.items);
    try self.uncompressedBuf.ensureUnusedCapacity(alloc, uncompressedSize);
    const bufOffset = try encoding.decompress(
        self.uncompressedBuf.unusedCapacitySlice(),
        self.compressedBuf.items,
    );
    self.uncompressedBuf.items.len = bufOffset;

    if (self.blockHeaders.len > 0) alloc.free(self.blockHeaders);
    self.blockHeaders = try BlockHeader.decodeMany(alloc, self.uncompressedBuf.items, mi.blockHeadersCount);
    self.blockHeaderI = 0;
    return true;
}

fn decodeMetaIndexRecords(alloc: Allocator, metaindexBuf: std.ArrayList(u8), blocksCount: usize) ![]MetaIndexRecord {
    const decomporessedSize = try encoding.getFrameContentSize(metaindexBuf.items);
    const buf = try alloc.alloc(u8, decomporessedSize);
    defer alloc.free(buf);
    const bufOffset = try encoding.decompress(buf, metaindexBuf.items);

    const records = try alloc.alloc(MetaIndexRecord, blocksCount);
    errdefer alloc.free(records);

    var slice = buf[0..bufOffset];
    var i: u32 = 0;
    while (slice.len > 0) {
        // TODO: test if holding them on heap is better,
        // 1. create a mem pool to pop the objects quickly
        // 2. change lessThan to use pointers
        var rec = MetaIndexRecord{
            .firstItem = "",
            .blockHeadersCount = 0,
            .indexBlockOffset = 0,
            .indexBlockSize = 0,
        };
        const n = rec.decode(slice);
        slice = slice[n..];
        records[i] = rec;
        i += 1;
    }

    std.debug.assert(i == blocksCount);
    if (builtin.is_test) {
        std.debug.assert(std.sort.isSorted(MetaIndexRecord, records, {}, MetaIndexRecord.lessThan));
    }

    return records;
}

const testing = std.testing;

fn createTestMemBlock(alloc: Allocator, items: []const []const u8) !*MemBlock {
    var block = try MemBlock.init(alloc, 100);
    errdefer block.deinit(alloc);

    for (items) |item| {
        const ok = block.add(item);
        try testing.expect(ok);
    }

    return block;
}

test "BlockReader.blockReaderLessThan compares items correctly" {
    const alloc = testing.allocator;

    const items1 = [_][]const u8{ "apple", "banana", "cherry" };
    const items2 = [_][]const u8{ "apricot", "blueberry", "date" };

    const block1 = try createTestMemBlock(alloc, &items1);
    const block2 = try createTestMemBlock(alloc, &items2);

    var reader1 = try BlockReader.initFromMemBlock(alloc, block1);
    defer reader1.deinit(alloc);

    var reader2 = try BlockReader.initFromMemBlock(alloc, block2);
    defer reader2.deinit(alloc);

    // After sorting, "apple" < "apricot"
    const less = BlockReader.blockReaderLessThan(reader1, reader2);
    try testing.expect(less);
    try testing.expect(reader1.currentI == 0);
    try testing.expect(reader2.currentI == 0);
}

test "BlockReader.current returns correct item at currentI" {
    const alloc = testing.allocator;

    const items = [_][]const u8{ "first", "second", "third" };

    const block = try createTestMemBlock(alloc, &items);
    var reader = try BlockReader.initFromMemBlock(alloc, block);
    defer reader.deinit(alloc);

    // After sorting, test that current() returns the item at currentI
    // First item should be "first"
    const first = reader.current();
    try testing.expectEqualSlices(u8, "first", first);

    // Manually change currentI and verify current() updates
    reader.currentI = 1;
    const second = reader.current();
    try testing.expectEqualSlices(u8, "second", second);

    reader.currentI = 2;
    const third = reader.current();
    try testing.expectEqualSlices(u8, "third", third);
}

test "BlockReader.initFromMemTable reads items" {
    const alloc = testing.allocator;

    const items = [_][]const u8{ "alpha", "beta", "delta" };

    const block = try createTestMemBlock(alloc, &items);
    defer block.deinit(alloc);

    var blocks = [_]*MemBlock{block};
    var memTable = try MemTable.init(alloc, &blocks);
    defer memTable.deinit(alloc);

    var reader = try BlockReader.initFromMemTable(alloc, memTable);
    defer reader.deinit(alloc);

    try testing.expect(try reader.next(alloc));
    try testing.expect(reader.block != null);

    const decoded = reader.block.?.items.items;
    try testing.expectEqual(items.len, decoded.len);
    for (items, 0..) |item, i| {
        try testing.expectEqualSlices(u8, item, decoded[i]);
    }

    try testing.expect(!try reader.next(alloc));
}
