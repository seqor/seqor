const std = @import("std");
const Allocator = std.mem.Allocator;
const builtin = @import("builtin");

const encoding = @import("encoding");

const MemBlock = @import("MemBlock.zig");
const MemTable = @import("MemTable.zig");
const TableHeader = @import("TableHeader.zig");
const MetaIndexRecord = @import("MetaIndexRecord.zig");

const BlockReader = @This();

block: ?*MemBlock,
tableHeader: TableHeader,

metaIndexRecords: []MetaIndexRecord = &.{},

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
        .currentI = 0,
        .isRead = false,
    };
    return r;
}

pub fn initFromMemTable(alloc: Allocator, memTable: *MemTable) !*BlockReader {
    const metaIndexRecords = try decodeMetaIndexRecords(
        alloc,
        memTable.metaindexBuf,
        memTable.tableHeader.blocksCount,
    );

    const r = try alloc.create(BlockReader);
    r.* = .{
        // TODO: find an easy way to deinit it and metaIndexRecords
        .block = null,
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
    self.indexBuf.deinit(alloc);
    self.dataBuf.deinit(alloc);
    self.lensBuf.deinit(alloc);

    alloc.destroy(self);
}

pub fn blockReaderLessThan(one: *BlockReader, another: *BlockReader) bool {
    const first = one.current();
    const second = another.current();
    return std.mem.lessThan(u8, first, second);
}

pub inline fn current(self: *BlockReader) []const u8 {
    return self.block.?.data.items[self.currentI];
}

pub fn next(self: *BlockReader) !bool {
    if (self.isRead) return false;

    if (self.block) |block| {
        if (block.data.items.len == 0) return false;
        self.isRead = true;
        return true;
    }

    self.isRead = true;
    return false;
}

fn decodeMetaIndexRecords(alloc: Allocator, metaindexBuf: std.ArrayList(u8), blocksCount: usize) ![]MetaIndexRecord {
    const decomporessedSize = try encoding.getFrameContentSize(metaindexBuf.items);
    const buf = try alloc.alloc(u8, decomporessedSize);
    const bufOffset = try encoding.decompress(buf, metaindexBuf.items);

    const res = try alloc.alloc(MetaIndexRecord, blocksCount);
    var slice = buf[0..bufOffset];
    var i: usize = 0;
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
        res[i] = rec;
        i += 1;
    }

    std.debug.assert(i + 1 == blocksCount);
    if (builtin.is_test) {
        std.debug.assert(std.sort.isSorted(MetaIndexRecord, res, {}, MetaIndexRecord.lessThan));
    }

    return res;
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
