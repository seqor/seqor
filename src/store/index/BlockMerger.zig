const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;

const MemOrder = @import("../../stds/sort.zig").MemOrder;

const BlockReader = @import("BlockReader.zig");
const MemBlock = @import("MemBlock.zig");
const BlockWriter = @import("BlockWriter.zig");
const TableHeader = @import("TableHeader.zig");
const IndexKind = @import("Index.zig").IndexKind;
const TagRecordsMerger = @import("TagRecordsMerger.zig");

const Heap = @import("../../stds/Heap.zig").Heap;

const maxStreamsPerRecord = 32;

const BlockMerger = @This();

heap: Heap(*BlockReader, BlockReader.blockReaderLessThan),
block: *MemBlock,
firstItem: []const u8 = &[_]u8{},
lastItem: []const u8 = &[_]u8{},

/// init creates a BlockMerger instance from the readers
/// be aware it mutates readers list inside
pub fn init(alloc: Allocator, readers: *std.ArrayList(*BlockReader)) !BlockMerger {
    // TODO: collect metrics and experiment with flat array on 1-3 elements

    // TODO: experiment with Loser tree intead of heap:
    // https://grafana.com/blog/the-loser-tree-data-structure-how-to-optimize-merges-and-make-your-programs-run-faster/

    var i: usize = 0;
    while (i < readers.items.len) {
        const reader = readers.items[i];
        const hasNext = try reader.next();
        if (!hasNext) {
            _ = readers.swapRemove(i);
            continue;
        }
        i += 1;
    }

    var heap = Heap(*BlockReader, BlockReader.blockReaderLessThan).init(alloc, readers);
    heap.heapify();

    return .{
        .heap = heap,
        .block = undefined,
    };
}

pub fn merge(
    self: *BlockMerger,
    alloc: Allocator,
    writer: *BlockWriter,
    stopped: ?*std.atomic.Value(bool),
) !TableHeader {
    var tableHeader = TableHeader{};
    while (true) {
        if (self.heap.len() == 0) {
            try self.flush(alloc, writer, &tableHeader);
            return tableHeader;
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
            const nReader = self.heap.peekNext().?;
            nextItem = nReader.current();
            hasNextItem = true;
        }

        const items = reader.block.data.items;
        var compareEveryItem = true;
        if (reader.currentI < items.len) {
            const lastItem = items[items.len - 1];
            compareEveryItem = hasNextItem and (std.mem.order(u8, lastItem, nextItem) == .gt);
        }

        while (reader.currentI < items.len) {
            const item = reader.current();
            if (compareEveryItem and (std.mem.order(u8, item, nextItem) == .gt)) {
                break;
            }

            if (!try self.block.add(alloc, item)) {
                try self.flush(alloc, writer, &tableHeader);
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
    std.debug.assert(std.mem.order(u8, self.lastItem, blockLastItem) == .gt);
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
