const std = @import("std");
const Allocator = std.mem.Allocator;

const Entries = @import("Entries.zig");
const MemBlock = @import("MemBlock.zig");
const MemTable = @import("MemTable.zig");

const maxBlocksPerShard = 256;

// TODO: worth tuning on practice
const blocksInMemTable = 15;

const Self = @This();

flushInterval: u64,
entries: *Entries,

blocks: std.ArrayList(*MemBlock) = .empty,
mxBlocks: std.Thread.Mutex = .{},
flushAtUs: ?i64 = null,
blocksThresholdToFlush: u32,

pub fn init(alloc: Allocator, flushInterval: u64) !*Self {
    const entries = try Entries.init(alloc);
    errdefer entries.deinit(alloc);

    const t = try alloc.create(Self);
    t.* = .{
        .flushInterval = flushInterval,
        .entries = entries,
        .blocksThresholdToFlush = @intCast(entries.shards.len * maxBlocksPerShard),
    };
    return t;
}

pub fn add(self: *Self, alloc: Allocator, entries: [][]const u8) !void {
    const shard = self.entries.next();
    // TODO: handle a case when a shard block doesn't fit more entries
    const blocks = try shard.add(alloc, entries);
    if (blocks.len == 0) return;
    try self.flushBlocks(alloc, blocks);
}

fn flushBlocks(self: *Self, alloc: Allocator, blocks: []*MemBlock) !void {
    if (blocks.len == 0) return;

    self.mxBlocks.lock();
    defer self.mxBlocks.unlock();

    if (self.blocks.items.len == 0) {
        self.flushAtUs = std.time.microTimestamp() + std.time.us_per_s;
    }

    try self.blocks.appendSlice(alloc, blocks);
    if (self.blocks.items.len >= self.blocksThresholdToFlush) {
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
