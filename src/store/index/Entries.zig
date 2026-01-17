const std = @import("std");
const Allocator = std.mem.Allocator;

const getConf = @import("../../conf.zig").getConf;

const MemBlock = @import("MemBlock.zig");

const EntriesShard = struct {
    mx: std.Thread.Mutex,
    blocks: std.ArrayList(*MemBlock),
    flushAtUs: i64,

    pub fn add(self: *EntriesShard, alloc: Allocator, entries: [][]const u8) ![]*MemBlock {
        self.mx.lock();
        defer self.mx.unlock();

        // TODO: throttle max block per entries shard

        if (self.blocks.items.len == 0) {
            const b = try alloc.create(MemBlock);
            try self.blocks.append(alloc, b);
            self.flushAtUs = std.time.microTimestamp() + std.time.us_per_s;
        }

        var block = self.blocks.items[self.blocks.items.len - 1];

        for (entries) |entry| {
            if (try block.add(alloc, entry)) continue;

            block = try alloc.create(MemBlock);
            if (try block.add(alloc, entry)) {
                try self.blocks.append(alloc, block);
            }

            // TODO: handle too large entries, log an error
        }

        return self.blocks.items;
    }
};

const Entries = @This();

shardIdx: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
shards: []EntriesShard,

pub fn init(alloc: Allocator) !*Entries {
    const conf = getConf().server.pools;
    std.debug.assert(conf.cpus != 0);

    const shards = try alloc.alloc(EntriesShard, conf.cpus);
    errdefer alloc.free(shards);

    const e = try alloc.create(Entries);
    e.* = .{
        .shards = shards,
    };
    return e;
}

pub fn next(self: *Entries) *EntriesShard {
    const i = self.shardIdx.fetchAdd(1, .acquire) % self.shards.len;
    return &self.shards[i];
}

pub fn deinit(self: *Entries, alloc: Allocator) void {
    alloc.free(self.shards);
    alloc.destroy(self);
}
