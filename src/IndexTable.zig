const std = @import("std");
const Allocator = std.mem.Allocator;

const encoding = @import("encoding");
const Encoder = encoding.Encoder;

const getConf = @import("conf.zig").getConf;

// TODO: make it configurable,
// depending on used CPU model must be changed according its L1 cache size
const maxMemBlockSize = 32 * 1024;

const maxBlocksPerShard = 256;

// TODO: worth tuning on practice
const blocksInMemTable = 15;

const EncodingTye = enum(u8) {
    plain = 0,
    zstd = 1,
};

const EncodedMemBlock = struct {
    firstItem: []const u8,
    prefix: []const u8,
    itemsCount: u32,
    encodingType: EncodingTye,
};

const StorageBlock = struct {
    itemsData: std.ArrayList(u8) = .empty,
    lensData: std.ArrayList(u8) = .empty,

    pub fn reset(self: *StorageBlock) void {
        self.itemsData.clearRetainingCapacity();
        self.lensData.clearRetainingCapacity();
    }
};

const MemBlock = struct {
    data: std.ArrayList([]const u8),
    size: u32,
    prefix: []const u8,

    pub fn deinit(self: *MemBlock, alloc: Allocator) void {
        self.data.deinit(alloc);
        alloc.destroy(self);
    }

    pub fn add(self: *MemBlock, alloc: Allocator, entry: []const u8) !bool {
        if ((self.size + entry.len) > maxMemBlockSize) return false;

        try self.data.append(alloc, entry);
        self.size += @intCast(entry.len);
        return true;
    }

    pub fn sortData(self: *MemBlock) void {
        // TODO: evaluate the chances of the data being sorted, might improve performance a lot

        self.setPrefixes();
        self.sort();
    }

    pub fn setPrefixes(self: *MemBlock) void {
        if (self.data.items.len == 0) return;

        if (self.data.items.len == 1) {
            self.prefix = self.data.items[0];
            return;
        }

        var prefix = self.data.items[0];
        for (self.data.items[1..]) |entry| {
            if (std.mem.startsWith(u8, entry, prefix)) {
                continue;
            }

            prefix = findPrefix(prefix, entry);
            if (prefix.len == 0) return;
        }

        self.prefix = prefix;
    }

    pub fn sort(self: *MemBlock) void {
        std.mem.sortUnstable([]const u8, self.data.items, self, memBlockEntryLessThan);
    }

    fn memBlockEntryLessThan(self: *MemBlock, one: []const u8, another: []const u8) bool {
        const prefixLen = self.prefix.len;

        const oneSuffix = one[prefixLen..];
        const anotherSuffix = another[prefixLen..];

        return std.mem.lessThan(u8, oneSuffix, anotherSuffix);
    }

    fn encode(
        self: *MemBlock,
        alloc: Allocator,
        sb: *StorageBlock,
    ) !EncodedMemBlock {
        std.debug.assert(self.data.items.len != 0);

        const firstItem = self.data.items[0];

        // TODO: consider making len limit 128
        if (self.size - self.prefix.len * self.data.items.len < 64 or self.data.items.len < 2) {
            try self.encodePlain(sb);
            return EncodedMemBlock{
                .firstItem = firstItem,
                .prefix = self.prefix,
                .itemsCount = @intCast(self.data.items.len),
                .encodingType = .plain,
            };
        }

        var itemsBuf = std.ArrayList(u8).empty;
        defer itemsBuf.deinit(alloc);

        var lens = try std.ArrayList(u32).initCapacity(alloc, self.data.items.len - 1);
        defer lens.deinit(alloc);

        var prevItem = firstItem[self.prefix.len..];
        var prevLen: u32 = 0;

        // write prefix lens
        for (self.data.items[1..]) |item| {
            const currItem = item[self.prefix.len..];
            const prefix = findPrefix(prevItem, currItem);
            try itemsBuf.appendSlice(alloc, currItem[prefix.len..]);

            const xLen = prefix.len ^ prevLen;
            lens.appendAssumeCapacity(@intCast(xLen));

            prevItem = currItem;
            prevLen = @intCast(prefix.len);
        }

        var fallbackFba = std.heap.stackFallback(2048, alloc);
        var fba = fallbackFba.get();
        const encodedPrefixLensBufSize = Encoder.varIntsBound(u32, lens.items);
        const encodedPrefixLensBuf = try fba.alloc(u8, encodedPrefixLensBufSize);
        defer fba.free(encodedPrefixLensBuf);
        var enc = Encoder.init(encodedPrefixLensBuf);
        enc.writeVarInts(u32, lens.items);

        sb.itemsData = try std.ArrayList(u8).initCapacity(alloc, itemsBuf.items.len);
        var bound = try encoding.compressBound(itemsBuf.items.len);
        const compressedItems = try alloc.alloc(u8, bound);
        defer alloc.free(compressedItems);
        var n = try encoding.compressAuto(compressedItems, itemsBuf.items);
        try sb.itemsData.appendSlice(alloc, compressedItems[0..n]);

        // write items lens
        lens.clearRetainingCapacity();

        prevLen = @intCast(firstItem.len - self.prefix.len);
        for (self.data.items[1..]) |item| {
            const itemLen: u32 = @intCast(item.len - self.prefix.len);
            const xLen = itemLen ^ prevLen;
            prevLen = itemLen;
            lens.appendAssumeCapacity(xLen);
        }

        const encodedLensBound = Encoder.varIntsBound(u32, lens.items);
        const encodedLens = try fba.alloc(u8, encodedLensBound);
        defer fba.free(encodedLens);
        enc = Encoder.init(encodedLens);
        enc.writeVarInts(u32, lens.items);

        sb.lensData = try std.ArrayList(u8).initCapacity(alloc, encodedPrefixLensBuf.len + encodedLens.len);
        sb.lensData.appendSliceAssumeCapacity(encodedPrefixLensBuf);
        bound = try encoding.compressBound(encodedLens.len);
        const compressedLens = try alloc.alloc(u8, bound);
        defer alloc.free(compressedLens);
        n = try encoding.compressAuto(compressedLens, encodedLens);
        sb.lensData.appendSliceAssumeCapacity(compressedLens[0..n]);

        // if compressed content is more than 90% of the original size - not worth it
        // TODO: consider tweaking the value up to 80-85%
        if (@as(f64, @floatFromInt(sb.itemsData.items.len)) >
            0.9 * @as(f64, @floatFromInt(self.size - self.prefix.len * self.data.items.len)))
        {
            sb.reset();
            try self.encodePlain(sb);
            return EncodedMemBlock{
                .firstItem = firstItem,
                .prefix = self.prefix,
                .itemsCount = @intCast(self.data.items.len),
                .encodingType = .plain,
            };
        }

        return EncodedMemBlock{
            .firstItem = try alloc.dupe(u8, firstItem),
            .prefix = try alloc.dupe(u8, self.prefix),
            .itemsCount = @intCast(self.data.items.len),
            .encodingType = .zstd,
        };
    }

    fn encodePlain(self: *MemBlock, sb: *StorageBlock) !void {
        _ = self;
        _ = sb;
        unreachable;
    }
};

fn findPrefix(first: []const u8, second: []const u8) []const u8 {
    const n = @min(first.len, second.len);
    var i: usize = 0;
    while (i < n and first[i] == second[i]) : (i += 1) {}
    return first[0..@intCast(i)];
}

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

const Entries = struct {
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
};

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
    var res = try std.ArrayList(*MemTable).initCapacity(alloc, tablesSize);
    errdefer {
        for (res.items) |memTable| memTable.deinit(alloc);
        res.deinit(alloc);
    }

    var tail = blocks[0..];
    // TODO: benchmark parallel mem table creation
    while (tail.len > 0) {
        const offset = @min(blocksInMemTable, tail.len);
        const head = tail[0..offset];
        tail = tail[offset..];

        const memTable = try MemTable.init(alloc, head);
        res.appendAssumeCapacity(memTable);
    }

    _ = self;
    _ = force;
    unreachable;
}

const BlockHeader = struct {
    firstItem: []const u8,
    prefix: []const u8,
    itemsCount: u32 = 0,
    encodingType: EncodingTye,
    itemsBlockOffset: u64 = 0,
    itemsBlockSize: u32 = 0,
    lensBlockOffset: u64 = 0,
    lensBlockSize: u32 = 0,

    fn encode(self: *const BlockHeader, dst: []u8) []u8 {
        _ = self;
        _ = dst;
        unreachable;
    }
};

const TableHeader = struct {
    itemsCount: u64,
    blocksCount: u64,
    firstItem: []const u8,
    lastItem: []const u8,
};

const MetaIndex = struct {
    firstItem: []const u8 = undefined,
    blockHeadersCount: u32 = 0,
    indexBlockOffset: u64 = 0,
    indexBlockSize: u32 = 0,

    fn encode(self: *const MetaIndex, dst: []u8) []u8 {
        _ = self;
        _ = dst;
        unreachable;
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
        errdefer {
            for (readers.items) |reader| reader.deinit(alloc);
            readers.deinit(alloc);
        }
        const t = try alloc.create(MemTable);
        errdefer alloc.destroy(t);

        if (blocks.len == 1) {
            // nothing to merge
            const b = blocks[0];
            b.sortData();

            const flushAtUs = std.time.microTimestamp() + std.time.us_per_s;
            try t.setup(alloc, b, flushAtUs);
            return t;
        }

        for (0..blocks.len) |i| {
            const reader = try BlockReader.init(alloc, blocks[i]);
            readers.appendAssumeCapacity(reader);
        }

        unreachable;
        // t.mergeIntoMemTable(readers);
        // return t;
    }

    pub fn deinit(self: *MemTable, alloc: Allocator) void {
        self.dataBuf.deinit(alloc);
        self.lensBuf.deinit(alloc);
        self.indexBuf.deinit(alloc);
        self.metaindexBuf.deinit(alloc);
        alloc.destroy(self);
    }

    fn setup(self: *MemTable, alloc: Allocator, block: *MemBlock, flushAtUs: i64) !void {
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

        var buf = std.ArrayList(u8).empty;
        defer buf.deinit(alloc);

        var bound = try encoding.compressBound(buf.items.len);
        const compressed = try alloc.alloc(u8, bound);
        var n = try encoding.compressAuto(compressed, buf.items);

        try self.indexBuf.appendSlice(alloc, compressed[0..n]);

        self.metaIndex.firstItem = self.blockHeader.firstItem;
        self.metaIndex.blockHeadersCount = 1;
        self.metaIndex.indexBlockOffset = 0;
        self.metaIndex.indexBlockSize = @intCast(n);

        buf.clearRetainingCapacity();
        const encodedMetaIndex = self.metaIndex.encode(buf.items);

        bound = try encoding.compressBound(encodedMetaIndex.len);
        const compressedMr = try alloc.alloc(u8, bound);
        defer alloc.free(compressedMr);
        n = try encoding.compressAuto(compressedMr, encodedMetaIndex);

        try self.metaindexBuf.appendSlice(alloc, compressedMr[0..n]);
    }

    fn mergeIntoMemTable(self: *MemTable, readers: std.ArrayList(*BlockReader)) void {
        _ = self;
        _ = readers;
    }
};

const BlockReader = struct {
    block: *MemBlock,

    pub fn init(alloc: Allocator, block: *MemBlock) !*BlockReader {
        block.sortData();

        const r = try alloc.create(BlockReader);
        r.* = .{
            .block = block,
        };
        return r;
    }

    pub fn deinit(self: *BlockReader, alloc: Allocator) void {
        self.block.deinit(alloc);
        alloc.destroy(self);
    }
};
