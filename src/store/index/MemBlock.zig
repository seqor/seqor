const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;

const MemOrder = @import("../../stds/sort.zig").MemOrder;
const encoding = @import("encoding");
const Encoder = encoding.Encoder;

const StorageBlock = @import("StorageBlock.zig");
const EncodingType = @import("BlockHeader.zig").EncodingType;

const EncodedMemBlock = struct {
    firstItem: []const u8,
    prefix: []const u8,
    itemsCount: u32,
    encodingType: EncodingType,
};

fn findPrefix(first: []const u8, second: []const u8) []const u8 {
    const n = @min(first.len, second.len);
    var i: usize = 0;
    while (i < n and first[i] == second[i]) : (i += 1) {}
    return first[0..@intCast(i)];
}

const MemBlock = @This();

// TODO: rename it, having items.items is not readable
items: std.ArrayList([]const u8),
size: u32,
prefix: []const u8 = "",

// buf may hold the underlying data memory in order be the memory owner,
// it happens in reading or merging path when we decode the stored content
buf: std.ArrayList(u8) = .empty,

pub fn init(
    alloc: Allocator,
    maxMemBlockSize: u32,
) !*MemBlock {
    var data = try std.ArrayList([]const u8).initCapacity(alloc, maxMemBlockSize);
    errdefer data.deinit(alloc);

    const b = try alloc.create(MemBlock);
    b.* = .{
        .items = data,
        .size = 0,
    };
    return b;
}

pub fn deinit(self: *MemBlock, alloc: Allocator) void {
    self.items.deinit(alloc);
    self.buf.deinit(alloc);
    alloc.destroy(self);
}

pub fn reset(self: *MemBlock) void {
    self.items.clearRetainingCapacity();
    self.size = 0;
    self.prefix = undefined;
}

pub fn add(self: *MemBlock, entry: []const u8) bool {
    if ((self.size + entry.len) > self.items.capacity) return false;

    self.items.appendAssumeCapacity(entry);
    self.size += @intCast(entry.len);
    return true;
}

pub fn sortData(self: *MemBlock) void {
    // TODO: evaluate the chances of the data being sorted, might improve performance a lot,
    // collect the metrics and if it's common enough optimize the algorithm

    self.setPrefix();
    self.sort();
}

pub fn setPrefix(self: *MemBlock) void {
    if (self.items.items.len == 0) return;

    if (self.items.items.len == 1) {
        self.prefix = self.items.items[0];
        return;
    }

    var prefix = self.items.items[0];
    for (self.items.items[1..]) |entry| {
        if (std.mem.startsWith(u8, entry, prefix)) {
            continue;
        }

        prefix = findPrefix(prefix, entry);
        if (prefix.len == 0) return;
    }

    self.prefix = prefix;
}
pub fn setPrefixSorted(self: *MemBlock) void {
    if (self.items.items.len <= 1) {
        self.prefix = "";
        return;
    }

    self.prefix = findPrefix(self.items.items[0], self.items.items[self.items.items.len - 1]);
}

pub fn sort(self: *MemBlock) void {
    std.mem.sortUnstable([]const u8, self.items.items, self, memBlockEntryLessThan);
}

fn memBlockEntryLessThan(self: *MemBlock, one: []const u8, another: []const u8) bool {
    const prefixLen = self.prefix.len;

    const oneSuffix = one[prefixLen..];
    const anotherSuffix = another[prefixLen..];

    return std.mem.lessThan(u8, oneSuffix, anotherSuffix);
}

inline fn assertIsSorted(self: *MemBlock) void {
    if (builtin.is_test) {
        std.debug.assert(std.sort.isSorted([]const u8, self.items.items, {}, MemOrder(u8).lessThanConst));
    }
}

pub fn encode(
    self: *MemBlock,
    alloc: Allocator,
    sb: *StorageBlock,
) !EncodedMemBlock {
    std.debug.assert(self.items.items.len != 0);
    // this API can't be called on unsorted data
    self.assertIsSorted();

    self.setPrefixSorted();
    const firstItem = self.items.items[0];

    // TODO: consider making len limit 128
    if (self.size - self.prefix.len * self.items.items.len < 64 or self.items.items.len < 2) {
        try self.encodePlain(alloc, sb);
        return EncodedMemBlock{
            .firstItem = firstItem,
            .prefix = self.prefix,
            .itemsCount = @intCast(self.items.items.len),
            .encodingType = .plain,
        };
    }

    var itemsBuf = std.ArrayList(u8).empty;
    defer itemsBuf.deinit(alloc);

    var lens = try std.ArrayList(u32).initCapacity(alloc, self.items.items.len - 1);
    defer lens.deinit(alloc);

    var prevItem = firstItem[self.prefix.len..];
    var prevLen: u32 = 0;

    // write prefix lens
    for (self.items.items[1..]) |item| {
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
    for (self.items.items[1..]) |item| {
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

    bound = try encoding.compressBound(encodedLens.len);
    sb.lensData = try std.ArrayList(u8).initCapacity(alloc, encodedPrefixLensBuf.len + bound);
    sb.lensData.appendSliceAssumeCapacity(encodedPrefixLensBuf);
    const compressedLens = try alloc.alloc(u8, bound);
    defer alloc.free(compressedLens);
    n = try encoding.compressAuto(compressedLens, encodedLens);
    sb.lensData.appendSliceAssumeCapacity(compressedLens[0..n]);

    // if compressed content is more than 90% of the original size - not worth it
    // TODO: consider tweaking the value up to 80-85%
    if (@as(f64, @floatFromInt(sb.itemsData.items.len)) >
        0.9 * @as(f64, @floatFromInt(self.size - self.prefix.len * self.items.items.len)))
    {
        sb.reset();
        try self.encodePlain(alloc, sb);
        return EncodedMemBlock{
            .firstItem = firstItem,
            .prefix = self.prefix,
            .itemsCount = @intCast(self.items.items.len),
            .encodingType = .plain,
        };
    }

    return EncodedMemBlock{
        .firstItem = firstItem,
        .prefix = self.prefix,
        .itemsCount = @intCast(self.items.items.len),
        .encodingType = .zstd,
    };
}

fn encodePlain(self: *MemBlock, alloc: Allocator, sb: *StorageBlock) !void {
    try sb.itemsData.ensureUnusedCapacity(
        alloc,
        self.size - self.prefix.len * self.items.items.len + self.prefix.len - self.items.items[0].len,
    );
    try sb.lensData.ensureUnusedCapacity(alloc, 2 * (self.items.items.len - 1));

    for (self.items.items[1..]) |item| {
        const suffix = item[self.prefix.len..];
        sb.itemsData.appendSliceAssumeCapacity(suffix);
    }

    // no chance any len value is larger than 16384 (0x4000)
    const slice = sb.lensData.unusedCapacitySlice();
    var enc = Encoder.init(slice);
    for (self.items.items[1..]) |item| {
        const len: u64 = @intCast(item.len - self.prefix.len);
        enc.writeVarInt(len);
    }
    sb.lensData.items.len = enc.offset;
}

pub fn decode(
    self: *MemBlock,
    alloc: Allocator,
    sb: *StorageBlock,
    firstItem: []const u8,
    prefix: []const u8,
    itemsCount: u32,
    encodingType: EncodingType,
) !void {
    std.debug.assert(itemsCount > 0);

    self.reset();

    self.prefix = prefix;

    switch (encodingType) {
        .plain => {
            try self.decodePlain(alloc, sb, firstItem, itemsCount);
            self.assertIsSorted();
            return;
        },
        .zstd => {
            // implementation is below
        },
    }

    const size = try encoding.getFrameContentSize(sb.lensData.items);
    const decompressedLensBuf = try alloc.alloc(u8, size);
    defer alloc.free(decompressedLensBuf);
    const decompressedLensSize = try encoding.decompress(decompressedLensBuf, sb.lensData.items);

    const lensBuf = try alloc.alloc(u64, itemsCount * 2);
    defer alloc.free(lensBuf);
    const prefixLens = lensBuf[0..itemsCount];
    const lens = lensBuf[itemsCount..];

    var fba = std.heap.stackFallback(2048, alloc);
    const fbaAlloc = fba.get();

    // read prefixes
    const decodedLens = try fbaAlloc.alloc(u64, itemsCount - 1);
    defer fbaAlloc.free(decodedLens);
    var dec = encoding.Decoder.init(decompressedLensBuf[0..decompressedLensSize]);
    dec.readVarInts(decodedLens);

    // decode prefixes
    prefixLens[0] = 0;
    for (0..decodedLens.len) |i| {
        const xLen = decodedLens[i];
        prefixLens[i + 1] = xLen ^ prefixLens[i];
    }

    dec.readVarInts(decodedLens);
    std.debug.assert(dec.offset == dec.buf.len);

    // decode lens
    lens[0] = @intCast(firstItem.len - prefix.len);
    var dataLen: usize = prefix.len * itemsCount + lens[0];
    for (0..decodedLens.len) |i| {
        const itemLen = decodedLens[i] ^ lens[i];
        lens[i + 1] = itemLen;
        dataLen += @intCast(itemLen);
    }

    // read items data
    const decompressedItemsSize = try encoding.getFrameContentSize(sb.itemsData.items);
    const decompressedItemsBuf = try alloc.alloc(u8, decompressedItemsSize);
    defer alloc.free(decompressedItemsBuf);
    const decompressedItemsOffset = try encoding.decompress(decompressedItemsBuf, sb.itemsData.items);

    try self.items.ensureUnusedCapacity(alloc, itemsCount);
    try self.buf.ensureUnusedCapacity(alloc, dataLen);
    self.buf.appendSliceAssumeCapacity(firstItem);
    self.items.appendAssumeCapacity(self.buf.items[0..firstItem.len]);

    var decompressedItemsSlice = decompressedItemsBuf[0..decompressedItemsOffset];
    var prevItem = self.buf.items[prefix.len..];
    for (1..itemsCount) |i| {
        const itemLen = lens[i];
        const prefixLen = prefixLens[i];

        const suffixLen = itemLen - prefixLen;
        std.debug.assert(decompressedItemsOffset >= suffixLen);
        std.debug.assert(prefixLen <= prevItem.len);

        const dataStart = self.buf.items.len;
        self.buf.appendSliceAssumeCapacity(prefix);
        self.buf.appendSliceAssumeCapacity(prevItem[0..prefixLen]);
        self.buf.appendSliceAssumeCapacity(decompressedItemsSlice[0..suffixLen]);
        self.items.appendAssumeCapacity(self.buf.items[dataStart .. dataStart + self.buf.items.len]);

        decompressedItemsSlice = decompressedItemsSlice[suffixLen..];
        prevItem = self.buf.items[self.buf.items.len - itemLen ..];
    }

    std.debug.assert(decompressedItemsSlice.len == 0);
    std.debug.assert(self.buf.items.len == dataLen);
    if (builtin.is_test) {
        self.assertIsSorted();
    }
}

pub fn decodePlain(self: *MemBlock, alloc: Allocator, sb: *StorageBlock, firstItem: []const u8, itemsCount: u32) !void {
    // decode lens
    const lensBuf = try alloc.alloc(u64, itemsCount);
    defer alloc.free(lensBuf);
    lensBuf[0] = firstItem.len - self.prefix.len;

    var dec = encoding.Decoder.init(sb.lensData.items);
    for (1..itemsCount) |i| {
        lensBuf[i] = dec.readVarInt();
    }
    std.debug.assert(dec.offset == dec.buf.len);

    // decode items
    const dataLen: usize = self.prefix.len * (itemsCount - 1) + firstItem.len + sb.itemsData.items.len;
    try self.items.ensureUnusedCapacity(alloc, itemsCount);
    try self.buf.ensureUnusedCapacity(alloc, dataLen);
    self.buf.appendSliceAssumeCapacity(firstItem);
    self.items.appendAssumeCapacity(self.buf.items[0..firstItem.len]);

    var itemsSlice = sb.itemsData.items;
    for (1..itemsCount) |i| {
        const itemLen = lensBuf[i];
        const start = self.buf.items.len;

        self.buf.appendSliceAssumeCapacity(self.prefix);
        self.buf.appendSliceAssumeCapacity(itemsSlice[0..itemLen]);
        self.items.appendAssumeCapacity(self.buf.items[start..self.buf.items.len]);
        itemsSlice = itemsSlice[itemLen..];
    }
    std.debug.assert(itemsSlice.len == 0);
    std.debug.assert(self.buf.items.len == dataLen);
}
