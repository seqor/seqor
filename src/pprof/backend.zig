const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const Writer = std.Io.Writer;
const flate = std.compress.flate;

pub const nameUnknown = "unknown";

const PprofAllocator = @import("PprofAllocator.zig");

// TODO: it's fake interning, perhaps worth implementing a better version
pub const Strings = struct {
    values: std.ArrayList([]const u8),
    index: std.StringHashMapUnmanaged(u32),

    pub fn init(alloc: Allocator) !Strings {
        var s: Strings = .{
            .values = .empty,
            .index = .empty,
        };
        try s.values.ensureUnusedCapacity(alloc, 32);
        // "" must be first by pprof convention
        s.values.appendAssumeCapacity("");
        return s;
    }

    pub fn deinit(self: *Strings, alloc: Allocator) void {
        self.values.deinit(alloc);
        self.index.deinit(alloc);
    }

    pub fn intern(self: *Strings, alloc: Allocator, value: []const u8) !u32 {
        if (self.index.get(value)) |val| return val;

        const id: u32 = @intCast(self.values.items.len);
        try self.index.put(alloc, value, id);
        try self.values.append(alloc, value);
        return id;
    }
};

// implements pprof memory profile api.
// uses deflate to implement gzip packaging
// TODO: algorithm is suboptimal, many copies,
// most of the write function can go directly to the writer skipping allocations
pub fn writeHeapProfile(io: Io, alloc: Allocator, pprofAlloc: *PprofAllocator, writer: *Writer) !void {
    const buckets = try pprofAlloc.snapshot(alloc);
    defer alloc.free(buckets);

    var strings: Strings = try .init(alloc);
    defer strings.deinit(alloc);
    var locationIds: std.AutoHashMapUnmanaged(usize, u64) = .empty;
    defer locationIds.deinit(alloc);
    var locationMsgs: std.ArrayList([]const u8) = .empty;
    defer locationMsgs.deinit(alloc);
    var functionMsgs: std.ArrayList([]const u8) = .empty;
    defer functionMsgs.deinit(alloc);

    // pprof ids are 1-based, 0 means "none"
    var nextId: u64 = 1;

    const countId = try strings.intern(alloc, "count");
    const bytesId = try strings.intern(alloc, "bytes");
    const allocObjectsId = try strings.intern(alloc, "alloc_objects");
    const allocSpaceId = try strings.intern(alloc, "alloc_space");
    const inuseObjectsId = try strings.intern(alloc, "inuse_objects");
    const inuseSpaceId = try strings.intern(alloc, "inuse_space");

    const selfInfo: ?*std.debug.SelfInfo = std.debug.getSelfDebugInfo() catch null;
    var syms: std.ArrayList(std.debug.Symbol) = .empty;
    defer syms.deinit(alloc);

    for (buckets) |bucket| {
        for (bucket.getAddrs()) |addr| {
            if (locationIds.contains(addr)) continue;

            // addr is a return addres, so we sub 1 to make
            // a lookup call addr;
            // -| does sub with overflow defend to not go lower than 0
            const lookupAddr = addr -| 1;
            syms.clearRetainingCapacity();
            const sym: std.debug.Symbol = blk: {
                if (selfInfo) |si| {
                    si.getSymbols(io, alloc, alloc, lookupAddr, false, &syms) catch |err| {
                        std.log.err("failed to get debug symbols, err={any}", .{err});
                        break :blk .unknown;
                    };

                    if (syms.items.len > 0) break :blk syms.items[0];
                }

                break :blk .unknown;
            };

            const nameId = try strings.intern(alloc, sym.name orelse nameUnknown);
            const fileId = try strings.intern(alloc, if (sym.source_location) |sl| sl.file_name else "");

            const line: u32 = if (sym.source_location) |sl| @intCast(sl.line) else 0;
            const funcId = nextId;
            nextId += 1;
            const fnMsg = try writeFunctionMsg(alloc, funcId, nameId, fileId);
            errdefer alloc.free(fnMsg);
            try functionMsgs.append(alloc, fnMsg);

            const lineMsg = try writeLineMsg(alloc, funcId, line);
            errdefer alloc.free(lineMsg);
            const locId = nextId;
            nextId += 1;
            try locationIds.put(alloc, addr, locId);

            const locationMsg = try writeLocationMsg(alloc, locId, addr, lineMsg);
            errdefer alloc.free(locationMsg);
            try locationMsgs.append(alloc, locationMsg);
        }
    }

    var sampleMsgs: std.ArrayList([]const u8) = .empty;
    defer {
        for (sampleMsgs.items) |msg| {
            alloc.free(msg);
        }
        sampleMsgs.deinit(alloc);
    }

    for (buckets) |bucket| {
        var locIds: std.ArrayList(u64) = .empty;
        defer locIds.deinit(alloc);

        for (bucket.getAddrs()) |addr| {
            const locId = locationIds.get(addr) orelse {
                std.log.err("failed to resolve location id", .{});
                return error.LocationIdNotFound;
            };
            try locIds.append(alloc, locId);
        }

        const values: [4]i64 = .{
            bucket.allocObjects,
            bucket.allocBytes,
            bucket.inuseObjects,
            bucket.inuseBytes,
        };
        const sampleMsg = try writeSampleMsg(alloc, locIds.items, &values);
        errdefer alloc.free(sampleMsg);
        try sampleMsgs.append(alloc, sampleMsg);
    }

    var profileW: Writer.Allocating = try .initCapacity(alloc, 4096);
    defer profileW.deinit();
    const pw = &profileW.writer;

    try putBytesField(pw, 1, try writeValueTypeMsg(alloc, allocObjectsId, countId));
    try putBytesField(pw, 1, try writeValueTypeMsg(alloc, allocSpaceId, bytesId));
    try putBytesField(pw, 1, try writeValueTypeMsg(alloc, inuseObjectsId, countId));
    try putBytesField(pw, 1, try writeValueTypeMsg(alloc, inuseSpaceId, bytesId));

    for (sampleMsgs.items) |m| try putBytesField(pw, 2, m);
    for (locationMsgs.items) |m| try putBytesField(pw, 4, m);
    for (functionMsgs.items) |m| try putBytesField(pw, 5, m);
    for (strings.values.items) |s| try putBytesField(pw, 6, s);

    try putBytesField(pw, 11, try writeValueTypeMsg(alloc, inuseObjectsId, countId));
    try putVarintField(pw, 12, 1);

    var deflateBuf: [flate.max_window_len]u8 = undefined;
    var compressor = try flate.Compress.init(writer, &deflateBuf, .gzip, .default);
    try compressor.writer.writeAll(profileW.written());
    try compressor.finish();
}

fn writeFunctionMsg(alloc: Allocator, id: u64, nameId: u32, filenameId: u32) ![]u8 {
    var w: Writer.Allocating = try .initCapacity(alloc, 32);
    try putVarintField(&w.writer, 1, id);
    try putVarintField(&w.writer, 2, nameId);
    try putVarintField(&w.writer, 3, nameId);
    try putVarintField(&w.writer, 4, filenameId);
    return w.written();
}

fn putVarintField(w: *Writer, field: u32, value: u64) !void {
    try putTag(w, field, 0);
    try putVarint(w, value);
}

// TODO: reuse what we have in Decoder, perhaps worth use std.leb directly in both
fn putVarint(w: *Writer, value: u64) !void {
    var v = value;
    while (v >= 0x80) {
        try w.writeByte(@intCast((v & 0x7f) | 0x80));
        v >>= 7;
    }
    try w.writeByte(@intCast(v));
}

fn putTag(w: *Writer, field: u32, wireType: u3) !void {
    try putVarint(w, (@as(u64, field) << 3) | wireType);
}

fn writeLineMsg(alloc: Allocator, functionId: u64, line: u64) ![]u8 {
    var w: Writer.Allocating = try .initCapacity(alloc, 16);
    try putVarintField(&w.writer, 1, functionId);
    try putVarintField(&w.writer, 2, line);
    return w.written();
}

fn writeLocationMsg(alloc: Allocator, id: u64, address: u64, lineMsg: []const u8) ![]u8 {
    var w: Writer.Allocating = try .initCapacity(alloc, lineMsg.len + 24);
    try putVarintField(&w.writer, 1, id);
    try putVarintField(&w.writer, 3, address);
    try putBytesField(&w.writer, 4, lineMsg);
    return w.written();
}

fn putBytesField(w: *Writer, field: u32, bytes: []const u8) !void {
    try putTag(w, field, 2);
    try putVarint(w, bytes.len);
    try w.writeAll(bytes);
}

fn writeSampleMsg(alloc: Allocator, locIds: []const u64, values: []const i64) ![]u8 {
    var w: Writer.Allocating = try .initCapacity(alloc, (locIds.len + values.len) * 2 + 16);
    try putPackedVarintField(&w.writer, alloc, 1, locIds);

    var valuesU64: std.ArrayList(u64) = try .initCapacity(alloc, values.len);
    for (values) |v| valuesU64.appendAssumeCapacity(@bitCast(v));
    try putPackedVarintField(&w.writer, alloc, 2, valuesU64.items);

    return w.written();
}

fn putPackedVarintField(w: *Writer, alloc: Allocator, field: u32, values: []const u64) !void {
    var bufWriter: Writer.Allocating = try .initCapacity(alloc, values.len * 2 + 8);
    for (values) |v| try putVarint(&bufWriter.writer, v);
    try putBytesField(w, field, bufWriter.written());
}

fn writeValueTypeMsg(arena: Allocator, typeId: u32, unitId: u32) ![]u8 {
    var aw: Writer.Allocating = try .initCapacity(arena, 16);
    try putVarintField(&aw.writer, 1, typeId);
    try putVarintField(&aw.writer, 2, unitId);
    return aw.written();
}
