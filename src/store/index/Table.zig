const std = @import("std");
const Allocator = std.mem.Allocator;

const MemTable = @import("MemTable.zig");
const DiskTable = @import("DiskTable.zig");

const Table = @This();

// either one has to be available
mem: ?*MemTable,
disk: *DiskTable,

inMerge: bool = false,

pub fn size(self: *Table) u64 {
    if (self.mem) |mem| {
        return mem.size();
    }
    return self.disk.?.size;
}
