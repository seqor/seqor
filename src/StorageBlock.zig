const std = @import("std");

const StorageBlock = @This();

itemsData: std.ArrayList(u8) = .empty,
lensData: std.ArrayList(u8) = .empty,

pub fn reset(self: *StorageBlock) void {
    self.itemsData.clearRetainingCapacity();
    self.lensData.clearRetainingCapacity();
}
