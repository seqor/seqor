pub const Field = struct {
    name: []const u8,
    value: []const u8,
};

pub const Processor = struct {
    pub fn pushLine(_: *Processor, _: i128, _: []const Field) !void {}
    pub fn flush(_: *Processor) !void {}
};
