const std = @import("std");

const getConf = @import("conf.zig").getConf;
const Line = @import("store/lines.zig").Line;

const MemTable = @import("store/inmem/TableMem.zig");

pub const DataShard = struct {
    lines: std.ArrayList(*const Line),

    fn mustFlush(_: *DataShard) bool {
        return true;
    }

    fn flush(self: *DataShard, allocator: std.mem.Allocator) !void {
        if (self.lines.items.len == 0) {
            return;
        }
        const memTable = try MemTable.init(allocator);
        try memTable.addLines(allocator, self.lines.items);
    }
};

pub const Data = struct {
    shards: []DataShard,
    mutex: std.Thread.Mutex,

    pool: *std.Thread.Pool,
    wg: std.Thread.WaitGroup,
    stopped: std.atomic.Value(bool),

    pub fn init(allocator: std.mem.Allocator) !*Data {
        const conf = getConf().server.pools;
        std.debug.assert(conf.cpus != 0);
        std.debug.assert(conf.workerThreads != 0);

        const shards = try allocator.alloc(DataShard, conf.cpus);
        errdefer allocator.free(shards);

        var pool = try allocator.create(std.Thread.Pool);
        errdefer allocator.destroy(pool);
        try pool.init(.{
            .allocator = allocator,
            .n_jobs = conf.workerThreads,
        });
        errdefer pool.deinit();

        const wg: std.Thread.WaitGroup = .{};

        const self = try allocator.create(Data);

        self.* = Data{
            .shards = shards,
            .mutex = .{},
            .pool = pool,
            .wg = wg,
            .stopped = std.atomic.Value(bool).init(false),
        };

        // the allocator is different from http life cycle,
        // but shared between all the background jobs
        // TODO: find a better allocator, perhaps an arena with regular reset
        self.pool.spawnWg(&self.wg, startMemTableFlusher, .{ self, std.heap.page_allocator });

        return self;
    }

    pub fn deinit(self: *Data, allocator: std.mem.Allocator) void {
        self.stopped.store(true, .release);
        self.wg.wait();
        self.pool.deinit();
        allocator.destroy(self.pool);
        allocator.free(self.shards);
        allocator.destroy(self);
    }

    fn startMemTableFlusher(self: *Data, allocator: std.mem.Allocator) void {
        while (self.stopped.load(.acquire)) {
            std.Thread.sleep(std.time.ns_per_s);
            self.flushMemTable(allocator, false);
        }
        self.flushMemTable(allocator, true);
    }

    fn flushMemTable(self: *Data, allocator: std.mem.Allocator, force: bool) void {
        _ = self;
        _ = allocator;
        std.debug.print("flush completed with force={}\n", .{force});
    }

    pub fn addLines(self: *Data, allocator: std.mem.Allocator, lines: std.ArrayList(*const Line)) !void {
        // TODO: remove this garbage,
        // add an atomic counter and scroll shards like ring buffer, every shard has its own mutex to data
        self.mutex.lock();
        defer self.mutex.unlock();
        var shard = &self.shards[0];
        shard.lines = lines;

        if (shard.mustFlush()) {
            try shard.flush(allocator);
        } else {
            // TODO: start a timer to flush a shard every sec
            // reset the timer on flush
        }
    }
};

const Conf = @import("conf.zig").Conf;
test "dataWorker" {
    _ = Conf.default();

    const alloc = std.testing.allocator;
    var d = try Data.init(alloc);
    std.Thread.sleep(2 * 1_000_000_000);
    d.deinit(alloc);
}
