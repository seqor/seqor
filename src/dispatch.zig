const AppConfig = @import("conf.zig").AppConfig;

const httpz = @import("httpz");

pub const AppContext = struct {
    conf: AppConfig,
};

pub const Dispatcher = struct {
    conf: AppConfig,

    pub fn dispatch(self: *Dispatcher, action: httpz.Action(*AppContext), req: *httpz.Request, res: *httpz.Response) !void {
        var ctx = AppContext{ .conf = self.conf };

        action(&ctx, req, res) catch {
            res.status = 500;
            res.body = "internal server error";
        };
    }
};
