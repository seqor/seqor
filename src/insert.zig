/// insert module provides write path for Seqor
const std = @import("std");

const httpz = @import("httpz");

const Context = @import("dispatch.zig").Context;

/// insertLokiJson defines a loki json insertion operation
pub fn insertLokiJson(ctx: *Context, r: *httpz.Request, res: *httpz.Response) !void {
    const contentType = r.headers.get("Content-Type");
    if (contentType != null and !std.mem.eql(u8, "application/json", contentType.?)) {
        res.status = 400;
        res.body = "grpc API is not supported";
        return;
    }
    const params = collectParams(r) catch {
        res.body = "failed to parse params";
        res.status = 500;
        return;
    };
    const body = r.body();
    if (body == null) {
        res.body = "given empty body";
        res.status = 400;
        return;
    }
    // TODO: validate a disk has enough space
    const encoding = r.headers.get("Content-Encoding") orelse "";
    const decompressed = decompress(body.?, encoding, ctx.conf.maxRequestSize) catch {
        res.body = "failed to decompress body";
        res.status = 500;
        return;
    };

    process(params, decompressed) catch {
        res.body = "failed to process logs";
        res.status = 500;
        return;
    };

    res.status = 200;
}

/// insertLokiReady defines a loki handler to signal its readiness
pub fn insertLokiReady(_: *Context, _: *httpz.Request, res: *httpz.Response) !void {
    res.status = 200;
    res.body = "ready";
}

const Params = struct {};

fn collectParams(r: *httpz.Request) !Params {
    _ = r;
    return .{};
}

fn decompress(body: []const u8, encoding: []const u8, maxRequestSize: u32) ![]u8 {
    _ = body;
    _ = maxRequestSize;
    if (std.mem.eql(u8, encoding, "snappy")) {
        // TODO: support snappy encoding
    }

    // TODO: support gzip to cover loki fully

    // TODO: suport the other compressions types here like zstd, datadog, etc.
    return "";
}

fn process(params: Params, data: []u8) !void {
    _ = params;
    _ = data;
}
