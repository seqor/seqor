const std = @import("std");

const datetime = @import("datetime").datetime;

const build = @import("build");
const Conf = @import("conf.zig").Conf;
const server = @import("server.zig");
const cli = @import("cli");

var cli_config = struct {
    config: []const u8 = "seqor.yaml",
}{};

pub fn main() !void {
    var runner = try cli.AppRunner.init(std.heap.page_allocator);

    const app = cli.App{
        .author = "Seqor",
        .version = build.version,
        .command = cli.Command{
            .name = "run",
            .description = cli.Description{
                .one_line = "Start the Seqor server",
            },
            .options = try runner.allocOptions(&.{
                .{
                    .long_name = "config-file",
                    .short_alias = 'c',
                    .help = "file path to configuration file",
                    .value_ref = runner.mkRef(&cli_config.config),
                },
            }),
            .target = cli.CommandTarget{
                .action = cli.CommandAction{ .exec = run_server },
            },
        },
    };

    return runner.run(&app);
}

// Action function to execute when the "short" command is invoked.
fn run_server() !void {
    std.debug.print("Seqor version {s}", .{build.version});

    const config = try Conf.init(std.heap.page_allocator, cli_config.config);
    const now_str = try datetime.Datetime.now().formatISO8601(std.heap.page_allocator, false);

    // TODO: introduce structured logger
    std.debug.print("Seqor in mono mode starting at port={d}, time={s}\n", .{ config.server.port, now_str });

    try server.startServer(std.heap.page_allocator, config);
}
