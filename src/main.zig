const std = @import("std");
const r4os = @import("r4os");

const MAX_PACKET: usize = r4os.abi.net_service_tcp_read_max;
const MAX_FILE: usize = 512 * 1024;
const MAX_PATH: usize = 128;

const Packet = struct {
    name: []const u8,
    size: usize,
    checksum: u32,
    body_start: usize,
    resume_mode: bool,
};

const Options = struct {
    port: u16,
    outfile: ?[]const u8,
    no_overwrite: bool,
    resume_mode: bool,
    retries: u32,
};

const App = struct {
    app: *r4os.App2,
    sys: r4os.r4sys.Context,
    output: *r4os.launch_io.Output,
    network: *r4os.network_stream.Session,
    budget: *r4os.rpc_budget.Budget,
    args: []const u8,
    family: u32,

    fn write(self: *const App, value: []const u8) void {
        self.output.write(value);
    }
    fn printU64(self: *const App, value: u64) void {
        self.output.number(value);
    }
    fn fileDelete(self: *const App, path: [*:0]const u8) i32 {
        return self.sys.fileDelete(path);
    }

    fn fileInfoRaw(self: *const App, path: [*:0]const u8, info: *r4os.abi.FileInfo) i32 {
        return self.sys.fileInfoRaw(path, info);
    }

    fn ticks(self: *const App) u64 {
        return self.sys.ticks();
    }

    fn pathNamesEqualCollated(self: *const App, left: [*:0]const u8, right: [*:0]const u8) i32 {
        return self.sys.pathNamesEqualCollated(left, right);
    }

    fn fileStreamBegin(self: *const App, path: [*:0]const u8, flags: u32) i32 {
        return self.sys.fileStreamBegin(path, flags);
    }

    fn fileStreamAbort(self: *const App, path: [*:0]const u8) i32 {
        return self.sys.fileStreamAbort(path);
    }

    fn finishStage(self: *const App, writer: *r4os.file_stream.WriterState) bool {
        const rc = self.sys.fileStreamFinish(writer.path, writer.offset, r4os.r4sys.file_stream_finish_keep_ownership);
        writer.active = false;
        writer.error_code = rc;
        return rc == r4os.abi.file_stream_result_ok;
    }

    fn publish(self: *const App, target: [*:0]const u8, stage: [*:0]const u8, backup: [*:0]const u8, create_only: bool) bool {
        const files = r4os.Files{ .sys = self.sys };
        return files.replaceAtomic(
            .{ .ptr = target, .len = @intCast(zSlice(target).len) },
            .{ .ptr = stage, .len = @intCast(zSlice(stage).len) },
            .{ .ptr = backup, .len = @intCast(zSlice(backup).len) },
            .{ .require_target_absent = create_only, .require_owned_stage = create_only },
        ) == .ok;
    }
};

pub fn r4_app_main(app: *r4os.App2) i32 {
    var output: r4os.launch_io.Output = undefined;
    if (r4os.launch_io.Output.init(app, &output) != 0) return 1;
    var selected_family: u32 = 0;
    const args = r4os.network_stream.arguments(app.args(), &selected_family) orelse {
        output.write("RECV: invalid argument grammar\r\n");
        _ = output.close();
        return 1;
    };
    var budget = r4os.rpc_budget.Budget.fromTimeout(output.clock.read, .{ .kind = 1, .nanoseconds = 30_000_000_000 }) catch {
        _ = output.close();
        return 1;
    };
    var network: r4os.network_stream.Session = undefined;
    const opened = r4os.network_stream.Session.open(app, &budget, &network);
    if (opened != 0) {
        output.write("RECV: TCPSVC unavailable or invalid\r\n");
        _ = output.close();
        return 1;
    }
    const ctx: App = .{ .app = app, .sys = app.system(), .output = &output, .network = &network, .budget = &budget, .args = args, .family = selected_family };
    const result = run(&ctx);
    const closed = network.close();
    if (closed != 0) output.write("RECV close: not confirmed\r\n");
    const io_closed = output.close();
    return if (output.failed or closed != 0 or io_closed != 0) 1 else result;
}

fn run(ctx: *const App) i32 {
    const args = trim(ctx.args);
    const options = parseOptions(args) orelse {
        usage(ctx);
        return 1;
    };

    var attempt: u32 = 0;
    while (attempt <= options.retries) : (attempt += 1) {
        if (options.retries != 0) {
            ctx.write("RECV attempt: ");
            ctx.printU64(attempt + 1);
            ctx.write("/");
            ctx.printU64(options.retries + 1);
            ctx.write("\r\n");
        }
        const result = recvOnce(ctx, options);
        if (result == 0) return 0;
        if (result == 2 or ctx.network.cleanup_failed or ctx.output.failed) return 1;
        _ = ctx.budget.remainingTicks() catch return 1;
        if (attempt < options.retries) ctx.write("RECV retry: next\r\n");
    }
    return 1;
}

fn recvOnce(ctx: *const App, options: Options) i32 {
    ctx.write("RECV listen ");
    ctx.printU64(options.port);
    ctx.write(": waiting\r\n");
    if (ctx.output.failed) return 1;
    const endpoint: r4os.abi.NetworkEndpoint = .{ .address = .{ .family = if (ctx.family == 6) 6 else 4 }, .port = options.port };
    var listener: r4os.network_stream.Socket = undefined;
    if (ctx.network.create(endpoint, true, ctx.budget, &listener) != 0) {
        ctx.write("RECV result: listen-error\r\n");
        return 1;
    }
    defer listener.close();
    var socket: r4os.network_stream.Socket = undefined;
    if (listener.accept(ctx.budget, &socket) != 0) {
        ctx.write("RECV listen: accept failed\r\n");
        return 1;
    }
    defer socket.close();
    var reader: r4os.network_stream.Reader = .{ .socket = &socket };
    var packet_buf: [128]u8 = undefined;
    const header = (reader.line(&packet_buf, ctx.budget) catch return 1) orelse {
        ctx.write("RECV result: packet-error\r\n");
        return 1;
    };
    ctx.write("RECV accepted: conn=");
    ctx.printU64(socket.id);
    ctx.write(" bytes=");
    ctx.printU64(header.len);
    ctx.write("\r\n");
    const packet = parsePacket(header) orelse {
        sendError(ctx, &socket, "packet", 0, 0);
        ctx.write("RECV result: packet-error\r\n");
        return 1;
    };

    if (packet.size > MAX_FILE) {
        sendError(ctx, &socket, "too-large", 0, packet.size);
        ctx.write("RECV result: too-large\r\n");
        return 1;
    }

    var output_buf: [MAX_PATH]u8 = undefined;
    const output = resolveOutputPath(output_buf[0..], options.outfile, packet.name) orelse {
        sendError(ctx, &socket, "path", 0, packet.size);
        ctx.write("RECV result: path-error\r\n");
        return 1;
    };
    var path_buf: [MAX_PATH:0]u8 = .{0} ** MAX_PATH;
    const path_z = copyZ(path_buf[0..], output) orelse {
        sendError(ctx, &socket, "path", 0, packet.size);
        ctx.write("RECV result: path-error\r\n");
        return 1;
    };
    var info_value: r4os.abi.FileInfo = .{};
    const info_rc = ctx.fileInfoRaw(path_z, &info_value);
    if (info_rc < 0 or (info_value.exists != 0 and info_value.is_dir != 0)) {
        sendError(ctx, &socket, "target", 0, packet.size);
        ctx.write("RECV result: target-error\r\n");
        return 1;
    }
    const existing_info: ?r4os.abi.FileInfo = if (info_rc > 0 and info_value.exists != 0) info_value else null;
    if (options.no_overwrite and existing_info != null and !packet.resume_mode) {
        sendError(ctx, &socket, "exists", 0, packet.size);
        ctx.write("RECV result: exists\r\n");
        return 1;
    }

    if (packet.resume_mode and !options.resume_mode) {
        sendError(ctx, &socket, "resume-disabled", 0, packet.size);
        ctx.write("RECV result: resume-disabled\r\n");
        return 1;
    }

    var stage_buf: [MAX_PATH:0]u8 = .{0} ** MAX_PATH;
    var backup_buf: [MAX_PATH:0]u8 = .{0} ** MAX_PATH;
    if (!createStage(ctx, path_z, &stage_buf, &backup_buf)) {
        sendError(ctx, &socket, "stage", 0, packet.size);
        ctx.write("RECV result: stage-error\r\n");
        return 1;
    }
    const stage: [*:0]const u8 = &stage_buf;
    const backup: [*:0]const u8 = &backup_buf;
    var writer = r4os.file_stream.WriterState{ .path = stage, .active = true };
    // Abort is bound to the stream's exact ownership, including after a
    // failed finish. It cannot delete a replacement by its path alone.
    defer _ = ctx.fileStreamAbort(stage);

    var file_len: usize = 0;
    var sum: u32 = 0;
    if (packet.resume_mode) {
        if (existing_info) |info| {
            if (info.is_dir != 0 or info.size > packet.size) {
                sendError(ctx, &socket, "resume-size", @intCast(info.size), packet.size);
                ctx.write("RECV result: resume-error\r\n");
                return 1;
            }
            file_len = @intCast(info.size);
            sum = copyPrefix(ctx, path_z, &writer, file_len) orelse {
                sendError(ctx, &socket, "resume-read", file_len, packet.size);
                ctx.write("RECV result: resume-error\r\n");
                return 1;
            };
        }
        if (file_len == packet.size and existing_info != null) {
            if (sum != packet.checksum) {
                sendError(ctx, &socket, "checksum", file_len, packet.size);
                ctx.write("RECV result: checksum-error\r\n");
                return 1;
            }
            var ok_buf: [48]u8 = undefined;
            const ok = buildOk(ok_buf[0..], packet.size, packet.checksum) orelse {
                ctx.write("RECV result: ack-build-error\r\n");
                return 1;
            };
            const sent_ok = socket.writeAll(ok, ctx.budget) catch return 1;
            if (!r4os.network_stream.complete(sent_ok)) {
                ctx.write("RECV ack: failed\r\n");
                return 1;
            }
            ctx.write("RECV resume: complete\r\n");
            ctx.write("RECV ack: ");
            ctx.write(trim(ok));
            ctx.write("\r\n");
            ctx.write("RECV result: ok\r\n");
            return 0;
        }
        var cont_buf: [32]u8 = undefined;
        const cont = buildContinue(cont_buf[0..], file_len) orelse {
            sendError(ctx, &socket, "resume", file_len, packet.size);
            ctx.write("RECV result: resume-error\r\n");
            return 1;
        };
        const cont_written = socket.writeAll(cont, ctx.budget) catch return 1;
        if (!r4os.network_stream.complete(cont_written)) {
            ctx.write("RECV resume: cont-failed\r\n");
            return 1;
        }
        ctx.write("RECV resume: offset=");
        ctx.printU64(file_len);
        ctx.write("\r\n");
    }

    var read_buf: [MAX_PACKET]u8 = undefined;
    while (file_len < packet.size) {
        const want = @min(read_buf.len, packet.size - file_len);
        const received = reader.read(read_buf[0..want], true, ctx.budget) catch return 1;
        const chunk = read_buf[0..@intCast(received.bytes)];
        if (!writeChunk(ctx, &writer, chunk)) {
            sendError(ctx, &socket, "save", file_len, packet.size);
            ctx.write("RECV result: save-error\r\n");
            return 1;
        }
        sum = checksumUpdate(sum, chunk);
        file_len += chunk.len;
        if (!r4os.network_stream.complete(received)) {
            sendError(ctx, &socket, "incomplete", file_len, packet.size);
            ctx.write("RECV result: incomplete prefix=");
            ctx.printU64(file_len);
            ctx.write("\r\n");
            return 1;
        }
    }

    if (sum != packet.checksum) {
        sendError(ctx, &socket, "checksum", file_len, packet.size);
        ctx.write("RECV result: checksum-error\r\n");
        return 1;
    }

    _ = ctx.budget.remainingTicks() catch return 1;
    if (!ctx.finishStage(&writer)) {
        sendError(ctx, &socket, "finish", file_len, packet.size);
        ctx.write("RECV result: finish-error\r\n");
        return 1;
    }
    _ = ctx.budget.remainingTicks() catch return 1;
    const create_only = options.no_overwrite and (!packet.resume_mode or existing_info == null);
    if (!ctx.publish(path_z, stage, backup, create_only)) {
        sendError(ctx, &socket, "publish", file_len, packet.size);
        ctx.write("RECV result: publish-error; retained paths: ");
        ctx.write(zSlice(stage));
        ctx.write(" ");
        ctx.write(zSlice(backup));
        ctx.write("\r\n");
        return 1;
    }
    // The confirmed replacement owns this previously absent backup name.
    // Never remove a backup after an ambiguous/failed publication.
    if (ctx.fileDelete(backup) < 0) {
        ctx.write("RECV backup retained: ");
        ctx.write(zSlice(backup));
        ctx.write("\r\n");
    }

    ctx.write("RECV file: ");
    ctx.write(packet.name);
    ctx.write(" size=");
    ctx.printU64(packet.size);
    ctx.write(" checksum=");
    ctx.printU64(packet.checksum);
    ctx.write("\r\n");

    ctx.write("RECV saved: ");
    ctx.printU64(file_len);
    ctx.write(" bytes to ");
    ctx.write(output);
    ctx.write("\r\n");

    var ack_buf: [48]u8 = undefined;
    const ack = buildOk(ack_buf[0..], packet.size, packet.checksum) orelse {
        ctx.write("RECV result: ack-build-error\r\n");
        return 1;
    };
    const written = socket.writeAll(ack, ctx.budget) catch return 2;
    if (!r4os.network_stream.complete(written)) {
        ctx.write("RECV ack: failed after publication\r\n");
        return 2;
    }

    ctx.write("RECV ack: ");
    ctx.write(trim(ack));
    ctx.write("\r\n");
    ctx.write("RECV result: ok\r\n");
    return 0;
}

fn usage(ctx: *const App) void {
    ctx.write("Usage: RECV [-4|-6] port [outfile|dir\\] [/NOOVERWRITE] [/RESUME] [/RETRY n]\r\n");
}

fn parseOptions(args: []const u8) ?Options {
    var rest = trim(args);
    const port_token = takeToken(rest) orelse return null;
    rest = port_token.rest;
    var outfile: ?[]const u8 = null;
    var no_overwrite = false;
    var resume_mode = false;
    var retries: u32 = 0;
    while (true) {
        const tok = takeToken(rest) orelse break;
        rest = tok.rest;
        if (bytesEqual(tok.token, "/NOOVERWRITE") or bytesEqual(tok.token, "/N")) {
            no_overwrite = true;
        } else if (bytesEqual(tok.token, "/RESUME") or bytesEqual(tok.token, "/R")) {
            resume_mode = true;
        } else if (bytesEqual(tok.token, "/RETRY")) {
            const retry_token = takeToken(rest) orelse return null;
            rest = retry_token.rest;
            retries = parseRetry(retry_token.token) orelse return null;
        } else if (outfile == null) {
            outfile = tok.token;
        } else {
            return null;
        }
    }
    return .{
        .port = parsePort(port_token.token) orelse return null,
        .outfile = outfile,
        .no_overwrite = no_overwrite,
        .resume_mode = resume_mode,
        .retries = retries,
    };
}

fn parsePacket(data: []const u8) ?Packet {
    const header_end = indexOfCrlf(data) orelse return null;
    const header = data[0..header_end];
    var rest = header;
    const magic = takeToken(rest) orelse return null;
    const resume_mode = if (bytesEqual(magic.token, "R4SEND2")) true else blk: {
        if (!bytesEqual(magic.token, "R4SEND1")) return null;
        break :blk false;
    };
    rest = magic.rest;
    const name_token = takeToken(rest) orelse return null;
    if (!validName(name_token.token)) return null;
    rest = name_token.rest;
    const size_token = takeToken(rest) orelse return null;
    rest = size_token.rest;
    const checksum_token = takeToken(rest) orelse return null;
    if (checksum_token.rest.len != 0) return null;

    const size = parseUsize(size_token.token) orelse return null;
    const sum = parseU32(checksum_token.token) orelse return null;
    if (size > MAX_FILE) return null;
    return .{
        .name = name_token.token,
        .size = size,
        .checksum = sum,
        .body_start = header_end + 2,
        .resume_mode = resume_mode,
    };
}

fn resolveOutputPath(out: []u8, target: ?[]const u8, packet_name: []const u8) ?[]const u8 {
    const value = target orelse return packet_name;
    if (value.len == 0) return packet_name;
    if (endsWithSlash(value)) {
        var len: usize = 0;
        if (!append(out, &len, value)) return null;
        if (!append(out, &len, packet_name)) return null;
        return out[0..len];
    }
    return value;
}

fn endsWithSlash(value: []const u8) bool {
    if (value.len == 0) return false;
    const last = value[value.len - 1];
    return last == '\\' or last == '/';
}

fn buildOk(out: []u8, size: usize, sum: u32) ?[]const u8 {
    var len: usize = 0;
    if (!append(out, &len, "OK ")) return null;
    if (!appendU64(out, &len, size)) return null;
    if (!append(out, &len, " ")) return null;
    if (!appendU64(out, &len, sum)) return null;
    if (!append(out, &len, "\r\n")) return null;
    return out[0..len];
}

fn buildContinue(out: []u8, offset: usize) ?[]const u8 {
    var len: usize = 0;
    if (!append(out, &len, "CONT ")) return null;
    if (!appendU64(out, &len, offset)) return null;
    if (!append(out, &len, "\r\n")) return null;
    return out[0..len];
}

fn sendError(ctx: *const App, socket: *r4os.network_stream.Socket, code: []const u8, saved: usize, expected: usize) void {
    var buf: [64]u8 = undefined;
    var len: usize = 0;
    if (!append(buf[0..], &len, "ERR ")) return;
    if (!append(buf[0..], &len, code)) return;
    if (!append(buf[0..], &len, " ")) return;
    if (!appendU64(buf[0..], &len, saved)) return;
    if (!append(buf[0..], &len, " ")) return;
    if (!appendU64(buf[0..], &len, expected)) return;
    if (!append(&buf, &len, "\r\n")) return;
    _ = socket.writeAll(buf[0..len], ctx.budget) catch return;
}

fn validName(value: []const u8) bool {
    if (value.len == 0 or value.len > 48) return false;
    for (value) |ch| {
        if (ch < 32 or ch == 127 or ch == '"' or isSpace(ch) or ch == '/' or ch == '\\' or ch == ':') return false;
    }
    return true;
}

fn checksumUpdate(seed: u32, data: []const u8) u32 {
    var out = seed;
    for (data) |ch| out +%= ch;
    return out;
}

fn writeChunk(ctx: *const App, writer: *r4os.file_stream.WriterState, bytes: []const u8) bool {
    if (!writer.active) return false;
    var sink = r4os.stream_transfer.adapters.FileStream.fromContext2(&ctx.app.context, writer.path, writer.offset) catch return false;
    const driver: r4os.stream_transfer.adapters.Driver = .{ .sys = ctx.sys, .clock = &ctx.network.clock, .budget = ctx.budget };
    const progress = driver.write(bytes, MAX_PACKET, r4os.stream_transfer.adapters.FileStream.step, &sink) catch return false;
    writer.offset += progress.bytes;
    writer.error_code = progress.error_code;
    return r4os.network_stream.complete(progress);
}

fn copyPrefix(ctx: *const App, path: [*:0]const u8, writer: *r4os.file_stream.WriterState, size: usize) ?u32 {
    const before = switch (r4os.metadata2.info(&ctx.app.context, path)) {
        .found => |value| value,
        else => return null,
    };
    if (before.flags & r4os.abi.file_metadata2_flag_byte_size_valid == 0 or before.byte_size != size) return null;
    const allocator = ctx.app.allocator() orelse return null;
    const snapshot = allocator.alloc(u8, size) catch return null;
    defer allocator.free(snapshot);
    var source = r4os.stream_transfer.adapters.FileRead.fromContext2(&ctx.app.context, path, 0) catch return null;
    const driver: r4os.stream_transfer.adapters.Driver = .{ .sys = ctx.sys, .clock = &ctx.network.clock, .budget = ctx.budget };
    const result = driver.read(snapshot, MAX_PACKET, r4os.stream_transfer.adapters.FileRead.step, &source, true) catch return null;
    if (!r4os.network_stream.complete(result)) return null;
    const after = switch (r4os.metadata2.info(&ctx.app.context, path)) {
        .found => |value| value,
        else => return null,
    };
    if (!std.meta.eql(before, after)) return null;
    // Stage writes publish a global namespace change. Complete source
    // validation before beginning those writes, retaining every identity field.
    if (!writeChunk(ctx, writer, snapshot)) return null;
    return checksumUpdate(0, snapshot);
}

var stage_sequence: u32 = 0;

fn siblingPath(out: [:0]u8, target: []const u8, id: u32, extension: []const u8) ?[*:0]const u8 {
    var prefix: usize = 0;
    for (target, 0..) |ch, i| {
        if (ch == '/' or ch == '\\' or ch == ':') prefix = i + 1;
    }
    var name: [12]u8 = undefined;
    @memcpy(name[0..2], "RC");
    const hex = "0123456789ABCDEF";
    for (0..6) |i| name[2 + i] = hex[(id >> @as(u5, @intCast((5 - i) * 4))) & 15];
    @memcpy(name[8..12], extension);
    if (prefix + name.len >= out.len) return null;
    @memcpy(out[0..prefix], target[0..prefix]);
    @memcpy(out[prefix..][0..name.len], &name);
    out[prefix + name.len] = 0;
    return @ptrCast(out.ptr);
}

fn createStage(ctx: *const App, target: [*:0]const u8, stage_buf: [:0]u8, backup_buf: [:0]u8) bool {
    const seed: u32 = @truncate(ctx.ticks() ^ @intFromPtr(ctx));
    for (0..64) |_| {
        stage_sequence +%= 1;
        const id = seed +% stage_sequence;
        const stage = siblingPath(stage_buf, zSlice(target), id, ".TMP") orelse return false;
        const backup = siblingPath(backup_buf, zSlice(target), id, ".BAK") orelse return false;
        if (ctx.pathNamesEqualCollated(target, stage) != 0 or ctx.pathNamesEqualCollated(target, backup) != 0) continue;
        var info: r4os.abi.FileInfo = .{};
        const rc = ctx.fileInfoRaw(backup, &info);
        if (rc < 0) return false;
        if (rc > 0 and info.exists != 0) continue;
        const opened = ctx.fileStreamBegin(stage, r4os.abi.file_stream_open_create);
        if (opened == r4os.abi.file_stream_result_ok) return true;
        if (opened != r4os.abi.file_stream_error_exists) {
            _ = ctx.fileStreamAbort(stage);
            return false;
        }
    }
    return false;
}

fn append(out: []u8, len: *usize, text: []const u8) bool {
    if (len.* + text.len > out.len) return false;
    @memcpy(out[len.* .. len.* + text.len], text);
    len.* += text.len;
    return true;
}

fn appendU64(out: []u8, len: *usize, value: u64) bool {
    var digits: [20]u8 = undefined;
    var count: usize = 0;
    var n = value;
    if (n == 0) return append(out, len, "0");
    while (n > 0) {
        digits[digits.len - 1 - count] = '0' + @as(u8, @intCast(n % 10));
        count += 1;
        n /= 10;
    }
    return append(out, len, digits[digits.len - count ..]);
}

const Token = struct { token: []const u8, rest: []const u8 };
fn takeToken(value: []const u8) ?Token {
    var failure: r4os.arguments.Failure = .{};
    var cursor = r4os.arguments.ArgCursor.init(value, .{}, &failure) catch return null;
    const token = (cursor.next() catch return null) orelse return null;
    return .{ .token = token.bytes(value) catch return null, .rest = cursor.remainingArguments() catch return null };
}

fn parsePort(value: []const u8) ?u16 {
    if (value.len == 0) return null;
    for (value) |byte| if (byte < '0' or byte > '9') return null;
    const number = std.fmt.parseInt(u16, value, 10) catch return null;
    return if (number == 0) null else number;
}

fn parseU32(value: []const u8) ?u32 {
    const parsed = parseU64(value) orelse return null;
    if (parsed > 0xFFFF_FFFF) return null;
    return @intCast(parsed);
}

fn parseUsize(value: []const u8) ?usize {
    if (value.len == 0) return null;
    for (value) |byte| if (byte < '0' or byte > '9') return null;
    const number = std.fmt.parseInt(usize, value, 10) catch return null;
    return number;
}

fn parseRetry(value: []const u8) ?u32 {
    const parsed = parseU32(value) orelse return null;
    if (parsed > 9) return null;
    return parsed;
}

fn parseU64(value: []const u8) ?u64 {
    if (value.len == 0) return null;
    for (value) |byte| if (byte < '0' or byte > '9') return null;
    const number = std.fmt.parseInt(u64, value, 10) catch return null;
    return number;
}

fn indexOfCrlf(value: []const u8) ?usize {
    var i: usize = 0;
    while (i + 1 < value.len) : (i += 1) {
        if (value[i] == '\r' and value[i + 1] == '\n') return i;
    }
    return null;
}

fn copyZ(out: [:0]u8, text: []const u8) ?[*:0]const u8 {
    if (text.len >= out.len) return null;
    @memcpy(out[0..text.len], text);
    out[text.len] = 0;
    return @ptrCast(out.ptr);
}

fn zSlice(ptr: [*:0]const u8) []const u8 {
    var len: usize = 0;
    while (ptr[len] != 0) : (len += 1) {}
    return ptr[0..len];
}

fn trim(value: []const u8) []const u8 {
    var start: usize = 0;
    var end = value.len;
    while (start < end and isSpace(value[start])) : (start += 1) {}
    while (end > start and isSpace(value[end - 1])) : (end -= 1) {}
    return value[start..end];
}

fn isSpace(ch: u8) bool {
    return ch == ' ' or ch == '\t' or ch == '\r' or ch == '\n';
}

fn bytesEqual(a: []const u8, b: []const u8) bool {
    if (a.len != b.len) return false;
    var i: usize = 0;
    while (i < a.len) : (i += 1) {
        if (a[i] != b[i]) return false;
    }
    return true;
}

test "migration recv preserves payload format and frames split control records" {
    const packet = parsePacket("R4SEND1 Grüße.bin 3 294\r\nabc").?;
    try std.testing.expectEqualStrings("Grüße.bin", packet.name);
    try std.testing.expectEqual(@as(usize, 3), packet.size);
    try std.testing.expectEqual(@as(u32, 294), packet.checksum);
    try std.testing.expect(!packet.resume_mode);
    try std.testing.expect(parsePacket("R4SEND1 x.bin 999999999999999999999999 1\r\n") == null);
    var buffer: [64]u8 = undefined;
    try std.testing.expectEqualStrings("OK 3 294\r\n", buildOk(&buffer, 3, 294).?);
    try std.testing.expectEqualStrings("CONT 12\r\n", buildContinue(&buffer, 12).?);
}
