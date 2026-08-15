const r4os = @import("r4os");

const MAX_PACKET: usize = r4os.abi.net_service_tcp_read_max;
const MAX_FILE: usize = 512 * 1024;
const MAX_PATH: usize = 128;
const BODY_WAIT_MS: u64 = 3000;

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
    sys: r4os.r4sys.Context,
    net: r4os.r4net.Context,

    fn init(r4_app: *r4os.App) ?App {
        return .{
            .sys = r4_app.system(),
            .net = r4_app.networkLowLevel() orelse return null,
        };
    }

    fn argsRaw(self: *const App) [*:0]const u8 {
        return self.sys.argsRaw();
    }

    fn write(self: *const App, value: []const u8) void {
        self.sys.write(value);
    }

    fn printU64(self: *const App, value: u64) void {
        self.sys.printU64(value);
    }

    fn ticksFromMilliseconds(self: *const App, ms: u64) u64 {
        return self.sys.ticksFromMilliseconds(ms);
    }

    fn fileInfo(self: *const App, path: [*:0]const u8) ?r4os.abi.FileInfo {
        return self.sys.fileInfo(path);
    }

    fn fileWrite(self: *const App, path: [*:0]const u8, data: []const u8) i32 {
        return self.sys.fileWrite(path, data);
    }

    fn fileAppend(self: *const App, path: [*:0]const u8, data: []const u8) i32 {
        return self.sys.fileAppend(path, data);
    }

    fn fileReadAt(self: *const App, path: [*:0]const u8, offset: u32, out: []u8) i32 {
        return self.sys.fileReadAt(path, offset, out);
    }

    fn fileDelete(self: *const App, path: [*:0]const u8) i32 {
        return self.sys.fileDelete(path);
    }

    fn tcpAcceptPollReadService(self: *const App, port: u16, out: []u8, result: *r4os.abi.TcpAcceptResult) i32 {
        return self.net.tcpAcceptPollReadService(port, out, result);
    }

    fn tcpWriteService(self: *const App, handle: u32, data: []const u8) i32 {
        return self.net.tcpWriteService(handle, data);
    }

    fn tcpReadWaitService(self: *const App, handle: u32, out: []u8, wait_ticks: u64) i32 {
        return self.net.tcpReadWaitService(handle, out, wait_ticks);
    }

    fn tcpCloseService(self: *const App, handle: u32) i32 {
        return self.net.tcpCloseService(handle);
    }
};

pub fn r4_app_main(r4_app: *r4os.App) i32 {
    const ctx = App.init(r4_app) orelse return r4os.abi.err_no_group;
    const args = trim(zSlice(ctx.argsRaw()));
    const options = parseOptions(args) orelse {
        usage(&ctx);
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
        const result = recvOnce(&ctx, options);
        if (result == 0) return 0;
        if (attempt < options.retries) ctx.write("RECV retry: next\r\n");
    }
    return 1;
}

fn recvOnce(ctx: *const App, options: Options) i32 {
    var packet_buf: [MAX_PACKET]u8 = undefined;
    var accept: r4os.abi.TcpAcceptResult = .{};

    ctx.write("RECV listen ");
    ctx.printU64(options.port);
    ctx.write(": waiting\r\n");

    const got = ctx.tcpAcceptPollReadService(options.port, packet_buf[0..], &accept);
    if (got <= 0 or accept.conn_id == 0) {
        ctx.write("RECV listen ");
        ctx.printU64(options.port);
        ctx.write(": ");
        ctx.write(if (got == 0) "timeout\r\n" else "failed\r\n");
        return 1;
    }

    ctx.write("RECV accepted: conn=");
    ctx.printU64(accept.conn_id);
    ctx.write(" bytes=");
    ctx.printU64(accept.bytes);
    ctx.write("\r\n");

    const packet = parsePacket(packet_buf[0..@intCast(got)]) orelse {
        sendError(ctx, accept.conn_id, "packet", 0, 0);
        _ = ctx.tcpCloseService(accept.conn_id);
        ctx.write("RECV result: packet-error\r\n");
        return 1;
    };

    if (packet.size > MAX_FILE) {
        sendError(ctx, accept.conn_id, "too-large", 0, packet.size);
        _ = ctx.tcpCloseService(accept.conn_id);
        ctx.write("RECV result: too-large\r\n");
        return 1;
    }

    var output_buf: [MAX_PATH]u8 = undefined;
    const output = resolveOutputPath(output_buf[0..], options.outfile, packet.name) orelse {
        sendError(ctx, accept.conn_id, "path", 0, packet.size);
        _ = ctx.tcpCloseService(accept.conn_id);
        ctx.write("RECV result: path-error\r\n");
        return 1;
    };
    var path_buf: [MAX_PATH:0]u8 = .{0} ** MAX_PATH;
    const path_z = copyZ(path_buf[0..], output) orelse {
        sendError(ctx, accept.conn_id, "path", 0, packet.size);
        _ = ctx.tcpCloseService(accept.conn_id);
        ctx.write("RECV result: path-error\r\n");
        return 1;
    };
    const existing_info = ctx.fileInfo(path_z);
    if (options.no_overwrite and existing_info != null and !options.resume_mode) {
        sendError(ctx, accept.conn_id, "exists", 0, packet.size);
        _ = ctx.tcpCloseService(accept.conn_id);
        ctx.write("RECV result: exists\r\n");
        return 1;
    }

    if (packet.resume_mode and !options.resume_mode) {
        sendError(ctx, accept.conn_id, "resume-disabled", 0, packet.size);
        _ = ctx.tcpCloseService(accept.conn_id);
        ctx.write("RECV result: resume-disabled\r\n");
        return 1;
    }

    var file_len: usize = 0;
    var sum: u32 = 0;
    if (packet.resume_mode) {
        if (existing_info) |info| {
            if (info.is_dir != 0 or info.size > packet.size) {
                sendError(ctx, accept.conn_id, "resume-size", @intCast(info.size), packet.size);
                _ = ctx.tcpCloseService(accept.conn_id);
                ctx.write("RECV result: resume-error\r\n");
                return 1;
            }
            file_len = @intCast(info.size);
            sum = checksumFile(ctx, path_z, file_len) orelse {
                sendError(ctx, accept.conn_id, "resume-read", file_len, packet.size);
                _ = ctx.tcpCloseService(accept.conn_id);
                ctx.write("RECV result: resume-error\r\n");
                return 1;
            };
        }
        if (file_len == packet.size) {
            if (sum != packet.checksum) {
                sendError(ctx, accept.conn_id, "checksum", file_len, packet.size);
                _ = ctx.tcpCloseService(accept.conn_id);
                ctx.write("RECV result: checksum-error\r\n");
                return 1;
            }
            var ok_buf: [48]u8 = undefined;
            const ok = buildOk(ok_buf[0..], packet.size, packet.checksum) orelse {
                _ = ctx.tcpCloseService(accept.conn_id);
                ctx.write("RECV result: ack-build-error\r\n");
                return 1;
            };
            _ = ctx.tcpWriteService(accept.conn_id, ok);
            _ = ctx.tcpCloseService(accept.conn_id);
            ctx.write("RECV resume: complete\r\n");
            ctx.write("RECV ack: ");
            ctx.write(ok);
            ctx.write("\r\n");
            ctx.write("RECV result: ok\r\n");
            return 0;
        }
        var cont_buf: [32]u8 = undefined;
        const cont = buildContinue(cont_buf[0..], file_len) orelse {
            sendError(ctx, accept.conn_id, "resume", file_len, packet.size);
            _ = ctx.tcpCloseService(accept.conn_id);
            ctx.write("RECV result: resume-error\r\n");
            return 1;
        };
        const cont_written = ctx.tcpWriteService(accept.conn_id, cont);
        if (cont_written < 0 or cont_written != @as(i32, @intCast(cont.len))) {
            _ = ctx.tcpCloseService(accept.conn_id);
            ctx.write("RECV resume: cont-failed\r\n");
            return 1;
        }
        ctx.write("RECV resume: offset=");
        ctx.printU64(file_len);
        ctx.write("\r\n");
    } else if (existing_info != null) {
        _ = ctx.fileDelete(path_z);
    }

    if (packet.size == 0) {
        const saved_empty = ctx.fileWrite(path_z, "");
        if (saved_empty < 0) {
            sendError(ctx, accept.conn_id, "save", 0, 0);
            _ = ctx.tcpCloseService(accept.conn_id);
            ctx.write("RECV result: save-error\r\n");
            return 1;
        }
    }

    if (packet.body_start < @as(usize, @intCast(got))) {
        const available = @as(usize, @intCast(got)) - packet.body_start;
        const copy_len = @min(available, packet.size);
        const chunk = packet_buf[packet.body_start .. packet.body_start + copy_len];
        const saved = ctx.fileAppend(path_z, chunk);
        if (saved < 0 or saved != @as(i32, @intCast(chunk.len))) {
            sendError(ctx, accept.conn_id, "save", file_len, packet.size);
            if (!packet.resume_mode) _ = ctx.fileDelete(path_z);
            _ = ctx.tcpCloseService(accept.conn_id);
            ctx.write("RECV result: save-error\r\n");
            return 1;
        }
        sum = checksumUpdate(sum, chunk);
        file_len += copy_len;
    }

    var read_buf: [MAX_PACKET]u8 = undefined;
    const body_wait_ticks = ctx.ticksFromMilliseconds(BODY_WAIT_MS);
    while (file_len < packet.size) {
        const got_more = ctx.tcpReadWaitService(accept.conn_id, read_buf[0..], body_wait_ticks);
        if (got_more <= 0) {
            sendError(ctx, accept.conn_id, "incomplete", file_len, packet.size);
            if (!packet.resume_mode) _ = ctx.fileDelete(path_z);
            _ = ctx.tcpCloseService(accept.conn_id);
            ctx.write("RECV result: incomplete\r\n");
            return 1;
        }
        const got_len: usize = @intCast(got_more);
        const copy_len = @min(got_len, packet.size - file_len);
        const chunk = read_buf[0..copy_len];
        const saved = ctx.fileAppend(path_z, chunk);
        if (saved < 0 or saved != @as(i32, @intCast(chunk.len))) {
            sendError(ctx, accept.conn_id, "save", file_len, packet.size);
            if (!packet.resume_mode) _ = ctx.fileDelete(path_z);
            _ = ctx.tcpCloseService(accept.conn_id);
            ctx.write("RECV result: save-error\r\n");
            return 1;
        }
        sum = checksumUpdate(sum, chunk);
        file_len += copy_len;
    }

    if (sum != packet.checksum) {
        sendError(ctx, accept.conn_id, "checksum", file_len, packet.size);
        if (!packet.resume_mode) _ = ctx.fileDelete(path_z);
        _ = ctx.tcpCloseService(accept.conn_id);
        ctx.write("RECV result: checksum-error\r\n");
        return 1;
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
        _ = ctx.tcpCloseService(accept.conn_id);
        ctx.write("RECV result: ack-build-error\r\n");
        return 1;
    };
    const written = ctx.tcpWriteService(accept.conn_id, ack);
    _ = ctx.tcpCloseService(accept.conn_id);
    if (written < 0 or written != @as(i32, @intCast(ack.len))) {
        ctx.write("RECV ack: failed\r\n");
        return 1;
    }

    ctx.write("RECV ack: ");
    ctx.write(ack);
    ctx.write("\r\n");
    ctx.write("RECV result: ok\r\n");
    return 0;
}

fn usage(ctx: *const App) void {
    ctx.write("Usage: RECV port [outfile|dir\\] [/NOOVERWRITE] [/RESUME] [/RETRY n]\r\n");
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
    return out[0..len];
}

fn buildContinue(out: []u8, offset: usize) ?[]const u8 {
    var len: usize = 0;
    if (!append(out, &len, "CONT ")) return null;
    if (!appendU64(out, &len, offset)) return null;
    return out[0..len];
}

fn sendError(ctx: *const App, conn_id: u32, code: []const u8, saved: usize, expected: usize) void {
    var buf: [64]u8 = undefined;
    var len: usize = 0;
    if (!append(buf[0..], &len, "ERR ")) return;
    if (!append(buf[0..], &len, code)) return;
    if (!append(buf[0..], &len, " ")) return;
    if (!appendU64(buf[0..], &len, saved)) return;
    if (!append(buf[0..], &len, " ")) return;
    if (!appendU64(buf[0..], &len, expected)) return;
    _ = ctx.tcpWriteService(conn_id, buf[0..len]);
}

fn validName(value: []const u8) bool {
    if (value.len == 0 or value.len > 48) return false;
    for (value) |ch| {
        if (isSpace(ch) or ch == '/' or ch == '\\' or ch == ':') return false;
    }
    return true;
}

fn checksumUpdate(seed: u32, data: []const u8) u32 {
    var out = seed;
    for (data) |ch| out +%= ch;
    return out;
}

fn checksumFile(ctx: *const App, path: [*:0]const u8, size: usize) ?u32 {
    var out: u32 = 0;
    var offset: usize = 0;
    var buf: [MAX_PACKET]u8 = undefined;
    while (offset < size) {
        const want = @min(buf.len, size - offset);
        const got = ctx.fileReadAt(path, @intCast(offset), buf[0..want]);
        if (got < 0 or got != @as(i32, @intCast(want))) return null;
        out = checksumUpdate(out, buf[0..@intCast(got)]);
        offset += @intCast(got);
    }
    return out;
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

const Token = struct {
    token: []const u8,
    rest: []const u8,
};

fn takeToken(value: []const u8) ?Token {
    const trimmed = trim(value);
    if (trimmed.len == 0) return null;
    var end: usize = 0;
    while (end < trimmed.len and !isSpace(trimmed[end])) : (end += 1) {}
    return .{
        .token = trimmed[0..end],
        .rest = if (end >= trimmed.len) "" else trim(trimmed[end..]),
    };
}

fn parsePort(value: []const u8) ?u16 {
    const parsed = parseU32(value) orelse return null;
    if (parsed == 0 or parsed > 65535) return null;
    return @intCast(parsed);
}

fn parseU32(value: []const u8) ?u32 {
    const parsed = parseU64(value) orelse return null;
    if (parsed > 0xFFFF_FFFF) return null;
    return @intCast(parsed);
}

fn parseUsize(value: []const u8) ?usize {
    const parsed = parseU64(value) orelse return null;
    if (parsed > MAX_FILE) return null;
    return @intCast(parsed);
}

fn parseRetry(value: []const u8) ?u32 {
    const parsed = parseU32(value) orelse return null;
    if (parsed > 9) return null;
    return parsed;
}

fn parseU64(value: []const u8) ?u64 {
    if (value.len == 0) return null;
    var out: u64 = 0;
    for (value) |ch| {
        if (ch < '0' or ch > '9') return null;
        out = out * 10 + @as(u64, ch - '0');
    }
    return out;
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
