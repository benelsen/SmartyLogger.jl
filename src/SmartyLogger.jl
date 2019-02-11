using Sockets, Logging, Dates
@info "Starting…"

using HTTP, Parameters, TimeZones
using PiGPIO, Smarty

include("influx.jl")

const key = hex2bytes(ENV["SMARTY_KEY"])

const influxdb_host = "db"
const influxdb_port = 8086

const socat_host = "socat"
const socat_port = 58100

const pigpiod_host = "pigpiod"
const pigpiod_port = 8888

const pi = PiGPIO.Pi(host = pigpiod_host, port = pigpiod_port)
PiGPIO.set_mode(pi, 24, PiGPIO.OUTPUT)
PiGPIO.write(pi, 24, PiGPIO.HIGH)

function reader(input_io::IO, channel_encrypted::Channel{EncryptedPacket}, log_bin_io::IO)
    flush(input_io)

    while !eof(input_io)
        @debug "$(now_utc_string()): " * "reader: trying to read from input"
        try
            p = read(input_io, EncryptedPacket)
            @debug "$(now_utc_string()): " * "reader: read EncryptedPacket"
            put!(channel_encrypted, p)
            @debug "$(now_utc_string()): " * "reader: put EncryptedPacket into channel"
            write(log_bin_io, p)
            @debug "$(now_utc_string()): " * "reader: wrote EncryptedPacket to log.bin"
            flush(log_bin_io)
        catch e
            if isa(e, ErrorException) && e.msg == "Could not parse EncryptedPacket. Unexpected start byte"
                @warn "$(now_utc_string()): " * "unexpected start byte"
                continue
            elseif isa(e, Smarty.MbedTLS.MbedException)
                @warn "$(now_utc_string()): " * "Error" e
            else
                @error "$(now_utc_string()): " * "Error" e
                # rethrow()
            end
        end
        yield()
    end
end

function decrypt(channel_encrypted::Channel{EncryptedPacket}, channel_decrypted::Channel{DecryptedPacket}, log_txt_io::IO)
    while true
        encrypted_packet = take!(channel_encrypted)
        @debug "$(now_utc_string()): " * "decrypt: received EncryptedPacket"
        decrypted_packet = decrypt_smarty_packet(key, encrypted_packet)
        @debug "$(now_utc_string()): " * "decrypt: decrypted EncryptedPacket"
        put!(channel_decrypted, decrypted_packet)
        @debug "$(now_utc_string()): " * "decrypt: put DecryptedPacket into channel"

        plaintext = String(deepcopy(decrypted_packet.plaintext))
        write(log_txt_io, plaintext, "\n")
        @debug "$(now_utc_string()): " * "decrypt: wrote DecryptedPacket to log.txt"
        flush(log_txt_io)
        yield()
    end
end

function parse(channel_decrypted::Channel{DecryptedPacket}, channel_parsed::Channel{ParsedPacket})
    while true
        decrypted_packet = take!(channel_decrypted)
        @debug "$(now_utc_string()): " * "parse: received DecryptedPacket"

        try
            parsed_packet = parse_telegram(decrypted_packet, check_crc = :warn)
            @debug "$(now_utc_string()): " * "parse: parsed DecryptedPacket"

            put!(channel_parsed, parsed_packet)
            @debug "$(now_utc_string()): " * "parse: put ParsedPacket into channel"
        catch e
            @error e
            rethrow()
        end

        yield()
    end
end

function dbinsert(channel_parsed::Channel{ParsedPacket})
    while true
        parsed_packet = take!(channel_parsed)
        @debug "$(now_utc_string()): " * "dbinsert: received ParsedPacket"

        lp = write_line_protocol(parsed_packet)
        @debug "$(now_utc_string()): " * "dbinsert: create lineprotocol"

        try
            res = HTTP.post("http://$(influxdb_host):$(influxdb_port)/write", [], lp;
                query = ["precision" => "ms", "db" => "smarty"],
                retry_non_idempotent = true)

            ts_packet = Dates.format(parsed_packet.datetime, dateformat"YYYY-mm-ddTHH:MM:SS.sss")
            @info "$(now_utc_string()): " * "packet $(parsed_packet.frame_counter) ($(ts_packet)) successfully processed"
        catch e
            if e isa HTTP.StatusError
                @warn "$(now_utc_string()): " * "db insert failed (http status code $(e.status))" e
                put!(channel_parsed, parsed_packet)
            elseif e isa IOError
                @warn "$(now_utc_string()): " * "db insert failed (IOError)" e
                put!(channel_parsed, parsed_packet)
            else
                @error e
                rethrow()
            end
        end
        yield()
    end
end

now_utc_string() = Dates.format(now(UTC), dateformat"YYYY-mm-ddTHH:MM:SS.sss")

function run_()
    @debug "$(now_utc_string()): " * "Args:" ARGS

    @debug "$(now_utc_string()): " * "Opening log.bin"
    log_bin_io = open("log/log.bin", "a+")
    atexit(() -> close(log_bin_io))

    @debug "$(now_utc_string()): " * "Opening log.txt"
    log_txt_io = open("log/log.txt", "a+")
    atexit(() -> close(log_txt_io))

    @debug "$(now_utc_string()): " * "Opening tcp socket to socat"
    sock = connect(socat_host, socat_port)
    atexit(() -> close(sock))

    # support for serial ports is still unsatisfying
    # sp = open("/dev/ttyAMA0", 115200)

    @debug "$(now_utc_string()): " * "Opening Channel{EncryptedPacket}"
    channel_encrypted = Channel{EncryptedPacket}(600)
    atexit(() -> close(channel_encrypted))

    @debug "$(now_utc_string()): " * "Opening Channel{DecryptedPacket}"
    channel_decrypted = Channel{DecryptedPacket}(600)
    atexit(() -> close(channel_decrypted))

    @debug "$(now_utc_string()): " * "Opening Channel{ParsedPacket}"
    channel_parsed = Channel{ParsedPacket}(600)
    atexit(() -> close(channel_parsed))


    @debug "$(now_utc_string()): " * "Starting task_dbinsert"
    task_dbinsert = @async dbinsert(channel_parsed)

    @debug "$(now_utc_string()): " * "Starting task_parse"
    task_parse = @async parse(channel_decrypted, channel_parsed)

    @debug "$(now_utc_string()): " * "Starting task_decrypt"
    task_decrypt = @async decrypt(channel_encrypted, channel_decrypted, log_txt_io)

    @debug "$(now_utc_string()): " * "Starting task_reader"
    task_reader = @async reader(sock, channel_encrypted, log_bin_io)

    @info "$(now_utc_string()): " * "Setting low"
    PiGPIO.write(pi, 24, PiGPIO.LOW)

    @info "$(now_utc_string()): " * "Ready"
    yield()

    @debug "$(now_utc_string()): " * "waiting for task_dbinsert to end"
    wait(task_dbinsert)
end

run_()
