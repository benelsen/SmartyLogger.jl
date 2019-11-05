@info "Starting…"

import Base: parse

using Logging, Dates
using HTTP, Parameters, TimeZones
using MQTT
using Smarty

include("influx.jl")

const SMARTY_KEY = haskey(ENV, "SMARTY_KEY") ? hex2bytes(ENV["SMARTY_KEY"]) : throw(error("Must provide SMARTY_KEY"))

const CLIENT_ID = haskey(ENV, "CLIENT_ID") ? ENV["CLIENT_ID"] : "SmartyLogger"

const INFLUXDB_HOST = haskey(ENV, "INFLUXDB_HOST") ? ENV["INFLUXDB_HOST"] : "localhost"
const INFLUXDB_PORT = haskey(ENV, "INFLUXDB_PORT") ? parse(Int, ENV["INFLUXDB_PORT"]) : 8086
const INFLUXDB_USER = haskey(ENV, "INFLUXDB_USER") ? ENV["INFLUXDB_USER"] : ""
const INFLUXDB_PASS = haskey(ENV, "INFLUXDB_PASS") ? ENV["INFLUXDB_PASS"] : ""

const MQTT_BROKER_HOST = haskey(ENV, "MQTT_BROKER_HOST") ? ENV["MQTT_BROKER_HOST"] : "localhost"
const MQTT_BROKER_PORT = haskey(ENV, "MQTT_BROKER_PORT") ? parse(Int, ENV["MQTT_BROKER_PORT"]) : 1883
const MQTT_BROKER_USER = haskey(ENV, "MQTT_BROKER_USER") ? ENV["MQTT_BROKER_USER"] : ""
const MQTT_BROKER_PASS = haskey(ENV, "MQTT_BROKER_PASS") ? ENV["MQTT_BROKER_PASS"] : ""

const LOG_ENCRYPTED = haskey(ENV, "LOG_ENCRYPTED") ? parse(Bool, ENV["LOG_ENCRYPTED"]) : false
const LOG_PLAINTEXT = haskey(ENV, "LOG_PLAINTEXT") ? parse(Bool, ENV["LOG_PLAINTEXT"]) : false

function reader(input_io::IO, channel_encrypted::Channel{EncryptedPacket}, log_bin_io::Union{IO, Nothing} = nothing)
    while !eof(input_io)
        @debug "$(now_utc_string()): " * "reader: trying to read from input"
        try
            p = read(input_io, EncryptedPacket)
            @debug "$(now_utc_string()): " * "reader: read EncryptedPacket"
            put!(channel_encrypted, p)
            @debug "$(now_utc_string()): " * "reader: put EncryptedPacket into channel"
            if !isnothing(log_bin_io)
                write(log_bin_io, p)
                @debug "$(now_utc_string()): " * "reader: wrote EncryptedPacket to log.bin"
            end
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
    end
    @debug "end reader"
end

function decrypt(channel_encrypted::Channel{EncryptedPacket}, channel_decrypted::Channel{DecryptedPacket}, log_txt_io::Union{IO, Nothing} = nothing)
    while true
        encrypted_packet = take!(channel_encrypted)
        @debug "$(now_utc_string()): " * "decrypt: received EncryptedPacket"
        decrypted_packet = decrypt_smarty_packet(SMARTY_KEY, encrypted_packet)
        @debug "$(now_utc_string()): " * "decrypt: decrypted EncryptedPacket"
        put!(channel_decrypted, decrypted_packet)
        @debug "$(now_utc_string()): " * "decrypt: put DecryptedPacket into channel"

        if !isnothing(log_txt_io)
            plaintext = String(deepcopy(decrypted_packet.plaintext))
            write(log_txt_io, plaintext, "\n")
            @debug "$(now_utc_string()): " * "decrypt: wrote DecryptedPacket to log.txt"
        end
        yield()
    end
    @debug "end decrypt"
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
    @debug "end parse"
end

function dbinsert(channel_parsed::Channel{ParsedPacket})
    while true
        parsed_packet = take!(channel_parsed)
        @debug "$(now_utc_string()): " * "dbinsert: received ParsedPacket"

        lp = write_line_protocol(parsed_packet)
        @debug "$(now_utc_string()): " * "dbinsert: create lineprotocol"

        try
            res = HTTP.post(
                "https://$(INFLUXDB_USER):$(INFLUXDB_PASS)@$(INFLUXDB_HOST):$(INFLUXDB_PORT)/write", [], lp;
                query = ["precision" => "ms", "db" => "smarty"],
                retry_non_idempotent = true,
                basic_authorization = true,
                require_ssl_verification = false)

            ts_packet = Dates.format(parsed_packet.datetime, dateformat"YYYY-mm-ddTHH:MM:SS.sss")
            @info "$(now_utc_string()): " * "packet $(parsed_packet.frame_counter) ($(ts_packet)) successfully processed"
        catch e
            if e isa HTTP.StatusError
                @warn "$(now_utc_string()): " * "db insert failed (http status code $(e.status))" e
                put!(channel_parsed, parsed_packet)
            elseif e isa Base.IOError
                @warn "$(now_utc_string()): " * "db insert failed (IOError)" e
                put!(channel_parsed, parsed_packet)
            else
                @error e
                rethrow()
            end
        end
        yield()
    end
    @debug "end dbinsert"
end

function logwrite(channel_parsed::Channel{ParsedPacket})
    while true
        parsed_packet = take!(channel_parsed)
        @debug "$(now_utc_string()): " * "logwrite: received ParsedPacket"

        @info parsed_packet

        yield()
    end
    @debug "end dbinsert"
end

now_utc_string() = Dates.format(now(UTC), dateformat"YYYY-mm-ddTHH:MM:SS.sss")

function run_()
    @debug "$(now_utc_string()): " * "Args:" ARGS

    if LOG_ENCRYPTED
        @debug "$(now_utc_string()): " * "Opening log.bin"
        log_bin_io = open("log/log.bin", "a")
        atexit(() -> close(log_bin_io))
    end

    if LOG_PLAINTEXT
        @debug "$(now_utc_string()): " * "Opening log.txt"
        log_txt_io = open("log/log.txt", "a")
        atexit(() -> close(log_txt_io))
    end

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

    # @debug "$(now_utc_string()): " * "Starting task_logwrite"
    # task_logwrite = @async logwrite(channel_parsed)

    @debug "$(now_utc_string()): " * "Starting task_parse"
    task_parse = @async parse(channel_decrypted, channel_parsed)

    @debug "$(now_utc_string()): " * "Starting task_decrypt"
    task_decrypt = @async decrypt(channel_encrypted, channel_decrypted, LOG_PLAINTEXT ? log_txt_io : nothing)

    # connect using mqtt
    @debug "$(now_utc_string()): " * "Creating mqtt client"

    function on_disconnect(reason)
        @info "MQTT disconnected"
        if !isnothing(reason)
            @info reason
        end
    end

    function on_msg(topic, data)
        # @debug topic data
        if topic == "smarty_data"
            io = PipeBuffer()
            write(io, data)
            reader(io, channel_encrypted, LOG_ENCRYPTED ? log_bin_io : nothing)
        end
    end

    @debug "$(now_utc_string()): " * "Creating mqtt Client"
    client = Client(on_msg, on_disconnect, 0)

    @debug "$(now_utc_string()): " * "Creating mqtt ConnectOpts"
    connect_opts = ConnectOpts(MQTT_BROKER_HOST, MQTT_BROKER_PORT)
    connect_opts.username = MQTT_BROKER_USER
    connect_opts.password = Vector{UInt8}(MQTT_BROKER_USER)
    connect_opts.client_id = CLIENT_ID

    @debug "$(now_utc_string()): " * "Connecting to mqtt broker"
    connect(client, connect_opts)
    atexit(() -> disconnect(client))

    subscribe(client, ("smarty_data", MQTT.AT_LEAST_ONCE))

    @info "$(now_utc_string()): " * "Ready"
    yield()

    @debug "$(now_utc_string()): " * "waiting for task_dbinsert to end"
    wait(task_dbinsert)
end

run_()
