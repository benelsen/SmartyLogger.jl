
struct LineProtocolPoint
    measurement::AbstractString
    tags::Dict{AbstractString, AbstractString}
    fields::Dict{AbstractString, Union{Float64, Int64, Bool, AbstractString}}
    datetime::Dates.AbstractDateTime
end

write_line_protocol(dp::Smarty.AbstractDataPoint) = nothing
write_line_protocol(dp::Smarty.ManufacturerFlagIdentifier) = nothing
write_line_protocol(dp::Smarty.ManufacturerTypeIdentifier) = nothing
write_line_protocol(dp::Smarty.EquipmentName) = nothing

function write_line_protocol(dp::Smarty.Version)
    "electricity,id=1 protocol_version=$(dp.version)"
end

function write_line_protocol(dp::Smarty.ElectricityEnergyMeterReading)
    "electricity,id=1,kind=$(dp.kind),direction=$(dp.direction),tariff=$(dp.tariff) energy=$(dp.value)"
end

function write_line_protocol(dp::Smarty.ElectricityPowerMeterReading)
    "electricity,id=1,kind=$(dp.kind),direction=$(dp.direction),tariff=$(dp.tariff) power=$(dp.value)"
end

function write_line_protocol(dp::Smarty.ActiveThresholdData)
    "electricity,id=1 active_threshold=$(dp.value)"
end

function write_line_protocol(dp::Smarty.BreakerStateData)
    "electricity,id=1 breakerstate=$(dp.value)"
end

function write_line_protocol(dp::Smarty.PowerFailureCount)
    "electricity,id=1,kind=$(dp.kind) powerfailure_count=$(dp.count)"
end

function write_line_protocol(dp::Smarty.VoltageEventsCount)
    "electricity,id=1,kind=$(dp.kind),phase=$(dp.phase) voltage_event_count=$(dp.count)"
end

function write_line_protocol(dp::Smarty.TextMessage)
    if length(dp.text) > 0
        "electricity,id=1,channel=$(dp.channel) message=\"$(dp.text)\""
    else
        nothing
    end
end

function write_line_protocol(dp::Smarty.InstantaneousReading)
    "electricity,id=1,kind=$(dp.kind),direction=$(dp.direction),phase=$(dp.phase) reading=$(dp.value)"
end

function write_line_protocol(pp::ParsedPacket)
    @unpack datetime, data = pp
    timestamp = TimeZones.zdt2unix(datetime)

    lps = String[]
    for p in data
        lp = write_line_protocol(p)
        if lp !== nothing
            push!(lps, "$(lp) $((timestamp * 1000) |> Int64)")
        end
    end

    join(lps, "\n")
end
