"""
    load_state_fips(;
            path = joinpath(@__DIR__, "data", "state_fips.csv"),
            cols_to_keep = [:fips, :state]
        )

Load a CSV file containing state FIPS codes and state names. The default path is
set to "data/state_fips.csv" relative to this file's directory. You can specify
which columns to keep using the `cols_to_keep` argument.

Returns a DataFrame with the specified columns as Strings.
"""
function load_state_fips(;
        path = joinpath(@__DIR__, "data", "state_fips.csv"),
        cols_to_keep = [:fips, :state]
    )

    state_fips = CSV.read(
        path, 
        DataFrame,
        types = Dict(
            cols_to_keep .=> String
            ),
        select = cols_to_keep
    )

    return state_fips

end

"""
    load_industry_codes(;
            path = joinpath(@__DIR__, "data", "industry_codes.csv"),
        )

Load a CSV file containing industry codes. The default path is set to
"data/industry_codes.csv" relative to this file's directory.

Returns a DataFrame with the `naics` column as Symbol and drops rows with missing values.
"""
function load_industry_codes(;
        path = joinpath(@__DIR__, "data", "industry_codes.csv"),
    )

    industry_codes = CSV.read(
        path, 
        DataFrame,
        types = Dict(:naics => Symbol),
        drop = [:Description]
        ) |>
        dropmissing


    return industry_codes

end


"""
    parse_value_by_unit(unit::String, value::Real)

Want values to be in billions of dollars
"""
function parse_value_by_unit(unit::AbstractString, value::Real)
    if contains(lowercase(unit), "thousand")
        return value / 1_000_000
    elseif contains(lowercase(unit), "million")
        return value / 1_000
    elseif contains(lowercase(unit), "billion")
        return value
    else
        return value
    end
end