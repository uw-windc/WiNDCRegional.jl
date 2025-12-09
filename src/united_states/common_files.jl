"""
    load_state_fips(;
            path = joinpath(@__DIR__, "data", "state_fips.csv"),
            cols_to_keep = [:fips, :state]
        )

Load a CSV file containing state FIPS codes and state names. The default path is
set to `data/state_fips.csv` relative to this file's directory. You can specify
which columns to keep using the `cols_to_keep` argument.

Returns a DataFrame with the specified columns as Strings.

## Optional Arguments

- `path::String`: Path to the state FIPS CSV file.
- `cols_to_keep::Vector{Symbol}`: Columns to keep from the CSV file. Default is `[:fips, :state]`.

It is recommended to keep the columns `:fips` and `:state` for proper mapping.
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

Returns a DataFrame with two columns, `LineCode` and `naics`. The `naics` column 
is converted to a Symbol and and missing values are dropped.

## Optional Arguments

- `path::String`: Path to the industry codes CSV file.
"""
function load_industry_codes(;
        path = joinpath(@__DIR__, "data", "industry_codes.csv"),
    )

    industry_codes = CSV.read(
        path, 
        DataFrame,
        types = Dict(:naics => Symbol),
        select = [:LineCode, :naics]
        ) |>
        dropmissing


    return industry_codes

end

"""
    load_pce_map(;
            path = joinpath(@__DIR__, "data", "pce_map.csv"),
        )

Load the PCE to NAICS mapping file. The default path is set to `data/pce_map.csv`
relative to this file's directory.

Returns a DataFrame with four columns: `Line Code`, `description_pcs`, `naics`, 
and `description`. It is recommended you mirror this structure if you provide 
your own mapping file.

## Optional Arguments

- `path::String`: Path to the PCE mapping CSV file.
"""
function load_pce_map(;
        path = joinpath(@__DIR__, "data", "pce_map.csv"),
    )

    pce_map = CSV.read(
        path,
        DataFrame,
    ) 
    return pce_map

end

"""
    load_faf_map(; 
            path = joinpath(@__DIR__,  "data", "faf_map.csv"),
            keep_cols::Vector{Symbol} = [:sctg2, :naics]
        )

Load the FAF to NAICS mapping file. Optionally keep only a subset of columns, by
specifying the `keep_cols` argument. 

Returns a DataFrame with the specified columns. The `naics` column is converted
to a Symbol.

## Optional Arguments

- `path::String`: Path to the FAF mapping CSV file.
- `keep_cols::Vector{Symbol}`: Columns to keep from the CSV file. Default is 
`[:sctg2, :naics]`.

"""
function load_faf_map(; 
        path = joinpath(@__DIR__,  "data", "faf_map.csv"),
        keep_cols::Vector{Symbol} = [:sctg2, :naics]
    )
    df = CSV.read(
        path,
        DataFrame;
        select = keep_cols,
        types = Dict(keep_cols .=> String)
        ) |>
        x -> transform(x, :naics => ByRow(Symbol) => :naics) 
        
    return df
end

"""
    load_usatrade_map(;
            path = joinpath(@__DIR__, "data", "usatrade_map.csv"),
            keep_cols::Vector{Symbol} = [:naics, :naics4]
        )

Load the USA Trade to NAICS mapping file. Optionally keep only a subset of columns, by
specifying the `keep_cols` argument.

Returns a DataFrame with the specified columns. The `naics` and `naics4` columns
are converted to Symbols.

## Optional Arguments

- `path::String`: Path to the USA Trade mapping CSV file.
- `keep_cols::Vector{Symbol}`: Columns to keep from the CSV file. Default is 
`[:naics, :naics4]`.
"""
function load_usatrade_map(;
        path = joinpath(@__DIR__, "data", "usatrade_map.csv"),
        keep_cols::Vector{Symbol} = [:naics, :naics4]
    )
    df = CSV.read(
        path,
        DataFrame;
        select = keep_cols,
        types = Symbol
    )

    return df
end

"""
    load_sgf_map(;
            path = joinpath(@__DIR__, "data", "sgf_map.csv"),
            keep_cols::Vector{Symbol} = [:naics, :sgf_code]
        )


Load the SGF to NAICS mapping file. Optionally keep only a subset of columns, by
specifying the `keep_cols` argument.

Returns a DataFrame with the specified columns. All data is returned as Strings.

## Optional Arguments

- `path::String`: Path to the SGF mapping CSV file.
- `keep_cols::Vector{Symbol}`: Columns to keep from the CSV file. Default is 
`[:naics, :sgf_code]`.
"""
function load_sgf_map(;
        path = joinpath(@__DIR__, "data", "sgf_map.csv"),
        keep_cols::Vector{Symbol} = [:naics, :sgf_code]
    )

    sgf_map = CSV.read(
        path,
        DataFrame,
        select = keep_cols,
        types = Dict(keep_cols .=> String)
    ) 
    return sgf_map
end

"""
    load_sgf_states(;
            path = joinpath(@__DIR__, "data", "sgf_states.csv"),
        )

Load the SGF states mapping file. The default path is set to `data/sgf_states.csv`
relative to this file's directory.

Returns a DataFrame with two columns: `code` and `state`. Both columns are
converted to Strings.

## Optional Arguments

- `path::String`: Path to the SGF states CSV file.
"""
function load_sgf_states(;
        path = joinpath(@__DIR__, "data", "sgf_states.csv"),
    )

    sgf_states = CSV.read(
        path,
        DataFrame,
        types = Dict(:code => String)
    ) 

    return sgf_states
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



"""
    disaggregate_by_shares(
            summary::WiNDCNational.National,
            disaggregate::DataFrame,
            parameter::Vector{Symbol};
            domain = :commodity,
        )

    disaggregate_by_shares(
            summary::WiNDCNational.National,
            disaggregate::DataFrame,
            parameter::Symbol;
            domain = :commodity,
        )

Disaggregate the national-level `parameter` from the `summary` data to the 
regional level using shares from the `disaggregate` DataFrame.

The `disaggregate` DataFrame should contain the columns:
- `:year`: The year of the data.
- `:state`: The state identifier.
- `:naics`: The industry code.
- `:value`: The share value for disaggregation.
- `:name`: The name of the industry or sector.

The function returns a DataFrame with the disaggregated values for each state 
and industry code.

This function will identify values in `summary` that are not present in `disaggregate`
and will disaggreagte by equal shares. 
"""
function disaggregate_by_shares(
        summary::WiNDCNational.National,
        disaggregate::DataFrame,
        parameter::Vector{Symbol};
        domain = :commodity,
        fill_missing = true
    )

    column = WiNDCNational.sets(summary, domain) |> x -> x[1, :domain]

    disag_good_years = disaggregate |>
        x -> select(x, :naics, :year, :name) |>
        x -> unique(x, [:naics, :year])


    existing_goods_years = table(summary, parameter..., domain) |> 
        x-> select(x, column, :year) |> 
        unique        

    states_years = disaggregate |>
        x -> select(x, :year, :state) |>
        x -> unique(x, [:year, :state])

    if fill_missing
        missing_goods = leftjoin(
                existing_goods_years,
                disag_good_years,
                on = [column => :naics, :year]
            ) |>
            x -> subset(x, :name => ByRow(ismissing)) |>
            x -> leftjoin(
                x,
                states_years,
                on = :year
            ) |>
            x -> coalesce.(x, "labor") |> # This should not be here
            x -> transform(x, :name => ByRow(y -> 1) => :value) |>
            x -> select(x, :year, :state, column => :naics, :value, :name)
    
        new_disag = vcat(
            disaggregate,
            missing_goods
        )
    else
        new_disag = disaggregate
    end
    
    df = innerjoin( # missing sectors get distributed by shares
            table(summary, parameter...),
            select(new_disag, :year, :state, :naics, :value => :disag),
            on = [:year, column => :naics]
        ) |>
        x -> groupby(x, [:row, :col, :year, :parameter]) |>
        x -> combine(x, 
            :state => identity => :region,
            [:value, :disag] => ((v,share) -> v.*share./sum(share)) => :value
        )

    return df
end


function disaggregate_by_shares(
        summary::WiNDCNational.National,
        disaggregate::DataFrame,
        parameter::Symbol;
        domain = :commodity,
        fill_missing = true
    )

    return disaggregate_by_shares(
        summary,
        disaggregate,
        [parameter];
        domain = domain,
        fill_missing = fill_missing
    )

end

"""
    extend_data(X::DataFrame, column::Symbol, old_value, new_value)

Extend data from `X` by copying rows where `column` equals `old_value` to new 
rows where `column` equals `new_value`. This is used, for example, the maximum
year is 2023, but we need to extend to 2024 by copying 2023 data. 

The function takes four arguments:

- `X::DataFrame`: The dataframe to append.
- `column::Symbol`: The column to search for the old value.
- `exisiting_data`: The base data to use for the new values
- `new_data`: The name of the new data.
"""
function extend_data(X::DataFrame, column::Symbol, exisiting_data, new_data)
    return X |>
            x -> unstack(x, column, :value) |>
            x -> transform(x, Symbol(exisiting_data) => identity => Symbol(new_data)) |>
            x -> stack(x, Not(filter(∉([String(column),"value"]), names(X))); variable_name = column, value_name = "value") |>
            dropmissing

end