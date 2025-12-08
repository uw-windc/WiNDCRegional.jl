function parse_sgf_line(line::AbstractString)
    state_fips = line[1:2]
    census_code = line[15:17]
    value = parse(Int, line[18:29])/1_000_000
    return (fips = state_fips, sgf_code = census_code, value = value)
end

function parse_sgf_line_99(line::AbstractString)
    state_fips = line[1:2]
    census_code = line[22:24]
    value = parse(Int, line[25:35])/1_000_000
    return (fips = state_fips, sgf_code = census_code, value = value)
end



function load_sgf(base_path::String, match::RegexMatch{String})
    year = parse(Int, match[:year])
    year = year > 90 ? 1900 + year : 2000 + year

    if year == 1999
        out = load_sgf(base_path, match.match; parser = parse_sgf_line_99)
    else
        out = load_sgf(base_path, match.match)
    end

    out |> x->transform!(x, :sgf_code => ByRow(y -> year) => :year)

    return out
end

function load_sgf(base_path::String, path::AbstractString; parser = parse_sgf_line)
    a = open(joinpath(base_path, path), "r") do x
        DataFrame(parser.(eachline(x)))
    end

    return a
end




"""
    load_state_finances(
        file_pattern::Regex,
        directory_path::String;
        sgf_map::DataFrame = WiNDCRegional.load_sgf_map(),
        sgf_states::DataFrame = WiNDCRegional.load_sgf_states(),
        replacement_data::Dict = Dict()
        )

Load state government finance data that matches the file pattern in the given
directory path. 

## Required Arguments

- `file_pattern::Regex`: A regex pattern to match the filenames, typically of the 
    form ^(?<year>\\d{2})(state|data)35.txt\$
- `directory_path::String`: Location of the data files.

## Optional Arguments

- `sgf_map::DataFrame`: A dataframe mapping SGF codes to NAICS codes. Default is
    [`WiNDCRegional.load_sgf_map()`](@ref).
- `sgf_states::DataFrame`: A dataframe mapping FIPS codes to state names. Default is
    [`WiNDCRegional.load_sgf_states()`](@ref).
- `replacement_data::Dict`: A dictionary of replacement data to append to the
    dataframe. The keys are column names, and the values are dictionaries
    mapping new data names to existing data names. The replacement is done using
    [`WiNDCRegional.sgf_append_data`](@ref).
"""
function load_state_finances(
        file_pattern::Regex,
        directory_path::String;
        sgf_map::DataFrame = WiNDCRegional.load_sgf_map(),
        sgf_states::DataFrame = WiNDCRegional.load_sgf_states(),
        replacement_data::Dict = Dict()
    )


    census_files = readdir(directory_path) |>
        x -> match.(file_pattern, x) |>
        x -> filter(!isnothing, x)


    df = vcat(load_sgf.(directory_path, census_files)...)

    census_data = innerjoin(
        df,
        sgf_map,
        on = :sgf_code
    ) |>
    x -> groupby(x, [:fips, :year, :naics]) |>
    x -> combine(x, :value => sum => :value) |>
    x -> innerjoin(x, sgf_states, on = :fips=>:code) |>
    x -> select(x, :naics, :year, :state, :value) |>
    x -> transform(x, 
        :naics => ByRow(Symbol) => :naics,
        :state => ByRow(y -> "government_final_demand") => :name
    ) 

    for (column,Y) in replacement_data
        for (new_data, existing_data) in Y
            census_data = extend_data(census_data, Symbol(column), existing_data, new_data)
        end
    end


    return census_data
end
