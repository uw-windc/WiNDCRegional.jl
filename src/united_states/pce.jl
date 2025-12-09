"""
    function load_pce_data(
        path::String,
        name::String;
        state_fips::DataFrame = load_state_fips(),
        pce_map::DataFrame = load_pce_map(),
    )

Load the Personal Consumption Expenditures (PCE) data.

## Required Arguments

- `path::String`: Path to the PCE CSV file.
- `name::String`: Name to assign to the loaded data, e.g. the state name.

## Optional Arguments

- `state_fips::DataFrame`: [`load_state_fips`](@ref) DataFrame mapping state FIPS codes to state abbreviations.
- `pce_map::DataFrame`: [`load_pce_map`](@ref) DataFrame mapping PCE LineCodes to NAICS codes.

## Data Source

This data can be downloaded from 
[the BEA website](https://apps.bea.gov/regional/downloadzip.htm), select 
`Personal Consumption Expenditures (PCE) by State` and download the `PCE` data.
"""
function load_pce_data(
    path::String,
    name::String;
    state_fips::DataFrame = load_state_fips(),
    pce_map::DataFrame = load_pce_map(),
)

    df = CSV.read(
        path, 
        DataFrame,
        footerskip = 4,
        drop = [:GeoName, :Region, :TableName, :IndustryClassification, :Description],
        types = Dict(:GeoFIPS => String)
        ) |>
        #x -> subset(x, :LineCode => ByRow(==(1))) |>
        x -> stack(x, Not(:GeoFIPS, :LineCode, :Unit), variable_name=:year, value_name=:value) |>
        x -> transform(x, 
            :year => ByRow(y -> parse(Int, y)) => :year,
            [:Unit, :value] => ByRow((u,v) -> parse_value_by_unit(u,v)) => :value,
        ) |>
        x -> innerjoin(x, state_fips, on = :GeoFIPS => :fips) |>
        x -> innerjoin(x, pce_map, on = :LineCode) |>
        x -> select(x, :year, :state, :value, :naics) |>
        x -> transform(x, 
            :naics => ByRow(Symbol) => :naics,
            :state => ByRow(y -> name) => :name
            )

    return df

end