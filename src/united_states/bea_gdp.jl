

function add_good(state_year::DataFrame, good::Symbol)
    df =  transform(state_year,
        :state => ByRow(y -> 1) => :value,
        :state => ByRow(y -> good) => :naics
    )

    return df
end

"""
    load_state_gdp(
        path::String, 
        name::String;
        state_fips::DataFrame = load_state_fips(),
        industry_codes::DataFrame = load_industry_codes(),
        )

Load SAGDP data. 

## Required Arguments

- `path::String`: Path to the SAGDP CSV file.
- `name::String`: Name to assign to the loaded data, e.g. `gdp`.

## Optional Arguments

- `state_fips::DataFrame`: DataFrame mapping state FIPS codes to state abbreviations.
- `industry_codes::DataFrame`: DataFrame mapping BEA industry codes to NAICS codes.

By default, we use the default maps provided by [`load_state_fips()`](@ref) and [`load_industry_codes()`](@ref).
However, any mapping DataFrame with the same structure can be provided.

## Data Source

This data can be downloaded from 
[the BEA website](https://apps.bea.gov/regional/downloadzip.htm), select `Gross
Domestic Product (GDP) by State` and download the `SAGDP` data.

"""
function load_state_gdp(
    path::String, 
    name::String;
    state_fips::DataFrame = load_state_fips(),
    industry_codes::DataFrame = load_industry_codes(),
    )

    df = CSV.read(
        path,
        DataFrame,
        footerskip = 4,
                missingstring = [
                "(NA)", #Not Available
                "(D)", #Not shown to avoid disclosure of confidential information; estimates are included in higher-level totals.
                "(NM)", #Not Meaningful
                "(L)", #Below $50,000
                "(T)", #The estimate is suppressed to cover corresponding estimate for earnings in state personal income. Estimates for this item are included in the total.
            ],
        drop = [:GeoName, :Region, :TableName, :IndustryClassification],
        types = Dict(:GeoFIPS => String)
        ) |>
        x -> stack(x, Not(:GeoFIPS, :LineCode, :Unit, :Description), variable_name=:year, value_name=:value) |>
        dropmissing |>
        x -> transform(x, 
            :year => ByRow(y -> parse(Int, y)) => :year,
            :LineCode => ByRow(y -> name) => :name,
            [:Unit, :value] => ByRow((u,v) -> parse_value_by_unit(u,v)) => :value,
            #:Description => ByRow(y -> strip(y)) => :Description
        ) |>
        x -> innerjoin(x, state_fips, on = :GeoFIPS => :fips) |>
        x -> innerjoin(x, industry_codes, on = :LineCode) |>
        x -> select(x, Not(:GeoFIPS, :LineCode, :Unit, :Description)) |>
        x -> subset(x, :value => ByRow(!iszero))


    return df


end


