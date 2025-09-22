

function add_good(state_year::DataFrame, good::Symbol)
    df =  transform(state_year,
        :state => ByRow(y -> 1) => :value,
        :state => ByRow(y -> good) => :naics
    )

    return df
end


function load_state_gdp(
    path::String, 
    state_fips::DataFrame,
    industry_codes::DataFrame,
    name::String
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


