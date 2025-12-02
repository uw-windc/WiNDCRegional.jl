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