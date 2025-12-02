
"""
    load_usda_raw_trade_data(
        path::String,
        flow::String;
        value_col::Symbol = flow == "exports" ? :Total_Exports_Value_US_ : :Customs_Value_Gen_US_,
        state_fips::DataFrame = load_state_fips(),
        usatrade_map::DataFrame = load_usatrade_map(),
    )

Load data exported from the USA Trade online portal. The input `flow` should be 
either `"exports"` or `"imports"`.

The `value_col` should be either `:Total_Exports_Value_US_` or `:Customs_Value_Gen_US_` 
depending on whether you are loading export or import data.

This will return a normalized DataFrame with columns `:year`, `:state`, `:naics`, 
`:value`, and `:flow`.
"""
function load_usda_raw_trade_data(
        path::String,
        flow::String;
        value_col::Symbol = flow == "exports" ? :Total_Exports_Value_US_ : :Customs_Value_Gen_US_,
        state_fips::DataFrame = load_state_fips(),
        usatrade_map::DataFrame = load_usatrade_map(),
    )


    df = CSV.read(
            #joinpath(base_dir, "USATradeOnline", "State Exports by NAICS Commodities.csv"),
            path,
            DataFrame;
            header = 4,
            select = 1:5,
            normalizenames = true,
            silencewarnings = true
        ) |>
        x -> select(x, :State => :state, :Country => :country, :Time => :year, :Commodity => :naics, value_col => :value) |>
        x -> subset(x, 
            #:year => ByRow(y -> !occursin("through",y)),
            :country => ByRow(==("World Total"))
        ) |>
        x -> transform(x, 
            #:year => (y -> parse.(Int, y)) => :year,
            :naics => ByRow(y -> match(r"(\d{4}) .*", y).captures[1] |> Symbol) => :naics4,
            :value => ByRow(y -> y |> z -> replace(z, "," => "") |> z->parse(Float64, z)/1_000_000) => :value,
            :state => ByRow(y -> replace(y, "Dist" => "District")) => :state
        ) |>
        x -> select(x, Not(:naics)) |>
        x -> innerjoin(x, select(state_fips, :state), on = :state) |>
        x -> innerjoin(x, usatrade_map, on = :naics4) |>
        x -> groupby(x, [:year, :state, :naics]) |>
        x -> combine(x, :value => sum => :value) |>
        x -> transform(x, :state => ByRow(y -> flow) => :flow)


    return df
end

"""
    load_usda_agricultural_flow(path::String)

Load additional agricultural trade flow data from USDA. This data is used to
supplement the USA Trade data for NAICS code 111CA.

Returns a DataFrame with columns `:year`, `:state`, `:naics`, `:value`, and `:flow`.
"""
function load_usda_agricultural_flow(path::String)
    extra_ag_flow = XLSX.readdata(
        path,
        "Total Exports",
        "A3:X55"
    ) |>
    x -> DataFrame(x[4:end, :], ["state", x[1, 2:end]...]) |>
    x -> stack(x, Not(:state); variable_name = :year, value_name = :value) |>
    x -> transform(x, :year => ByRow(y -> parse(Int, y)) => :year) |>
    x -> groupby(x, [:year]) |>
    x -> combine(x, 
        :state => identity => :state,
        :value => (y -> y ./ sum(y)) => :value
        ) |>
    x -> transform(x, 
        :state => ByRow(y -> Symbol("111CA")) => :naics,
        :state => ByRow(y -> "exports") => :flow
    ) 

    return extra_ag_flow
end
    
"""
    load_trade_shares(
        export_path::String,
        import_path::String,
        usda_agricultural_flow_path::String;
        state_fips::DataFrame = load_state_fips(),
        usatrade_map::DataFrame = load_usatrade_map()
    )

Load and process trade data from USA Trade, [`load_usda_raw_trade_data`](@ref), 
and USDA agricultural flow data. [`load_usda_agricultural_flow`](@ref).

## Build Process

 1. Load export and import data using [`load_usda_raw_trade_data`](@ref).
 2. Calculate the state trade shares, i.e. the share of each state's exports/imports
    for each NAICS code and year.
 3. There is missing data, some years have no trade data for certain commodities. 
        set a default value for each state and commodity/flow pair based on the
        yearly total share.
 4. Remove NAICS code 111CA (Crop Production) from the USA Trade data and append 
    the USDA agricultural flow data for this NAICS code.
 5. Backfill missing years, to 1997, for each state/commodity/flow pair using the earliest
    available year for that pair.
"""
function load_trade_shares(
        export_path::String,
        import_path::String,
        usda_agricultural_flow_path::String;
        state_fips::DataFrame = load_state_fips(),
        usatrade_map::DataFrame = load_usatrade_map()
    )

    df = vcat(
        load_usda_raw_trade_data(
            export_path,
            "exports";
            state_fips = state_fips,
            usatrade_map = usatrade_map
        ),
        load_usda_raw_trade_data(
            import_path,
            "imports";
            state_fips = state_fips,
            usatrade_map = usatrade_map
        ) 
    )
        
    state_trade_shares = df |>
        x -> groupby(x, [:year, :naics, :flow]) |>
        x -> combine(x, 
        :state => identity => :state,
        :value => (y -> y ./ sum(y)) => :value,
        )


    all_things = crossjoin(
        state_trade_shares |> x -> select(x, :year) |> unique,
        state_trade_shares |> x -> select(x, :naics) |> unique,   
        state_trade_shares |> x -> select(x, :state) |> unique,
        state_trade_shares |> x -> select(x, :flow) |> unique,
    )

    default_values = df |>
        x -> groupby(x, [:naics, :flow]) |>
        x -> combine(x, 
            :state => identity => :state,
            :value => identity => :value,
            :value => (y -> y./sum(y)) => :share
        ) |>
        x -> groupby(x, [:naics, :flow, :state]) |>
        x -> combine(x, :share => sum => :default) 
        
        
    extra_ag_flow = load_usda_agricultural_flow(usda_agricultural_flow_path)


    trade_shares = outerjoin(state_trade_shares, all_things, on = [:year, :naics, :state, :flow]) |>
        x -> leftjoin(x, default_values, on = [:naics, :state, :flow]) |>
        x -> groupby(x, [:year, :naics, :flow]) |>
        x -> combine(x,
            :state => identity => :state,
            [:value, :default] => ((y,d) -> all(ismissing.(y)) ? d : y ) => :value
        ) |>
        x -> dropmissing(x) |>
        x -> subset(x, :naics => ByRow(!=(Symbol("111CA")))) |>
        x -> vcat(x, extra_ag_flow) 


    missing_years = DataFrame([
        (base_year = 2002, year = 2001),
        (base_year = 2002, year = 2000),
        (base_year = 2002, year = 1999),
        (base_year = 2002, year = 1998),
        (base_year = 2002, year = 1997),
        (base_year = 2000, year = 1999),
        (base_year = 2000, year = 1998),
        (base_year = 2000, year = 1997),
    ])

    trade_shares = trade_shares |>
        x -> groupby(x, [:naics, :flow, :state]) |>
        x -> combine(x,
            [:value, :year] => ((v,y) -> (v[argmin(y)], minimum(y))) => :is_min,
        ) |>
        x -> transform(x,
            :is_min => ByRow(identity) => [:value,:year]
        ) |>
        x -> leftjoin(
            missing_years,
            x,
            on = [:base_year => :year]
        ) |>
        x -> select(x, Not(:is_min, :base_year)) 



    return trade_shares


end
    

