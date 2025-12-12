
"""
    load_usa_raw_trade_data(
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

## Data Source

This data is available from the 
[USA Trade Online portal](https://usatrade.census.gov/). This requires a log-in. 
For both `Imports` and `Exports` we want NAICS data. When selecting data, we want 
every state (this is different that All States), the most disaggregated 
commodities (third level), and for `Exports` we want `World Total` and for `Imports` 
we want both `World Total` and `Canada` in the Countries column.


"""
function load_usa_raw_trade_data(
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
            :country => ByRow(==("World Total"))
        ) |>
        x -> transform(x, 
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
    load_usda_agricultural_flow(
            path::String,
            sheet::String,
            range::String; 
            agriculture_code = Symbol("111CA"),
            flow::String = "exports"
        )

Load additional agricultural trade flow data from USDA. This data is used to
supplement the USA Trade data for the `agricultural_code`.

Returns a DataFrame with columns `:year`, `:state`, `:naics`, `:value`, and `:flow`.

## Required Arguments

- path::String: Path to the Excel file.
- sheet::String: Sheet name in the Excel file.
- range::String: Cell range in the sheet to read.

## Optional Arguments

- agriculture_code::Symbol: The NAICS code for the agricultural commodity. Default is `:111CA`.
- flow::String: The trade flow type, either `exports` or `imports`. Default is `exports`.

## Data Source

This loads the file [`Commodity_detail_by_state_cy.xlsx`](https://ers.usda.gov/sites/default/files/_laserfiche/DataFiles/100812/commodity_detail_by_state_cy.xlsx). 
This is a very fragile link that may change over time. If you find this link is broken,
raise an issue on the [WiNDCRegional GitHub repository](https://github.com/uw-windc/WiNDCRegional.jl).
"""
function load_usda_agricultural_flow(
        path::String,
        sheet::String,
        range::String; 
        agriculture_code = Symbol("111CA"),
        flow::String = "exports",
        replacement_data::Dict = Dict()
    )
    extra_ag_flow = XLSX.readdata(
        path,
        sheet,
        range
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
        :state => ByRow(y -> agriculture_code) => :naics,
        :state => ByRow(y -> flow) => :flow
    ) 
    
    for (column,Y) in replacement_data
        for (new_data, existing_data) in Y
            extra_ag_flow = extend_data(extra_ag_flow, Symbol(column), existing_data, new_data)
        end
    end

    extra_ag_flow |>
        x -> transform!(x,
            :year => ByRow(y-> !isa(y,Int) ? parse(Int, y) : y) => :year
        )

    return extra_ag_flow
end
    
"""
    load_trade_shares(
        exports::DataFrame,
        imports::DataFrame,
        usda_agricultural_flow::DataFrame;
        agricultural_code = Symbol("111CA"),
        base_year::Int = 1997
    )

Use exports, imports and usda agricultural flow data to calculate the foreign flow
of goods.

## Required Arguments

- `exports::DataFrame`: Export data from [`load_usa_raw_trade_data`](@ref).
- `imports::DataFrame`: Import data from [`load_usa_raw_trade_data`](@ref).
- `usda_agricultural_flow::DataFrame`: Agricultural flow data from 
    [`load_usda_agricultural_flow`](@ref).

## Optional Arguments
- `agricultural_code::Symbol`: The NAICS code for the agricultural commodity. 
    Default is `:111CA`.
- `base_year::Int`: The base year for backfilling missing years. Default is 
    `1997`, the first year of summary data.

## Loading Process

First we calculate the trade shares for each state, commodity, flow, and year. This
is given by:

```math
\\frac{V}{\\sum_{{\\rm state}} V}
```

There are years with no trade data for certain commodities. We use the yearly region
share to set a default value for each state and commodity/flow pair. The yearly
region share is defined by:

```math
\\frac{sum_{{\\rm year}} V}{sum_{{\\rm state}}\\left(sum_{{\\rm year}} V\\right)}
```

These two values are combined such that if there is no trade data for a given year,
the default value is used instead. We then remove all values where with naics code
`agricultural_code` and flow `exports` from the data, and append the USDA agricultural
flow data.

Finally, we back fill missing years for 1997-2001 using the next available year's data. 
Note that the minimum year for each state/commodity/flow combination is used to 
determine which years need to be back filled, with a maximum year of `2002`.
"""
function load_trade_shares(
        exports::DataFrame,
        imports::DataFrame,
        usda_agricultural_flow::DataFrame;
        agricultural_code = Symbol("111CA"),
        base_year::Int = 1997
    )

    df = vcat(exports, imports)

    default_values = df |>
        x -> groupby(x, [:naics, :flow, :state]) |>
        x -> combine(x,  :value => sum => :value) |>
        x -> groupby(x, [:naics, :flow]) |>
        x -> combine(x,
            :state => identity => :state,
            :value => (y -> y./sum(y)) => :default
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

    trade_shares = outerjoin(state_trade_shares, all_things, on = [:year, :naics, :state, :flow]) |>
        x -> leftjoin(x, default_values, on = [:naics, :state, :flow]) |>
        x -> groupby(x, [:year, :naics, :flow]) |>
        x -> combine(x,
            :state => identity => :state,
            [:value, :default] => ((y,d) -> all(ismissing.(y)) ? d : y ) => :value
        ) |>
        x -> dropmissing(x) |>
        x -> subset(x, [:naics,:flow] => ByRow((n,f) -> !(n == agricultural_code && f == "exports"))) |>
        x -> vcat(x, usda_agricultural_flow) 

    minimum_years = trade_shares |>
        x -> groupby(x, [:naics, :flow, :state]) |>
        x -> combine(x, :year => minimum => :year) |>
        x -> subset(x, :year => ByRow(<=(2002))) |>
        x -> innerjoin(
            x,
            trade_shares,
            on = [:naics, :flow, :state, :year]
        )

    fill_years = minimum_years |>
        x -> unique(x, :year) |>
        x -> select(x, :year) |>
        x -> crossjoin(x, DataFrame(new_year = base_year:2002)) |>
        x -> subset(x, [:year, :new_year] => ByRow(>))

    trade_shares = vcat(
        trade_shares, 
        minimum_years |>
            x -> leftjoin(x, fill_years, on = [:year]) |>
            x -> select(x, :naics, :flow, :state, :value, :new_year => :year)
    )

    return trade_shares
end
    

