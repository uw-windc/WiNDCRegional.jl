"""
    faf_cols_to_keep(i,name, cols_to_keep, regex_cols_to_keep, max_year) 

Helper function for loading FAF data. We should keep columns in `cols_to_keep` or
matching `regex_cols_to_keep` with year less than or equal to `max_year`.

The standard value of `regex_cols_to_keep` is `r"^value_(\d{4})$"`, which matches columns
named like `value_1997`, `value_2020`, etc.
"""
function faf_cols_to_keep(i,name, cols_to_keep, regex_cols_to_keep, max_year) 
    if (name in cols_to_keep)
        return true
    end

    if isnothing(match(regex_cols_to_keep, string(name)))
        return false
    end

    year = parse(Int, match(regex_cols_to_keep, string(name)).captures[1])
    if year > max_year
        return false
    end

    return true
#    return (name in cols_to_keep) || !isnothing(match(regex_cols_to_keep, string(name)))
end

raw"""
    load_faf_base(
        path::String,
        state_fips::DataFrame,
        faf_map::DataFrame;
        cols_to_keep = [
            :fr_orig,
            :dms_origst,
            :dms_destst,
            :fr_dest,
            :trade_type,
            :sctg2,
        ],
        regex_cols_to_keep = r"^value_(\d{4})$",
        max_year = 2023,
    )

The FAF provides two files, one with data from 2017 onward, and one with 
reprocessed data from 1997-2012. This function provides the common loading logic.
"""
function load_faf_base(
        path::String,
        state_fips::DataFrame,
        faf_map::DataFrame;
        cols_to_keep = [
            :fr_orig,
            :dms_origst,
            :dms_destst,
            :fr_dest,
            :trade_type,
            :sctg2,
        ],
        regex_cols_to_keep = r"^value_(\d{4})$",
        max_year = 2023,
    )

    df = CSV.read(
        path, #joinpath(base_dir, "FAF", "FAF5.7.1_State.csv"),
        DataFrame;
        select = (i,name) -> faf_cols_to_keep(i,name, cols_to_keep, regex_cols_to_keep, max_year),
        types = Dict(
            cols_to_keep .=> String,
        )
    ) |>
    x -> stack(x, Not(cols_to_keep), variable_name = :year, value_name = :value) |>
    x -> transform(x, :year => ByRow(y -> parse(Int, replace(string(y), "value_" => ""))) => :year) |>
    x -> subset(x, :value => ByRow(y -> !ismissing(y) && abs(y) > 1e-5)) |>
    x -> groupby(x, [:dms_origst, :dms_destst, :sctg2, :year]) |>
    x -> combine(x, :value => sum => :value) |>
    x -> innerjoin(x, transform(state_fips, :fips => (y -> first.(y, 2)) => :fips), on = :dms_origst => :fips) |>
    x -> rename(x, :state => :origin) |>
    x -> innerjoin(x, transform(state_fips, :fips => (y -> first.(y, 2)) => :fips), on = :dms_destst => :fips) |>
    x -> rename(x, :state => :destination) |>
    x -> leftjoin(x, faf_map, on = :sctg2) |>
    x -> select(x, :origin, :destination, :year, :naics, :value)

    return df
end

raw"""
    load_faf_data(
        state_path::String,
        reprocessed_path::String,
        state_fips::DataFrame,
        faf_map::DataFrame;
        cols_to_keep = [
            :fr_orig,
            :dms_origst,
            :dms_destst,
            :fr_dest,
            :trade_type,
            :sctg2,
        ],
        regex_cols_to_keep = r"^value_(\d{4})$",
        max_year = 2023,
    )

Load the two necessary FAF files and aggregate to demand. The returned DataFrame
has columns:

- `destination` - FIPS code of destination state
- `year` - Year of the trade flow
- `naics` - NAICS code of the traded good
- `local` - And indicator on if the flow is local (within state) or national (between states)
- `value` - Value of the traded good in millions of USD

The returned DataFrame has data for all years 1997 to `max_year`. The reprocessed
data only has data on a five year basis (1997, 2002, 2007, 2012), the intermediate 
years uses the data from the closest available year.
"""
function load_faf_data(
        state_path::String,
        reprocessed_path::String,
        state_fips::DataFrame,
        faf_map::DataFrame;
        cols_to_keep = [
            :fr_orig,
            :dms_origst,
            :dms_destst,
            :fr_dest,
            :trade_type,
            :sctg2,
        ],
        regex_cols_to_keep = r"^value_(\d{4})$",
        max_year = 2023,
    )

    domestic_trade_post_2017 = load_faf_base(
        state_path,
        state_fips,
        faf_map;
        cols_to_keep = cols_to_keep,
        regex_cols_to_keep = regex_cols_to_keep,
        max_year = max_year
    ) 

    years = [
        (partial_year = 1997, year = 1997),
        (partial_year = 1997, year = 1998),
        (partial_year = 1997, year = 1999),
        (partial_year = 2002, year = 2000),
        (partial_year = 2002, year = 2001),
        (partial_year = 2002, year = 2002),
        (partial_year = 2002, year = 2003),
        (partial_year = 2002, year = 2004),
        (partial_year = 2007, year = 2005),
        (partial_year = 2007, year = 2006),
        (partial_year = 2007, year = 2007),
        (partial_year = 2007, year = 2008),
        (partial_year = 2007, year = 2009),
        (partial_year = 2012, year = 2010),
        (partial_year = 2012, year = 2011),
        (partial_year = 2012, year = 2012),
        (partial_year = 2012, year = 2013),
        (partial_year = 2012, year = 2014),
        (partial_year = 2017, year = 2015),
        (partial_year = 2017, year = 2016),
    ]

    append!(years, [(partial_year = y, year = y) for y in 2017:max_year])

    years = DataFrame(years)


    reprocessed_trade_pre_2017 = load_faf_base(
        reprocessed_path,
        state_fips,
        faf_map;
        cols_to_keep = cols_to_keep,
        regex_cols_to_keep = regex_cols_to_keep,
        max_year = max_year
    ) 

    demand_trade_goods =  vcat(domestic_trade_post_2017, reprocessed_trade_pre_2017) |>
        x -> rename(x, :year => :partial_year) |>
        x -> leftjoin(x, years, on = :partial_year) |>
        x -> select(x, :origin, :destination, :year, :naics, :value) |>
        x -> transform(x, 
            [:origin, :destination] => ByRow((o,d) -> o==d ? :local : :national) => :local
        ) |>
        x -> groupby(x, [:destination, :year, :naics, :local]) |>
        x -> combine(x, :value => sum => :value)

    return demand_trade_goods
end

"""
    load_regional_purchase_coefficients(
        demand_trade::DataFrame,
        commodity::DataFrame
    )

The regional purchase coefficients are used to determine the mixture of domestic 
vs. national demand in the absorption market. The coefficients are calculated
based on the FAF data and the commodity data. The returned DataFrame has columns:

- `state` - FIPS code of the state
- `year` - Year of the coefficient
- `naics` - NAICS code of the good
- `rpc` - Regional purchase coefficient, between 0 and 1

The computation has two steps:

1. Identify non-trade goods, which are goods that do not appear in the FAF data.
   Pin the RPC to be the average over the traded goods.
2. Compute the RPC as local / (local + national)
"""
function load_regional_purchase_coefficients(
    demand_trade::DataFrame,
    commodity::DataFrame
    )


    trade_goods = demand_trade |> x-> select(x, :naics) |> x -> unique(x, :naics) |> x-> x[!, :naics]

    non_trade_goods = commodity |> 
        x -> select(x, :name => :naics) |>
        x -> subset(x, :naics => ByRow(y -> !(y in trade_goods)))


    demand = vcat(
        demand_trade,
        demand_trade |>
            x -> groupby(x, [:year, :destination, :local]) |>
            x -> combine(x, :value => (y-> sum(y)/length(y)) => :value) |>
            x -> subset(x, :year => ByRow(==(2017))) |>
            x -> crossjoin(x, non_trade_goods)
    ) |>
    x -> unstack(x, :local, :value) |>
    x -> transform(x,
        [:local, :national] => ((l,n) -> l ./ (l .+ n)) => :rpc
    ) |>
    x -> select(x, :destination => :state, :year, :naics, :rpc) |>
    dropmissing

    return demand
end