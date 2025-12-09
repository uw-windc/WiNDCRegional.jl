"""
    labor_shares(
        summary::National,
        raw_data::Dict;
    )

Compute the share of total value added (`labor` + `capital`) attributed to labor for each
region, sector, and year. 

Returns a DataFrame with columns: `year`, `region`, `col` (sector), and `value` (labor share).

## Required Arguments

- `summary::National`: A `National` object containing the national summary data.
- `raw_data::Dict`: A dictionary containing the raw data DataFrames.


## Raw Data Used

- `:gdp`
- `:labor`
- `:capital`
- `:tax`
- `:subsidy`

## Motivation

The `captial` data provided by the BEA has unavoidable negative values for some 
years, sectors, and regions. To address this, we compute labor shares by reconciling 
the reported GDP data with GDP computed from GSP components (labor, capital, tax, 
subsidy). We then use a least squares optimization approach to estimate labor 
shares that are consistent with both the national-level labor shares and the
regional GDP data.

## Data Source

This data can be downloaded from 
[the BEA website](https://apps.bea.gov/regional/downloadzip.htm), select `Gross
Domestic Product (GDP) by State` and download the `SAGDP` data.
"""
function labor_shares(
    summary::National,
    raw_data::Dict;
)
    gdp = raw_data[:gdp]
    region_share = gdp |>
        x -> groupby(x, [:year, :name, :naics]) |>
        x -> combine(x,
            :state => identity => :state,
            :value => (y -> y./sum(y)) => :share
        )


    labor = raw_data[:labor]
    capital = raw_data[:capital]
    tax = raw_data[:tax]
    subsidy = raw_data[:subsidy]

    calculated_gdp = compute_gdp_from_gsp(
        labor,
        capital,
        tax,
        subsidy,
    )

    difference = innerjoin(
        calculated_gdp,
        gdp |> x -> select(x, Not(:name)),
        on = [:year, :state, :naics],
        renamecols = "_calc" => "_report"
        ) |>
        x -> transform(x,
            [:value_report, :value_calc] => ByRow(-) => :value,
            :year => ByRow(y -> :gdp_diff) => :name
        ) |>
        x -> select(x, [:year, :state, :naics, :name, :value])

    klshare_nat = table(summary, :Value_Added) |>
        x -> groupby(x, [:col, :year]) |>
        x -> combine(x,
            [:row, :parameter] .=> identity .=> [:row, :parameter],
            :value => (y -> y./sum(y)) => :share
        ) 


    gsp_nat_shares = vcat(
        difference |> x -> transform(x, :name => ByRow(y -> "capital") => :name),
        labor,
        capital
    ) |>
        x -> groupby(x, [:year , :naics, :name]) |>
        x -> combine(x,
            :value => sum => :value
        ) |>
        x -> rename(x, :naics => :col, :name => :parameter) |>
        x -> transform(x,
            :parameter => (y->Symbol.(y, "_demand")) => :parameter
        ) |>
        x -> groupby(x, [:col, :year]) |>
        x -> combine(x,
            [:parameter] .=> identity .=> [:parameter],
            :value => (y -> y./sum(y)) => :share
        ) 

    delta_shares = innerjoin(
        klshare_nat,
        gsp_nat_shares,
        on = [:col, :year, :parameter],
        renamecols = "_nat" => "_gsp"
    ) |>
    x -> transform(x,
        [:share_nat, :share_gsp] => ByRow(/) => :delta
    ) |>
    x -> select(x, [:col, :year, :parameter, :delta])



    klshare = vcat(
        difference |> x -> transform(x, :name => ByRow(y -> "capital") => :name),
        labor,
        capital
    ) |>
        x -> groupby(x, [:year , :state, :naics, :name]) |>
        x -> combine(x,
            :value => sum => :value
        ) |>
        x -> rename(x, :naics => :col, :name => :parameter) |>
        x -> transform(x,
            :parameter => (y->Symbol.(y, "_demand")) => :parameter
        ) |>
        x -> groupby(x, [:col, :state, :year]) |>
        x -> combine(x,
            [:parameter] .=> identity .=> [:parameter],
            :value => (y -> y./sum(y)) => :share
        ) |>
        x -> outerjoin(
            x,
            delta_shares,
            on = [:col, :year, :parameter],
        ) |>
        x -> transform(x,
            [:share, :delta] => ByRow(*) => :share
        ) |>
        x -> select(x, [:col, :state, :year, :parameter, :share])


    states = klshare |>
        x -> select(x, :state) |>
        dropmissing |>
        x -> unique(x) 

    klshare_nat_state = crossjoin(
        states,
        klshare_nat,
    )


    klshare_fin = outerjoin(
        klshare,
        klshare_nat_state,
        on = [:col, :year, :parameter, :state],
        renamecols = "" => "_nat"
    ) |>
    x -> transform(x,
        [:share, :share_nat] => ByRow((s, n) ->
            (ismissing(s) || s<0 || abs(s-n) > .75) ? n : s
        ) => :share_final
    ) |>
    x -> select(x, :col, :state, :year, :parameter, :share_final => :share) |>
    x -> unstack(x, :parameter, :share) |>
    x -> transform(x,
        :labor_demand => ByRow(y -> 1-y) => :capital_demand
    ) |>
    x -> stack(x, [:labor_demand, :capital_demand], variable_name = :parameter, value_name = :share)


    sectors = elements(summary, :sector) |> x -> x[!, :name]
    yrs = unique(klshare.year)

    years = DataFrame(
            year = unique(klshare.year)
        ) |>
        x -> crossjoin(
            x,
            x,
            renamecols = "" => "_lyr"
        ) |>
        x -> subset(x,
            [:year, :year_lyr] => ByRow((y, ly) -> abs(y - ly) <= 4)
        )

    M = labor_share_model(
        summary,
        years,
        sectors,
        states[!, :state],
        klshare_fin,
        klshare_nat_state,
        region_share,
        gdp,
        klshare,
    )

    optimize!(M)

    labor_shares = DataFrame([
            (year = yr, region = r, col = s, value = value(M[:L_SHR][yr, s, r]))
            for yr in yrs, s in sectors, r in states[!, :state]
        ]) #|>
        x -> subset(x, :value => ByRow(>(0)))

    return labor_shares
end


function compute_gdp_from_gsp(
    labor::DataFrame,
    capital::DataFrame,
    tax::DataFrame,
    subsidy::DataFrame,
)
    return vcat(
            labor,
            capital,
            tax,
            subsidy
        ) |>
        x -> groupby(x, [:year, :state, :naics]) |>
        x -> combine(x, :value => sum => :value) 

end


function labor_share_model(
    summary::National,
    years::DataFrame,
    sectors::Vector{Any},
    states::Vector{String},
    klshare_fin::DataFrame,
    klshare_nat_state::DataFrame,
    region_share::DataFrame,
    gdp::DataFrame,
    klshare::DataFrame,

)
    yrs = years[!,:year] |> unique

    M = Model(Ipopt.Optimizer)

    @variables(M, begin
        L_SHR[yr=yrs, s= sectors, r = states] >= 0
        K_SHR[yr=yrs, s= sectors, r = states] >= 0
    end);


    outerjoin(
        klshare_fin |> x -> unstack(x, :parameter, :share),
        klshare_nat_state |> x -> select(x, Not(:row)) |> x -> unstack(x, :parameter, :share),
        on = [:col, :year, :state],
        renamecols = "" => "_nat"
    ) |>
    df -> for row in eachrow(df)
        set_start_value(L_SHR[row[:year], row[:col], row[:state]], row[:labor_demand])
        set_start_value(K_SHR[row[:year], row[:col], row[:state]], row[:capital_demand])
        set_lower_bound(L_SHR[row[:year], row[:col], row[:state]], .25*row[:labor_demand_nat])
        set_lower_bound(K_SHR[row[:year], row[:col], row[:state]], .25*row[:capital_demand_nat])
    end


    antijoin(
        crossjoin(
            DataFrame(state = states),
            DataFrame(col = sectors),
            DataFrame(year = yrs)
        ),
        region_share |>
            x->select(x, :year, :naics => :col, :state),
        on = [:year, :col, :state]
    ) |>
    df -> for row in eachrow(df)
        fix(L_SHR[row[:year], row[:col], row[:state]], 0.0; force=true)
        fix(K_SHR[row[:year], row[:col], row[:state]], 0.0; force=true)
    end



    innerjoin(
        gdp |>
            x -> select(x, :year, :naics => :col,  :state, :value => :gdp),

        klshare_fin |>
            x->subset(x,
                :parameter => ByRow(==("labor_demand"))
            ) |>
            x -> leftjoin(
                x,
                years,
                on = :year,
            ) |>
            x -> select(x, :col, :year, :state, :share => :labor, :year_lyr),
        on = [:col, :state, :year],
    ) |>
    x -> transform(x,
        [:year, :col, :state] => ByRow((yr, s, r) -> L_SHR[yr, s, r]) => :L_SHR
    ) |>
    x -> transform(x,
        [:gdp, :labor, :L_SHR] => ByRow((g, l, L) -> abs.(g).*(L./l .- 1)^2) => :obj
    ) |>
    eqn -> 
    @objective(M, Min, sum(eqn.obj));


    ## Heads up: 445, 452, 4A0, ORE and GSLG have missing values in region_share

    innerjoin(
        region_share |>
            x -> select(x, :naics => :col, :year, :state, :share => :region_share)|>
            x -> transform(x,
                [:year, :col, :state] => ByRow((yr, s, r) -> (L_SHR[yr, s, r], K_SHR[yr, s, r])) => [:L_SHR, :K_SHR]
            ) |>
            x -> groupby(x, [:col, :year]) |>
            x -> combine(x,
                [:region_share, :L_SHR] => ((rs, ls) -> sum(rs .* ls)) => :L,
                [:region_share, :K_SHR] => ((rs, ks) -> sum(rs .* ks)) => :K
            ),

        table(summary, :Value_Added; normalize = :Use) |>
            x -> select(x, Not(:row)) |>
            x -> unstack(x, :parameter, :value),
        on = [:col, :year],
    ) |>
    df -> 
    @constraints(M, begin 
        lshrdef[row = eachrow(df)], row[:L]*(row[:labor_demand] + row[:capital_demand]) == row[:labor_demand]
        kshrdef[row = eachrow(df)], row[:K]*(row[:labor_demand] + row[:capital_demand]) == row[:capital_demand]
    end);

    region_share |>
    x -> transform(x,
        [:year, :naics, :state] => ByRow((yr, s, r) -> (L_SHR[yr, s, r], K_SHR[yr, s, r])) => [:L_SHR, :K_SHR]
    ) |>
    df ->
    @constraint(M, shrconstr[row = eachrow(df)], 
        row[:L_SHR] + row[:K_SHR] == 1
    );

    return M

end