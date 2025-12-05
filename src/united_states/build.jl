"""
    initialize_table(summary::National)

Create an empty State table structure based on the sets and elements of the 
provided National summary. 

Add one set, State, containing all US states. These are loaded using the provided
`state_fips.csv` file. 
"""
function initialize_table(summary::National)
    state_fips = load_state_fips()

    S = sets(summary) |>
        x -> vcat(x,
            DataFrame([
                (name = :State, description = "States", domain = :region)
            ])
        )

    E = elements(summary) |>
        x -> vcat(x,
            DataFrame([
                (set = :State, name = s, description = s) for s in state_fips[!, :state]
            ])
        )

    state_table = State(
        DataFrame(row = [], col = [], region = [], year = [], parameter = [], value = []),
        S,
        E,
        regularity_check = true
    )
end


"""
    create_state_table(
        summary::National,
        data_directory::String,
    )

Disaggregate the national summary into a state-level table, using data files
located in `data_directory`.


"""
function create_state_table(
        summary::National,
        data_directory::String,
    )

    state_table = initialize_table(summary)
    state_table = WiNDCRegional.disaggregate_intermediate(state_table, summary, data_directory)
    state_table = WiNDCRegional.disaggregate_labor_capital(state_table, summary, data_directory)
    state_table = WiNDCRegional.disaggregate_output_tax(state_table, summary, data_directory)


    state_table = WiNDCRegional.disaggregate_investement_final_demand(state_table, summary, data_directory)
    state_table = WiNDCRegional.disaggregate_personal_consumption(state_table, summary, data_directory)
    state_table = WiNDCRegional.disaggregate_household_supply(state_table, summary, data_directory)
    state_table = WiNDCRegional.disaggregate_government_final_demand(state_table, summary, data_directory)

    trade_shares = load_trade_shares(
        summary,
        joinpath(data_directory, "USATradeOnline", "State Exports by NAICS Commodities.csv"),
        joinpath(data_directory, "USATradeOnline", "State Imports by NAICS Commodities.csv"),
        joinpath(data_directory, "USATradeOnline", "commodity_detail_by_state_cy.xlsx"),
    )

    state_table = WiNDCRegional.disaggregate_foreign_exports(state_table, summary, data_directory, trade_shares)
    state_table = WiNDCRegional.create_reexports(state_table)
    state_table = WiNDCRegional.disaggregate_foreign_imports( state_table, summary, data_directory)
    state_table = WiNDCRegional.disaggregate_margin_demand(state_table, summary, data_directory)
    state_table = WiNDCRegional.disaggregate_duty(state_table, summary, data_directory)
    state_table = WiNDCRegional.disaggregate_tax(state_table, summary, data_directory)

    adjusted_demand = Dict(
        Symbol("22") => .9,
        Symbol("23") => .9,
    )

    rpc_new = load_regional_purchase_coefficients(
        summary,
        "FAF5.7.1_State.csv",
        "FAF5.7.1_Reprocessed_1997-2012_State.csv",
        data_directory;
        adjusted_demand = adjusted_demand
    ) 

    state_table = WiNDCRegional.create_regional_demand(state_table, rpc_new,)
    state_table = WiNDCRegional.create_regional_margin_supply(state_table, summary, rpc_new,)

    return state_table
end

"""
    disaggregate_intermediate(
            state_table::State,    
            summary::National,
            data_directory::String;
            gdp_path = "SAGDP2__ALL_AREAS_1997_2024.csv"
        )

Disaggregate the intermediate demand and supply from the national summary into 
state-level using BEA GSP data.

Dependent on:
    - "SAGDP2__ALL_AREAS_1997_2024.csv"
"""
function disaggregate_intermediate(
        state_table::State,    
        summary::National,
        data_directory::String;
        gdp_path = "SAGDP2__ALL_AREAS_1997_2024.csv"
    )

    gdp = load_state_gdp(
        joinpath(data_directory, "bea_gdp", gdp_path),
        "gdp"
    )

    state_intermediate = disaggregate_by_shares(
        summary,
        gdp,
        [:Intermediate_Demand, :Intermediate_Supply];
        domain = :sector
        )

    df = table(state_table)
    df = vcat(df,state_intermediate)

    return State(df, sets(state_table), elements(state_table))
end


function disaggregate_labor_capital(
        state_table::State,    
        summary::National,
        data_directory::String;
        gdp_path = "SAGDP2__ALL_AREAS_1997_2024.csv",
    )

    gdp = load_state_gdp(joinpath(data_directory, "bea_gdp", gdp_path), "gdp")
    region_share = gdp |>
        x -> groupby(x, [:year, :name, :naics]) |>
        x -> combine(x,
            :state => identity => :state,
            :value => (y -> y./sum(y)) => :share
        )

    labor_share = labor_shares(
        summary,
        data_directory
    )

    labor_capital_state = innerjoin(
            region_share |> x -> select(x, :year, :naics => :col, :state=>:region, :share),
            
            table(summary, :Value_Added) |>
                x -> groupby(x, [:col, :year]) |>
                x -> combine(x, :value => sum => :value),
            on = [:col, :year]
        ) |>
        x -> innerjoin(
            x,
            labor_share |> x -> rename(x, :value => :labor),
            on = [:col, :region, :year],
        ) |>
        x -> transform(x,
            [:share, :value, :labor] => ((s,v,l) -> s.*v.*l) => :labor_demand,
            [:share, :value, :labor] => ((s,v,l) -> s.*v.*(1 .-l)) => :capital_demand
        ) |>
        x -> select(x, :col, :year, :region, :labor_demand, :capital_demand) |>
        x -> stack(x, [:labor_demand, :capital_demand], variable_name = :parameter, value_name = :value) |>
        x -> transform(x,
            :parameter => ByRow(Symbol) => :parameter
        ) |>
        x -> leftjoin(
            x,
            elements(summary, :Value_Added; base=true) |> x -> select(x, :name => :row, :set=>:parameter),
            on = :parameter
        )




    df = table(state_table)
    df = vcat(df,labor_capital_state)

    return State(df, sets(state_table), elements(state_table))
end



"""
    disaggregate_subsidy(
        state_table::State,    
        summary::National,
        data_directory::String;
        subsidy_path = "SAGDP5__ALL_AREAS_1997_2024.csv"
    )

Disaggregate the subsidy from the national summary into state-level using BEA
GSP data.

Dependent on:
    - "SAGDP5__ALL_AREAS_1997_2024.csv"
"""
function disaggregate_subsidy(
        state_table::State,    
        summary::National,
        data_directory::String;
        subsidy_path = "SAGDP5__ALL_AREAS_1997_2024.csv"
    )

    subsidy = load_state_gdp(
        joinpath(data_directory, "bea_gdp", subsidy_path),
        "subsidy"
    )


    state_subsidy = disaggregate_by_shares(
        summary,
        subsidy,
        :Subsidy;
        domain = :commodity
        )

    df = table(state_table)
    df = vcat(df,state_subsidy)

    return State(df, sets(state_table), elements(state_table))
end


"""
    disaggregate_tax(
        state_table::State,    
        summary::National,
        data_directory::String;
        tax_path = "SAGDP6__ALL_AREAS_1997_2024.csv"
    )

Disaggregate the tax from the national summary into state-level using BEA
GSP data.

Dependent on:
    - [`WiNDCRegional.absorption`](@ref)
    - Summary absorption tax rate

"""
function disaggregate_tax(
        state_table::State,    
        summary::National,
        data_directory::String;
        tax_path = "SAGDP6__ALL_AREAS_1997_2024.csv"
    )


    col_label = elements(summary, :Tax; base = true)[1,1]

    state_tax = outerjoin(
            WiNDCRegional.absorption(state_table),
            absorption_tax_rate(summary, output = :tax_rate) |> x -> select(x, Not(:parameter)),
            on = [:year, :row],
        ) |>
        x -> transform(x,
            [:value, :tax_rate] => ByRow((v,tr) -> -v*tr) => :value,
            :parameter => ByRow(y -> (col_label, :Tax)) => [:col, :parameter]
        ) |>
        x -> select(x, :row, :col, :region, :year, :parameter, :value) |>
        x -> subset(x,
            :value => ByRow(y -> abs(y) > 1e-6)
        )

    df = table(state_table)
    df = vcat(df,state_tax)
    return State(df, sets(state_table), elements(state_table))
end


"""
    disaggregate_investement_final_demand(
        state_table::State,    
        summary::National,
        data_directory::String;
        investment_path = "SAGDP2__ALL_AREAS_1997_2024.csv"
    )

Disaggregate the investment final demand from the national summary into state-level
using BEA GSP data. Also aggregates all investment final demand to a single 
element `invest`, updates the elements table accordingly.

Dependent on:
    - "SAGDP2__ALL_AREAS_1997_2024.csv"
"""
function disaggregate_investement_final_demand(
        state_table::State,    
        summary::National,
        data_directory::String;
        gdp_path = "SAGDP2__ALL_AREAS_1997_2024.csv"
    )

    gdp = load_state_gdp(
        joinpath(data_directory, "bea_gdp", gdp_path),
        "gdp"
    )

    state_investment = disaggregate_by_shares(
        summary,
        gdp,
        [:Investment_Final_Demand];
        domain = :commodity
    ) |>
    x -> groupby(x, [:row, :year, :parameter, :region]) |>
    x -> combine(x, :value => sum => :value) |>
    x -> transform(x, :row => ByRow(y -> :invest) => :col)



    df = table(state_table)
    df = vcat(df,state_investment)

    E = elements(state_table) |>
        x -> subset(x,
            :set => ByRow(!=(:investment_final_demand))
        ) |>
        x -> vcat(x,
            DataFrame([
                (set = :investment_final_demand, name = :invest, description = "Investment Final Demand")
            ])
        )

    return State(df, sets(state_table), E)
end

"""
    disaggregate_personal_consumption(
        state_table::State,    
        summary::National,
        data_directory::String;
        pce_path = "SAPCE1__ALL_AREAS_1997_2024.csv"
    )

Disaggregate the personal consumption expenditure from the national summary into
state-level using BEA PCE data.

Dependent on:
    - "SAPCE1__ALL_AREAS_1997_2024.csv"
"""
function disaggregate_personal_consumption(
        state_table::State,    
        summary::National,
        data_directory::String;
        pce_path = "SAPCE1__ALL_AREAS_1997_2024.csv"
    )

    pce = load_pce_data(
        joinpath(data_directory, "PCE", pce_path),
        "pce"
    )

    state_pce = disaggregate_by_shares(
        summary,
        pce,
        [:Personal_Consumption];
        domain = :commodity
    )

    df = table(state_table)
    df = vcat(df,state_pce)

    return State(df, sets(state_table), elements(state_table))
end

"""
    disaggregate_household_supply(
        state_table::State,    
        summary::National,
        data_directory::String;
        pce_path = "SAPCE1__ALL_AREAS_1997_2024.csv"
    )

Disaggregate the household supply from the national summary into state-level using
BEA PCE data.

Dependent on:
    - "SAPCE1__ALL_AREAS_1997_2024.csv"
"""
function disaggregate_household_supply(
        state_table::State,    
        summary::National,
        data_directory::String;
        pce_path = "SAPCE1__ALL_AREAS_1997_2024.csv"
    )

    pce = load_pce_data(
        joinpath(data_directory, "PCE", pce_path),
        "pce"
    )

    state_household_supply = disaggregate_by_shares(
        summary,
        pce,
        :Household_Supply;
        domain = :commodity
    )

    df = vcat(table(state_table), state_household_supply)
    return State(df, sets(state_table), elements(state_table))
end

"""
    disaggregate_government_final_demand(
        state_table::State,    
        summary::National,
        data_directory::String;
        sgf_map = WiNDCRegional.load_sgf_map(),
        sgf_states = WiNDCRegional.load_sgf_states()
    )

Disaggregate the government final demand from the national summary into state-level
using Census SGF data. Also aggregates all government final demand to a single 
element `govern`, updates the elements table accordingly.

Dependent on:
    - Census SGF data files
"""
function disaggregate_government_final_demand(
        state_table::State,    
        summary::National,
        data_directory::String;
        sgf_map = WiNDCRegional.load_sgf_map(),
        sgf_states = WiNDCRegional.load_sgf_states()
    )

    census_data = load_state_finances(
            summary,
            joinpath(data_directory, "SGF");
            sgf_states = sgf_states,
            sgf_map = sgf_map
        ) |>
        x -> rename(x, :naics => :row, :state => :region) |>
        x -> groupby(x, [:year, :row]) |>
        x -> combine(x, 
            :region => identity => :region,
            :value => (y -> y./sum(y)) => :value
        )


    state_government = leftjoin(
            table(summary, :Government_Final_Demand) |>
                x -> groupby(x, 
                        [:year, :row]
                ) |>
                x -> combine(x, :value => sum => :value),
            census_data |> x -> rename(x, :value => :sgf),
            on = [:year, :row],
        ) |>
        x -> transform(x, 
            [:value, :sgf] => ByRow(*) => :value,
            :row => ByRow(y -> (:govern, :government_final_demand)) => [:col, :parameter]
        ) |>
        x -> select(x, :row, :col, :year, :region, :parameter, :value) 

    df = vcat(table(state_table), state_government)

    E = elements(state_table) |>
        x -> subset(x, :set => ByRow(!=(:government_final_demand))) |>
        x -> vcat(x,
            DataFrame([
                (set = :government_final_demand, name = :govern, description = "Government Final Demand")
            ])
        )

    return State(df, sets(state_table), E)
end

"""
    disaggregate_output_tax(
        state_table::State,    
        summary::National,
        data_directory::String
    )

Disaggregate the output tax from the national summary into state-level using 
national level tax rates and total output by state.

Dependent on:
    - Intermediate Demand
    - Labor Demand
    - Capital Demand
"""
function disaggregate_output_tax(
        state_table::State,    
        summary::National,
        data_directory::String
    )

    # Ensure we maintain labeling consistency 
    tax_code = elements(summary, :Output_Tax; base=true) |>
        x -> only(x)[:name]


    state_output_tax = outerjoin(
        table(state_table, :Intermediate_Supply) |>
            x -> groupby(x, [:year, :col, :region]) |>
            x -> combine(x, :value => sum => :output),
        WiNDCNational.output_tax_rate(summary),
        on = [:year, :col]
        ) |>
        x -> transform(x, 
            [:output, :value] => ((o,r) -> -o .* r) => :output_tax,
            :value => ByRow(y -> tax_code) => :row
        ) |>
        x -> subset(x, :output_tax => ByRow(y -> abs(y)>1e-5)) |>
        x -> select(x, :row, :col, :region, :year, :parameter, :output_tax => :value)


    df = table(state_table)
    df = vcat(df,state_output_tax)

    return State(df, sets(state_table), elements(state_table))
end


"""
    disaggregate_foreign_exports(
            state_table::State,    
            summary::National,
            data_directory::String,
            trade_shares::DataFrame;
            gdp_path = "SAGDP2__ALL_AREAS_1997_2024.csv"
        )

Disaggregate the foreign exports from the national summary into state-level using
BEA GSP data and trade shares.

Dependent on:
    - "SAGDP2__ALL_AREAS_1997_2024.csv"
    - trade shares DataFrame, produced by
      [`WiNDCRegional.load_trade_shares`](@ref)
"""
function disaggregate_foreign_exports(
        state_table::State,    
        summary::National,
        data_directory::String,
        trade_shares::DataFrame;
        gdp_path = "SAGDP2__ALL_AREAS_1997_2024.csv"
    )

    gdp = load_state_gdp(
        joinpath(data_directory, "bea_gdp", gdp_path),
        "gdp"
    )

    region_share = gdp |>
        x -> groupby(x, [:year, :name, :naics]) |>
        x -> combine(x,
            :state => identity => :state,
            :value => (y -> y./sum(y)) => :share
        )

    all_goods_years = crossjoin(
        elements(summary, :commodity) |> x -> select(x, :name => :naics),
        elements(summary, :year) |> x -> select(x, :name => :year),
    )


    goods_years_in_trade = trade_shares |>
        x -> select(x, :year, :naics) |>
        unique

    non_trade_goods_years = antijoin(
        all_goods_years,
        goods_years_in_trade,
        on = [:naics, :year]
    ) 

    non_trade_gdp = innerjoin(
        non_trade_goods_years,
        region_share |> x -> select(x, :year, :naics, :state, :name, :share),
        on = [:year, :naics]
    )

    export_disag = vcat(
        trade_shares |> 
            x -> rename(x, :flow => :name, :value => :share) |>
            x -> subset(x, :name => ByRow(==("exports"))),
        non_trade_gdp
    ) 

    state_exports = innerjoin(
            table(summary, :Export),
            export_disag,
            on = [:year, :row => :naics]
        ) |>
        x -> transform(x,
            [:share, :value] => ByRow(*) => :value,
        ) |>
        x -> select(x, :row, :col, :year, :state=>:region, :parameter, :value)

    #state_exports = disaggregate_by_shares(
    #    summary,
    #    export_disag,
    #    [:Export];
    #    domain = :commodity,
    #    fill_missing = false
    #    )

    df = vcat(table(state_table), state_exports)

    return State(df, sets(state_table), elements(state_table))
end

"""
    create_reexports(
        state_table::State
    )

Reexports are defined as the negative portion of the difference between total 
supply and exports.

Dependent on:
    - [`WiNDCRegional.total_supply`](@ref)
    - exports
"""
function create_reexports(
        state_table::State
    )

    reexports = innerjoin(
        WiNDCRegional.total_supply(state_table; output = :supply),
        table(state_table, :Export) |> x -> select(x, :row, :year, :region, :value => :exports),
        on = [:row, :year, :region]
    ) |>
    x -> transform(x,
        [:supply, :exports] => ByRow((a,b) -> (a+b)) => :value,
        :row => ByRow(y -> (:reexport, :reexport)) => [:col, :parameter]
    ) |>
    x -> subset(x, :value => ByRow(<(0))) |>
    x -> select(x, :row, :col, :region, :year, :parameter, :value)

    df = vcat(table(state_table), reexports)
    S = sets(state_table) |>
        x -> vcat(x,
            DataFrame([
                (name = :reexport, description = "Reexports", domain = :sector),
                (name = :Reexport, description = "Reexports", domain = :parameter)
            ])
        )
    E = elements(state_table) |>
        x -> vcat(x,
            DataFrame([
                (set = :Reexport, name = :reexport, description = "Reexports parameter"),
                (set = :reexport, name = :reexport, description = "Reexports parameter"),
            ])
        )

    return State(df, S, E)
end

"""
    disaggregate_foreign_imports(
        state_table::State,    
        summary::National,
        data_directory::String;
    )

Disaggregate the foreign imports from the national summary into state-level using
[`absorption`](@ref).

Dependent on:
    - [`absorption`](@ref)
"""
function disaggregate_foreign_imports(
        state_table::State,    
        summary::National,
        data_directory::String;
    )

    state_imports = disaggregate_by_shares(
        summary,
        absorption(state_table) |> 
            x -> select(x, :row => :naics, :parameter => :name, :year, :region => :state, :value),
        :Import;
        domain = :commodity
        )

    df = vcat(table(state_table), state_imports)

    return State(df, sets(state_table), elements(state_table))
end

"""
    disaggregate_margin_demand(
        state_table::State,    
        summary::National,
        data_directory::String;
    )

Disaggregate the margin demand from the national summary into state-level using
[`absorption`](@ref).

Dependent on:
    - [`absorption`](@ref)
"""
function disaggregate_margin_demand(
        state_table::State,    
        summary::National,
        data_directory::String;
    )

    state_imports = disaggregate_by_shares(
        summary,
        absorption(state_table) |> 
            x -> select(x, :row => :naics, :parameter => :name, :year, :region => :state, :value),
        :Margin_Demand;
        domain = :commodity
        )

    df = vcat(table(state_table), state_imports)

    return State(df, sets(state_table), elements(state_table))

    return State(df, sets(state_table), elements(state_table))
end

"""
    disaggregate_duty(
        state_table::State,    
        summary::National,
        data_directory::String
    )

Disaggregate the duty from the national summary into state-level using national
level duty rates and total imports by state.

Dependent on:
    - imports
"""
function disaggregate_duty(
        state_table::State,    
        summary::National,
        data_directory::String
    )
        
    duty_code, duty_set = elements(summary, :Duty; base=true) |>
        x -> (only(x)[:name], only(x)[:set])

    state_duty = rightjoin(
        table(state_table, :Import) |> x-> rename(x, :value => :import),
        WiNDCNational.import_tariff_rate(summary; output = :duty_rate, minimal=true) |> x-> select(x, Not(:parameter)),
        on = [:year, :row]
        ) |>
        x -> transform(x, 
            [:import, :duty_rate] => ((o,r) -> o .* r) => :value,
            :duty_rate => ByRow(y -> (duty_code, duty_set)) => [:col, :parameter]
        ) |>
        x -> subset(x, :value => ByRow(y -> abs(y)>1e-5)) |>
        x -> select(x, :row, :col, :year, :region, :parameter, :value)

    df = vcat(table(state_table), state_duty)

    return State(df, sets(state_table), elements(state_table))
end


"""
    adjust_by_absorption(
        state_table::State,    
    )

Adjust state-level tables by absorption-related components, including:

- Reexports
- Exports
- Household Supply
"""
function adjust_by_absorption(
        state_table::State,    
    )

    # Want to be negative
    diff = vcat(
        WiNDCRegional.absorption(state_table),
        table(state_table, :Tax, :Subsidy, :Reexport, :Import, :Duty, :Margin_Demand)
    ) |>
    x -> groupby(x, [:year, :region, :row]) |>
    x -> combine(x, :value => sum => :diff) |>
    x -> subset(x,
        :diff => ByRow(>(0))
    ) |>
    x -> sort(x, :diff)

    reexports = leftjoin(
            table(state_table, :Reexport),
            diff,
            on = [:year, :region, :row]
        ) |>
        x -> coalesce.(x,0) |>
        x -> transform(x,
                [:value, :diff] => ByRow(-) => :value
        ) |> x -> select(x, Not(:diff))


    state_exports = leftjoin(
            table(state_table, :Export),
            diff,
            on = [:year, :region, :row]
        ) |>
        x -> coalesce.(x,0) |>
        x -> transform(x,
            [:value, :diff] => ByRow(-) => :value
        ) |> x -> select(x, Not(:diff))

    state_household_supply = table(state_table, :Household_Supply) |>
        x -> leftjoin(
            x,
            diff,
            on = [:year, :region, :row]
        ) |>
        x -> coalesce.(x,0) |>
        x -> transform(x,
            [:value, :diff] => ByRow(+) => :value,
        ) |> x -> select(x, Not(:diff))


    df = table(state_table) |>
        x -> subset(x,
            :parameter => ByRow(y -> !(y in (:reexport, :export, :household_supply)))
        ) |>
        x -> vcat(
            x,
            reexports,
            state_exports,
            state_household_supply
        )

    return State(df, sets(state_table), elements(state_table))
end

"""
    create_regional_demand(
            state_table::State,
            rpc::DataFrame,
        )

Create regional demand (local + national) based on regional purchase coefficients.

Dependent on:
    - [`WiNDCRegional.load_regional_purchase_coefficients`](@ref)
    - [`WiNDCRegional.absorption`](@ref)
    - [`WiNDCRegional.total_supply`](@ref)
"""
function create_regional_demand(
        state_table::State,
        rpc::DataFrame,
    )

    domestic_demand_absorption = vcat(
            WiNDCRegional.absorption(state_table),
            table(state_table, :Tax, :Subsidy, :Reexport, :Import, :Duty, :Margin_Demand),
        ) |>
        x -> groupby(x, [:year, :region, :row]) |>
        x -> combine(x, :value => sum => :value) |>
        x -> subset(x, :value => ByRow(<(1e-6)))  |>
        x -> transform(x, :value => ByRow(-) => :value)

    domestic_reexport_demand = vcat(
        WiNDCRegional.total_supply(state_table),
        table(state_table, :Export, :Reexport; normalize = :Reexport),
    ) |>
        x -> groupby(x, [:year, :region, :row]) |>
        x -> combine(x, :value => sum => :value)


    regional_demand = vcat(
        domestic_demand_absorption,
        domestic_reexport_demand,
    ) |>
    x -> groupby(x, [:row, :year, :region]) |>
    x -> combine(x, :value => (y -> length(y)==1 ? 0 : minimum(y)) => :value) 



    dd0_ = leftjoin(
        regional_demand,
        rpc |> x -> rename(x, :naics => :row),
        on = [:year, :region, :row]
    ) |>
    x -> coalesce.(x,1) |>
    x -> transform(x,
        [:value, :rpc] => ByRow((a,b) -> a*b) => :value,
        :row => ByRow(y -> (:local_demand, :local_demand)) => [:col, :parameter]
    ) |> 
    x -> select(x, Not(:rpc))


    nd0_ = leftjoin(
            domestic_demand_absorption,
            dd0_ |>
                x -> select(x, :year, :region, :row, :value => :dd0),
            on = [:year, :row, :region],
        ) |>
        x -> transform(x,
            [:value, :dd0] => ByRow((a,b) -> a - b) => :value,
            :row => ByRow(y -> (:national_demand, :national_demand)) => [:col, :parameter]
        ) |>
        x -> select(x, Not(:dd0))

    df = vcat(table(state_table), dd0_, nd0_)
    S = sets(state_table) |>
        x -> vcat(x,
            DataFrame([
                (name = :local_demand, description = "Local Demand", domain = :col),
                (name = :national_demand, description = "National Demand", domain = :col),
                (name = :Local_Demand, description = "Local Demand", domain = :parameter),
                (name = :National_Demand, description = "National Demand", domain = :parameter)
            ])
        )

    E = elements(state_table) |>
        x -> vcat(x,
            DataFrame([
                (set = :Local_Demand, name = :local_demand, description = "Local Demand parameter"),
                (set = :National_Demand, name = :national_demand, description = "National Demand parameter"),
                (set = :local_demand, name = :local_demand, description = "Local Demand element"),
                (set = :national_demand, name = :national_demand, description = "National Demand element"),
            ])
        )

    return State(df, S, E)

end


function create_regional_margin_supply(
        state_table::State,
        summary::National,
        rpc::DataFrame
    )

    margin_shares = table(state_table, :Margin_Demand) |>
        x -> groupby(x, [:col, :year, :region]) |>
        x -> combine(x, :value => sum => :value) |>
        x -> groupby(x, [:year, :col]) |>
        x -> combine(x,
            [:region] .=> identity .=> [:region],
            :value => (y -> y./sum(y)) => :share
        )

    total_margin_supply = outerjoin(
            table(summary, :Margin_Supply),
            margin_shares,
            on = [:year, :col]
        ) |>
        x -> transform(x,
            [:value, :share] => ((v,s) -> v .* s) => :value
        ) |>
        x -> select(x, :row, :col, :year, :region, :parameter, :value)
        

    trade_shares = total_margin_supply |>
            x -> groupby(x, [:year, :region, :row]) |>
            x -> combine(x, 
                :col => identity .=> :col,
                :value => (y-> y./sum(y)) => :share
            )

    temp = vcat(
        WiNDCRegional.total_supply(state_table),
        table(state_table, :Export, :Reexport, :Local_Demand; normalize = :Reexport),
    ) |>
    x -> groupby(x, [:year, :region, :row]) |>
    x -> combine(x, :value => sum => :value) |>
    x -> subset(x, :value => ByRow(>(0))) |>
    x -> transform(x, :value => ByRow(-) => :value)



    TEST = innerjoin(
        trade_shares,
        temp,
        on = [:year, :region, :row]
    )  |>
    x -> transform(x,
        [:share, :value] => ((s,v) -> s .* v) => :value
    ) |>
    x -> select(x, :row, :col, :year, :region, :value)

    TEST2 = innerjoin(
        total_margin_supply,
        rpc,
        on = [:year, :region, :row => :naics]
    ) |>
    x -> transform(x,
        [:value, :rpc] => ByRow((a,b) -> a*b) => :value
    ) |>
    x -> select(x, :row, :col, :year, :region, :value)


    local_margin_supply = vcat(
            TEST,
            TEST2,
        ) |>
        x -> groupby(x, [:row, :year, :region, :col]) |>
        x -> combine(x, :value => maximum => :value) |>
        x -> transform(x, :row => ByRow(y -> :local_margin_supply) => :parameter)


    region_margin_supply = outerjoin(
        total_margin_supply |> x -> select(x, :row, :col, :year, :region, :value => :totmarg),
        local_margin_supply,
        on = [:row, :col, :year, :region]
    ) |>
    x -> coalesce.(x,0) |>
    x -> transform(x,
        [:totmarg, :value] => ByRow(-) => :value,
        :row => ByRow(y -> :region_margin_supply) => :parameter    
    ) |>
    x -> select(x, :row, :col, :year, :region, :parameter, :value)

    df = vcat(table(state_table), local_margin_supply, region_margin_supply)

    S = sets(state_table) |>
        x -> vcat(x,
            DataFrame([
                (name = :Local_Margin_Supply, description = "Local Margin Supply", domain = :parameter),
                (name = :National_Margin_Supply, description = "National Margin Supply", domain = :parameter)
            ])
        )
    E = elements(state_table) |>
        x -> vcat(x,
            DataFrame([
                (set = :Local_Margin_Supply, name = :local_margin_supply, description = "Local Margin Supply parameter"),
                (set = :National_Margin_Supply, name = :national_margin_supply, description = "National Margin Supply parameter"),
                (set = :Margin_Supply, name = :local_margin_supply, description = "Local Margin Supply element"),
                (set = :Margin_Supply, name = :national_margin_supply, description = "National Margin Supply element"),
            ])
        )  

    return State(df, S, E)


end