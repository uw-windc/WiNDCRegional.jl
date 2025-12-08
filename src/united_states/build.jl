"""
    initialize_table(summary::National)

Create an empty State table structure based on the sets and elements of the 
provided National summary. 

Add one set, State, containing all US states. These are loaded using the provided
`state_fips.csv` file. 

NOTE: Update with set listing
"""
function initialize_table(summary::National, raw_data::Dict)
    state_fips = raw_data[:state_map]

    sets_to_keep = [
        :capital_demand,
        :commodity,
        :duty,
        :export,
        :import,
        :labor_demand,
        :margin,
        :output_tax,
        :personal_consumption,
        :sector,
        :tax,
        :trade,
        :transport,
        :year
    ]

    aggregate_parameters = [
        :Other_Final_Demand,
        :Use,
        :Supply,
        :Final_Demand,
        :Value_Added
    ]

    S = sets(summary, sets_to_keep..., aggregate_parameters...) |>
        x -> vcat(x,
            DataFrame([
                (name = :state, description = "States", domain = :region)
            ])
        )

    E = elements(summary, sets_to_keep...) |>
        x -> vcat(x,
            DataFrame([
                (set = :state, name = s, description = s) for s in state_fips[!, :state]
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
    load_map_data(map_dictionary::Dict)

Load map data specified in the `metadata:maps` section of the regional.yaml file.
Returns a dictionary of DataFrames, with keys corresponding to the map types.

If the paths are not speicifed in `regional.yaml`, the default maps will be loaded.

## Required Arguments

- `map_dictionary::Dict`: A dictionary with keys the name of the map and values the 
    path to the map data file. If the value is `nothing`, the default map will be loaded.

## Default Loading Functions

- `state_map` => [`WiNDCRegional.load_state_fips`](@ref)
- `gdp_map` => [`WiNDCRegional.load_industry_codes`](@ref)
- `pce_map` => [`WiNDCRegional.load_pce_map`](@ref)
- `sgf_map` => [`WiNDCRegional.load_sgf_map`](@ref)
- `sgf_states_map` => [`WiNDCRegional.load_sgf_states`](@ref)
- `trade_map` => [`WiNDCRegional.load_usatrade_map`](@ref)
- `faf_map` => [`WiNDCRegional.load_faf_map`](@ref)
"""
function load_map_data(map_dictionary::Dict)
    load_functions = Dict(
        "state_map" => WiNDCRegional.load_state_fips,
        "gdp_map" => WiNDCRegional.load_industry_codes,
        "pce_map" => WiNDCRegional.load_pce_map,
        "sgf_map" => WiNDCRegional.load_sgf_map,
        "sgf_states_map" => WiNDCRegional.load_sgf_states,
        "trade_map" => WiNDCRegional.load_usatrade_map,
        "faf_map" => WiNDCRegional.load_faf_map,
    )   

    out = Dict{Symbol,DataFrame}()
    for (key, path) in map_dictionary
        if haskey(load_functions, key)
            out[Symbol(key)] = isnothing(path) ? load_functions[key]() : load_functions[key](path)
        else
            error("No load function defined for map type: $key")
        end
    end

    return out

end



"""
    load_raw_data(
            summary::National,
            data_info::Dict, 
            data_directory::String, 
            maps::Dict{Symbol, DataFrame}
        )

Load data files specified in `regional.yaml` needed for disaggregation of the US
WiNDCNational summary into state-level WiNDCRegional table.

Returns a dictionary of DataFrames, with keys corresponding to key in the `data`
section of `regional.yaml`.

## Required Arguments

- `summary::National`: The national summary WiNDCNational table.
- `data_info::Dict`: The `data` section of the `regional.yaml` file.
- `data_directory::String`: The base directory where data files are located. This 
    is specified in the `metadata:data_directory` field of `regional.yaml`.
- `maps::Dict{Symbol, DataFrame}`: A dictionary of mapping DataFrames loaded using
    [`WiNDCRegional.load_map_data`](@ref).

## Data Loading Functions

- `state_gdp` - All values => [`WiNDCRegional.load_state_gdp`](@ref)
- `pce` => [`WiNDCRegional.load_pce_data`](@ref)
- `sgf` => [`WiNDCRegional.load_state_finances`](@ref)
- `trade_shares`
    - `exports` and `imports` => [`WiNDCRegional.load_usa_raw_trade_data`](@ref)
    - `ag_time_series` => [`WiNDCRegional.load_usda_agricultural_flow`](@ref)
    - trade shares => [`WiNDCRegional.load_trade_shares`](@ref)
- `rpc` => [`WiNDCRegional.load_regional_purchase_coefficients`](@ref)
"""
function load_raw_data(
        summary::National,
        data_info::Dict, 
        data_directory::String, 
        maps::Dict{Symbol, DataFrame}
    )
    out = Dict()
    
    # State GDP
    state_data = data_info["state_gdp"]
    state_dir = state_data["metadata"]["base_directory"] # Careful

    # Load GDP data
    for (name, data) in state_data
        name == "metadata" ? continue : nothing
        out[Symbol(name)] = load_state_gdp(
            joinpath(data_directory, state_dir, data["path"]), 
            name;
            state_fips = maps[:state_map],
            industry_codes = maps[:gdp_map],
        )
    end


    # PCE data
    pce_data = data_info["personal_consumption"]
    state_dir = pce_data["metadata"]["base_directory"]

    out[:pce] = load_pce_data(
        joinpath(data_directory, state_dir, pce_data["pce"]["path"]), 
        "pce";
        state_fips = maps[:state_map],
        pce_map = maps[:pce_map],
        )

    # SGF data
    finance_data = data_info["state_finances"]

    out[:sgf] = WiNDCRegional.load_state_finances(
         Regex(finance_data["sgf"]["path"]), 
        joinpath(data_directory, finance_data["metadata"]["base_directory"]); 
        replacement_data = finance_data["sgf"]["replacement"],
        sgf_states = maps[:sgf_states_map],
        sgf_map = maps[:sgf_map],
        )


    # Trade data
    trade_data = data_info["trade"]
    trade_path = joinpath(data_directory, trade_data["metadata"]["base_directory"])
    ### exports
    exports = WiNDCRegional.load_usa_raw_trade_data(
        joinpath(trade_path, trade_data["exports"]["path"]),
        "exports";
        value_col = Symbol(trade_data["exports"]["sheet"]),
        state_fips = maps[:state_map],
        usatrade_map = maps[:trade_map]
    )

    ### imports
    imports = WiNDCRegional.load_usa_raw_trade_data(
        joinpath(trade_path, trade_data["imports"]["path"]),
        "imports";
        value_col = Symbol(trade_data["imports"]["sheet"]),
        state_fips = maps[:state_map],
        usatrade_map = maps[:trade_map]
    )


    ### ag
    ag_flow = WiNDCRegional.load_usda_agricultural_flow(
        joinpath(trade_path, trade_data["ag_time_series"]["path"]),
        trade_data["ag_time_series"]["sheet"],
        trade_data["ag_time_series"]["range"];
        agriculture_code = Symbol(trade_data["metadata"]["agriculture_code"]),
        replacement_data = trade_data["ag_time_series"]["replacement"]
    )


    out[:trade_shares] = WiNDCRegional.load_trade_shares(
        exports,
        imports, 
        ag_flow
    )

    # FAF
    faf = data_info["freight_analysis_framework"]
    faf_path = joinpath(data_directory, faf["metadata"]["base_directory"])

    adjusted_demand = Dict{Symbol, Float64}(Symbol(k) => v for (k,v) in faf["metadata"]["adjusted_demand"])

    out[:rpc] = load_regional_purchase_coefficients(
        summary,
        joinpath(faf_path, faf["state"]["path"]),
        joinpath(faf_path, faf["reprocessed_state"]["path"]);
        adjusted_demand = adjusted_demand,
        state_fips = maps[:state_map],
        faf_map = maps[:faf_map],
        cols_to_keep = Symbol.(faf["metadata"]["columns"]),
        regex_cols_to_keep = Regex(faf["metadata"]["column_regex"]),
        max_year = faf["metadata"]["max_year"],
    ) 

    return out

end


"""
    create_state_table(
        summary::National,
        regional_info::Dict,
    )

Disaggregate the US WiNDCNational summary-level data into a state-level table, 
using raw data files located in `data_directory`.



"""
function create_state_table(
        summary::National,
        regional_info::Dict,
    )

    metadata = regional_info["metadata"]
    data_directory = metadata["data_directory"]

    maps = WiNDCRegional.load_map_data(metadata["maps"])

    raw_data = WiNDCRegional.load_raw_data(
        summary,
        regional_info["data"],
        data_directory,
        maps
    )



    state_table = initialize_table(summary, maps)
    state_table = WiNDCRegional.disaggregate_intermediate(state_table, summary, raw_data)
    state_table = WiNDCRegional.disaggregate_labor_capital(state_table, summary, raw_data)
    state_table = WiNDCRegional.disaggregate_output_tax(state_table, summary, raw_data)


    state_table = WiNDCRegional.disaggregate_investment_final_demand(state_table, summary, raw_data)
    state_table = WiNDCRegional.disaggregate_personal_consumption(state_table, summary, raw_data)
    state_table = WiNDCRegional.disaggregate_household_supply(state_table, summary, raw_data)
    state_table = WiNDCRegional.disaggregate_government_final_demand(state_table, summary, raw_data)


    state_table = WiNDCRegional.disaggregate_foreign_exports(state_table, summary, raw_data)
    state_table = WiNDCRegional.create_reexports(state_table, summary, raw_data)
    state_table = WiNDCRegional.disaggregate_foreign_imports( state_table, summary, raw_data)
    state_table = WiNDCRegional.disaggregate_margin_demand(state_table, summary, raw_data)
    state_table = WiNDCRegional.disaggregate_duty(state_table, summary, raw_data)
    state_table = WiNDCRegional.disaggregate_tax(state_table, summary, raw_data)


    state_table = WiNDCRegional.create_regional_demand(state_table, summary, raw_data)
    state_table = WiNDCRegional.create_regional_margin_supply(state_table, summary, raw_data)

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

Raw data used:
    - `gdp`

New parameters:
    - `Intermediate_Demand` with element `intermediate_demand`
    - `Intermediate_Supply` with element `intermediate_supply`

New Sets:

"""
function disaggregate_intermediate(
        state_table::State,    
        summary::National,
        raw_data::Dict;
    )

    gdp = raw_data[:gdp]

    state_intermediate = disaggregate_by_shares(
        summary,
        gdp,
        [:Intermediate_Demand, :Intermediate_Supply];
        domain = :sector
        )

    df = vcat(table(state_table),state_intermediate)

    S = sets(state_table) |>
        x -> vcat(x,
            DataFrame([
                (name = :Intermediate_Demand, description = "Intermediate Demand", domain = :parameter),
                (name = :Intermediate_Supply, description = "Intermediate Supply", domain = :parameter),
            ])
        )

    E = elements(state_table) |>
        x -> vcat(x,
            DataFrame([
                (set = :Intermediate_Demand, name = :intermediate_demand, description = "Intermediate Demand"),
                (set = :Intermediate_Supply, name = :intermediate_supply, description = "Intermediate Supply"),
                (set = :Use, name = :intermediate_demand, description = "Intermediate Demand"),
                (set = :Supply, name = :intermediate_supply, description = "Intermediate Supply"),
            ])
        )


    return State(df, S, E)
end

"""
    disaggregate_labor_capital(
            state_table::State,    
            summary::National,
            raw_data::Dict;
        )

Disaggregate the labor and capital demand from the national summary into state-level
using BEA GSP data.

Raw data used:
    - `gdp`
    - `labor`
    - `capital`
    - `tax`
    - `subsidy`

New parameters:
    - `Labor_Demand` with element `labor_demand`
    - `Capital_Demand` with element `capital_demand`
"""
function disaggregate_labor_capital(
        state_table::State,    
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

    labor_share = labor_shares(summary, raw_data)

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

    df = vcat(table(state_table),labor_capital_state)

    S = sets(state_table) |>
        x -> vcat(x,
            DataFrame([
                (name = :Labor_Demand, description = "Labor Demand", domain = :parameter),
                (name = :Capital_Demand, description = "Capital Demand", domain = :parameter),
            ])
        )
    E = elements(state_table) |>
        x -> vcat(x,
            DataFrame([
                (set = :Labor_Demand, name = :labor_demand, description = "Labor Demand"),
                (set = :Capital_Demand, name = :capital_demand, description = "Capital Demand"),
                (set = :Use, name = :labor_demand, description = "Labor Demand"),
                (set = :Use, name = :capital_demand, description = "Capital Demand"),
                (set = :Value_Added, name = :labor_demand, description = "Labor Demand"),
                (set = :Value_Added, name = :capital_demand, description = "Capital Demand"),
            ])
        )

    return State(df, S, E)
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

New parameters:
    - `Output_Tax` with element `output_tax`
"""
function disaggregate_output_tax(
        state_table::State,    
        summary::National,
        raw_data::Dict;
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


    df = vcat(table(state_table),state_output_tax)

    S = sets(state_table) |>
        x -> vcat(x,
            DataFrame([
                (name = :Output_Tax, description = "Output Tax", domain = :parameter),
            ])
        )

    E = elements(state_table) |>
        x -> vcat(x,
            DataFrame([
                (set = :Output_Tax, name = :output_tax, description = "Output Tax"),
                (set = :Use, name = :output_tax, description = "Output Tax"),
            ])
        )

    return State(df, S, E)
end




"""
    disaggregate_tax(
        state_table::State,    
        summary::National,
        raw_data::Dict;
    )

Disaggregate the tax from the national summary into state-level using BEA
GSP data.

Note: This will be inclusive of subsidies.

Dependent on:
    - [`WiNDCRegional.absorption`](@ref)
    - Summary absorption tax rate

"""
function disaggregate_tax(
        state_table::State,    
        summary::National,
        raw_data::Dict;
    )


    col_label = elements(summary, :Tax; base = true)[1,1]

    state_tax = outerjoin(
            WiNDCRegional.absorption(state_table),
            absorption_tax_rate(summary, output = :tax_rate) |> x -> select(x, Not(:parameter)),
            on = [:year, :row],
        ) |>
        x -> transform(x,
            [:value, :tax_rate] => ByRow((v,tr) -> -v*tr) => :value,
            :parameter => ByRow(y -> (col_label, :tax)) => [:col, :parameter]
        ) |>
        x -> select(x, :row, :col, :region, :year, :parameter, :value) |>
        x -> subset(x,
            :value => ByRow(y -> abs(y) > 1e-6)
        )
    df = vcat(table(state_table),state_tax)

    S = sets(state_table) |>
        x -> vcat(x,
            DataFrame([
                (name = :Tax, description = "Tax", domain = :parameter),
            ])
        )

    E = elements(state_table) |>
        x -> vcat(x,
            DataFrame([
                (set = :Tax, name = :tax, description = "Tax"),
                (set = :Supply, name = :tax, description = "Tax"),
            ])
        )

    return State(df, S, E)
end


"""
    disaggregate_investement_final_demand(
        state_table::State,    
        summary::National,
        raw_data::Dict;
    )

Disaggregate the investment final demand from the national summary into state-level
using BEA GSP data. Also aggregates all investment final demand to a single 
element `invest`, updates the elements table accordingly.

Raw data used:
    - `gdp`

New parameter:
    - `Investment_Final_Demand` with element `investment_final_demand`

New Sets:
    - `investment_final_demand` with element `invest` in domain `col`
"""
function disaggregate_investment_final_demand(
        state_table::State,    
        summary::National,
        raw_data::Dict;
    )

    gdp = raw_data[:gdp]

    state_investment = disaggregate_by_shares(
        summary,
        gdp,
        [:Investment_Final_Demand];
        domain = :commodity
    ) |>
    x -> groupby(x, [:row, :year, :parameter, :region]) |>
    x -> combine(x, :value => sum => :value) |>
    x -> transform(x, :row => ByRow(y -> :invest) => :col)


    df = vcat(table(state_table),state_investment)

    S = sets(state_table) |>
        x -> vcat(x,
            DataFrame([
                (name = :Investment_Final_Demand, description = "Investment Final Demand", domain = :parameter),
                (name = :investment_final_demand, description = "Investment Final Demand", domain = :col),
            ])
        )

    E = elements(state_table) |>
        x -> vcat(x,
            DataFrame([
                (set = :investment_final_demand, name = :invest, description = "Investment Final Demand"),
                (set = :Investment_Final_Demand, name = :investment_final_demand, description = "Investment Final Demand"),
                (set = :Use, name = :investment_final_demand, description = "Investment Final Demand"),
                (set = :Other_Final_Demand, name = :investment_final_demand, description = "Investment Final Demand"),
                (set = :Final_Demand, name = :investment_final_demand, description = "Investment Final Demand"),
            ])
        )

    return State(df, S, E)
end

"""
    disaggregate_personal_consumption(
        state_table::State,    
        summary::National,
        raw_data::Dict;
    )

Disaggregate the personal consumption expenditure from the national summary into
state-level using BEA PCE data.

Raw data used:
    - `pce`

New Parameter
    - `Personal_Consumption` with element `personal_consumption`
"""
function disaggregate_personal_consumption(
        state_table::State,    
        summary::National,
        raw_data::Dict;
    )

    pce = raw_data[:pce]

    state_pce = disaggregate_by_shares(
        summary,
        pce,
        [:Personal_Consumption];
        domain = :commodity
    )

    df = vcat(table(state_table),state_pce)

    S = sets(state_table) |>
        x -> vcat(x,
            DataFrame([
                (name = :Personal_Consumption, description = "Personal Consumption", domain = :parameter),
            ])
        )

    E = elements(state_table) |>
        x -> vcat(x,
            DataFrame([
                (set = :Personal_Consumption, name = :personal_consumption, description = "Personal Consumption"),
                (set = :Use, name = :personal_consumption, description = "Personal Consumption"),
                (set = :Final_Demand, name = :personal_consumption, description = "Personal Consumption"),
                (set = :Other_Final_Demand, name = :personal_consumption, description = "Personal Consumption"),
            ])
        )

    return State(df, S, E)
end

"""
    disaggregate_household_supply(
        state_table::State,    
        summary::National,
        raw_data::Dict;
    )

Disaggregate the household supply from the national summary into state-level using
BEA PCE data.

Raw data used:
    - `pce`

New Parameter
    - `Household_Supply` with element `household_supply`
"""
function disaggregate_household_supply(
        state_table::State,    
        summary::National,
        raw_data::Dict;
    )

    pce = raw_data[:pce]

    state_household_supply = disaggregate_by_shares(
        summary,
        pce,
        :Household_Supply;
        domain = :commodity
    )

    df = vcat(table(state_table), state_household_supply)


    S = sets(state_table) |>
        x -> vcat(x,
            DataFrame([
                (name = :Household_Supply, description = "Household Supply", domain = :parameter),
            ])
        )

    E = elements(state_table) |>
        x -> vcat(x,
            DataFrame([
                (set = :Household_Supply, name = :household_supply, description = "Household Supply"),
                (set = :Supply, name = :household_supply, description = "Household Supply"),
            ])
        )

    return State(df, S, E)
end

"""
    disaggregate_government_final_demand(
        state_table::State,    
        summary::National,
        raw_data::Dict;
    )

Disaggregate the government final demand from the national summary into state-level
using Census SGF data. Also aggregates all government final demand to a single 
element `govern`, updates the elements table accordingly.

Raw data used:
    - `sgf`

New parameter:
    - `Government_Final_Demand` with element `government_final_demand`

New Sets:
    - `government_final_demand` with element `govern` in domain `col`
"""
function disaggregate_government_final_demand(
        state_table::State,    
        summary::National,
        raw_data::Dict;
    )

    census_data = raw_data[:sgf] |>
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

    S = sets(state_table) |>
        x -> vcat(x,
            DataFrame([
                (name = :Government_Final_Demand, description = "Government Final Demand", domain = :parameter),
                (name = :government_final_demand, description = "Government Final Demand", domain = :col),
            ])
        )

    E = elements(state_table) |>
        x -> vcat(x,
            DataFrame([
                (set = :government_final_demand, name = :govern, description = "Government Final Demand"),
                (set = :Government_Final_Demand, name = :government_final_demand, description = "Government Final Demand"),
                (set = :Use, name = :government_final_demand, description = "Government Final Demand"),
                (set = :Other_Final_Demand, name = :government_final_demand, description = "Government Final Demand"),
                (set = :Final_Demand, name = :government_final_demand, description = "Government Final Demand"),
            ])
        )

    return State(df, S, E)
end




"""
    disaggregate_foreign_exports(
            state_table::State,    
            summary::National,
            raw_data::Dict;
        )

Disaggregate the foreign exports from the national summary into state-level using
BEA GSP data and trade shares.

Raw data used:
    - `gdp`
    - `trade_shares`

New Parameter
    - `Export` with element `export`
"""
function disaggregate_foreign_exports(
        state_table::State,    
        summary::National,
        raw_data::Dict;
    )

    gdp = raw_data[:gdp]
    trade_shares = raw_data[:trade_shares]

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

    df = vcat(table(state_table), state_exports)

    S = sets(state_table) |>
        x -> vcat(x,
            DataFrame([
                (name = :Export, description = "Exports", domain = :parameter),
            ])
        )

    E = elements(state_table) |>
        x -> vcat(x,
            DataFrame([
                (set = :Export, name = :export, description = "Exports"),
                (set = :Use, name = :export, description = "Exports"),
                (set = :Final_Demand, name = :export, description = "Exports"),
            ])
        )

    return State(df, S, E)
end

"""
    create_reexports(
        state_table::State,
        summary::National,
        raw_data::Dict;
    )

Reexports are defined as the negative portion of the difference between total 
supply and exports.

Dependent on:
    - [`WiNDCRegional.total_supply`](@ref)
    - exports

New Parameter
    - `Reexport` with element `reexport`

New Sets:
    - `reexport` with element `reexport` in domain `col`
"""
function create_reexports(
        state_table::State,
        summary::National,
        raw_data::Dict;
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
                (name = :reexport, description = "Reexports", domain = :row),
                (name = :Reexport, description = "Reexports", domain = :parameter)
            ])
        )
    E = elements(state_table) |>
        x -> vcat(x,
            DataFrame([
                (set = :Reexport, name = :reexport, description = "Reexports parameter"),
                (set = :reexport, name = :reexport, description = "Reexports parameter"),
                (set = :Use, name = :reexport, description = "Reexports parameter"),
            ])
        )

    return State(df, S, E)
end

"""
    disaggregate_foreign_imports(
        state_table::State,    
        summary::National,
        raw_data::Dict;
    )

Disaggregate the foreign imports from the national summary into state-level using
[`absorption`](@ref).

Dependent on:
    - [`absorption`](@ref)

New Parameter:
    - `Import` with element `import`
"""
function disaggregate_foreign_imports(
        state_table::State,    
        summary::National,
        raw_data::Dict;
    )

    state_imports = disaggregate_by_shares(
        summary,
        absorption(state_table) |> 
            x -> select(x, :row => :naics, :parameter => :name, :year, :region => :state, :value),
        :Import;
        domain = :commodity
        )

    df = vcat(table(state_table), state_imports)

    S = sets(state_table) |>
        x -> vcat(x,
            DataFrame([
                (name = :Import, description = "Imports", domain = :parameter),
            ])
        )

    E = elements(state_table) |>
        x -> vcat(x,
            DataFrame([
                (set = :Import, name = :import, description = "Imports"),
                (set = :Supply, name = :import, description = "Imports"),
            ])
        )

    return State(df, S, E)
end

"""
    disaggregate_margin_demand(
        state_table::State,    
        summary::National,
        raw_data::Dict;
    )

Disaggregate the margin demand from the national summary into state-level using
[`absorption`](@ref).

Dependent on:
    - [`absorption`](@ref)
"""
function disaggregate_margin_demand(
        state_table::State,    
        summary::National,
        raw_data::Dict;
    )

    state_imports = disaggregate_by_shares(
        summary,
        absorption(state_table) |> 
            x -> select(x, :row => :naics, :parameter => :name, :year, :region => :state, :value),
        :Margin_Demand;
        domain = :commodity
        )

    df = vcat(table(state_table), state_imports)

    S = sets(state_table) |>
        x -> vcat(x,
            DataFrame([
                (name = :Margin_Demand, description = "Margin Demand", domain = :parameter),
            ])
        )

    E = elements(state_table) |>
        x -> vcat(x,
            DataFrame([
                (set = :Margin_Demand, name = :margin_demand, description = "Margin Demand"),
                (set = :Supply, name = :margin_demand, description = "Margin Demand"),
            ])
        )


    return State(df, S, E)
end

"""
    disaggregate_duty(
        state_table::State,    
        summary::National,
        raw_data::Dict;
    )

Disaggregate the duty from the national summary into state-level using national
level duty rates and total imports by state.

Dependent on:
    - imports

New Parameter:
    - `Duty` with element `duty`
"""
function disaggregate_duty(
        state_table::State,    
        summary::National,
        raw_data::Dict;
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

    S = sets(state_table) |>
        x -> vcat(x,
            DataFrame([
                (name = :Duty, description = "Duty", domain = :parameter),
            ])
        )

    E = elements(state_table) |>
        x -> vcat(x,
            DataFrame([
                (set = :Duty, name = duty_code, description = "Duty"),
                (set = :Supply, name = duty_code, description = "Duty"),
            ])
        )

    return State(df, S, E)
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
            summary::National,
            raw_data::Dict;
        )

Create regional demand (local + national) based on regional purchase coefficients.


Raw data used:
    - `rpc`

Dependent on:
    - [`WiNDCRegional.absorption`](@ref)
    - [`WiNDCRegional.total_supply`](@ref)

New Parameters:
    - `local_demand` with element `local_demand`
    - `national_demand` with element `national_demand`

New Sets:
    - `local_demand` with element `local_demand` in domain `col`
    - `national_demand` with element `national_demand` in domain `col`
"""
function create_regional_demand(
        state_table::State,
        summary::National,
        raw_data::Dict;
    )

    rpc = raw_data[:rpc]

    domestic_demand_absorption = vcat(
            WiNDCRegional.absorption(state_table),
            table(state_table, :Tax, :Reexport, :Import, :Duty, :Margin_Demand),
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
                (set = :supply, name = :local_demand, description = "Local Demand element"),
                (set = :supply, name = :national_demand, description = "National Demand element"),
            ])
        )

    return State(df, S, E)

end

"""
    create_regional_margin_supply(
        state_table::State,
        summary::National,
        raw_data::Dict;
    )

Raw data used:
    - `rpc`

New Parameters:
    - `Local_Margin_Supply` with element `local_margin_supply`
    - `National_Margin_Supply` with element `region_margin_supply`
    - `Margin_Supply` with elements `local_margin_supply` and `region_margin_supply`

    
"""
function create_regional_margin_supply(
        state_table::State,
        summary::National,
        raw_data::Dict;
    )

    rpc = raw_data[:rpc]
    
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
                (name = :Margin_Supply, description = "Margin Supply", domain = :parameter),
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