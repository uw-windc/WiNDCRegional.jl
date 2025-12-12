"""
    zero_profit(X::AbstractRegionalTable; column = :value, output = :value)

Calculate the zero profit condition. For each sector, the zero profit 
condition is defined as the sum of the following parameters:

- `Intermediate_Demand`
- `Intermediate_Supply`
- `Value_Added`

## Required Arguments

- `data::AbstractRegionalTable`: The national data.

## Keyword Arguments

- `column::Symbol = :value`: The column to be used for the calculation.
- `output::Symbol = :value`: The name of the output column.
- `parameter::Symbol = `:zero_profit`: The name of the parameter column.
- `minimal::Bool = true`: Whether to return a minimal output. If true, only the 
    essential columns are returned: [:row, :year, :parameter, output]. 

## Output

Returns a DataFrame with columns [:row, :col, :year, :parameter, output], where 
`output` is the renamed `column` column. Note that `:col` is filled with `:zp` 
and `:parameter` is filled with `parameter`.
"""
function zero_profit(
        data::AbstractRegionalTable; 
        column::Symbol = :value, 
        output::Symbol = :value,
        parameter::Symbol = :zero_profit,
        minimal::Bool = true
    )
    X = table(data, :sector) |>
        x -> groupby(x, [:col, :region, :year]) |>
        x -> combine(x, column => sum => output) |>
        x -> transform(x,
            :col => ByRow(_ -> (:zp, parameter)) => [:row, :parameter]
        ) |>
        x -> select(x, [:row, :col, :region, :year, :parameter, output])
        
    if minimal
        X |>
            x -> select!(x, [:col, :region, :year, :parameter, output])
    end

    return X
end

"""
    market_clearance(
        data::AbstractRegionalTable; 
        column::Symbol = :value, 
        output::Symbol = :value,
        parameter::Symbol = :market_clearance,
        minimal::Bool = true
    )

Calculate the market clearance condition. For each commodity, the market 
clearance condition is defined as the sum of the following parameters:

- `Intermediate_Demand`
- `Final_Demand`
- `Intermediate_Supply`
- `Household_Supply`
- `Margin_Supply`
- `Margin_Demand`
- `Imports`
- `Tax`
- `Duty`
- `Subsidies`

## Required Arguments

- `data::AbstractRegionalTable`: The national data.

## Keyword Arguments

- `column::Symbol = :value`: The column to be used for the calculation.
- `output::Symbol = :value`: The name of the output column.
- `parameter::Symbol = `:market_clearance`: The name of the parameter column.
- `minimal::Bool = true`: Whether to return a minimal output. If true, only the 
    essential columns are returned: [:row, :year, :parameter, output]. 

## Output

Returns a DataFrame with columns [:row, :col, :year, :parameter, output], where 
`output` is the renamed `column` column. Note that `:col` is filled with `:mc` 
and `:parameter` is filled with `parameter`.
"""
function market_clearance(
        data::AbstractRegionalTable; 
        column::Symbol = :value, 
        output::Symbol = :value,
        parameter::Symbol = :market_clearance,
        minimal::Bool = true
    )

    X = table(data,
        :Intermediate_Demand,
        :Other_Final_Demand,
        :Reexport,
        :Tax,
        :National_Demand,
        :Local_Demand,
        :Import,
        :Duty,
        :Margin_Demand,
    )  |>
        x -> groupby(x, [:row, :region, :year]) |>
        x -> combine(x, column => sum => output) |>
        x -> transform!(x, 
            :row => ByRow(_ -> (:mc, parameter)) => [:col, :parameter]
        ) |>
        x -> select(x, [:row, :col, :region, :year, :parameter, output])
        
        
    if minimal
        X |>
            x -> select!(x, [:row, :region, :year, :parameter, output])
    end
    return X
end

"""
    margin_balance(
        data::AbstractRegionalTable; 
        column::Symbol = :value, 
        output::Symbol = :value,
        parameter::Symbol = :margin_balance,
        minimal::Bool = true
    )

Calculate the margin balance condition. For each margin, the margin 
balance condition is defined as the sum of the following parameters:

- `Margin_Supply`
- `Margin_Demand`

## Required Arguments

- `data::AbstractRegionalTable`: The national data.

## Keyword Arguments

- `column::Symbol = :value`: The column to be used for the calculation.
- `output::Symbol = :value`: The name of the output column.
- `parameter::Symbol = `:margin_balance`: The name of the parameter column.
- `minimal::Bool = true`: Whether to return a minimal output. If true, only the 
    essential columns are returned: [:col, :year, :parameter, output]. 

## Output

Returns a DataFrame with columns [:row, :col, :year, :parameter, output], where 
`output` is the renamed `column` column. Note that `:col` is filled with `:mb` 
and `:parameter` is filled with `parameter`.
"""
function margin_balance(
        data::AbstractRegionalTable; 
        column::Symbol = :value, 
        output::Symbol = :value,
        parameter::Symbol = :margin_balance,
        minimal::Bool = true
    )
    X = table(data, :margin) |>
        x -> groupby(x, [:col, :year]) |>
        x -> combine(x, column => sum => output) |>
        x -> transform(x, 
            :col => ByRow(_ -> (:mb, parameter)) => [:row, :parameter]
        ) |>
        x -> select(x, [:row, :col, :year, :parameter, output])
        
    if minimal
        X |>
            x -> select!(x, [:col, :year, :parameter, output])
    end

    return X
end

"""
    total_supply(data::AbstractRegionalTable; column::Symbol = :value, output::Symbol = :value)

Calculate the total supply for each commodity. The total supply is defined as the sum of:

- `Intermediate_Supply`
- `Household_Supply`
"""
function total_supply(data::AbstractRegionalTable; column::Symbol = :value, output::Symbol = :value)
    return table(data, :Intermediate_Supply, :Household_Supply) |>
        x -> groupby(x, [:row, :region, :year]) |>
        x -> combine(x, column => sum => output) |>
        x -> transform(x, :row => ByRow(y -> (:tot_sup, :total_supply)) => [:col, :parameter])
end

"""
    absorption(
        state_table::AbstractRegionalTable; 
        column::Symbol = :value, 
        output::Symbol = :value
    )

Calculate the absorption for each commodity in each region. The absorption is 
defined as the sum of:

- `Intermediate_Demand`
- `Other_Final_Demand`

Note that absorption is negative.
"""
function absorption(
        state_table::AbstractRegionalTable; 
        column::Symbol = :value, 
        output::Symbol = :value,
        normalize::Bool = false
    )
    return table(state_table, :Intermediate_Demand, :Other_Final_Demand) |>
        x -> groupby(x, [:row, :year, :region]) |>
        x -> combine(x, column => sum => output) |>
        x -> transform(x, 
            :value => ByRow(y -> normalize ? -y : y) => :value,
            :row => ByRow(y -> (:abs, :absorption)) => [:col, :parameter]
        )
end

"""
    output_tax_rate(
            state_table::AbstractRegionalTable; 
            column::Symbol = :value, 
            output::Symbol = :value,
            parameter = :output_tax_rate,
            minimal::Bool = false
        )

Calculate the output tax rate for each commodity in each region. The output tax rate
is defined as the ratio of `Output_Tax` to the sum over commodities of `Intermediate_Supply`.

## Required Arguments

- `state_table::AbstractRegionalTable`: The regional data.

## Keyword Arguments

- `column::Symbol = :value`: The column to be used for the calculation.
- `output::Symbol = :value`: The name of the output column.
- `parameter::Symbol = `:output_tax_rate`: The name of the parameter column.
- `minimal::Bool = false`: Whether to return a minimal output. If true, only the 
    essential columns are returned: [:col, :region, :year, :parameter, output].
"""
function output_tax_rate(
        state_table::AbstractRegionalTable; 
        column::Symbol = :value, 
        output::Symbol = :value,
        parameter = :output_tax_rate,
        minimal::Bool = false
    )

    df = innerjoin(
        table(state_table, :Intermediate_Supply) |> 
            x -> groupby(x, [:col, :region, :year]) |>
            x -> combine(x, column => sum => :is),
        table(state_table, :Output_Tax; column = column, normalize= :Use),
        on = [:col, :region, :year]
        ) |>
        x -> transform(x,
            [:is, column] => ByRow((is, ot) -> ot / is) => output,
            :col => ByRow(y -> (:otr, parameter)) => [:row, :parameter]
        )  |>
        x -> select(x, [:row, :col, :region, :year, :parameter, output])

    if minimal
        df |>
            x -> select!(x, [:col, :region, :year, :parameter, output])
    end

    return df

end

"""
    tax_rate(
        state_table::AbstractRegionalTable; 
        column::Symbol = :value, 
        output::Symbol = :value,
        parameter = :tax_rate,
        minimal::Bool = false
    )

Calculate the tax rate for each commodity in each region. The tax rate
is defined as the ratio of `Tax` to `Absorption`.

## Required Arguments

- `state_table::AbstractRegionalTable`: The regional data.

## Keyword Arguments

- `column::Symbol = :value`: The column to be used for the calculation.
- `output::Symbol = :value`: The name of the output column.
- `parameter::Symbol = `:tax_rate`: The name of the parameter column.
- `minimal::Bool = false`: Whether to return a minimal output. If true, only the 
    essential columns are returned: [:col, :region, :year, :parameter, output].
"""
function tax_rate(
        state_table::AbstractRegionalTable; 
        column::Symbol = :value, 
        output::Symbol = :value,
        parameter = :tax_rate,
        minimal::Bool = false
    )

    df = innerjoin(
        table(state_table, :Tax),
        WiNDCRegional.absorption(state_table; column=column, normalize=true) |> x-> select(x, :row, :year, :region, :value => :absorption),
        on = [:row, :region, :year]
        ) |>
        x -> transform(x,
            [column, :absorption] => ByRow((tx, ab) -> tx / ab) => output,
            :row => ByRow(y -> (:tr, parameter)) => [:col, :parameter]
        ) |>
        x -> select(x, [:row, :col, :region, :year, :parameter, output])

    if minimal
        df |>
            x -> select!(x, [:row, :region, :year, :parameter, output])
    end

    return df

end



"""  
  duty_rate(
        state_table::AbstractRegionalTable; 
        column::Symbol = :value, 
        output::Symbol = :value,
        parameter = :duty_rate,
        minimal::Bool = false
    )
   
    
Calculate the duty rate for each commodity in each region. The duty rate
is defined as the ratio of `Duty` to `Import`.

## Required Arguments

- `state_table::AbstractRegionalTable`: The regional data.

## Keyword Arguments

- `column::Symbol = :value`: The column to be used for the calculation.
- `output::Symbol = :value`: The name of the output column.
- `parameter::Symbol = `:duty_rate`: The name of the parameter column.
- `minimal::Bool = false`: Whether to return a minimal output. If true, only the 
    essential columns are returned: [:col, :region, :year, :parameter, output].
"""
function duty_rate(
        state_table::AbstractRegionalTable; 
        column::Symbol = :value, 
        output::Symbol = :value,
        parameter = :duty_rate,
        minimal::Bool = false
    )

    df = innerjoin(
        table(state_table, :Duty),
        table(state_table, :Import) |> x-> select(x, :row, :year, :region, :value => :import),
        on = [:row, :region, :year]
        ) |>
        x -> transform(x,
            [column, :import] => ByRow((dy, ts) -> dy / ts) => output,
            :row => ByRow(y -> (:dr, parameter)) => [:col, :parameter]
        ) |>
        x -> select(x, [:row, :col, :region, :year, :parameter, output])

    if minimal
        df |>
            x -> select!(x, [:row, :region, :year, :parameter, output])
    end

    return df

end

"""
    regional_local_supply(
        state_year::AbstractRegionalTable;
        column::Symbol = :value,
        output::Symbol = :value,
        parameter::Symbol = :region_local_supply,
        minimal::Bool = false
    )

Calculate the regional local supply for each commodity in each region. The regional local supply
is defined as the sum of `Local_Margin_Supply` and `Local_Demand`.
"""
function regional_local_supply(
        state_year::AbstractRegionalTable;
        column::Symbol = :value,
        output::Symbol = :value,
        parameter::Symbol = :region_local_supply,
        minimal::Bool = false
    )

    df = table(state_year, :Local_Margin_Supply, :Local_Demand; normalize=:Use) |>
        x -> groupby(x, [:row, :region, :year]) |>
        x -> combine(x, column => sum => output) |>
        x -> transform(x,
            :row => ByRow(y -> (:rls, parameter)) => [:col, :parameter]
        ) |>
        x -> select(x, [:row, :col, :region, :year, :parameter, output])

    if minimal
        df |>
            x -> select!(x, [:row, :region, :year, :parameter, output])
    end

    return df
end

"""
    netports(
        state_year::AbstractRegionalTable;
        column::Symbol = :value,
        output::Symbol = :value,
        parameter::Symbol = :netport,
        minimal::Bool = false
    )

Calculate the netports for each commodity in each region. The netport
is defined as the difference between `Export` and `Reexport`.
"""
function netports(
        state_year::AbstractRegionalTable;
        column::Symbol = :value,
        output::Symbol = :value,
        parameter::Symbol = :netport,
        minimal::Bool = false
    )

    df = table(state_year, :Export, :Reexport; normalize = :Export) |>
        x -> groupby(x, [:row, :region, :year]) |>
        x -> combine(x, column => sum => output) |>
        x -> transform(x,
            :row => ByRow(y -> (:netport, parameter)) => [:col, :parameter]
        ) |>
        x -> select(x, [:row, :col, :region, :year, :parameter, output])

    if minimal
        df |>
            x -> select!(x, [:row, :region, :year, :parameter, output])
    end

    return df
end


"""
    regional_national_supply(
        state_year::AbstractRegionalTable;
        column::Symbol = :value,
        output::Symbol = :value,
        parameter::Symbol = :region_national_supply,
        minimal::Bool = false
    )

Calculate the regional national supply for each commodity in each region. The regional national supply
is defined as total supply minus net exports minus regional local supply.

"""
function regional_national_supply(
        state_year::AbstractRegionalTable;
        column::Symbol = :value,
        output::Symbol = :value,
        parameter::Symbol = :region_national_supply,
        minimal::Bool = false
    )

    df = outerjoin(
        total_supply(state_year; column = column, output = :total_supply) |> x -> select(x, :row, :region, :year, :total_supply),
        netports(state_year; column = column, output = :netport) |> x -> select(x, :row, :region, :year, :netport),
        regional_local_supply(state_year; column = column, output = :rls) |> x -> select(x, :row, :region, :year, :rls),
        on = [:row, :region, :year]
        ) |>
    x -> coalesce.(x, 0) |>
    x -> transform(x,
        [:total_supply, :netport, :rls] => ByRow((ts, np, rls) -> ts - np - rls) => output,
        :row => ByRow(y -> (:rns, parameter)) => [:col, :parameter]
    ) |>
    x -> select!(x, [:row, :col, :region, :year, :parameter, output])

    if minimal
        df |>
            x -> select!(x, [:row, :region, :year, :parameter, output])
    end

    return df

end

"""
    balance_of_payments(
        state_year::AbstractRegionalTable;
        column::Symbol = :value,
        output::Symbol = :value,
        parameter::Symbol = :bopdef,
        minimal::Bool = false
    )

Calculate the balance of payments for each commodity in each region. The balance
of payments is defined as the sum of:

- `Export`
- `Import`

## Required Arguments

- `state_year::AbstractRegionalTable`: The regional data.

## Keyword Arguments

- `column::Symbol = :value`: The column to be used for the calculation.
- `output::Symbol = :value`: The name of the output column.
- `parameter::Symbol = :bopdef`: The name of the parameter column.
- `minimal::Bool = false`: Whether to return a minimal output. If true, only the 
    essential columns are returned: [:region, :year, :parameter, output].
"""
function balance_of_payments(
        state_year::AbstractRegionalTable;
        column::Symbol = :value,
        output::Symbol = :value,
        parameter::Symbol = :bopdef,
        minimal::Bool = false
    )

    df = table(state_year, :Export, :Import) |>
        x -> groupby(x, [:region, :year]) |>
        x -> combine(x, column => sum => output) |>
        x -> transform(x,
            :region => ByRow(y -> (:bopdef, :bopdef, parameter)) => [:row, :col, :parameter]
        ) |>
        x -> select(x, [:row, :col, :region, :year, :parameter, output])

    if minimal
        df |>
            x -> select!(x, [:region, :year, :parameter, output])
    end

    return df
end

"""
    household_adjustment(
        state_year::AbstractRegionalTable;
        column::Symbol = :value,
        output::Symbol = :value,
        parameter::Symbol = :house_adjustment,
        minimal::Bool = false
    )

Calculate the household adjustment for each commodity in each region. The household adjustment
is defined as the sum of:

- `Other_Final_Demand`
- [`balance_of_payments`](@ref)
- `Value_Added`
- `Household_Supply`
- `Output_Tax`
- `Tax`
- `Duty`

## Required Arguments

- `state_year::AbstractRegionalTable`: The regional data.

## Keyword Arguments

- `column::Symbol = :value`: The column to be used for the calculation.
- `output::Symbol = :value`: The name of the output column.
- `parameter::Symbol = :house_adjustment`: The name of the parameter column.
- `minimal::Bool = false`: Whether to return a minimal output. If true, only the 
    essential columns are returned: [:row, :region, :year, :parameter, output].
"""
function household_adjustment(
        state_year::AbstractRegionalTable;
        column::Symbol = :value,
        output::Symbol = :value,
        parameter::Symbol = :house_adjustment,
        minimal::Bool = false
    )

    df = vcat(
        table(state_year, 
            :Other_Final_Demand;
            normalize = :Use
            ),
        balance_of_payments(state_year; column = column) |> x -> transform(x, column => ByRow(-) => column),
        table(state_year, 
            :Value_Added,
            :Household_Supply,
            :Output_Tax,
            :Tax,
            :Duty;
            normalize = :Supply
        )
    ) |>
    x -> groupby(x, [:region, :year]) |>
    x -> combine(x, column => sum => output) |>
    x -> transform(x,
        :region => ByRow(y -> (:hhadj, :hhadj, :house_adjustment)) => [:row, :col, :parameter]
    ) |>
    x -> select(x, [:row, :col, :region, :year, :parameter, output])

    if minimal
        df |>
            x -> select!(x, [:row, :region, :year, :parameter, output])
    end

    return df
end

