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
    X = table(data, :commodity) |>
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
        output::Symbol = :value
    )
    return table(state_table, :Intermediate_Demand, :Other_Final_Demand) |>
        x -> groupby(x, [:row, :year, :region]) |>
        x -> combine(x, column => sum => output) |>
        x -> transform(x, :row => ByRow(y -> (:abs, :absorption)) => [:col, :parameter])
end

"""
    balance_of_payments(
        state_table::AbstractRegionalTable; 
        column::Symbol = :value, 
        output::Symbol = :value
    )

Calculate the balance of payments for each commodity in each region. The balance
of payments is defined as the sum of:

- `Imports`
- `Exports`
"""
function balance_of_payments(
        state_table::AbstractRegionalTable; 
        column::Symbol = :value, 
        output::Symbol = :value
    )
    return table(state_table, :Imports, :Exports) |>
        x -> groupby(x, [:row, :year, :region]) |>
        x -> combine(x, column => sum => output) |>
        x -> transform(x, :row => ByRow(y -> (:bop, :balance_of_payments)) => [:col, :parameter])
end