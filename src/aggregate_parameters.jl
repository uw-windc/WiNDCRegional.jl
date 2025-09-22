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
        x -> select(x, [:row, :col, :year, :parameter, output])
        
    if minimal
        X |>
            x -> select!(x, [:col, :year, :parameter, output])
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
        x -> groupby(x, [:row, :year]) |>
        x -> combine(x, column => sum => output) |>
        x -> transform!(x, 
            :row => ByRow(_ -> (:mc, parameter)) => [:col, :parameter]
        ) |>
        x -> select(x, [:row, :col, :year, :parameter, output])
        
        
    if minimal
        X |>
            x -> select!(x, [:row, :year, :parameter, output])
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