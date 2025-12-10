function regional_model(state_table::State;  year = 2024)
    state_year = State(
        table(state_table, :year => year),
        sets(state_table),
        elements(state_table),
    )

    states = elements(state_table, :state) |> x -> x[!, :name]
    sectors = elements(state_table, :sector) |> x -> x[!, :name]
    commodities = elements(state_table, :commodity) |> x -> x[!, :name]
    margins = elements(state_table, :margin) |> x -> x[!, :name]
    labor_demand = elements(state_table, :labor_demand) |> x -> x[!, :name]
    capital_demand = elements(state_table, :capital_demand) |> x -> x[!, :name]
    imports = elements(state_table, :import) |> x -> x[!, :name]
    personal_consumption = elements(state_table, :personal_consumption) |> x -> x[!, :name]

    output_tax_rate = WiNDCRegional.output_tax_rate(state_year)
    tax_rate = WiNDCRegional.tax_rate(state_year)
    duty_rate = WiNDCRegional.duty_rate(state_year)
    
    M = MPSGEModel()

    vcat(
        output_tax_rate,
        tax_rate,
        duty_rate
        ) |>
        x -> DefaultDict(0, 
            Dict((row[:row], row[:col], row[:region], row[:parameter]) => row[:value] for row in eachrow(x))
        ) |> Q->
    @parameters(M, begin
        OTR[r=states, s=sectors], Q[:otr, s, r, :output_tax_rate], (description = "Output tax rate",)
        TR[r=states, g=commodities], Q[g, :tr, r, :tax_rate], (description = "Tax rate",)
        DR[r=states, g=commodities], Q[g, :dr, r, :duty_rate], (description = "Duty rate",)
    end)
        
    @sectors(M, begin
        Y[r=states, s=sectors], (description = "Production",)
        X[r=states, g=commodities], (description = "Disposition",)
        A[r=states, g=commodities], (description = "Absorption",)
        C[r=states], (description = "Aggregate final demand",)
        MS[r=states, m=margins], (description = "Margin supply",)
    end)

    @commodities(M, begin
        PA[r=states, g=commodities], (description = "Regional market (input)",)
        PY[r=states, g=commodities], (description = "Regional market (output)",)
        PD[r=states, g=commodities], (description = "Local market price",)
        PN[g=commodities], (description = "National market",)
        PL[r=states], (description = "Wage rate",)
        PK[r=states, s=sectors], (description = "Rental rate of capital",)
        PM[r=states, m=margins], (description = "Margin price",)
        PC[r=states], (description = "Consumer price index",)
        PFX, (description = "Foreign exchange",)
    end)

    @consumer(M, RA[r=states], description = "Representative agent",)

    sectoral_output(state_year; output = :DefaultDict) |> Q->
    @production(M, Y[r=states, s=sectors], [t=0, s=0, va=>s=1], begin
        @output(PY[r,g=commodities], Q[g, s, r, :intermediate_supply],                     t, taxes = [Tax(RA[r], M[:OTR][r,s])], reference_price = 1-Q[:otr, s, r, :output_tax_rate])
        @input(PA[r, g=commodities], Q[g, s, r, :intermediate_demand],                     s)
        @input(PL[r],            sum(Q[l, s, r, :labor_demand] for l in labor_demand),     va)
        @input(PK[r, s],         sum(Q[k, s, r, :capital_demand] for k in capital_demand), va)
    end)

        
    disposition_data(state_year; output = :DefaultDict) |> Q->
    @production(M, X[r=states, g=commodities], [s=0, t=4], begin
        @output(PFX,      Q[g, r, :netport],                t)
        @output(PN[g],    Q[g, r, :region_national_supply], t)
        @output(PD[r, g], Q[g, r, :region_local_supply],    t)
        @input(PY[r, g],  Q[g, r, :total_supply],           s)
    end)


    armington_data(state_year; output = :DefaultDict) |> Q-> 
    @production(M, A[r=states, g=commodities], [t=0, s=0, dm => s = 2, d=>dm=4], begin
        @output(PA[r, g],        Q[g, :abs, r, :absorption],                  t, taxes = [Tax(RA[r], M[:TR][r,g])], reference_price = 1 - Q[g, :tr, r, :tax_rate])
        @output(PFX,             Q[g, :reexport, r, :reexport],               t)
        @input(PN[g],            Q[g, :national_demand, r, :national_demand], d)
        @input(PD[r, g],         Q[g, :local_demand, r, :local_demand],       d)
        @input(PFX,          sum(Q[g, i, r, :import] for i in imports),       dm, taxes = [Tax(RA[r], M[:DR][r,g])], reference_price = 1 + Q[g, :dr, r, :duty_rate])
        @input(PM[r, m=margins], Q[g, m, r, :margin_demand],                  s)
    end)

    margin_supply_demand(state_year; output = :DefaultDict) |> Q->
    @production(M, MS[r=states, m=margins], [t=0, s=0], begin
        @output(PM[r, m],        sum(Q[g, m, r, :margin_demand] for g in commodities), t)
        @input(PN[g=commodities],    Q[g, m, r, :national_margin_supply],              s)
        @input(PD[r, g=commodities], Q[g, m, r, :local_margin_supply],                 s)
    end)

    consumption_data(state_year; output = :DefaultDict) |> Q->
    @production(M, C[r=states], [t=0, s=1], begin
        @output(PC[r],           sum(Q[g, r, :personal_consumption] for g in commodities), t)
        @input(PA[r, g=commodities], Q[g, r, :personal_consumption],                       s)
    end)

    representative_agent_data(state_year; output = :DefaultDict) |> Q -> 
    @demand(M, RA[r=states], begin
        @final_demand(PC[r],              sum(Q[g, s, r, :personal_consumption] for g in commodities, s in personal_consumption))
        @endowment(PY[r, g=commodities],  sum(Q[g, s, r, :household_supply] for s in personal_consumption))
        @endowment(PFX,                       Q[:bopdef, :bopdef, r, :bopdef] + Q[:hhadj, :hhadj, r, :house_adjustment])
        @endowment(PA[r, g=commodities],     -Q[g, :govern, r, :government_final_demand] -Q[g, :invest, r, :investment_final_demand])
        @endowment(PL[r],                 sum(Q[g, s, r, :labor_demand] for g in labor_demand, s in sectors))
        @endowment(PK[r, s=sectors],      sum(Q[g, s, r, :capital_demand] for g in capital_demand))
    end)

    return M

end

"""
    sectoral_output(data::T, output = :DataFrame) where T<:AbstractRegionalTable

Extracts sectoral output-related parameters from the regional data table.

```julia

vcat(
    table(data, 
        :Intermediate_Supply, 
        :Intermediate_Demand, 
        :Labor_Demand, 
        :Capital_Demand;
        normalize = :Use
        ),
    output_tax_rate(data)
    )
```
"""
function sectoral_output(data::T; output = :DataFrame) where T<:AbstractRegionalTable

    df = vcat(
        table(data, 
            :Intermediate_Supply, 
            :Intermediate_Demand, 
            :Labor_Demand, 
            :Capital_Demand;
            normalize = :Use
            ),
        output_tax_rate(data)
        )

    if output == :DataFrame
        return df
    elseif output == :DefaultDict
        return DefaultDict(0,
            Dict((row[:row], row[:col], row[:region], row[:parameter]) => row[:value] for row in eachrow(df))
        )
    else
        error("Unsupported output type: $output")
    end
    
end



"""
    disposition_data(data::T, output = :DataFrame) where T<:AbstractRegionalTable

Extracts disposition-related parameters from the regional data table.

```julia
    vcat(
        regional_local_supply(data),
        netports(data),
        total_supply(data),
        regional_national_supply(data)
        )
```

## Aggregate Data

- [`WiNDCRegional.regional_local_supply`](@ref)
- [`WiNDCRegional.netports`](@ref)
- [`WiNDCRegional.total_supply`](@ref)
- [`WiNDCRegional.regional_national_supply`](@ref)
"""
function disposition_data(data::T; output = :DataFrame) where T<:AbstractRegionalTable

    df = vcat(
        regional_local_supply(data),
        netports(data),
        total_supply(data),
        regional_national_supply(data)
        )

    if output == :DataFrame
        return df
    elseif output == :DefaultDict
        return DefaultDict(0,
            Dict((row[:row], row[:region], row[:parameter]) => row[:value] for row in eachrow(df))
        )
    else
        error("Unsupported output type: $output")
    end
    
end

"""
    armington_data(data::T, output = :DataFrame) where T<:AbstractRegionalTable

Extracts Armington-related parameters from the regional data table.

```julia
    vcat(
        absorption(data; normalize = true),
        table(data, :Reexport, :National_Demand, :Local_Demand, :Import, :Margin_Demand; normalize = :Use),
        tax_rate(data),
        duty_rate(data)
    )
```

## Aggregate data

- [`WiNDCRegional.absorption`](@ref)
- [`WiNDCRegional.tax_rate`](@ref)
- [`WiNDCRegional.duty_rate`](@ref)
"""
function armington_data(data::T; output = :DataFrame) where T<:AbstractRegionalTable

    df = vcat(
        absorption(data; normalize = true),
        table(data, :Reexport, :National_Demand, :Local_Demand, :Import, :Margin_Demand; normalize = :Use),
        tax_rate(data),
        duty_rate(data)
    ) 

    if output == :DataFrame
        return df
    elseif output == :DefaultDict
        return DefaultDict(0,
            Dict((row[:row], row[:col], row[:region], row[:parameter]) => row[:value] for row in eachrow(df))
        )
    else
        error("Unsupported output type: $output")
    end
    
end

"""
    margin_supply_demand(data::T, output = :DataFrame) where T<:AbstractRegionalTable

Extracts margin supply and demand parameters from the regional data table.

```julia
    table(data, :Margin_Demand, :Margin_Supply; normalize = :Use)
```
"""
function margin_supply_demand(data::T; output = :DataFrame) where T<:AbstractRegionalTable

    df = table(data, :Margin_Demand, :Margin_Supply; normalize = :Use)

    if output == :DataFrame
        return df
    elseif output == :DefaultDict
        return DefaultDict(0,
            Dict((row[:row], row[:col], row[:region], row[:parameter]) => row[:value] for row in eachrow(df))
        )
    else
        error("Unsupported output type: $output")
    end
    
end


"""
    consumption_data(data::T, output = :DataFrame) where T<:AbstractRegionalTable

Extracts consumption-related parameters from the regional data table.

```julia
    table(data, :Personal_Consumption; normalize = :Use)
```
"""
function consumption_data(data::T; output = :DataFrame) where T<:AbstractRegionalTable

    df = table(data, :Personal_Consumption; normalize = :Use)

    if output == :DataFrame
        return df
    elseif output == :DefaultDict
        return DefaultDict(0,
            Dict((row[:row], row[:region], row[:parameter]) => row[:value] for row in eachrow(df))
        )
    else
        error("Unsupported output type: $output")
    end
    
end


function representative_agent_data(data::T; output = :DataFrame) where T<:AbstractRegionalTable

    df = vcat(
        table(data,
            :Personal_Consumption,
            :Household_Supply,
            :Other_Final_Demand,
            :Value_Added;
            normalize = :Use
            ) ,
        household_adjustment(data),
        balance_of_payments(data)
    )

    if output == :DataFrame
        return df
    elseif output == :DefaultDict
        return DefaultDict(0,
            Dict((row[:row], row[:col], row[:region], row[:parameter]) => row[:value] for row in eachrow(df))
        )
    else
        error("Unsupported output type: $output")
    end
    
end