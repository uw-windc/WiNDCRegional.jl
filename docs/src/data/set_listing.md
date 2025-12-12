# Set and Parameter Listing

A WiNDC Regional table is a DataFrame with columns:

- `row`
- `col`
- `region`
- `year`
- `parameter`
- `value`

The `value` column contains the data values, while the other columns define the dimensions of the data. The `row`, `col`, `region`, and `year` have values given by the sets defined in the model and the `parameter` column indicates the type of data in that row. Each column only contains values from the sets in that domain.

We provide a listing of the sets and parameters used in the WiNDC Regional model. This information can also be accessed using:

```julia
sets(state_table)
```

You can further access the elements using:

```julia
elements(state_table)
elements(state_table, :set_name)
```

Specific sets/parameters can be extracted from the data table using:

```julia
table(state_table, :set_name)
```

You can specify multiple sets and specific values:

```julia
table(state_table, :set1, :set2 => set_element, :year => 2024)
```


## Sets

| Set Name | Description | Domain | 
|---|---|---|
| duty | Duty | col |
| export | Exports | col |
| government_final_demand | Government Final Demand | col |
| import | Imports | col |
| investment_final_demand | Investment Final Demand | col |
| local_demand | Local Demand | col |
| margin | Margin sectors | col |
| national_demand | National Demand | col |
| personal_consumption | Personal consumption expenditures | col |
| reexport | Reexports | col |
| sector | Sectors | col |
| tax | Tax | col |
| trade | Trade | col |
| transport | Transport | col |
| state | States | region |
| capital_demand | Gross operating surplus | row |
| commodity | Commodities | row |
| labor_demand | Compensation of employees | row |
| output_tax | Output tax | row |
| year |  | year |


## Parameters

| Parameter Name | Description | Domain | 
|---|---|---|
| Capital_Demand | Capital Demand | parameter |
| Duty | Duty | parameter |
| Export | Exports | parameter |
| Final_Demand | Final demand | parameter |
| Government_Final_Demand | Government Final Demand | parameter |
| Household_Supply | Household Supply | parameter |
| Import | Imports | parameter |
| Intermediate_Demand | Intermediate Demand | parameter |
| Intermediate_Supply | Intermediate Supply | parameter |
| Investment_Final_Demand | Investment Final Demand | parameter |
| Labor_Demand | Labor Demand | parameter |
| Local_Demand | Local Demand | parameter |
| Local_Margin_Supply | Local Margin Supply | parameter |
| Margin_Demand | Margin Demand | parameter |
| Margin_Supply | Margin Supply | parameter |
| National_Demand | National Demand | parameter |
| National_Margin_Supply | National Margin Supply | parameter |
| Other_Final_Demand | Non-export components of final demand | parameter |
| Output_Tax | Output Tax | parameter |
| Personal_Consumption | Personal Consumption | parameter |
| Reexport | Reexports | parameter |
| Supply | Supply (or output) sections of the IO table | parameter |
| Tax | Tax | parameter |
| Use | Use (or input) sections of the IO table | parameter |
| Value_Added | Value added | parameter |