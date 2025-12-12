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
| government\_final\_demand | Government Final Demand | col |
| import | Imports | col |
| investment\_final\_demand | Investment Final Demand | col |
| local\_demand | Local Demand | col |
| margin | Margin sectors | col |
| national\_demand | National Demand | col |
| personal\_consumption | Personal consumption expenditures | col |
| reexport | Reexports | col |
| sector | Sectors | col |
| tax | Tax | col |
| trade | Trade | col |
| transport | Transport | col |
| state | States | region |
| capital\_demand | Gross operating surplus | row |
| commodity | Commodities | row |
| labor\_demand | Compensation of employees | row |
| output\_tax | Output tax | row |
| year |  | year |


## Parameters

| Parameter Name | Description | Domain | 
|---|---|---|
| Capital\_Demand | Capital Demand | parameter |
| Duty | Duty | parameter |
| Export | Exports | parameter |
| Final\_Demand | Final demand | parameter |
| Government\_Final\_Demand | Government Final Demand | parameter |
| Household\_Supply | Household Supply | parameter |
| Import | Imports | parameter |
| Intermediate\_Demand | Intermediate Demand | parameter |
| Intermediate\_Supply | Intermediate Supply | parameter |
| Investment\_Final\_Demand | Investment Final Demand | parameter |
| Labor\_Demand | Labor Demand | parameter |
| Local\_Demand | Local Demand | parameter |
| Local\_Margin\_Supply | Local Margin Supply | parameter |
| Margin\_Demand | Margin Demand | parameter |
| Margin\_Supply | Margin Supply | parameter |
| National\_Demand | National Demand | parameter |
| National\_Margin\_Supply | National Margin Supply | parameter |
| Other\_Final\_Demand | Non-export components of final demand | parameter |
| Output\_Tax | Output Tax | parameter |
| Personal\_Consumption | Personal Consumption | parameter |
| Reexport | Reexports | parameter |
| Supply | Supply (or output) sections of the IO table | parameter |
| Tax | Tax | parameter |
| Use | Use (or input) sections of the IO table | parameter |
| Value\_Added | Value added | parameter |