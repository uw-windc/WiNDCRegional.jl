abstract type AbstractRegionalTable <: WiNDCtable end

WiNDCContainer.domain(data::AbstractRegionalTable) = [:row, :col, :region, :year]
WiNDCContainer.base_table(data::AbstractRegionalTable) = data.data
WiNDCContainer.sets(data::AbstractRegionalTable) = data.sets
WiNDCContainer.elements(data::AbstractRegionalTable) = data.elements

"""
    State

The primary container for state data tables. There are three fields, all dataframes:
- `data`: The main data table.
- `sets`: The sets table, describing the different sets used in the model.
- `elements`: The elements table, describing the different elements in the model.
"""
struct State <: AbstractRegionalTable
    data::DataFrame
    sets::DataFrame
    elements::DataFrame
end
