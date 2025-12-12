# Regional Model

The model is constructed using the [`WiNDCRegional.regional_model`](@ref) function, which takes in the disaggregated state-level data table and an optional year parameter. The model is built to reflect the economic activities of individual states or regions based on the disaggregated data.

```julia
M = WiNDCRegional.regional_model(state_table; year = 2024);
```