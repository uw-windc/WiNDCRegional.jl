# Data Maps

The input data needs to be regularized to contain consistent codes for states, sectors, and commodities. The following functions load mapping tables to perform these regularizations. These files are located [here](https://github.com/uw-windc/WiNDCRegional.jl/tree/main/src/united_states/data). 

It should be possible to point these functions to alternative files if the user wishes to customize the mappings. However, this hasn't yet been tested.


- [`WiNDCRegional.load_state_fips`](@ref)
- [`WiNDCRegional.load_industry_codes`](@ref)
- [`WiNDCRegional.load_pce_map`](@ref)
- [`WiNDCRegional.load_faf_map`](@ref)
- [`WiNDCRegional.load_usatrade_map`](@ref)
- [`WiNDCRegional.load_sgf_map`](@ref)
- [`WiNDCRegional.load_sgf_states`](@ref)