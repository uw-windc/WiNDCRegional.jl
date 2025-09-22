module WiNDCRegional

    using DataFrames, CSV, WiNDCContainer, WiNDCNational

    include("structs.jl")

    export State


    include("aggregate_parameters.jl")

    export zero_profit, market_clearance, margin_balance

    include("united_states/common_files.jl")

    export load_state_fips, load_industry_codes, disaggregate_by_shares, load_pce_map
    
    include("united_states/bea_gdp.jl")

    export add_good, load_state_gdp, load_industry_codes


    include("united_states/pce.jl")

    export load_pce_data

    include("united_states/state_finances.jl")

    export load_sgf_map, load_state_finances
end
