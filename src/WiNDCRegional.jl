module WiNDCRegional

    using DataFrames, CSV, WiNDCContainer, WiNDCNational

    include("structs.jl")

    export State


    include("aggregate_parameters.jl")

    export zero_profit, market_clearance, margin_balance

    include("united_states/common_files.jl")

    export load_state_fips, load_industry_codes, disaggregate_by_shares, load_pce_map, load_sgf_map,
        load_usatrade_map
    
    include("united_states/bea_gdp.jl")

    export add_good, load_state_gdp, load_industry_codes


    include("united_states/pce.jl")

    export load_pce_data

    include("united_states/state_finances.jl")

    export load_state_finances

    include("united_states/freight_analysis_framework.jl")

    export load_faf_base, load_faf_data, load_regional_purchase_coefficients

end
