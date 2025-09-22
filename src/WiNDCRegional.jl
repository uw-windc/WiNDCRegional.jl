module WiNDCRegional

    using DataFrames, CSV, WiNDCContainer, WiNDCNational

    include("structs.jl")

    export State


    include("aggregate_parameters.jl")

    export zero_profit, market_clearance, margin_balance

    include("united_states/common_files.jl")

    export load_state_fips, load_industry_codes
    
    include("united_states/bea_gdp.jl")

    export add_good, load_state_gdp, load_industry_codes

end
