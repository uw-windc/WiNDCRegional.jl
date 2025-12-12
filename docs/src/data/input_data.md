# Raw Data

The following functions load the raw input data required for the disaggregation process. These functions read data files from specified directories and return the data in a structured format suitable for further processing. Download the raw data files from [the following link](https://windc.wisc.edu/download?filename=b1b69504efef7fc44d6c18c7dd4dc4e5) and extract them to a local directory on your machine.

The file paths are specified in the `regional.yaml` configuration file, allowing users to customize the data sources as needed.


- [`WiNDCRegional.load_state_gdp`](@ref)
- [`WiNDCRegional.load_pce_data`](@ref)
- [`WiNDCRegional.load_state_finances`](@ref)
- [`WiNDCRegional.load_faf_data`](@ref)
- [`WiNDCRegional.load_usa_raw_trade_data`](@ref)
- [`WiNDCRegional.load_usda_agricultural_flow`](@ref)
