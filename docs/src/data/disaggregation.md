# Disaggregation

The function responsible for performing the disaggregation of national-level WiNDC results into regional-level data is [`WiNDCRegional.create_state_table`](@ref). This function takes in the calibrated national summary data along with regional configuration information from a YAML file and produces a state-level data table suitable for use in regional modeling.

