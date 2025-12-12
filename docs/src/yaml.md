# YAML Configuration

The disaggregation process is configured through a YAML file that specifies the necessary metadata and data paths. This file is included in the raw data download and should not need to be modified by the user.

However, users may wish to customize certain aspects of the configuration. The YAML file contains two main sections: `metadata` and `data`.

!!! note "Early Build"
    The YAML configuration structure may be subject to change as the package is further developed. Users are encouraged to check for updates in future releases.


This data is loaded using the [`load_regional_yaml`](@ref) function, which reads the YAML file and returns a dictionary containing the configuration information.

## Metadata Section

The `metadata` section contains four keys:

- `title`: A string representing the title of the disaggregation project.
- `description`: A string providing a brief description of the disaggregation project.
- `data_directory`: A string specifying the path to the directory where the raw data files are stored. This directory should also contain `regional.yaml`.
- `maps`: This specifies the mapping files used in the [Data Maps](@ref). By default, the maps have not values indicating that the default mapping files included with the package should be used. If the user wishes to provide custom mapping files, they can specify the paths here.

Example metadata section:

```yaml
metadata:
  title: Regional Data Configuration
  description: Configuration file for regional data sources
  data_directory: 'path/to/data/directory'
  maps:
    state_map:
    gdp_map:
    pce_map:
    sgf_map:
    sgf_states_map:
    trade_map:
    faf_map:
```

## Data Section

The `data` section contains information on loading the raw input data loaded by the functions in [Raw Data](@ref). Each key corresponds to a specific data source, and the values provide additional details such as file names or parameters required for loading the data.

More work needs to be done to make each section be consistent with each other, but it should be straightforward to modify the YAML file to point to different data files if needed.

Example data section:

```yaml
data:
  summary:
    path: summary_local.yaml
    base_directory: 'path/to/summary/data/directory'
  state_gdp:
    metadata:
      base_directory: bea_gdp
    gdp:
      description: "State GDP"
      path: SAGDP2__ALL_AREAS_1997_2024.csv
    labor:
      description: "Compensation of Employees"
      path: SAGDP4__ALL_AREAS_1997_2024.csv
    capital:
      description: "Gross Operating Surplus"
      path: SAGDP7__ALL_AREAS_1997_2024.csv
    tax:
      description: "Taxes on production and imports"
      path: SAGDP6__ALL_AREAS_1997_2024.csv
    subsidy:
      description: "Subsidies"
      path: SAGDP5__ALL_AREAS_1997_2024.csv
  personal_consumption:
    metadata:
      base_directory: PCE
    pce:
      description: "Personal Consumption Expenditures"
      path: SAPCE1__ALL_AREAS_1997_2024.csv
  state_finances:
    metadata:
      base_directory: SGF
    sgf:
      description: "State Government Finance"
      path: '^(?<year>\d{2})(state|data)35.txt$'
      replacement:
        year:
          2024: 2023
        state:
          District of Columbia: Maryland
  trade:
    metadata:
      base_directory: USATradeOnline
      agriculture_code: 111CA
    exports:
      description: "Exports"
      path: State Exports by NAICS Commodities.csv
      sheet: Total_Exports_Value_US_
    imports:
      description: "Imports"
      path: State Imports by NAICS Commodities.csv
      sheet: Customs_Value_Gen_US_
    ag_time_series:
      description: "Agricultural Trade Time Series"
      path: commodity_detail_by_state_cy.xlsx
      sheet: Total Exports
      range: A3:Y55
      replacement:
        year:
          2024: 2023
  freight_analysis_framework:
    metadata:
      base_directory: FAF
      column_regex: '^value_(\d{4})$'
      columns:
        - fr_orig
        - dms_origst
        - dms_destst
        - fr_dest
        - trade_type
        - sctg2
      max_year: 2024
      adjusted_demand:
        22: .9
        23: .9
    state:
      description: "FAF state level data"
      path: FAF5.7.1_State.csv
    reprocessed_state:
      description: "Reprocessed FAF state level data"
      path: FAF5.7.1_Reprocessed_1997-2012_State.csv
```

!!! note "Summary Data"
    The `summary` section in the `data` portion of the YAML file is currently unused. Future versions of the package will incorporate this section, simplifying the process of creating the regional data.

## Sets Section

The `sets` section is currently in development and is not yet implemented. Future versions of the package will utilize this section to define specific sets used in the disaggregation process.