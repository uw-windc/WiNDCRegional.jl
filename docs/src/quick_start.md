# Quick Start

This guide provides a brief overview of how to use the WiNDCRegional package to disaggregate national-level WiNDC model results into regional-level data. Follow the steps below to get started quickly.

## Download the Raw Data

The raw data can be downloaded from the WiNDC website. Extract the data files to a local directory on your machine. You will need to provide the path to this directory when loading the data into Julia.

## Set up Julia Environment

It is recommended to set up a Julia environment when ever you begin a new project. You can do this by creating a new directory, starting Julia, and activating it in Julia:

```julia
julia> ]
pkg> activate .
```

Then we install the necessary packages:

```julia
pkg> add DataFrames, YAML, MPSGE, WiNDCNational
pkg> add https://github.com/uw-windc/WiNDCRegional.jl
```

We will update this process once `WiNDCRegional` is a registered package.

## Perform the Disaggregation

The disaggregation process is still being refined, but the following code will perform a basic disaggregation of national-level WiNDC results into regional-level data.

```julia
using DataFrames, YAML, WiNDCRegional, MPSGE, WiNDCNational

## Load the YAML file
info = load_regional_yaml("path/to/your/data/directory")


## Load the national records. Issue with `Used` and `Other`, so they get removed.
summary_raw = WiNDCNational.build_us_table()
df = table(summary_raw) |>
    x -> subset(x,
        :row => ByRow(y -> !(y in (:Used, :Other)))
    )
summary_raw = National(df, sets(summary_raw), elements(summary_raw))
summary,_ = calibrate(summary_raw)

state_table = create_state_table(summary, info)
```

We will be updating the disaggregation process to be a single function call in the future.


## Verify Results in CGE Model

To ensure the disaggregation process was successful, we perform a benchmark verification using a CGE model.

```julia
M = WiNDCRegional.regional_model(state_table; year = 2024);
solve!(M, cumulative_iteration_limit = 0)
```

This should give a very small residual, indicating that the disaggregation was successful and the data is balanced. 

If you receive strange errors during the solve (like `NaN` values), it is likely that you lack a PATHSolver license. Refer to the [PATHSolver.jl](https://github.com/chkwon/PATHSolver.jl#license) documentation for more information on obtaining a license.