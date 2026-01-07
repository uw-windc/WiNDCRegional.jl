# WiNDCRegional

[![Dev](https://img.shields.io/badge/docs-dev-blue.svg)](https://uw-windc.github.io/WiNDCRegional.jl/dev/)

This package contains methods to disaggregate [WiNDCNational](https://github.com/uw-windc/WiNDCNational.jl) to the 51 US states (including DC).

## Installation

To install the stable release version, use the Julia package manager:

```julia
pkg> add WiNDCRegional
```

## Basic Usage

Download the raw data from [this link](https://windc.wisc.edu/download?filename=b1b69504efef7fc44d6c18c7dd4dc4e5) and extract it to a folder on your computer. Then, you can run the following code:

```julia
using WiNDCRegional

state_table = create_state_table("path/to/your/data/directory")
```