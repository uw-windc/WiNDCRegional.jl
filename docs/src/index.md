```@meta
CurrentModule = WiNDCRegional
```

# WiNDCRegional

At the national level, the Bureau of Economic Analysis (BEA) provides comprehensive Input/Output (I/O) data for the United States economy at the national level. However, regional I/O data is not directly available, necessitating the disaggregation of national data into regional components. The WiNDCRegional package facilitates this disaggregation process, enabling users to create regional I/O tables based on national data.

The disaggregation process involves several key steps:

1. **Data Mapping**: Regularize the input data to ensure consistent codes for states, sectors, and commodities using mapping tables.
2. **Data Collection**: Gather relevant regional data, including state-level GDP, personal consumption expenditures (PCE), state finances, freight data, trade data, and agricultural flow data.
3. **Disaggregation**: Utilize the collected and mapped data to disaggregate the national I/O table into regional I/O tables, reflecting the economic activities of individual states or regions.
4. **Validation**: Validate the disaggregated regional I/O tables using a CGE model to ensure accuracy and reliability.

The WiNDCRegional package provides functions to facilitate each of these steps, allowing users to effectively disaggregate national economic data into regional components for analysis and modeling. 


