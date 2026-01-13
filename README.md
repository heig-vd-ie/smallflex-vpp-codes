# Smallflex-vpp-code

Smallflex-vpp-code is a framework designed for the optimal design and operation of Virtual Power Plants (VPPs) that 
integrate small-scale hydropower units with distributed renewable resources such as wind, solar photovoltaic (PV) 
systems, and Battery Energy Storage Systems (BESS). Its primary objective is to maximize VPP revenue by participating 
in electricity markets through energy production and flexibility services for ancillary support.

The VPP targets three key electricity markets: day-ahead, intraday balancing, and flexibility (ancillary services). 
Since the number of possible VPP design configurations is limited, the profit and risk of each configuration can be 
evaluated individually.

To address computational tractability, a hierarchical optimization approach is applied across three stages:

1. Long-term planning
2. Short-term/day-ahead scheduling
3. Real-time balancing

This structure ensures efficient estimation of optimal operation while considering the limited set of design schemes.

## Initial Setup

To install all dependencies on a new machine, run:
```sh
make install-all
```

You will need a Gurobi license to run this project. Visit [https://license.gurobi.com/](https://license.gurobi.com/), 
request a new WSL license for your machine, and save it to `~/gurobi_license/gurobi.lic`.

### Activating the Virtual Environment and Setting Environment Variables

Each time you start working on the project, activate the virtual environment by running:
```sh
make venv-activate
```
## Run examples

The examples are located in the **examples** folder. The provided example focuses exclusively on the **Aegina–Alstafel hydropower plant** use case.

The input data are protected and confidential, so they are not freely available. You must contact the project owner to obtain the necessary credentials (the `.secrets.toml` file) required to download the input data.

1.  Run `01_download_from_switch_file.py` to download the input data.  
2.  Run `02_vpp_design_scheme_optimization.py` to execute the algorithm. In this example, a `market_prices.csv` file is included to demonstrate how to provide custom market prices. If you want to use your own market price data, simply update the file path passed as an argument to the `vpp_design_scheme` function.





