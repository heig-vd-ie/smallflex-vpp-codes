# Guide (March 2023)

## I. Description of the project

The SmallFlex project encompasses several stages, starting from loading time series data, integrating data and generating scenarios, loading case studies, and concluding with the optimization algorithm component.

- __Loading Time Series Data__: This involves scripts like ``01_load_data.py`` and directories such as ``scripts/preliminary`` and ``src/auxiliary`` and ``src/pre_analyses``.
- __Integration of Data and Scenario Generation__: This phase includes scripts like ``01_generate_scenarios.py`` and directories such as ``scripts/forecast`` and ``src/federation``.

- __Loading Case Studies__: This step involves scripts like ``03_generate_case_studies.py`` and directories such as ``scripts/hill_chart`` and ``src/units.json``.

- __Optimization Algorithm__: This part is implemented in ``src/algorithm/st_lanning_opt.py`` (*not completed yet*).

The project utilizes SQLite database for recording the data. Upon executing the scripts, the database will be located in ``.cache/interim/case.db``.

## II. Input time series data

To run the VPP design algorithm code, we gathered hydrological, meteorological, and power market data.

### A. Hydrological data

In regards to hydrology data for Griessee/Alstafel, Merzenbach, Blinnenwerk, and Morel, we propose utilizing flow regime models like PREVAH. These locations are situated along small rivers with sporadic monitoring stations, making flow regime models a suitable choice. We have reached out to Massimiliano Zappa to explore this approach further

- Current data:
  - [BAFU - Discharge and water level](https://www.hydrodaten.admin.ch/en/seen-und-fluesse/messstationen-zustand)
  - [BAFU - map displayed](https://map.geo.admin.ch/?lang=en&topic=bafu&zoom=5&bgLayer=ch.swisstopo.pixelkarte-farbe&catalogNodes=2771,2772&layers=ch.bafu.hydroweb-messstationen_zustand,ch.bafu.hydroweb-messstationen_temperatur&E=2672033.38&N=1148141.35&layers_visibility=true,false)
  - [EPFL wiki](https://wiki.epfl.ch/hydrodata)
  - [PREVAH matlab code](https://wiki.epfl.ch/hydrodata/documents/CatchmentAggregation_RegimeV24March2016.zip)

- Notes:
  - KW Altstafel: Model of Griessee lake, input discharge of Griesgletscher.
  - Merzenbach: No monitoring station available.
  - Blinnenwerk (Blinne): No monitoring station available.
  - Morel: Dependency on Muhlebach and Ernen (this unit is excluded for WP7 due to complexity).

- Potential Solutions:
  - Utilize power outputs of existing plants over the years.
  - Utilize data from Rhone-Gletsch and scale it accordingly.
  - Employ flow regime and simulated data with the PREVAH model. (Considerations for very small units are needed.)

Currently, we only possess data from Rhone-Gletsch (``.cache/data/hydrometeo``), and we have used the nominal flow of each river to generate discharge flow data.

### B. Meteorological data

Regarding meteorological data, we have made significant progress. We now have access to Swissmeteo and data provided by Massimiliano Zappa, effectively resolving the issue.

- We have obtained data for Ulrichen (``.cache/data/meteoswiss_ulrichen``).
- [MeteoStation](https://www.meteoswiss.admin.ch/services-and-publications/applications/measurement-values-and-measuring-networks.html#param=messnetz-automatisch&lang=en&station=ULR&chart=hour)
- A complete data set: This includes temperature, irradiation, and wind speed data from various altitudes and locations within the GOMS region, which we will utilize (``.cache/data/Gries``).

### C. Power market data

We evaluated two possible options for obtaining electricity market pricing data.

- Initially, we extracted data from the Swissgrid website, storing the collected data in ``.cache/data/swissgrid``.
- Additionally, we reached out to Alpiq, who provided us with a set of historical electrical market data stored in ``.cache/data/alpiq``.

### D. Data of projects in Goms region

Presently, we have defined three case studies outlined in ``src/units.json``. To generate hill chart tables for the units under consideration, we utilized the notebook ``scripts/hill_chart/find_hill_chart.ipynb``. PowerVison could potentially provide us with further insights on this matter.

## III. Formulation

The problem has been formulate within ``docs/SmallFlex-WP7-VPP-design.pdf``.

## IV. Commands

- Install requirements with either ``pip install -r requirements.txt`` or using poetry (``poetry install``) or conda (``conda env create -f environment.yml``).

- Build database and fill it with available timeseries data:

```shell
python -i scripts/01_load_data.py
```

- Federate data and build scenarios and forecasts (primary implementaion):

```shell
python.exe -i scripts/02_generate_scenarios.py
```

- Add case studies data into db:

```shell
python.exe -i scripts/03_generate_case_studies.py
```

- Visualization:

```shell
streamlit run visualization/home.py
```
