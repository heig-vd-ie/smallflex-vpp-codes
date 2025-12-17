from datetime import datetime
from typing import Literal

import typed_settings as ts

@ts.settings
class InputFiles:
    duckdb_input: str

@ts.settings
class OutputFiles:
    baseline: str
    output: str
    results_plot: str

@ts.settings
class Settings:
    switch_link: str
    switch_pass: str
    input_files: InputFiles
    output_files: OutputFiles



settings = ts.load(Settings, appname="smallflex", config_files=[
    "!settings.toml",
    ".secrets.toml",
])



