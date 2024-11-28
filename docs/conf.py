# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information
import os
import sys
# sys.path.insert(0, os.path.abspath('..'))
# sys.path.insert(0, os.path.abspath('../../'))
sys.path.insert(0, os.path.abspath('../src'))


project = 'SmallFlex'
copyright = '2024, Luca Tomasini'
author = 'Luca Tomasini'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.mathjax",
    "sphinx.ext.autodoc",
    "sphinx.ext.coverage",
    "sphinx.ext.napoleon",
    "sphinx.ext.autosectionlabel",
    "sphinx_rtd_size",
    "nbsphinx",
    "myst_parser",
    "sphinx_copybutton",
    "sphinx_rtd_theme"
]


templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']



# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "sphinx_rtd_theme"
html_static_path = ['_static']

html_css_files = [
    "custom.css",  # Include the custom CSS file
]

sphinx_rtd_size_width = "75%"

mathjax3_config = {
    "chtml": {'displayAlign': 'left'},
    "tex": {
        "tags": "ams",  # Use AMS-style tagging for equations
        "tagSide": "right",  # Align equation labels to the left
        "tagIndent": "0em",  # Optional: adjust the indentation
        "useLabelIds": True,  # Add labels to elements
    }
}