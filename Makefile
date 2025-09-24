# Makefile for Dig-A-Plan setup (supports Linux and WSL)
include Makefile.common.mak

UTILITY_FUNCTIONS_REPO := utility-functions
UTILITY_FUNCTIONS_BRANCH := main
UTILITY_FUNCTIONS_VERSION := 0.1.0

SMALLFLEX_DATASCHEMA_REPO := smallflex-data-schema
SMALLFLEX_DATASCHEMA_BRANCH := main
SMALLFLEX_DATASCHEMA_VERSION := 0.1.2

vis:  ## Run Streamlit visualization
	streamlit run d:/smallflex-vpp-codes/visualization/home.py

fetch-all:  ## Fetch all dependencies
	@$(MAKE) fetch-wheel REPO=$(UTILITY_FUNCTIONS_REPO) BRANCH=$(UTILITY_FUNCTIONS_BRANCH) VERSION=$(UTILITY_FUNCTIONS_VERSION)
	@$(MAKE) fetch-wheel REPO=$(SMALLFLEX_DATASCHEMA_REPO) BRANCH=$(SMALLFLEX_DATASCHEMA_BRANCH) VERSION=$(SMALLFLEX_DATASCHEMA_VERSION)