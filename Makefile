SHELL := /bin/bash
CONDAROOT := $(shell conda info | grep "base environment" | sed 's/^.*:\s\+\(\/\S*\)\s\+.*$$/\1/')/bin
ROOTDIR := $(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))

default: dev-update

install:  
	@conda env create --file $(ROOTDIR)/environment.yml -n elysium_env
	@source $(CONDAROOT)/activate elysium_env
	@source scripts/run.sh
	@pip install .  --upgrade

update:  
	@conda env update --file $(ROOTDIR)/environment.yml -n elysium_env
	@source $(CONDAROOT)/activate elysium_env
	@source scripts/run.sh

dev-update:  
	@source $(CONDAROOT)/activate elysium_env
	@source scripts/run.sh
	@pip install .  --upgrade
