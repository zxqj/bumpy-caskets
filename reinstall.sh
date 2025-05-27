#!/bin/bash
rm -rf src/*.egg-info
rm -rf build
pipx uninstall backup
pipx install .
