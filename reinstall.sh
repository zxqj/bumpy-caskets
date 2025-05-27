#!/bin/bash
rm -r *.egg-info
rm -r build
pipx uninstall backup
pipx install .
