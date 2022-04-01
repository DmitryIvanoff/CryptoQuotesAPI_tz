#!/bin/bash
set -x

# imports
isort --recursive  --force-single-line-imports --apply src
autoflake --remove-all-unused-imports --recursive --remove-unused-variables --in-place src --exclude=__init__.py
isort --apply src
black src