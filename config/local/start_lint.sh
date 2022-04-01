#!/bin/bash
set -x

mypy src
isort --check-only src
black src --check
