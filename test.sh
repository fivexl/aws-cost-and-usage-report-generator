#!/usr/bin/env bash

set -ex

flake8 *.py
pylint -E *.py