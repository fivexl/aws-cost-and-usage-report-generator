#!/usr/bin/env bash

set -ex

pylint -E *.py
flake8 *.py
