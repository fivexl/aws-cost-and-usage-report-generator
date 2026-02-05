#!/usr/bin/env bash

[ -d env ] && rm -rf env
[ -d .venv ] && rm -rf .venv

uv venv
source .venv/bin/activate
uv pip install -r requirements.txt
