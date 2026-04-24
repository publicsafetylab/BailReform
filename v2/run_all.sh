#!/bin/bash
set -e

cd "$(dirname "$0")/data_prep"
PYTHON="../.venv/bin/python3"

PRE=12

for THRESH in 0.75 0.90; do
  echo "══ threshold=$THRESH ══"

  echo "── get_sample: FL (t=$THRESH) ──"
  $PYTHON get_sample.py -s fl --pre_cycles $PRE -t $THRESH

  echo "── get_sample: GA (t=$THRESH) ──"
  $PYTHON get_sample.py -s ga --pre_cycles $PRE -t $THRESH
done

echo "── check_denominators ──"
$PYTHON check_denominators.py

echo "── incarceration_rate ──"
$PYTHON incarceration_rate.py

echo "── incapacitation ──"
$PYTHON incapacitation.py

echo "── rebooking_rate ──"
$PYTHON rebooking_rate.py
