#!/bin/bash

cd data_prep \
  \
  && echo "running get_roster_list.py --save" \
  && python3 get_roster_list.py --save \
  && echo "running get_roster_list.py -d --save" \
  && python3 get_roster_list.py -d --save \
  && echo "running get_roster_list.py -c --save" \
  && python3 get_roster_list.py -c --save \
  \
  && echo "running average_daily_population.py" \
  && python3 average_daily_population.py \
  && echo "running average_daily_population.py -d" \
  && python3 average_daily_population.py -d \
  && echo "running average_daily_population.py -c" \
  && python3 average_daily_population.py -c \
  && echo "running average_daily_population.py -s ga" \
  && python3 average_daily_population.py -s ga \
  && echo "running average_daily_population.py -s ga -d" \
  && python3 average_daily_population.py -s ga -d \
  && echo "running average_daily_population.py -s ga -c" \
  && python3 average_daily_population.py -s ga -c \
  \
  && echo "running length_of_stay_proportions.py" \
  && python3 length_of_stay_proportions.py \
  && echo "running length_of_stay_proportions.py -d" \
  && python3 length_of_stay_proportions.py -d \
  && echo "running length_of_stay_proportions.py -c" \
  && python3 length_of_stay_proportions.py -c \
  && echo "running length_of_stay_proportions.py -s ga" \
  && python3 length_of_stay_proportions.py -s ga \
  && echo "running length_of_stay_proportions.py -d -s ga" \
  && python3 length_of_stay_proportions.py -d -s ga \
  && echo "running length_of_stay_proportions.py -c -s ga" \
  && python3 length_of_stay_proportions.py -c -s ga \
  \
  && echo "running incapacitation_proportions.py" \
  && python3 incapacitation_proportions.py \
  && echo "running incapacitation_proportions.py -d" \
  && python3 incapacitation_proportions.py -d \
  && echo "running incapacitation_proportions.py -c" \
  && python3 incapacitation_proportions.py -c \
  && echo "running incapacitation_proportions.py -s ga" \
  && python3 incapacitation_proportions.py -s ga \
  && echo "running incapacitation_proportions.py -d -s ga" \
  && python3 incapacitation_proportions.py -d -s ga \
  && echo "running incapacitation_proportions.py -c -s ga" \
  && python3 incapacitation_proportions.py -c -s ga \
  \
  && echo "running rebooking_proportions.py" \
  && python3 rebooking_proportions.py \
  && echo "running rebooking_proportions.py -d" \
  && python3 rebooking_proportions.py -d \
  && echo "running rebooking_proportions.py -c" \
  && python3 rebooking_proportions.py -c \
  && echo "running rebooking_proportions.py -s ga" \
  && python3 rebooking_proportions.py -s ga \
  && echo "running rebooking_proportions.py -d -s ga" \
  && python3 rebooking_proportions.py -d -s ga \
  && echo "running rebooking_proportions.py -c -s ga" \
  && python3 rebooking_proportions.py -c -s ga \
  \
  && echo "running average_daily_demographics.py" \
  && python3 average_daily_demographics.py \
  && echo "running average_daily_demographics.py -s ga" \
  && python3 average_daily_demographics.py -s ga
/