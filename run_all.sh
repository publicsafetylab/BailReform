#!/bin/bash

cd data_prep || exit

echo "running get_roster_list.py --save" \
  && python3 get_roster_list.py --save \
  && echo "running get_roster_list.py -r demographics --save" \
  && python3 get_roster_list.py -r demographics --save \
  && echo "running get_roster_list.py -r charges --save" \
  && python3 get_roster_list.py -r charges --save

echo "running average_daily_population.py" \
  && python3 average_daily_population.py \
  && echo "running average_daily_population.py -r demographics" \
  && python3 average_daily_population.py -r demographics \
  && echo "running average_daily_population.py -r charges" \
  && python3 average_daily_population.py -r charges \
  && echo "running average_daily_population.py -r demographics -btc" \
  && python3 average_daily_population.py -r demographics -btc \
  && echo "running average_daily_population.py -r charges -btc" \
  && python3 average_daily_population.py -r charges -btc \
  && echo "running average_daily_population.py -s ga" \
  && python3 average_daily_population.py -s ga \
  && echo "running average_daily_population.py -s ga -r demographics" \
  && python3 average_daily_population.py -s ga -r demographics \
  && echo "running average_daily_population.py -s ga -r charges" \
  && python3 average_daily_population.py -s ga -r charges \
  && echo "running average_daily_population.py -s ga -r demographics -btc" \
  && python3 average_daily_population.py -s ga -r demographics -btc \
  && echo "running average_daily_population.py -s ga -r charges -btc" \
  && python3 average_daily_population.py -s ga -r charges -btc

echo "running average_daily_demographics.py -r demographics" \
  && python3 average_daily_demographics.py -r demographics \
  && echo "running average_daily_demographics.py -r demographics -btc" \
  && python3 average_daily_demographics.py -r demographics -btc \
  && echo "running average_daily_demographics.py -s ga -r demographics" \
  && python3 average_daily_demographics.py -s ga -r demographics \
  && echo "running average_daily_demographics.py -s ga -r demographics -btc" \
  && python3 average_daily_demographics.py -s ga -r demographics -btc

echo "running length_of_stay_proportions.py" \
  && python3 length_of_stay_proportions.py \
  && echo "running length_of_stay_proportions.py -r demographics" \
  && python3 length_of_stay_proportions.py -r demographics \
  && echo "running length_of_stay_proportions.py -r charges" \
  && python3 length_of_stay_proportions.py -r charges \
  && echo "running length_of_stay_proportions.py -r demographics -btc" \
  && python3 length_of_stay_proportions.py -r demographics -btc \
  && echo "running length_of_stay_proportions.py -r charges -btc" \
  && python3 length_of_stay_proportions.py -r charges -btc \
  && echo "running length_of_stay_proportions.py -s ga" \
  && python3 length_of_stay_proportions.py -s ga \
  && echo "running length_of_stay_proportions.py -s ga -r demographics" \
  && python3 length_of_stay_proportions.py -s ga -r demographics \
  && echo "running length_of_stay_proportions.py -s ga -r charges" \
  && python3 length_of_stay_proportions.py -s ga -r charges \
  && echo "running length_of_stay_proportions.py -s ga -r demographics -btc" \
  && python3 length_of_stay_proportions.py -s ga -r demographics -btc \
  && echo "running length_of_stay_proportions.py -s ga -r charges -btc" \
  && python3 length_of_stay_proportions.py -s ga -r charges -btc

echo "running incapacitation_proportions.py" \
  && python3 incapacitation_proportions.py \
  && echo "running incapacitation_proportions.py -r demographics" \
  && python3 incapacitation_proportions.py -r demographics \
  && echo "running incapacitation_proportions.py -r charges" \
  && python3 incapacitation_proportions.py -r charges \
  && echo "running incapacitation_proportions.py -r demographics -btc" \
  && python3 incapacitation_proportions.py -r demographics -btc \
  && echo "running incapacitation_proportions.py -r charges -btc" \
  && python3 incapacitation_proportions.py -r charges -btc \
  && echo "running incapacitation_proportions.py -s ga" \
  && python3 incapacitation_proportions.py -s ga \
  && echo "running incapacitation_proportions.py -s ga -r demographics" \
  && python3 incapacitation_proportions.py -s ga -r demographics \
  && echo "running incapacitation_proportions.py -s ga -r charges" \
  && python3 incapacitation_proportions.py -s ga -r charges \
  && echo "running incapacitation_proportions.py -s ga -r demographics -btc" \
  && python3 incapacitation_proportions.py -s ga -r demographics -btc \
  && echo "running incapacitation_proportions.py -s ga -r charges -btc" \
  && python3 incapacitation_proportions.py -s ga -r charges -btc

echo "running rebooking_proportions.py" \
  && python3 rebooking_proportions.py \
  && echo "running rebooking_proportions.py -r demographics" \
  && python3 rebooking_proportions.py -r demographics \
  && echo "running rebooking_proportions.py -r charges" \
  && python3 rebooking_proportions.py -r charges \
  && echo "running rebooking_proportions.py -r demographics -btc" \
  && python3 rebooking_proportions.py -r demographics -btc \
  && echo "running rebooking_proportions.py -r charges -btc" \
  && python3 rebooking_proportions.py -r charges -btc \
  && echo "running rebooking_proportions.py -r demographics -brtc" \
  && python3 rebooking_proportions.py -r demographics -brtc \
  && echo "running rebooking_proportions.py -r charges -brtc" \
  && python3 rebooking_proportions.py -r charges -brtc \
  && echo "running rebooking_proportions.py -r demographics -btc -brtc" \
  && python3 rebooking_proportions.py -r demographics -btc -brtc \
  && echo "running rebooking_proportions.py -r charges -btc -brtc" \
  && python3 rebooking_proportions.py -r charges -btc -brtc \
  && echo "running rebooking_proportions.py -s ga" \
  && python3 rebooking_proportions.py -s ga \
  && echo "running rebooking_proportions.py -s ga -r demographics" \
  && python3 rebooking_proportions.py -s ga -r demographics \
  && echo "running rebooking_proportions.py -s ga -r charges" \
  && python3 rebooking_proportions.py -s ga -r charges \
  && echo "running rebooking_proportions.py -s ga -r demographics -btc" \
  && python3 rebooking_proportions.py -s ga -r demographics -btc \
  && echo "running rebooking_proportions.py -s ga -r charges -btc" \
  && python3 rebooking_proportions.py -s ga -r charges -btc \
  && echo "running rebooking_proportions.py -s ga -r demographics -brtc" \
  && python3 rebooking_proportions.py -s ga -r demographics -brtc \
  && echo "running rebooking_proportions.py -s ga -r charges -brtc" \
  && python3 rebooking_proportions.py -s ga -r charges -brtc \
  && echo "running rebooking_proportions.py -s ga -r demographics -btc -brtc" \
  && python3 rebooking_proportions.py -s ga -r demographics -btc -brtc \
  && echo "running rebooking_proportions.py -s ga -r charges -btc -brtc" \
  && python3 rebooking_proportions.py -s ga -r charges -btc -brtc