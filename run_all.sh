cd data_prep \
  && python3 get_roster_list.py --save \
  && python3 get_roster_list.py -d --save \
  && python3 average_daily_population.py -s fl \
  && python3 average_daily_population.py -s fl -d \
  && python3 average_daily_population.py -s ga \
  && python3 average_daily_population.py -s ga -d \

#  && python3 length_of_stay_proportions.py -s fl \
#  && python3 rebooking_proportions.py -s fl \
#
#  && python3 average_daily_population.py -s ga \
#  && python3 length_of_stay_proportions.py -s ga \
#  && python3 rebooking_proportions.py -s ga \
/