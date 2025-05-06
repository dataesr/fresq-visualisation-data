from project.server.main.tasks import *

raw_data_suffix = 'latest'

get_etabs(raw_data_suffix)
transform_raw_data(raw_data_suffix)
get_mentions(raw_data_suffix)

