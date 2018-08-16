from collections import defaultdict

pools = []
global_demand = None
job_generator = None
monitoring_data = defaultdict(dict)  # {tme: {variable: value, ...}}
cost = 0
