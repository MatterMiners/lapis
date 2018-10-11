from collections import defaultdict

pools = []
job_queue = []
job_generator = None
job_scheduler = None
monitoring_data = defaultdict(dict)  # {tme: {variable: value, ...}}
cost = 0
