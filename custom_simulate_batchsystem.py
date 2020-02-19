import sys
from custom_simulate import ini_and_run

ini_and_run(
    job_file=sys.argv[1],
    pool_files=[sys.argv[2], sys.argv[3]],
    storage_file=sys.argv[4],
    storage_type="filehitrate",
    log_file=sys.argv[5],
    remote_throughput=float(sys.argv[6]),
    calculation_efficiency=float(sys.argv[7]),
    log_telegraf=False,
    pre_job_rank=sys.argv[8],
    machine_ads=sys.argv[9],
    job_ads=sys.argv[10],
)
