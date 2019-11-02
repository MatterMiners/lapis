def extrapolate_walltime_to_maximal_efficiency(job, maximal_efficiency: float = 0.8):
    return (job.used_resources["cores"] / maximal_efficiency) * job.walltime


# TODO: add models depending on fraction of cached files etc
walltime_models = {"maxeff": extrapolate_walltime_to_maximal_efficiency}
