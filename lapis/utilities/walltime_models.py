from lapis.job import Job


def extrapolate_walltime_to_maximal_efficiency(
    job: Job, original_walltime, maximal_efficiency: float = 0.8
):

    return (job.used_resources["cores"] / maximal_efficiency) * original_walltime


# TODO: add models depending on fraction of cached files etc
walltime_models = {"maxeff": extrapolate_walltime_to_maximal_efficiency}
