import csv
from lapis.storage import Storage


def storage_reader(
    storage,
    storage_content,
    unit_conversion_mapping: dict = {
        "cachesizeGB": 1024 * 1024 * 1024,
        "throughput_limit": 1024 * 1024 * 1024,
    },  # noqa: B006
):
    storage_content = storage_content_reader(storage_content)
    reader = csv.DictReader(storage, delimiter=" ", quotechar="'")
    for row in reader:
        yield Storage(
            name=row["name"],
            sitename=row["sitename"],
            storagesize=int(
                row["cachesizeGB"] * unit_conversion_mapping.get("cachesizeGB", 1)
            ),
            throughput_limit=int(row["throughput_limit"]),
            files=storage_content[row["name"]],
        )


def storage_content_reader(
    file_name,
    unit_conversion_mapping: dict = {
        "filesize": 1024 * 1024 * 1024,
        "usedsize": 1024 * 1024 * 1024,
    },
):
    reader = csv.DictReader(file_name, delimiter=" ", quotechar="'")
    cache_information = dict()
    for row in reader:
        if row["cachename"] not in cache_information.keys():
            cache_information[row["cachename"]] = dict()
        cache_information[row["cachename"]][row["filename"]] = dict()
        for key in [
            "filesize",
            "usedsize",
            "cachedsince",
            "lastaccessed",
            "numberofaccesses",
        ]:
            cache_information[row["cachename"]][row["filename"]][key] = int(
                row[key] * unit_conversion_mapping.get(key, 1)
            )
    if not cache_information:
        cache_information = None
    return cache_information
