import csv
from lapis.storage import Storage


def storage_reader(storage, storage_content):
    storage_content = storage_content_reader(storage_content)
    reader = csv.DictReader(storage, delimiter=" ", quotechar="'")
    for row in reader:
        yield Storage(
            name=row["name"],
            sitename=row["sitename"],
            storagesize=int(row["cachesizeMB"]),
            content=storage_content[row["name"]],
        )


def storage_content_reader(file_name):
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
            cache_information[row["cachename"]][row["filename"]][key] = int(row[key])
    if not cache_information:
        cache_information = None
    return cache_information
