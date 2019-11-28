from typing import List

from lapis.files import StoredFile


def sort_files_by_cachedsince(stored_files: set) -> List[StoredFile]:
    return sorted(list(stored_files), key=lambda x: x.cachedsince)


# async def fifo(size, storage):
#     print("hit fifo")
#     print(storage.files.keys())
#     # FIFO, test different implementations
#     sorted_content = sorted(
#         list(storage.files.items()), key=lambda x: x.filespecs.cachedsince
#     )
#     print("sorted", sorted_content)
#     while size < 0:
#         print("hit while")
#         size += sorted_content[0][1]["cachedsizeMB"]
#         storage.files.pop(sorted_content[0][0])
#         await sleep(storage.placement_duration)
#         await storage._usedstorage.decrease(
#             **{"usedsize": sorted_content[0][1]["cachedsizeMB"]})
#         print(storage.usedstorage)
#         sorted_content.pop(0)
#     print("after fifo ", storage.files.keys())
#
#
# def last_accessed(size, storage):
#     # FIFO, test different implementations
#     sorted_content = sorted(
#         list(storage.content.items()), key=lambda x: x[1]["lastaccessed"]
#     )
#     while size < 0:
#         size += sorted_content[0][1]["cachedsizeMB"]
#         storage.content.pop(sorted_content[0][0])
#         storage.usedstorage -= sorted_content[0][1]["cachedsizeMB"]
#         sorted_content.pop(0)
#
#
# cache_cleanup = {"fifo": fifo, "lastaccessed": last_accessed}
