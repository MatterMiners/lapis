def fifo(size, storage):
    # FIFO, test different implementations
    sorted_content = sorted(
        list(storage.content.items()), key=lambda x: x[1]["cachedsince"]
    )
    while size < 0:
        size += sorted_content[0][1]["cachedsizeMB"]
        storage.content.pop(sorted_content[0][0])
        storage.usedstorage -= sorted_content[0][1]["cachedsizeMB"]
        sorted_content.pop(0)


def last_accessed(size, storage):
    # FIFO, test different implementations
    sorted_content = sorted(
        list(storage.content.items()), key=lambda x: x[1]["lastaccessed"]
    )
    while size < 0:
        size += sorted_content[0][1]["cachedsizeMB"]
        storage.content.pop(sorted_content[0][0])
        storage.usedstorage -= sorted_content[0][1]["cachedsizeMB"]
        sorted_content.pop(0)


cache_cleanup = {"fifo": fifo, "lastaccessed": last_accessed}
