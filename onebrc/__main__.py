import pathlib
import os
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed

from onebrc.agg import agg_zero, reduce_agg
from onebrc.chunks import get_file_chunks
from onebrc.models import Context
from onebrc.worker import worker
import time

# PATH = pathlib.Path('/Users/iv/Code/1brc/measurements_10m.txt')
PATH = pathlib.Path("/Users/iv/Code/1brc/measurements.txt")
CONTEXT = Context(
    file=PATH,
    cores=os.cpu_count(),
    file_size=os.stat(PATH).st_size,
    page_size=os.sysconf("SC_PAGE_SIZE"),
)


def print_result(final_result: dict[str, list[float]]):
    result_measurements = [None] * len(final_result)
    for idx, key in enumerate(sorted(final_result)):
        value = final_result[key]
        result_measurements[idx] = (
            f"{key.decode('utf8')}={value[1]:.1f}/{value[2] / value[3]:.1f}/{value[0]:.1f}"
        )
    print("{" + ", ".join(result_measurements) + "}")


def main():
    start_ts = time.monotonic()
    futures = []
    final_result = defaultdict(agg_zero)
    print(CONTEXT.file_size)
    with ProcessPoolExecutor(max_workers=CONTEXT.cores) as ex:
        # with ThreadPoolExecutor(max_workers=CONTEXT.cores) as ex:
        for chunk in get_file_chunks(CONTEXT):
            futures.append(ex.submit(worker, chunk, CONTEXT))
        for result in as_completed(futures):
            for key, value in result.result().items():
                final_result[key] = reduce_agg(final_result[key], value)

    print_result(final_result)
    end_ts = time.monotonic()
    print(f"Time: {end_ts - start_ts:.1f} s")


if __name__ == "__main__":
    main()
