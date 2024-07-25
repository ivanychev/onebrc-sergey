import line_profiler

from onebrc.chunks import FileChunk
from onebrc.models import Context
import mmap


@line_profiler.profile
def worker(chunk: FileChunk, ctx: Context):
    results = {}

    page_offset = chunk.align_with_page_size(ctx.page_size)

    with ctx.file.open("r+b") as f, mmap.mmap(
        f.fileno(),
        length=chunk.size + (chunk.offset - page_offset),
        access=mmap.ACCESS_READ,
        offset=page_offset,
    ) as mm:
        mm.seek(chunk.offset - page_offset)
        for line in iter(mm.readline, b""):
            name, _, value = line.partition(b";")
            value = float(value)
            agg = results.setdefault(name, [value, value, value, 1])
            if agg[3] == 1:
                continue
            if value > agg[0]:
                agg[0] = value
            elif value < agg[1]:
                agg[1] = value
            agg[2] += value
            agg[3] += 1

    return results
