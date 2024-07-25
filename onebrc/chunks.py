import mmap
from dataclasses import dataclass

from onebrc.models import Context


@dataclass(frozen=True)
class FileChunk:
    offset: int
    size: int

    def align_with_page_size(self, page_size: int) -> int:
        return self.offset - (self.offset % page_size)


def get_file_chunks(ctx: Context) -> list[FileChunk]:
    chunk_size = ctx.file_size // ctx.cores
    chunks = [i * chunk_size for i in range(ctx.cores)] + [ctx.file_size]
    with ctx.file.open("r+b") as f, mmap.mmap(
        f.fileno(), 0, access=mmap.ACCESS_READ
    ) as mm:
        for i in range(1, len(chunks) - 1):
            chunks[i] = mm.find(b"\n", chunks[i], chunks[i + 1]) + 1
            yield FileChunk(offset=chunks[i - 1], size=chunks[i] - chunks[i - 1])
    yield FileChunk(offset=chunks[-2], size=chunks[-1] - chunks[-2])
