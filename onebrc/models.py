import pathlib
from dataclasses import dataclass


@dataclass(frozen=True)
class Context:
    file: pathlib.Path
    cores: int
    file_size: int
    page_size: int
