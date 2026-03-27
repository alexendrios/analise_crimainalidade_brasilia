from dataclasses import dataclass
from typing import Callable, Optional
import pandas as pd


@dataclass
class PipelineStep:
    nome: str
    func: Callable[[], Optional[pd.DataFrame]]
    output: Optional[str] = None
    retries: int = 2
    timeout: int = 300
