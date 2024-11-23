import os
from dataclasses import dataclass

from common.long_term_uc_io import INPUT_FUNC_PARAMS_SUBFOLDER

@dataclass
class PlotParams:
    json_file: str = os.path.join(INPUT_FUNC_PARAMS_SUBFOLDER, "plot_params.json")
    
    def read_and_check(self):
        return None
    