from dataclasses import dataclass

from long_term_uc.common.constants_datatypes import DatatypesNames
from long_term_uc.utils.type_checker import apply_params_type_check


AVAILABLE_ANALYSIS_TYPES = ["calc", "plot"]
AVAILABLE_DATA_TYPES = list(DatatypesNames.__annotations__.values())
DATA_SUBTYPE_KEY = "data_subtype"  # TODO[Q2OJ]: cleaner way to set/get it?
ORDERED_DATA_ANALYSIS_ATTRS = ["analysis_type", "data_type", "data_subtype", "country", 
                               "year", "climatic_year"]  # TODO[Q2OJ]: cleaner way to set/get it? (with data_subtype in 3rd pos)
RAW_TYPES_FOR_CHECK = {"analysis_type": "str", "data_type": "str", "data_subtype": "str", 
                       "country": "str", "year": "int", "climatic_year": "int"}


@dataclass
class DataAnalysis:
    analysis_type: str
    data_type: str
    country: str
    year: int
    climatic_year: int
    data_subtype: str = None

    
    def check_types(self):
        """
        Check coherence of types
        """
        dict_for_check = self.__dict__
        if self.data_subtype is None:
            del dict_for_check[DATA_SUBTYPE_KEY]
        apply_params_type_check(dict_for_check, types_for_check=RAW_TYPES_FOR_CHECK, 
                                param_name="Data analysis params - to set the calc./plot to be done")
