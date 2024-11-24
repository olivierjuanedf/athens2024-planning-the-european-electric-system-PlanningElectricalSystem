from typing import List
import pandas as pd
from datetime import datetime

from long_term_uc.common.long_term_uc_io import COLUMN_NAMES, DATE_FORMAT
from long_term_uc.utils.basic_utils import str_sanitizer
from long_term_uc.utils.df_utils import cast_df_col_as_date, concatenate_dfs, selec_in_df_based_on_list, \
    get_subdf_from_date_range



def filter_input_data(df: pd.DataFrame, date_col: str, climatic_year_col: str, period_start: datetime, 
                      period_end: datetime, climatic_year: int) -> pd.DataFrame:
    # ERAA date format not automatically cast by pd
    df = cast_df_col_as_date(df=df, date_col=date_col, date_format=DATE_FORMAT)
    # keep only wanted date range
    df_filtered = get_subdf_from_date_range(df=df, date_col=date_col, date_min=period_start, date_max=period_end)
    # then selected climatic year
    df_filtered = selec_in_df_based_on_list(df=df_filtered, selec_col=climatic_year_col, 
                                            selec_vals=[climatic_year])
    return df_filtered


def set_aggreg_cf_prod_types_data(df_cf_list: List[pd.DataFrame], pt_agg_col: str, date_col: str, val_col: str) -> pd.DataFrame:
    # concatenate, aggreg. over prod type of same aggreg. type and avg
    df_cf_agg = concatenate_dfs(dfs=df_cf_list)
    df_cf_agg = df_cf_agg.groupby([pt_agg_col, date_col]).agg({val_col: "mean"}).reset_index()
    return df_cf_agg


def gen_capa_pt_str_sanitizer(gen_capa_prod_type: str) -> str:
    # very ad-hoc operation
    sanitized_gen_capa_pt = gen_capa_prod_type.replace(" - ", " ")
    sanitized_gen_capa_pt = str_sanitizer(raw_str=sanitized_gen_capa_pt, 
                                          ad_hoc_replacements={"gas_": "gas", "(": "", ")": ""})
    return sanitized_gen_capa_pt


def select_interco_capas(df_intercos_capa: pd.DataFrame, countries: List[str]) -> pd.DataFrame:
    selection_col = "selected"
    # add selection column
    origin_col = COLUMN_NAMES.zone_origin
    destination_col = COLUMN_NAMES.zone_destination
    df_intercos_capa[selection_col] = \
        df_intercos_capa.apply(lambda col: 1 if (col[origin_col] in countries 
                                                 and col[destination_col] in countries) else 0, axis=1)
    # keep only lines with both origin and destination zones in the list of available countries
    df_intercos_capa = df_intercos_capa[df_intercos_capa[selection_col] == 1]
    # remove selection column
    all_cols = list(df_intercos_capa.columns)
    all_cols.remove(selection_col)
    df_intercos_capa = df_intercos_capa[all_cols]
    return df_intercos_capa
    