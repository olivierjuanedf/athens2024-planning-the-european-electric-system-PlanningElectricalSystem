import os
from dataclasses import dataclass
from typing import Dict, List, Tuple
import pandas as pd

from long_term_uc.common.constants_datatypes import DATATYPE_NAMES
from long_term_uc.common.error_msgs import print_out_msg
from long_term_uc.common.long_term_uc_io import COLUMN_NAMES, DT_FILE_PREFIX, DT_SUBFOLDERS, FILES_FORMAT, \
    GEN_CAPA_SUBDT_COLS, INPUT_CY_STRESS_TEST_SUBFOLDER, INPUT_ERAA_FOLDER
from long_term_uc.common.uc_run_params import UCRunParams
from long_term_uc.include.dataset_builder import GenerationUnitData, GEN_UNITS_PYPSA_PARAMS, get_val_of_agg_pt_in_df, set_gen_unit_name
from long_term_uc.utils.df_utils import create_dict_from_cols_in_df, selec_in_df_based_on_list, set_aggreg_col_based_on_corresp
from long_term_uc.utils.eraa_data_reader import filter_input_data, gen_capa_pt_str_sanitizer, select_interco_capas, \
    set_aggreg_cf_prod_types_data


@dataclass
class Dataset:
    agg_prod_types_with_cf_data: List[str]
    source: str = "eraa"
    is_stress_test: bool = False
    demand: Dict[str, pd.DataFrame] = None
    agg_cf_data: Dict[str, pd.DataFrame] = None
    agg_gen_capa_data: Dict[str, pd.DataFrame] = None
    interco_capas: Dict[Tuple[str, str], float] = None
    # {country: list of associated generation units data}
    generation_units_data: Dict[str, List[GenerationUnitData]] = None

    def get_countries_data(self, uc_run_params: UCRunParams, aggreg_prod_types_def: Dict[str, List[str]]):
        """
        Get ERAA data necessary for the selected countries
        :param uc_run_params: UC run parameters, from which main reading infos will be obtained
        :param agg_prod_types_with_cf_data: aggreg. production types for which CF data must be read
        :param aggreg_prod_types_def: per-datatype definition of aggreg. to indiv. production types
        :returns: {country: df with demand of this country}, {country: df with - per aggreg. prod type CF}, 
        {country: df with installed generation capas}, df with all interconnection capas (for considered 
        countries and year)
        """
        # set shorter names for simplicity
        countries = uc_run_params.selected_countries
        year = uc_run_params.selected_target_year
        climatic_year = uc_run_params.selected_climatic_year
        selec_agg_prod_types = uc_run_params.selected_prod_types
        power_capacities = uc_run_params.updated_capacities_prod_types
        period_start = uc_run_params.uc_period_start
        period_end = uc_run_params.uc_period_end
        # get - per datatype - folder names
        demand_folder = os.path.join(INPUT_ERAA_FOLDER, DT_SUBFOLDERS.demand)
        res_cf_folder = os.path.join(INPUT_ERAA_FOLDER, DT_SUBFOLDERS.res_capa_factors)
        gen_capas_folder = os.path.join(INPUT_ERAA_FOLDER, DT_SUBFOLDERS.generation_capas)
        interco_capas_folder = os.path.join(INPUT_ERAA_FOLDER, DT_SUBFOLDERS.interco_capas)
        # file prefix
        demand_prefix = DT_FILE_PREFIX.demand
        res_cf_prefix = DT_FILE_PREFIX.res_capa_factors
        gen_capas_prefix = DT_FILE_PREFIX.generation_capas
        interco_capas_prefix = DT_FILE_PREFIX.interco_capas
        # column names
        date_col = COLUMN_NAMES.date
        climatic_year_col = COLUMN_NAMES.climatic_year
        prod_type_col = COLUMN_NAMES.production_type
        prod_type_agg_col = f"{prod_type_col}_agg"
        value_col = COLUMN_NAMES.value
        # separators for csv reader
        column_sep = FILES_FORMAT.column_sep
        decimal_sep = FILES_FORMAT.decimal_sep

        self.demand = {}
        self.agg_cf_data = {}
        self.agg_gen_capa_data = {}

        n_spaces_msg = 2

        aggreg_pt_cf_def = aggreg_prod_types_def[DATATYPE_NAMES.capa_factor]
        aggreg_pt_gen_capa_def = aggreg_prod_types_def[DATATYPE_NAMES.installed_capa]

        for country in countries:
            print(f"For country: {country}")
            # read csv files
            # [Coding trick] f"{year}_{country}" directly fullfill string with value of year 
            # and country variables (f-string completion)
            current_suffix = f"{year}_{country}"  # common suffix to all ERAA data files
            # get demand
            print("Get demand")
            if self.is_stress_test is True:
                demand_folder_full = f"{demand_folder}/{INPUT_CY_STRESS_TEST_SUBFOLDER}"
            else:
                demand_folder_full = f"{demand_folder}"
            demand_file = f"{demand_folder_full}/{demand_prefix}_{current_suffix}.csv"
            current_df_demand = pd.read_csv(demand_file, sep=column_sep, decimal=decimal_sep)
            # then keep only selected period date range and climatic year
            self.demand[country] = filter_input_data(df=current_df_demand, date_col=date_col,
                                                     climatic_year_col=climatic_year_col, period_start=period_start,
                                                     period_end=period_end, climatic_year=climatic_year)

            # get RES capacity factor data
            print("Get RES capacity factors")
            self.agg_cf_data[country] = {}
            df_res_cf_list = []
            for agg_prod_type in selec_agg_prod_types[country]:
                # if prod type with CF data
                if agg_prod_type in self.agg_prod_types_with_cf_data:
                    print(n_spaces_msg * " " + f"- For aggreg. prod. type: {agg_prod_type}")
                    current_agg_pt_df_res_cf_list = []
                    for prod_type in aggreg_pt_cf_def[agg_prod_type]:
                        if self.is_stress_test is True:
                            res_cf_folder_full = f"{res_cf_folder}/{INPUT_CY_STRESS_TEST_SUBFOLDER}"
                        else:
                            res_cf_folder_full = f"{res_cf_folder}"
                        cf_filename = f"{res_cf_prefix}_{prod_type}_{current_suffix}.csv" 
                        cf_data_file = f"{res_cf_folder_full}/{cf_filename}"
                        if os.path.exists(cf_data_file) is False:
                            print(2*n_spaces_msg * " " + f"[WARNING] RES capa. factor data file does not exist: {prod_type} not accounted for here")
                        else:
                            print(2*n_spaces_msg * " " + f"* Prod. type: {prod_type}")
                            current_df_res_cf = pd.read_csv(cf_data_file, sep=column_sep, decimal=decimal_sep) 
                            current_df_res_cf = \
                                filter_input_data(df=current_df_res_cf, date_col=date_col,
                                                climatic_year_col=climatic_year_col, 
                                                period_start=period_start, period_end=period_end, 
                                                climatic_year=climatic_year)
                            if len(current_df_res_cf) == 0:
                                print(2*n_spaces_msg * " " + f"[WARNING] No RES capa. factor data for prod. type {prod_type} and climatic year {climatic_year}")
                            else:
                                # add column with production type (for later aggreg.)
                                current_df_res_cf[prod_type_agg_col] = agg_prod_type
                                current_agg_pt_df_res_cf_list.append(current_df_res_cf)
                    if len(current_agg_pt_df_res_cf_list) == 0:
                        print(n_spaces_msg * " " + f"[WARNING] No data available for aggregate RES prod. type {agg_prod_type} -> not accounted for in UC model here")
                    else:
                        df_res_cf_list.extend(current_agg_pt_df_res_cf_list)

            # concatenate, aggreg. over prod type of same aggreg. type and avg
            if len(df_res_cf_list) == 0:
                print(n_spaces_msg * " " + f"[WARNING] No RES data available for country {country} -> not accounted for in UC model here")
            else:
                self.agg_cf_data[country] = \
                    set_aggreg_cf_prod_types_data(df_cf_list=df_res_cf_list, pt_agg_col=prod_type_agg_col, date_col=date_col,
                                                val_col=value_col)
            
            # get installed generation capacity data
            print("Get installed generation capacities (unique file per country and year, with all prod. types in it)")
            gen_capa_data_file = f"{gen_capas_folder}/{gen_capas_prefix}_{current_suffix}.csv"
            if os.path.exists(gen_capa_data_file) is False:
                print_out_msg(msg_level="warning", msg=f"Generation capas data file does not exist: {country} not accounted for here")
            else:
                current_df_gen_capa = pd.read_csv(gen_capa_data_file, sep=column_sep, decimal=decimal_sep)
                # Keep sanitize prod. types col values
                current_df_gen_capa[prod_type_col] = current_df_gen_capa[prod_type_col].apply(gen_capa_pt_str_sanitizer)
                # Keep only selected aggreg. prod. types
                current_df_gen_capa = \
                    set_aggreg_col_based_on_corresp(df=current_df_gen_capa, col_name=prod_type_col,
                                                    created_agg_col_name=prod_type_agg_col, val_cols=GEN_CAPA_SUBDT_COLS, 
                                                    agg_corresp=aggreg_pt_gen_capa_def, common_aggreg_ope="sum")
                current_df_gen_capa = \
                    selec_in_df_based_on_list(df=current_df_gen_capa, selec_col=prod_type_agg_col,
                                            selec_vals=selec_agg_prod_types[country])
                if country in power_capacities:
                    for k, v in power_capacities[country].items():
                        current_df_gen_capa.loc[current_df_gen_capa['production_type_agg']==k, 'power_capacity'] = v
                
                if 'failure' in selec_agg_prod_types[country]:
                    failure_df = pd.DataFrame.from_dict({
                        'production_type_agg': ['failure'],
                        'power_capacity': [uc_run_params.failure_power_capa],
                        'power_capacity_turbine': [0.0],
                        'power_capacity_pumping': [0.0],
                        'power_capacity_injection': [0.0],
                        'power_capacity_offtake': [0.0],
                        'energy_capacity': [0.0]
                    })
                    current_df_gen_capa = pd.concat([current_df_gen_capa, failure_df], ignore_index=True)
                
                if country in power_capacities:
                    for k, v in power_capacities[country].items():
                        current_df_gen_capa.loc[current_df_gen_capa['production_type_agg']==k, 'power_capacity'] = v
                self.agg_gen_capa_data[country] = current_df_gen_capa
                print('#'*100)
                print(current_df_gen_capa)
                print('#'*100)

        # read interconnection capas file
        print("Get interconnection capacities, with unique file for all nodes (zones=countries) and year")
        interco_capas_data_file = f"{interco_capas_folder}/{interco_capas_prefix}_{year}.csv"
        if os.path.exists(interco_capas_data_file) is False:
            print_out_msg(msg_level="warning", msg=f"Generation capas data file does not exist: {country} not accounted for here")
        else:
            df_interco_capas = pd.read_csv(interco_capas_data_file, sep=column_sep, decimal=decimal_sep)
        # and select information needed for selected countries
        df_interco_capas = select_interco_capas(df_intercos_capa=df_interco_capas, countries=countries)
        # set as dictionary
        origin_col = COLUMN_NAMES.zone_origin
        destination_col = COLUMN_NAMES.zone_destination
        tuple_key_col = "tuple_key"
        df_interco_capas[tuple_key_col] = \
            df_interco_capas.apply(lambda col: (col[origin_col], col[destination_col]),
                                axis=1)
        interco_capas = create_dict_from_cols_in_df(df=df_interco_capas, key_col=tuple_key_col, val_col=value_col)
        # add interco capas values set by user
        interco_capas |= uc_run_params.interco_capas_updated_values
        self.interco_capas = interco_capas
    
    def get_generation_units_data(self, uc_run_params: UCRunParams, pypsa_unit_params_per_agg_pt: Dict[str, dict],
                                  units_complem_params_per_agg_pt: Dict[str, Dict[str, str]]):
        """
        Get generation units data to create them hereafter
        :param pypsa_unit_params_per_agg_pt: dict of per aggreg. prod type main Pypsa params
        :param units_complem_params_per_agg_pt: # for each aggreg. prod type, a dict. {complem. param name: source - "from_json_tb_modif"/"from_eraa_data"}
        """
        countries = list(self.agg_gen_capa_data)
        prod_type_col = COLUMN_NAMES.production_type
        prod_type_agg_col = f"{prod_type_col}_agg"
        value_col = COLUMN_NAMES.value
        # TODO: set as global constants/unify...
        power_capa_key = "power_capa"
        capa_factor_key = "capa_factors"

        n_spaces_msg = 2

        self.generation_units_data = {}
        for country in countries:
            print_out_msg(msg_level="info", msg=f"- for country {country}")
            self.generation_units_data[country] = []
            current_capa_data = self.agg_gen_capa_data[country]
            current_res_cf_data = self.agg_cf_data[country]
            # get list of assets to be treated from capa. data
            agg_prod_types = list(set(current_capa_data[prod_type_agg_col]))
            # initialize set of params for each unit by using pypsa default values
            current_assets_data = {agg_pt: pypsa_unit_params_per_agg_pt[agg_pt] for agg_pt in agg_prod_types}
            # and loop over pt to add complementary params
            for agg_pt in agg_prod_types:
                print_out_msg(msg_level="info", msg=n_spaces_msg * " " + f"* for aggreg. prod. type {agg_pt}")
                # set and add asset name
                gen_unit_name = set_gen_unit_name(country=country, agg_prod_type=agg_pt)
                current_assets_data[agg_pt]["name"] = gen_unit_name
                # and "type" (the aggreg. prod types used here, with a direct corresp. to PyPSA generators; 
                # made explicit in JSON fixed params files)
                current_assets_data[agg_pt]["type"] = agg_pt
                if agg_pt in units_complem_params_per_agg_pt and len(units_complem_params_per_agg_pt[agg_pt]) > 0:
                    # add pnom attribute if needed
                    if power_capa_key in units_complem_params_per_agg_pt[agg_pt]:
                        print_out_msg(msg_level="info", msg=2*n_spaces_msg * " " + f"-> add {power_capa_key}")
                        current_power_capa = \
                            get_val_of_agg_pt_in_df(df_data=current_capa_data, prod_type_agg_col=prod_type_agg_col,
                                                    agg_prod_type=agg_pt, value_col="power_capacity",
                                                    static_val=True)
                        current_assets_data[agg_pt][GEN_UNITS_PYPSA_PARAMS.power_capa] = int(current_power_capa)
                            
                    # add pmax_pu when variable for RES/fatal units
                    if capa_factor_key in units_complem_params_per_agg_pt[agg_pt]:
                        print_out_msg(msg_level="info", msg=2*n_spaces_msg * " " + f"-> add {capa_factor_key}")
                        current_assets_data[agg_pt][GEN_UNITS_PYPSA_PARAMS.capa_factors] = \
                            get_val_of_agg_pt_in_df(df_data=current_res_cf_data, prod_type_agg_col=prod_type_agg_col,
                                                    agg_prod_type=agg_pt, value_col=value_col, static_val=False)
                    # max hours for storage-like assets (energy capa/power capa)

                    # marginal costs/efficiency, from FuelSources
                elif agg_pt == 'failure':
                    current_assets_data[agg_pt][GEN_UNITS_PYPSA_PARAMS.power_capa] = self.agg_gen_capa_data[country].loc[self.agg_gen_capa_data[country]['production_type_agg']=='failure', 'power_capacity'].iloc[0]
                    current_assets_data[agg_pt][GEN_UNITS_PYPSA_PARAMS.marginal_cost] = uc_run_params.failure_penalty
                    current_assets_data[agg_pt][GEN_UNITS_PYPSA_PARAMS.committable] = False
                power_capacity = self.agg_gen_capa_data[country].loc[self.agg_gen_capa_data[country]['production_type_agg']==agg_pt, 'power_capacity'].iloc[0]
                current_assets_data[agg_pt][GEN_UNITS_PYPSA_PARAMS.power_capa] = power_capacity
                energy_capacity = self.agg_gen_capa_data[country].loc[self.agg_gen_capa_data[country]['production_type_agg']==agg_pt, 'energy_capacity'].iloc[0]
                power_capacity_turbine = self.agg_gen_capa_data[country].loc[self.agg_gen_capa_data[country]['production_type_agg']==agg_pt, 'power_capacity_turbine'].iloc[0]
                if energy_capacity > 0:
                    power_capacity_pumping = self.agg_gen_capa_data[country].loc[self.agg_gen_capa_data[country]['production_type_agg']==agg_pt, 'power_capacity_pumping'].iloc[0]
                    if power_capacity_turbine > 0:
                        p_nom = max(abs(power_capacity_turbine), abs(power_capacity_pumping))
                        p_min_pu = power_capacity_pumping / p_nom
                        p_max_pu = power_capacity_turbine / p_nom
                        current_assets_data[agg_pt][GEN_UNITS_PYPSA_PARAMS.power_capa] = p_nom
                        current_assets_data[agg_pt][GEN_UNITS_PYPSA_PARAMS.min_power] = p_min_pu
                        current_assets_data[agg_pt][GEN_UNITS_PYPSA_PARAMS.capa_factors] = p_max_pu
                        max_hours = energy_capacity / p_nom
                        current_assets_data[agg_pt][GEN_UNITS_PYPSA_PARAMS.max_hours] = max_hours
                    power_capacity_injection = self.agg_gen_capa_data[country].loc[self.agg_gen_capa_data[country]['production_type_agg']==agg_pt, 'power_capacity_injection'].iloc[0]
                    power_capacity_offtake = self.agg_gen_capa_data[country].loc[self.agg_gen_capa_data[country]['production_type_agg']==agg_pt, 'power_capacity_offtake'].iloc[0]
                    if power_capacity_injection > 0:
                        p_nom = max(abs(power_capacity_injection), abs(power_capacity_offtake))
                        p_min_pu = -power_capacity_offtake / p_nom
                        p_max_pu = power_capacity_injection / p_nom
                        current_assets_data[agg_pt][GEN_UNITS_PYPSA_PARAMS.power_capa] = p_nom
                        current_assets_data[agg_pt][GEN_UNITS_PYPSA_PARAMS.min_power] = p_min_pu
                        current_assets_data[agg_pt][GEN_UNITS_PYPSA_PARAMS.capa_factors] = p_max_pu
                        max_hours = energy_capacity / p_nom
                        current_assets_data[agg_pt][GEN_UNITS_PYPSA_PARAMS.max_hours] = max_hours
                    if power_capacity > 0:
                        current_assets_data[agg_pt][GEN_UNITS_PYPSA_PARAMS.power_capa] = power_capacity
                elif power_capacity_turbine > 0:
                    p_nom = abs(power_capacity_turbine)
                    current_assets_data[agg_pt][GEN_UNITS_PYPSA_PARAMS.power_capa] = p_nom
                    current_assets_data[agg_pt][GEN_UNITS_PYPSA_PARAMS.min_power] = 0
                    current_assets_data[agg_pt][GEN_UNITS_PYPSA_PARAMS.capa_factors] = 1
                    if power_capacity > 0:
                        current_assets_data[agg_pt][GEN_UNITS_PYPSA_PARAMS.power_capa] = power_capacity
                    
                
                self.generation_units_data[country].append(GenerationUnitData(**current_assets_data[agg_pt]))

    def set_committable_param(self):
        for country, val in self.generation_units_data.items():
            for i in range(len(val)):
                val[i].committable = False
