"""
Read JSON parametrization files... and check coherence of them
"""
import warnings
#deactivate some annoying and useless warnings in pypsa/pandas
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=UserWarning)

from long_term_uc.utils.read import read_and_check_uc_run_params
from long_term_uc.utils.basic_utils import get_period_str
from long_term_uc.include.dataset import Dataset
from long_term_uc.utils.read import read_and_check_pypsa_static_params
from long_term_uc.utils.pypsa_utils import OPTIM_RESOL_STATUS
from long_term_uc.include.dataset_builder import PypsaModel
from long_term_uc.common.fuel_sources import FUEL_SOURCES


usage_params, eraa_data_descr, uc_run_params = read_and_check_uc_run_params()

"""
Get needed data (demand, RES Capa. Factors, installed generation capacities)
"""
uc_period_msg = get_period_str(period_start=uc_run_params.uc_period_start, 
                               period_end=uc_run_params.uc_period_end)

print(f"Read needed ERAA ({eraa_data_descr.eraa_edition}) data for period {uc_period_msg}")
# initialize dataset object
eraa_dataset = Dataset(source=f"eraa_{eraa_data_descr.eraa_edition}", 
                       agg_prod_types_with_cf_data=eraa_data_descr.agg_prod_types_with_cf_data, 
                       is_stress_test=uc_run_params.is_stress_test)

eraa_dataset.get_countries_data(uc_run_params=uc_run_params,
                                aggreg_prod_types_def=eraa_data_descr.aggreg_prod_types_def)

print("Get generation units data, from both ERAA data - read just before - and JSON parameter file")
eraa_dataset.get_generation_units_data(uc_run_params=uc_run_params, 
                                       pypsa_unit_params_per_agg_pt=eraa_data_descr.pypsa_unit_params_per_agg_pt,
                                       units_complem_params_per_agg_pt=eraa_data_descr.units_complem_params_per_agg_pt)
eraa_dataset.set_committable_param()
# TODO[perpi]: connect this properly
#if len(uc_run_params.updated_fuel_sources_params) > 0:
#   generation_units_data = overwrite_gen_units_fuel_src_params(generation_units_data=generation_units_data, 
#                                                               updated_fuel_sources_params=uc_run_params.updated_fuel_sources_params)

print("Check that 'minimal' PyPSA parameters for unit creation have been provided (in JSON files)/read (from ERAA data)")
pypsa_static_params = read_and_check_pypsa_static_params()
eraa_dataset.control_min_pypsa_params_per_gen_units(pypsa_min_unit_params_per_agg_pt=pypsa_static_params.min_unit_params_per_agg_pt)

# create PyPSA network
pypsa_model = PypsaModel(name="my little europe")
date_idx = eraa_dataset.demand[uc_run_params.selected_countries[0]].index
import pandas as pd
horizon = pd.date_range(
    start = uc_run_params.uc_period_start.replace(year=uc_run_params.selected_target_year),
    end = uc_run_params.uc_period_end.replace(year=uc_run_params.selected_target_year),
    freq = 'h'
)
pypsa_model.init_pypsa_network(date_idx=date_idx, date_range=horizon)
# add GPS coordinates
selec_countries_gps_coords = \
  {country: gps_coords for country, gps_coords in eraa_data_descr.gps_coordinates.items() 
   if country in uc_run_params.selected_countries}
pypsa_model.add_gps_coordinates(countries_gps_coords=selec_countries_gps_coords)
pypsa_model.add_energy_carrier(fuel_sources=FUEL_SOURCES)
pypsa_model.add_generators(generators_data=eraa_dataset.generation_units_data)
pypsa_model.add_loads(demand=eraa_dataset.demand)
pypsa_model.add_interco_links(countries=uc_run_params.selected_countries, interco_capas=eraa_dataset.interco_capas)
print("PyPSA network main properties:", pypsa_model.network)
# plot network
from long_term_uc.include.plotter import PlotParams
plot_params = PlotParams()
plot_params.read_and_check()
pypsa_model.plot_network()
result = pypsa_model.optimize_network(year=uc_run_params.selected_target_year,
                                      n_countries=len(uc_run_params.selected_countries),
                                      period_start=uc_run_params.uc_period_start)
print("THE END of European PyPSA-ERAA UC simulation... now you can hack it!")

pypsa_opt_resol_status = OPTIM_RESOL_STATUS.optimal
# TODO[perpi]: reactivate prod plot, using colors set in JSON file
if result[1] == pypsa_opt_resol_status:
  # get objective value, and associated optimal decisions / dual variables
  objective_value = pypsa_model.get_opt_value(pypsa_resol_status=pypsa_opt_resol_status)
  pypsa_model.get_prod_var_opt()
  pypsa_model.get_sde_dual_var_opt()
  # plot "marginal price" figure
  pypsa_model.plot_marginal_price(plot_params=plot_params, year=uc_run_params.selected_target_year, 
                                  climatic_year=uc_run_params.selected_climatic_year,
                                  start_horizon=uc_run_params.uc_period_start) 

  # IV.9) Save optimal decision to an output file
  pypsa_model.save_opt_decisions_to_csv(year=uc_run_params.selected_target_year,
                                        climatic_year=uc_run_params.selected_climatic_year,
                                        start_horizon=uc_run_params.uc_period_start)

  # IV.10) Save marginal prices to an output file
  pypsa_model.save_marginal_prices_to_csv(year=uc_run_params.selected_target_year,
                                          climatic_year=uc_run_params.selected_climatic_year,
                                          start_horizon=uc_run_params.uc_period_start)
else:
   print(f"Optimisation resolution status is not {pypsa_opt_resol_status} -> output data (resp. figures) cannot be saved (resp. plotted)")
   