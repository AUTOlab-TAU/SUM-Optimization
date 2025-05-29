import time
import subprocess
import sys
import math
import pandas as pd
import numpy as np
import random
import biogeme.database as db
from biogeme.expressions import Variable, Beta
import biogeme.biogeme as bio
from biogeme import models
from util.fleetpy import *
from util.dcacalc import *
from util.util import *
from util.setup import *
from util.sample import *
# from plotnine import ggplot, aes, geom_line, geom_point, scale_shape_manual, theme_minimal, scale_linetype_manual, scale_size_manual
from datetime import datetime
import argparse
import json

# GET THE CONFIGURATION FROM COMMANDLINE --config STRING
parser = argparse.ArgumentParser()
parser.add_argument("--config", type=json.loads, required=True)
inner_loop_args =parser.parse_args().config

# ISOLATE CONFIGS SENT DIRECTLY TO FLEETPY
fleetpy_params = get_fleetpy_params(inner_loop_args)

# GET ALL PARAMETERS
scenario_basename = "SUM"
config_name = inner_loop_args["config_name"]
stop_loop_threshold = inner_loop_args["stop_loop_threshold"]
num_iterations = inner_loop_args["maxiter"]
num_replications = inner_loop_args["reps"]
nsm_cost = inner_loop_args["nsm_cost"]
stop_loop_threshold_triggered = False
framework_start_time = datetime.now()
framework_start_time_txt = framework_start_time.strftime('%Y-%m-%d_%H_%M_%S')
request_pool = load_request_pool()
request_pool_car_mean_travel_time = np.mean(request_pool["car_time"])
sim_start_time = math.floor(min(request_pool["rq_time"]))
sim_end_time = math.ceil(max(request_pool["rq_time"]))
print(f"Starting SUM Simulation-Optimization Framework at {framework_start_time_txt}")

# WRITE FLEETPY CONSTANT CONFIG (DOES NOT CHANGE DURING INNER LOOP)
write_fleetpy_constant_config_file(fr"{fleetpy_path}\studies\jerusalem\scenarios\constant_config.csv",fleetpy_constant_config,sim_start_time,sim_end_time)

# INNER LOOP BEGINS HERE
last_iter = num_iterations # if interloop threshold is triggered, last_iter = this iteration
for i in range(num_iterations):
    iter_start_time = datetime.now()
    print("----------------------------------------------------------------------------------")
    print(f"                        Starting main loop iteration {i} of {num_iterations}")
    print("----------------------------------------------------------------------------------")

    # create new demand for each replication
    demand_start_time = datetime.now()
    requests = [] # misnomer, list of dataframes of all travelers (not just NSM) for all reps
    iter_demand_modesplit = [] # list of per 
    for r in range(num_replications):
        df = sample_request_pool(request_pool,demand_ratio)

        # UPDATE PER TRAVELER UTILITY DATA WITH CURRENT NSM STATS

        # NSM time & dist
        df["nsm_travel_time"] = df["car_time"] * nsmstats.get_smooth('nsm_car_time_ratio')
        df["nsm_wait_time"] = nsmstats.get_smooth('nsm_wait_time')
        df["nsm_total_time"] = df["nsm_travel_time"] + df["nsm_wait_time"]


        # Costs (faster to set in sample frame if possible)
        df["car_cost"] = df["car_dist"] * car_fuel_consumption_liters_per_km * fuel_cost_shekels_per_liter
        df["pt_cost"] = pt_cost
        df["nsm_cost"] = nsm_cost

        # Availabilities
        df = calc_all_availabilities(df,avails)

        # Utilities
        df = calc_all_utilities(df,choicestats.get_all_current_smooth(),nsmstats.get_all_current_smooth())

        # Exponential of utilities (for calculating probabilities)
        # these will be zero if a mode is unavailable
        df = calc_all_exps(df,modes)

        # Probabilities
        df = calc_all_probs(df,modes)

        # Choice
        df["choice"] = df.apply(select_choice,axis=1)
        df["nsmuser"] = (df["choice"] == 4).astype(int)

        if len(df[df["choice"]==4])==0:
            # no NSM users. service configuration failed.
            # avoid division by zero error
            # handle this more elegantly in the future
            break

        df['served'] = -1 # default value

        #save all travelers
        df.to_csv(f"{workpath}\\i{i:03}_r{r:03}_all_presim_demand.csv",index=False)
        requests.append(df)
        iter_demand_modesplit.append(get_mode_stats(df))

        #save nsm demand
        rqfile = f"i{i:03}_r{r:03}_nsm.csv"
        df[df["choice"]==4].to_csv(f"{fleetpy_demand_path}\\{rqfile}",index=False)

    demand_end_time = datetime.now()
    demand_dur = demand_end_time - demand_start_time
    demand_dur_rep = demand_dur/num_replications
    print(f"Completed demand realization for iter {i} in {round(demand_dur.total_seconds(),2)}s ({round(demand_dur_rep.total_seconds(),2)}s for each of {num_replications} reps)")

    # RUN FLEETPY ON REQUESTS
    # overwrite scenario file with new scenario name & demand source file
    write_fleetpy_scenario_config_file(fr"{fleetpy_path}\studies\jerusalem\scenarios\scenario_config.csv",i,num_replications,scenario_basename,fleetpy_params) # inner_loop_args)
    command = [sys.executable, fr"{fleetpy_path}\run_private_jerusalem.py"]
    simulation_start_time = datetime.now()
    result = subprocess.run(command,capture_output=False, text=True) # change to True for print statements from simulations
    # Print the output and any error messages
    #print("Output:", result.stdout)
    #print("Error:", result.stderr)
    simulation_end_time = datetime.now()
    simulation_dur = simulation_end_time - simulation_start_time
    print(f"Completed simulation for iter {i} in {round(simulation_dur.total_seconds(),2)}s")

    # Calculate NSM metrics
    iterstat_start_time = datetime.now()
    simdata = get_fleetpy_simdata_allreps(i,num_replications,scenario_basename) # dict with key = {"mean":mean,"stdev":stdev}
    simstats = calc_fleetpy_simstats(simdata)
    for stat in ["ratio","count"]:
        for mode in iter_demand_modesplit[0][stat].keys(): #take modes from first entry
            valuelist = []
            for repdata in iter_demand_modesplit:
                valuelist.append(repdata[stat][mode])
            modestats[stat][mode].append(mean(valuelist))

    # store the key simstats (mean values) for smoothing
    nsmstats.add("service_rate",simstats["modal split"]["mean"])
    nsmstats.add("occupancy",simstats["occupancy"]["mean"])
    userdata = get_fleetpy_userdata_allreps(i,num_replications,scenario_basename)
    iternsm = calc_userstats_allreps(userdata)
    # add this iteration's summary stats (over all reps) to nsmstats for smoothing (over iterations)
    for stat in iternsm:
        val = iternsm[stat]['mean']
        nsmstats.add(stat,val)

    iterstat_end_time = datetime.now()
    iterstat_dur = iterstat_end_time - iterstat_start_time
    iterstat_dur_rep = iterstat_dur / num_replications
    print(f"Completed statistics for iter {i} in {round(iterstat_dur.total_seconds(),2)}s ({round(iterstat_dur_rep.total_seconds(),2)}s for each of {num_replications} reps)")

    # update traveler data
    update_start_time = datetime.now()
    for r in range(num_replications):
        df = userdata[r]
        # first unserved requests reassigned based on original utilities
        print("ORIGINAL MODE STATS")
        print_mode_stats(df)
        df = update_fleetpy_unservedchoose(df)
        print("AFTER UNSERVED SWITCH")
        print_mode_stats(df)


        # # Update values of nonusers (including unserved who just switched to alternatives)
        df.loc[df["served"] != 1, "nsm_wait_time"] = nsmstats.get_smooth('nsm_wait_time')
        df.loc[df["served"] != 1, "nsm_travel_time"] = df.loc[df["served"] != 1, "car_time"] * nsmstats.get_smooth('nsm_car_time_ratio')
        df.loc[df["served"] != 1, "nsm_total_time"] = df.loc[df["served"] != 1, "nsm_wait_time"] + df.loc[df["served"] != 1, "nsm_travel_time"]


        # compute new NSM utilities & all probabilities
        df = recalc_nsm_util_and_exp(df,choicestats.get_all_current_smooth(),nsmstats.get_all_current_smooth())
        df = calc_all_probs(df,modes)

        # give some NSM users (those who received service) a chance to switch
        mask = (df['served'] == 1) #& (np.random.rand(len(df)) < user_can_switch_rate) # these users offered chance to switch
        df.loc[mask,"choice"] = df[mask].apply(select_choice,axis=1)
        df.loc[mask,"modified"] = 1
        print("AFTER SERVED CAN SWITCH")
        print_mode_stats(df)


        # ive some non-NSM users (never requested) a chance to switch
        mask = (df['served'] == -1) #& (np.random.rand(len(df)) < nonuser_can_switch_rate) # these nonusers offered chance to switch
        df.loc[mask,"choice"] = df[mask].apply(select_choice,axis=1)
        df.loc[mask,"modified"] = 1
        print("AFTER NON-USERS CAN SWITCH")
        print_mode_stats(df)

        df.to_csv(fr"{workpath}\\i{i:03}_r{r:03}_all_postsim_demand.csv",index=False)


    update_end_time = datetime.now()
    update_dur = update_end_time - update_start_time
    update_dur_rep = update_dur / num_replications
    print(f"Completed update of utilities and choices based on nsm performance for iter {i} in {round(update_dur.total_seconds(),2)}s ({round(update_dur_rep.total_seconds(),2)}s for each of {num_replications} reps)")


    dca_start_time = datetime.now()
    iterchoice = []
    for r in range(num_replications):
        # RE-ANALYZE MODE CHOICE
        dca_start_time = datetime.now()
        # Load df into biogeme database class instance
        database = db.Database ("jerusalem",userdata[r])

        # get quick access to database columns (faster than df["hhveh"])
        UNSERVED = Variable("unserved")
        NSMUSER = Variable("nsmuser")
        HHVEH = Variable("hhveh")
        INCOME = Variable("incom")
        WALK_TIME = Variable("walk_time")
        WALK_DIST = Variable("walk_dist")
        BIKE_REG_TIME = Variable("bike_regular_time")
        BIKE_REG_DIST = Variable("bike_regular_dist")
        BIKE_ELEC_TIME = Variable("bike_electric_time")
        BIKE_ELEC_DIST = Variable("bike_electric_dist")
        CAR_TIME = Variable("car_time")
        CAR_DIST = Variable("car_dist")
        PT_TIME = Variable("pt_time")
        PT_DIST = Variable("pt_dist")
        NSM_TOTAL_TIME = Variable("nsm_total_time")
        #NSM_DIST = Variable("nsm_dist")
        MEDIAN_PT = (Variable("orig_median_pt") + Variable("dest_median_pt"))/2.0
        WALK_AV = Variable("walk_av")
        BIKE_AV = Variable("bike_av")
        CAR_AV = Variable("car_av")
        PT_AV = Variable("pt_av")
        NSM_AV = Variable("nsm_av")
        CAR_COST = Variable("car_cost")
        PT_COST = Variable("pt_cost")
        NSM_COST = Variable("nsm_cost")

        # Consolidated mode
        # 0=walk
        # 1=bike
        # 2=car,truck ride or drive
        # 3=PT only (bus & light rail)
        # 4=NSM
        CHOICE = Variable("choice")

        # SCALE VARIABLES HERE TO KEEP ASC & BETA VALUES AROUND 1
        # Not implemented

        # parameters to estimate
        ASC_WALK =Beta("ASC_WALK", 0, None , None , 0)
        ASC_BIKE = Beta("ASC_BIKE", 0, None , None , 0)
        ASC_CAR = Beta("ASC_CAR", 0, None , None , 0) # includes motorcycle, truck, etc
        ASC_PT = Beta("ASC_PT", 0, None , None , 0)
        ASC_NSM = Beta("ASC_NSM", 0, None , None , 0)
        B_TIME = Beta("B_TIME", 0, None , None , 0)    # optionally change "None" to min or max value
        B_COST = Beta("B_COST", 0, None , None, 0)    # to, for example, TIME, COST, and/or RISK to
        B_RISK = Beta("B_RISK", 0, None , None, 0)    # be negative
        #B_MEDIAN_PT = Beta("B_MEDIAN_PT", 0, None , None, 0)


        # B_HHVEH = Beta("B_HHVEH", 0, None , None , 0)


        NSM_RISK  = 1.0 - nsmstats.get_smooth('service_rate')
        # WALK_RISK = 0.0
        # BIKE_RISK = 0.0
        # CAR_RISK  = 0.05
        # PT_RISK   = 0.1


        # utilities
        # 0=walk 1=bike 2=car,truck,motorcycle 3=PT only, park & ride, etc
        V0 = ASC_WALK + B_TIME * WALK_TIME  # assumes no cost/effort?
        V1 = ASC_BIKE + B_TIME * BIKE_ELEC_TIME  # assumes electric bikes in Jerusalem and no cost
        V2 = ASC_CAR + B_TIME * CAR_TIME + B_COST * CAR_COST    # includes motorcycles
        V3 = ASC_PT + B_TIME * PT_TIME + B_COST * PT_COST       #+ B_MEDIAN_PT + MEDIAN_PT
        V4 = ASC_NSM + B_TIME * NSM_TOTAL_TIME + B_COST * NSM_COST + B_RISK * NSM_RISK * NSMUSER # estimate B_RISK from travelers who tried to use the NSM

        V = {0: V0, 1: V1, 2: V2, 3: V3, 4:V4 } # where 0,1,2,3 are mode choice alternatives


        # BOOLEAN AVAILABILITY I'M USING BETAS AND COLUMNS FOR HOW AVAILABLE
        av = {0: WALK_AV, 1:BIKE_AV, 2: CAR_AV, 3: PT_AV, 4: NSM_AV} # boolean variables storing whether a given alternative is available

        # the model
        logprob = models.loglogit(V , av , CHOICE)

        # biogeme object  GIVE IT A NAME
        the_biogeme = bio.BIOGEME(database,logprob)
        the_biogeme.modelName = f"jerusalem_{framework_start_time_txt}_{i:03}"

        # likelihood when all coefficients are zero
        the_biogeme.calculateNullLoglikelihood(av)

        # estimate model parameters (generates html file also)
        results = the_biogeme.estimate()
        estimates = (results.getEstimatedParameters().to_dict())
        iterchoice.append(estimates)

    # add mean model values to track over iterations
    for stat in iterchoice[0]['Value']: # just to get model parameters
        vals = []
        pvals = []
        for r in range(num_replications):
            vals.append(iterchoice[r]['Value'][stat])
            pvals.append(iterchoice[r]['Rob. p-value'][stat])
        choicestats.add(stat,mean(vals),mean(pvals)) # this probably needs to be weighted by demand per rep

    dca_end_time = datetime.now()
    dca_dur = dca_end_time - dca_start_time
    dca_dur_rep = dca_dur / num_replications
    print(f"Completed DCA for iter {i} in {round(dca_dur.total_seconds(),2)}s ({round(dca_dur_rep.total_seconds(),2)}s for each of {num_replications} reps)")
    iter_end_time = datetime.now()
    iter_dur = iter_end_time - iter_start_time
    print(f"Completed iter {i} in {round(iter_dur.total_seconds(),2)}s")

    if i>0: #mode 4 is NSM
        cur_NSM_prc = abs(modestats["ratio"][4][i]) * 100
        prev_NSM_prc = abs(modestats["ratio"][4][i-1]) * 100
        delta = abs(cur_NSM_prc - prev_NSM_prc)
        if delta <= stop_loop_threshold:
            print(f"Absolute modesplit delta between current iter {i} ({round(cur_NSM_prc,4)}) and previous iter {i-1} ({round(prev_NSM_prc,4)}) is {delta:04}")
            print(f"This is below the inner loop stop threshold of {stop_loop_threshold}. Stopping inner loop.")
            stop_loop_threshold_triggered = True
            last_iter = i
            break
                  

framework_end_time = datetime.now()
framework_end_time_txt = framework_end_time.strftime('%Y-%m-%d_%H_%M_%S')
framework_dur = framework_end_time - framework_start_time
print(f"Started:   {framework_start_time_txt}")
print(f"Completed: {framework_end_time_txt}")
print(f"framework execution time: {(framework_dur.total_seconds())/60} minutes")


# SAVE CONFIGURATION RESULTS

# report raw values (not smoothed)
final_occupancy = nsmstats.data['value']['occupancy'][i]
final_service_rate = nsmstats.data['value']['service_rate'][i] * 100
final_nsm_wait_time = nsmstats.data['value']['nsm_wait_time'][i]
final_nsm_car_time_ratio = nsmstats.data['value']['nsm_car_time_ratio'][i]
final_mode_split = {}
for mode in modestats["ratio"]:
    final_mode_split[mode] = modestats["ratio"][mode][i] * 100 # modestats all raw, never smoothed

# Open the file and write the header + data
with open(f"{result_path}\\{config_name}_results.csv", mode="w", newline="") as file:
    writer = csv.writer(file)

    # Write the header
    writer.writerow(["stat", "value"])

    # Write rows from the dictionary
    writer.writerow(["occupancy",final_occupancy])
    writer.writerow(["service_rate",final_service_rate])
    writer.writerow(["nsm_wait_time",final_nsm_wait_time])
    writer.writerow(["nsm_car_time_ratio",final_nsm_car_time_ratio])
    writer.writerow(["threshold_triggered",stop_loop_threshold_triggered])
    writer.writerow(["last_iter",last_iter])
    writer.writerow(["num_iterations",num_iterations])
    for mode in final_mode_split:
        writer.writerow([f"mode_{mode}", final_mode_split[mode]])

# Create results df
nsmstats_df = nsmstats.to_ggplot()
nsmstats_df.to_csv(f"{result_path}\\{config_name}_nsmstats.csv",index=False)
choicestats_df = choicestats.to_ggplot()
choicestats_df.to_csv(rf"{result_path}\\{config_name}_choicestats.csv",index=False)

# do same for modesplit, which is not a StatsGroup
temp = []
for mode in modestats["ratio"]:
    for i, val in enumerate(modestats['ratio'][mode]):
        temp.append({'iter': i, 'mode': mode, 'ratio': val})
modesplit_df = pd.DataFrame(temp)
modesplit_df.to_csv(rf"{result_path}\\{config_name}_modesplit.csv",index=False)
modesplit_df['mode'] = modesplit_df['mode'].astype('category')


# if make_figures:

#     # create more specific dfs for plotting
#     ASC_df = choicestats_df[choicestats_df['stat'].isin(['ASC_WALK','ASC_BIKE','ASC_CAR','ASC_PT','ASC_NSM'])]
#     bval_df = choicestats_df[choicestats_df['stat'].isin(['B_COST','B_TIME','B_RISK'])]
#     nsm_df = nsmstats_df[nsmstats_df['stat'].isin(['occupancy','service_rate','nsm_car_time_ratio'])]
#     wait_df = nsmstats_df[nsmstats_df['stat'].isin(['nsm_wait_time'])]

#     # Define line types and sizes for each Stat
#     line_types = {'ASC_WALK': 'solid', 'ASC_BIKE': 'solid', 'ASC_CAR': 'dotted',
#                 'ASC_PT': 'dotted', 'ASC_NSM': 'solid', 'B_COST': 'dashdot', 'B_TIME': 'dotted', 'service_rate': 'solid'}

#     line_sizes = {'ASC_WALK': 0.5, 'ASC_BIKE': 0.5, 'ASC_CAR': 0.5,
#                 'ASC_PT': 0.75, 'ASC_NSM': 3.0, 'B_COST': 0.75, 'B_TIME': 1.0, 'service_rate': 1.5}

#     ASCplot = (
#         ggplot(ASC_df, aes(x='iter', y='smoothed', group='stat', color='stat'))
#         + geom_line()  # Add lines for each Stat
#         + geom_point(aes(shape='pval < 0.05'), size=1.0)  # Use shape to indicate significance
#         + scale_shape_manual(values={True: 'o', False: 'x'})  # Solid for significant, hollow for non-significant
#         + theme_minimal()  # Use a minimal theme for better aesthetics
#     )
#     # Save the plot to a PNG file
#     ASCplot.save(filename=f"{result_path}\\{config_name}_ASC.png", dpi=300, height=6, width=8, units='in')


#     BVALplot = (
#         ggplot(bval_df, aes(x='iter', y='smoothed', group='stat', color='stat'))
#         + geom_line()  # Add lines for each Stat
#         + geom_point(aes(shape='pval < 0.05'), size=1.0)  # Use shape to indicate significance
#         + scale_shape_manual(values={True: 'o', False: 'x'})  # Solid for significant, hollow for non-significant
#         + theme_minimal()  # Use a minimal theme for better aesthetics
#     )
#     # Save the plot to a PNG file
#     BVALplot.save(filename=f"{result_path}\\{config_name}_BVAL.png", dpi=300, height=6, width=8, units='in')



#     NSMplot = (
#         ggplot(nsm_df, aes(x='iter', y='smoothed', group='stat', color='stat'))
#         + geom_line()  # Add lines for each Stat
#         + geom_point(aes(shape='pval < 0.05'), size=1.0)  # Use shape to indicate significance
#         + scale_shape_manual(values={True: 'o', False: 'x'})  # Solid for significant, hollow for non-significant
#         + theme_minimal()  # Use a minimal theme for better aesthetics
#     )
#     # Save the plot to a PNG file
#     NSMplot.save(filename=f"{result_path}\\{config_name}_NSM.png", dpi=300, height=6, width=8, units='in')


#     WAITplot = (
#         ggplot(wait_df, aes(x='iter', y='smoothed', group='stat', color='stat'))
#         + geom_line()  # Add lines for each Stat
#         + geom_point(aes(shape='pval < 0.05'), size=1.0)  # Use shape to indicate significance
#         + scale_shape_manual(values={True: 'o', False: 'x'})  # Solid for significant, hollow for non-significant
#         + theme_minimal()  # Use a minimal theme for better aesthetics
#     )
#     # Save the plot to a PNG file
#     WAITplot.save(filename=f"{result_path}\\{config_name}_WAIT.png", dpi=300, height=6, width=8, units='in')

#     mode_plot = (
#         ggplot(modesplit_df, aes(x='iter', y='ratio', group='mode', color='mode'))
#         + geom_line()  # Add lines for each Stat
#         + theme_minimal()  # Use a minimal theme for better aesthetics
#     )
#     # Save the plot to a PNG file
#     mode_plot.save(filename=f"{result_path}\\{config_name}_MODESPLIT.png", dpi=300, height=6, width=8, units='in')













