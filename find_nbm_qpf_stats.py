#!/usr/local/anaconda3/envs/py37/bin/python

"""-------------------------------------------------------------
	Script Name: 	find_nbm_max_qpf.py
	Description: 	Find NBM max QPF over burnscars
	Author: 		Chad Kahler (WRH STID)
	Date:			8/22/2018
-------------------------------------------------------------"""

#---------------------------------------------------------------
# Import python packages
#---------------------------------------------------------------
import os, sys, datetime, time, string, json, shutil, traceback, re, glob
from osgeo import gdal, ogr, osr
import numpy as np
from rasterstats import zonal_stats

#-------------------------------------------------------
# Global configuration options
#-------------------------------------------------------
current_dir = os.path.dirname(os.path.realpath(sys.argv[0]))
buffer_dir = current_dir + "/data/buffer"
perim_dir = current_dir + "/data/perimeter"
data_dir = current_dir + "/data/nbm/json"
json_dir = current_dir + "/data/qpf_threshold_geojson"
nbm_data_dir = current_dir + "/data/nbm/geotiff"
nbm_images_dir = current_dir + "/data/nbm/images"
web_dir = "/var/www/html/debrisflow/data"

buffer_active_dir = current_dir + "/data/buffer_active"
perim_active_dir = current_dir + "/data/perimeter_active"
json_active_dir = current_dir + "/data/json_active"

complete_count = 36
process_again = True

def maxPrecipCategory(val):
	pcat = "00"
	if val >= 2.54:
		pcat = "01"
	elif val >= 5.08:
		pcat = "02"
	elif val >= 7.62:
		pcat = "03"
	elif val >= 10.16:
		pcat = "04"
	elif val >= 12.7:
		pcat = "05"
	elif val >= 15.24:
		pcat = "06"
	elif val >= 17.78:
		pcat = "07"
	elif val >= 20.32:
		pcat = "08"
	elif val >= 22.86:
		pcat = "09"
	elif val >= 25.4:
		pcat = "10"

	return pcat

def maxPrecipCategoryInch(val):
	pcat = "00"
	if val >= 0.1:
		pcat = "01"
	elif val >= 0.2:
		pcat = "02"
	elif val >= 0.3:
		pcat = "03"
	elif val >= 0.4:
		pcat = "04"
	elif val >= 0.5:
		pcat = "05"
	elif val >= 0.6:
		pcat = "06"
	elif val >= 0.7:
		pcat = "07"
	elif val >= 0.8:
		pcat = "08"
	elif val >= 0.9:
		pcat = "09"
	elif val >= 1.0:
		pcat = "10"

	return pcat

#-------------------------------------------------------
# Evaluate NBM precip over fires
#------------------------------------------------------
def findMaxQPFAmount(dt):
	"""
    this function will loop through current forecast and evaluate max QPF over fire
	and output to json file
    """
	print("\n#----------------------------------------------------")
	print("# Evaluating NBM 1 Hour precip accumulation valid: %s" % dt.strftime("%b %d, %Y %H"))
	print("#----------------------------------------------------\n")

	precip_dict = []

	json_file = data_dir + "/nbm." + dt.strftime("%Y%m%d%H") + ".json"

	if not os.path.exists(json_file) or process_again:

		for root, dirs, files in os.walk(buffer_dir, topdown=True):
			for f in files:
				if f.endswith(".shp"):
					
					buffer_shp_path = os.path.join(root,f)
					perim_file = f.replace("10mi_buffer","perimeter")
					perim_shp_path = os.path.join(perim_dir,perim_file)

					try:
						# print("\nFound buffer shapefile: %s" % buffer_shp_path)

						name_array = f.split("_")

						json_buffer = f.replace(".shp",".geojson")
						json_fire = json_buffer.replace("10mi_buffer","perimeter")
						
						with open(os.path.join(json_dir,json_buffer)) as buffer_path:
							buffer_info = json.load(buffer_path)

						fire_dict = {}

						fire_dict["year"] = buffer_info["features"][0]["properties"]["Year"]
						fire_dict["state"] = buffer_info["features"][0]["properties"]["State"]
						fire_dict["name"] = (buffer_info["features"][0]["properties"]["Name"]).title()
						fire_dict["perimeter"] = json_fire
						fire_dict["buffer"] = json_buffer
						fire_dict["coordinates"] = [buffer_info["features"][0]["properties"]["Center_Lat"], buffer_info["features"][0]["properties"]["Center_Lon"]]

						nbm_directory = nbm_data_dir + "/" + dt.strftime("%Y%m%d") + "/" + dt.strftime("%H")
	
						fire_dict["qpf_max"] = []
						fire_dict["qpf_mean"] = []
						fire_dict["qpf_range"] = []
						fire_dict["qpf_sum"] = []
						fire_dict["qpf_valid"] = []

						# initialize full run variables
						run_maxval = 0
						run_meanval = 0
						run_rangeval = 0
						run_sumval = 0

						for i in range(1,37):

							dt_init = dt + datetime.timedelta(hours=i)
							
							if i == 1:
								if i == 1:
									run_maxtime = dt_init.strftime("%Y%m%d%H")
									run_meantime = dt_init.strftime("%Y%m%d%H")
									run_rangetime = dt_init.strftime("%Y%m%d%H")
									run_sumtime = dt_init.strftime("%Y%m%d%H")
									
							nbm_file = "nbm.qpf.%s.tif" % (dt_init.strftime("%Y%m%d%H"))
							nbm_path = nbm_directory + "/" + nbm_file

							if os.path.exists(nbm_path):
														
								# Calculate overlapping area for buffer and NBM
								stats = zonal_stats(buffer_shp_path,nbm_path,nodata=-9999,stats=['max','mean','range','sum'])
								
								# Summarize PRATE by polygon
								maxval = 0
								meanval = 0
								rangeval = 0
								sumval = 0

								if stats[0] and stats[0]['sum'] is not None:
									if stats[0]['max'] > 0:
										maxval = stats[0]['max']
									if stats[0]['mean'] > 0:
										meanval = stats[0]['mean']
									if stats[0]['range'] > 0:
										rangeval = stats[0]['range']
									if stats[0]['sum'] > 0:
										sumval = stats[0]['sum']

								if maxval > run_maxval:
									run_maxval = maxval
									run_maxtime = dt_init.strftime("%Y%m%d%H")

								if meanval > run_meanval:
									run_meanval = meanval
									run_meantime = dt_init.strftime("%Y%m%d%H")

								if rangeval > run_rangeval:
									run_rangeval = rangeval
									run_rangetime = dt_init.strftime("%Y%m%d%H")

								if sumval > run_sumval:
									run_sumval = sumval
									run_sumtime = dt_init.strftime("%Y%m%d%H")

								# print("Max in: " + str(maxval))
								fire_dict["qpf_max"].append( round(maxval,2) )
								fire_dict["qpf_mean"].append( round(meanval,2) )
								fire_dict["qpf_range"].append( round(rangeval,2) )
								fire_dict["qpf_sum"].append( round(sumval,2) )
								fire_dict["qpf_valid"].append( dt_init.strftime("%Y%m%d%H") )

							else:
								print("Unable to find: %s" % nbm_path)

						if os.path.exists(json_dir + "/" + "_".join(name_array[:3]).lower() + "_basin_60min_" + maxPrecipCategoryInch(run_maxval) + "in_probs.geojson"):
							fire_dict["perimeter"] = "_".join(name_array[:3]).lower() + "_basin_60min_" + maxPrecipCategoryInch(run_maxval) + "in_probs.geojson"

						fire_dict["run_qpf_max"] = { "valid" : run_maxtime , "value" : "%0.2f" % run_maxval }
						fire_dict["run_qpf_mean"] = { "valid" : run_meantime , "value" : "%0.2f" % run_meanval }
						fire_dict["run_qpf_range"] = { "valid" : run_rangetime , "value" : "%0.2f" % run_rangeval }
						fire_dict["run_qpf_sum"] = { "valid" : run_sumtime , "value" : "%0.2f" % run_sumval }
						
						chk_inf = np.isposinf(fire_dict["qpf_mean"])

						if True not in chk_inf:

							if len(fire_dict["qpf_max"]) > 0:
								print("Fire: " + fire_dict["name"] + " (" + str(fire_dict["year"]) + ")")
								precip_dict.append(fire_dict)

						else:
							print("Found Infinity -- Skipping %s" % fire_dict["name"])

					except Exception as err:
						print(traceback.format_exc())
						continue

		print("\n#----------------------------------------------------")
		print("# Processing active fire perimeters")
		print("#----------------------------------------------------\n")

		# check active fires	
		for root, dirs, files in os.walk(buffer_active_dir, topdown=True):
			for f in files:
				if f.endswith(".shp"):
					
					buffer_shp_path = os.path.join(root,f)
					perim_file = f.replace("10mi_buffer","perimeter")
					perim_shp_path = os.path.join(perim_active_dir,perim_file)

					try:
						# print("\nFound buffer shapefile: %s" % buffer_shp_path)

						name_array = f.split("_")

						json_buffer = f.replace(".shp",".geojson")
						json_fire = json_buffer.replace("10mi_buffer","perimeter")
						
						with open(os.path.join(json_active_dir,json_buffer)) as buffer_path:
							buffer_info = json.load(buffer_path)

						fire_dict = {}

						create_dt = buffer_info["features"][0]["properties"]["CreateDate"]
						create_dt_array = create_dt.split("/")
						unit_id = buffer_info["features"][0]["properties"]["UnitID"]

						fire_dict["year"] = create_dt_array[0]
						if unit_id is not None:
							fire_dict["state"] = unit_id[-2:]
						else:
							fire_dict["state"] = ""

						fire_dict["name"] = (buffer_info["features"][0]["properties"]["IncidentNa"]).title()
						fire_dict["perimeter"] = "active/" + json_fire
						fire_dict["buffer"] = "active/" + json_buffer
						fire_dict["coordinates"] = [buffer_info["features"][0]["properties"]["Center_Lat"], buffer_info["features"][0]["properties"]["Center_Lon"]]

						nbm_directory = nbm_data_dir + "/" + dt.strftime("%Y%m%d") + "/" + dt.strftime("%H")
	
						fire_dict["qpf_max"] = []
						fire_dict["qpf_mean"] = []
						fire_dict["qpf_range"] = []
						fire_dict["qpf_sum"] = []
						fire_dict["qpf_valid"] = []

						# initialize full run variables
						run_maxval = 0
						run_meanval = 0
						run_rangeval = 0
						run_sumval = 0

						add_dict = True

						for i in range(1,complete_count+1):

							dt_init = dt + datetime.timedelta(hours=i)
							
							if i == 1:
								if i == 1:
									run_maxtime = dt_init.strftime("%Y%m%d%H")
									run_meantime = dt_init.strftime("%Y%m%d%H")
									run_rangetime = dt_init.strftime("%Y%m%d%H")
									run_sumtime = dt_init.strftime("%Y%m%d%H")
									
							nbm_file = "nbm.qpf.%s.tif" % (dt_init.strftime("%Y%m%d%H"))
							nbm_path = nbm_directory + "/" + nbm_file

							if os.path.exists(nbm_path):
														
								# Calculate overlapping area for buffer and NBM
								stats = zonal_stats(buffer_shp_path,nbm_path,nodata=-9999,stats=['max','mean','range','sum'])

								if stats[0] and stats[0]['sum'] is not None:
									if np.isinf(float(stats[0]['mean'])):
										add_dict = False
									if np.isinf(float(stats[0]['sum'])):
										add_dict = False
								
								# Summarize PRATE by polygon
								maxval = 0
								meanval = 0
								rangeval = 0
								sumval = 0

								if stats[0] and stats[0]['sum'] is not None:
									if stats[0]['max'] > 0:
										maxval = stats[0]['max']
									if stats[0]['mean'] > 0:
										meanval = stats[0]['mean']
									if stats[0]['range'] > 0:
										rangeval = stats[0]['range']
									if stats[0]['sum'] > 0:
										sumval = stats[0]['sum']

								if maxval > run_maxval:
									run_maxval = maxval
									run_maxtime = dt_init.strftime("%Y%m%d%H")

								if meanval > run_meanval:
									run_meanval = meanval
									run_meantime = dt_init.strftime("%Y%m%d%H")

								if rangeval > run_rangeval:
									run_rangeval = rangeval
									run_rangetime = dt_init.strftime("%Y%m%d%H")

								if sumval > run_sumval:
									run_sumval = sumval
									run_sumtime = dt_init.strftime("%Y%m%d%H")

								# print("Max in: " + str(maxval))
								fire_dict["qpf_max"].append( round(maxval,2) )
								fire_dict["qpf_mean"].append( round(meanval,2) )
								fire_dict["qpf_range"].append( round(rangeval,2) )
								fire_dict["qpf_sum"].append( round(sumval,2) )
								fire_dict["qpf_valid"].append( dt_init.strftime("%Y%m%d%H") )

							else:
								print("Unable to find: %s" % nbm_path)

						# if os.path.exists(json_active_dir + "/" + "_".join(name_array[:3]).lower() + "_basin_60min_" + maxPrecipCategoryInch(run_maxval) + "in_probs.geojson"):
						# 	fire_dict["perimeter"] = "_".join(name_array[:3]).lower() + "_basin_60min_" + maxPrecipCategoryInch(run_maxval) + "in_probs.geojson"

						fire_dict["run_qpf_max"] = { "valid" : run_maxtime , "value" : "%0.2f" % run_maxval }
						fire_dict["run_qpf_mean"] = { "valid" : run_meantime , "value" : "%0.2f" % run_meanval }
						fire_dict["run_qpf_range"] = { "valid" : run_rangetime , "value" : "%0.2f" % run_rangeval }
						fire_dict["run_qpf_sum"] = { "valid" : run_sumtime , "value" : "%0.2f" % run_sumval }
						
						chk_inf = np.isposinf(fire_dict["qpf_mean"])

						if True not in chk_inf:

							if len(fire_dict["qpf_max"]) > 0:
								print("Fire: " + fire_dict["name"] + " (" + str(fire_dict["year"]) + ")")
								precip_dict.append(fire_dict)

						else:
							print("Found Infinity -- Skipping %s" % fire_dict["name"])

					except Exception as err:
						print(traceback.format_exc())
						continue			

		# Write output to file
		if len(precip_dict) > 0:
			print("\nSaving output: %s" % json_file)
			with open(json_file, "w") as outfile:
				json.dump(precip_dict, outfile,indent=4)
		else:
			print("No data to save: %s" % json_file)
	else:
		print("QPF output already exists for %sZ" % dt.strftime("%b %d, %Y %H"))

	os.system("/usr/bin/chmod -R 755 %s" % data_dir)	

	# print("\n#----------------------------------------------------")
	# print("# Finished generating NBM 15-min accumulation output.")
	# print("#----------------------------------------------------\n")

#-------------------------------------------------------
# Check if most of run is available before processing
#------------------------------------------------------
def shouldProcess(dt_input):

	res = { "proceed": False , "reason": "N/A" }

	tiff_count = 0
	nbm_directory = nbm_data_dir + "/" + dt_input.strftime("%Y%m%d") + "/" + dt_input.strftime("%H")

	for i in range(1,complete_count+1):
	
		dt_chk = dt_input + datetime.timedelta(hours=i)
									
		nbm_file = "nbm.qpf.%s.tif" % (dt_chk.strftime("%Y%m%d%H"))
		nbm_path = nbm_directory + "/" + nbm_file

		if os.path.exists(nbm_path):
			tiff_count += 1

	if tiff_count >= (complete_count * 0.75):
		res["proceed"] = True

	if res["proceed"]:

		json_file = data_dir + "/nbm." + dt_input.strftime("%Y%m%d%H") + ".json"
		file_count = 0

		if os.path.exists(json_file):

			with open(json_file) as jfile:
				data = json.load(jfile)
			
			file_count = len(data[0]["qpf_valid"])

			if file_count < tiff_count and process_again:
				res["proceed"] = True
			else:
				res["proceed"] = False
				res["reason"] = "nbm QPF stats already complete"
	else:
		res["reason"] = "Insufficient nbm GeoTiff files (%d/%d)" % (tiff_count, complete_count)

	return res

#-------------------------------------------------------
# Remove old data 
#------------------------------------------------------
def removeOldData():
	"""
	this function will remove data older than keep_hours hours
	"""
	keep_hours = 36

	print('\n#-------------------------------------------------------')
	print("# Removing data older than %d hours" % keep_hours)
	print('#------------------------------------------------------')

	now = datetime.datetime.utcnow()

	print("\nLooking for old QPF output...")
	for hr in range(keep_hours+1, 169):
		dt_remove = now - datetime.timedelta(hours=hr)

		json_file = "%s/nbm.%s.json" % (data_dir, dt_remove.strftime("%Y%m%d%H"))

		if os.path.exists(json_file):
			print("Removing json file: %s" % json_file)
			os.remove(json_file)				


def main():
	
	start = datetime.datetime.utcnow()
	print("\nScript executed at " + start.strftime("%a %b %d, %Y %H:%M:%S Z\n"))

	if len(sys.argv) > 1:

		init_yr = int(sys.argv[1])
		init_mo = int(sys.argv[2])
		init_dy = int(sys.argv[3])
		init_hr = int(sys.argv[4])
		dt_start = datetime.datetime(init_yr, init_mo, init_dy, init_hr, 0)		

		for lookback in range(0, 6):
	
			dt_init = dt_start - datetime.timedelta(hours=lookback)

			try:
					
				result = shouldProcess(dt_init)
				
				if result["proceed"]:
					findMaxQPFAmount(dt_init)
				else:
					print("\n##########################################################")
					print("## ")
					print("## Do not process run valid: %s" % dt_init.strftime("%Y-%m-%d %HZ"))
					print("## Reason: %s" % result["reason"])
					print("## ")
					print("##########################################################\n")
				
			except Exception as err:
				print(traceback.format_exc())
				continue

	else:

		json_files = glob.glob(nbm_images_dir + "/*.json")
		json_files.sort(reverse=True)

		count = 0

		for path in json_files:

			if count < 7:
				
				path_array = path.split("/")
				filename = path_array[-1:][0]
				file_array = filename.split(".")
				dt_string = file_array[1]
				dt_year = int(dt_string[0:4])
				dt_month = int(dt_string[4:6])
				dt_day = int(dt_string[6:8])
				dt_hour = int(dt_string[8:10])

				dt_init = datetime.datetime(dt_year, dt_month, dt_day, dt_hour)

				# dt_init = datetime.datetime(2020,10,30,0,0)

				try:
					
					result = shouldProcess(dt_init)
					
					if result["proceed"]:
						findMaxQPFAmount(dt_init)
					else:
						print("\n##########################################################")
						print("## ")
						print("## Do not process run valid: %s" % dt_init.strftime("%Y-%m-%d %HZ"))
						print("## Reason: %s" % result["reason"])
						print("## ")
						print("##########################################################\n")
					
				except Exception as err:
					print(traceback.format_exc())
					continue

			count += 1	

	removeOldData()

	print("\nRsyncing data to web...")
	rsync_cmd = "/usr/bin/rsync --delete --update -azvh %s/ chad.kahler@rsync3:/export/vhosts/dev/html/wrh/debrisflow/qpf/data/nbm/json/" % (data_dir)
	os.system(rsync_cmd)
	rsync_cmd = "/usr/bin/rsync --delete --update -azvh %s/ chad.kahler@rsync3:/export/vhosts/dev/html/wrh/debrisflow/qpf/data/nbm/images/" % nbm_images_dir
	os.system(rsync_cmd)

	rsync_cmd = "/usr/bin/rsync --delete --update -azvh %s/ chad.kahler@rsync3:/export/vhosts/www/html/wrh/debrisflow/qpf/data/nbm/json/" % (data_dir)
	os.system(rsync_cmd)
	rsync_cmd = "/usr/bin/rsync --delete --update -azvh %s/ chad.kahler@rsync3:/export/vhosts/www/html/wrh/debrisflow/qpf/data/nbm/images/" % nbm_images_dir
	os.system(rsync_cmd)
	
	end = datetime.datetime.utcnow()
	print("\nScript completed at " + end.strftime("%a %b %d, %Y %H:%M:%S Z"))
	diff_minute = (end-start).total_seconds()/60
	print("Script execution: %.2f" % diff_minute + " minutes\n")

if __name__ == "__main__":
    main()
