 #!/usr/local/anaconda3/envs/py37/bin/python

"""-------------------------------------------------------------
	Script Name: 	get_nbm_hourly.py
	Description: 	Downloads NBM hourly data 
	Author: 		Chad Kahler (WRH STID)
	Date:			11/9/18
-------------------------------------------------------------"""

#---------------------------------------------------------------
# Import python packages
#---------------------------------------------------------------
import os, sys, datetime, time, shutil, traceback, pycurl, json
from osgeo import gdal
 
#-------------------------------------------------------
# Global configuration options
#-------------------------------------------------------
current_dir = os.path.dirname(os.path.realpath(sys.argv[0]))
nbm_dir = "/nas/stid/data/nbm"
data_dir = current_dir + "/data/nbm"
geotiff_dir = data_dir + "/geotiff"
images_dir = data_dir + "/images"

#-------------------------------------------------------
# Global configuration options
#-------------------------------------------------------
num_hrs = 36
# dt = datetime.datetime.utcnow() - datetime.timedelta(hours=1)
# dt_datestring = dt.strftime("%Y%m%d")
process_again = False
files = []

#------------------------------------------------------
# Reproject, Re-Calculate and Convert as NetCDF
#------------------------------------------------------
def convertToRaster(dt):
	"""
    this function reproject, re-calculate, and convert to netcdf
    """

	date_dir = dt.strftime("%Y%m%d")
	hour_dir = dt.strftime("%H")

	nbm_path = nbm_dir + "/" + date_dir + "/" + hour_dir + "Z"
	geotiff_path = geotiff_dir + "/" + date_dir + "/" + hour_dir

	if os.path.exists(nbm_path):

		print("\nConverting NBM Grib2 files to GeoTiffs...")

		# make geotiff directories if they don't exist
		if not os.path.exists(geotiff_dir + "/" + date_dir):
			os.makedirs(geotiff_dir + "/" + date_dir)
		if not os.path.exists(geotiff_dir + "/" + date_dir + "/" + hour_dir):
			os.makedirs(geotiff_dir + "/" + date_dir + "/" + hour_dir)
		
		# make image directories if they don't exist
		if not os.path.exists(images_dir + "/" + date_dir):
			os.makedirs(images_dir + "/" + date_dir)
		if not os.path.exists(images_dir + "/" + date_dir + "/" + hour_dir):
			os.makedirs(images_dir + "/" + date_dir + "/" + hour_dir)

		for fhr in range(1,37):

			fcst_time = dt + datetime.timedelta(hours=fhr)
			final_tif = geotiff_path + "/nbm.qpf.%s.tif" % fcst_time.strftime("%Y%m%d%H")

			if not os.path.exists(final_tif) or process_again:

				nbm_file = "blend.t%02dz.core.f%03d.co.grib2" % (int(dt.strftime("%H")), fhr)
				nbm_fullpath = os.path.join(nbm_path, nbm_file)

				if os.path.exists(nbm_fullpath):

					print("Processing fcst hour: %d " % fhr)

					try:

						nbm_raster = gdal.Open(nbm_fullpath, gdal.GA_ReadOnly)
						
						for lyr in range(1,nbm_raster.RasterCount+1):
							qpf = nbm_raster.GetRasterBand(lyr)
							meta = qpf.GetMetadata()
							
							## EXTRACT QPF PARAMETER
							if meta['GRIB_ELEMENT'] == "QPF01":

								tmp_geotiff = os.path.join(geotiff_path, "nbm.qpf.%s.tmp.tif" % fcst_time.strftime("%Y%m%d%H"))
								cmd = "/home/chad.kahler/anaconda3/envs/py37/bin/gdal_translate -b %d %s -of GTiff %s > /dev/null 2>&1" % (lyr, nbm_fullpath, tmp_geotiff)
								os.system(cmd)

								if os.path.exists(tmp_geotiff):
									
									## REPROJECT AND CUT OUT SUBREGION
									proj_geotiff = tmp_geotiff.replace("tmp","proj")
									gdal.Warp(proj_geotiff, tmp_geotiff, dstSRS='EPSG:4326', outputBounds=[-128.,25.,-100.,55.], outputBoundsSRS='EPSG:4326')
									# cmd_proj = "/home/chad.kahler/anaconda3/envs/py37/bin/gdalwarp -overwrite %s %s -te -128. 25. -100. 55. -t_srs EPSG:4326 > /dev/null 2>&1" % (tmp_geotiff, proj_geotiff)
									# os.system(cmd_proj)

									if os.path.exists(proj_geotiff):

										## THIS REPROJECTION IS ONLY FOR WEB PURPOSES -- COULD IGNORE
										proj2_geotiff = proj_geotiff.replace("proj","proj2")
										gdal.Warp(proj2_geotiff, proj_geotiff, dstSRS='EPSG:3857')
										# cmd_proj2 = "/home/chad.kahler/anaconda3/envs/py37/bin/gdalwarp -overwrite %s %s -t_srs EPSG:3857 > /dev/null 2>&1" % (proj_geotiff, proj2_geotiff)
										# os.system(cmd_proj2)

										if os.path.exists(proj2_geotiff):

											## CONVERT TO INCHES
											cmd = "/home/chad.kahler/anaconda3/envs/py37/bin/gdal_calc.py -A " + proj2_geotiff + " --outfile=" + final_tif + " --calc='round_((A*0.0393701),2)' --quiet --overwrite"
											os.system(cmd)

											if os.path.exists(final_tif):
												# print("Successfully created: %s" % calculated_tif)
												if os.path.exists(tmp_geotiff):
													os.remove(tmp_geotiff)
												if os.path.exists(proj_geotiff):
													os.remove(proj_geotiff)
												if os.path.exists(proj2_geotiff):
													os.remove(proj2_geotiff)
					except Exception as err:
						print(traceback.format_exc())
						continue								
				else:
					print("NBM Grib2 file not available for fcst hour: %d" % fhr)
			else:
				print("NBM GeoTiff already exists for fcst hour: %d" % fhr)
	else:
		print("\nNBM Grib2 data not available for %sZ" % dt.strftime("%b %d, %Y, %H"))

#-------------------------------------------------------
# Convert file size to useful units
#------------------------------------------------------
def convertBytes(num):
    """
    this function will convert bytes to MB.... GB... etc
    """
    for x in ['bytes', 'KB', 'MB', 'GB', 'TB']:
        if num < 1024.0:
            return "%3.1f %s" % (num, x)
        num /= 1024.0

#-------------------------------------------------------
# Check file size and return in useful units
#------------------------------------------------------
def fileSize(file_path):
    """
    this function will return the file size
    """
    if os.path.isfile(file_path):
        file_info = os.stat(file_path)
        return convertBytes(file_info.st_size)

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

			print('\n#-------------------------------------------------------')
			print("# +++ CUSTOM DATE+++ ")
			print("# Processing NBM QPF initialized: %sZ" % dt_init.strftime("%b %d, %Y %H"))
			print('#------------------------------------------------------')

			convertToRaster(dt_init)
			

	else:

		for lookback in range(1,7):
		
			dt_start = datetime.datetime.utcnow() - datetime.timedelta(hours=lookback)
			dt_init = dt_start.replace(minute=0,second=0)

			print('\n#-------------------------------------------------------')
			print("# Processing NBM QPF initialized: %sZ" % dt_init.strftime("%b %d, %Y %H"))
			print('#------------------------------------------------------')

			convertToRaster(dt_init)


	os.system("/usr/bin/chmod -R 755 %s" % images_dir)

	end = datetime.datetime.utcnow()
	print("\nScript completed at " + end.strftime("%a %b %d, %Y %H:%M:%S Z"))
	diff_minute = (end-start).total_seconds()/60
	print("Script execution: %.2f" % diff_minute + " minutes\n")

if __name__ == "__main__":
    main()
