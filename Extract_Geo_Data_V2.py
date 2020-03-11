#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Nov 12 20:08:59 2019

@author: pnagula
"""

import ogr
from osgeo import osr
import pandas as pd
import os
from sqlalchemy import create_engine
from sqlalchemy import Float, Integer, String, Date

def Extract_GEO_Data(ifname,ext):
    if ext=='shp':
       driver = ogr.GetDriverByName('ESRI Shapefile')
    elif ext=='gdb':
       driver = ogr.GetDriverByName('OpenFileGDB')
       
    shp = driver.Open(ifname, 0) # 0 means read-only. 1 means writeable.

    if shp is None:
       print('Could not Open')
       return 0
    else:   
       layer = shp.GetLayer()
       layerDefinition = layer.GetLayerDefn()
       sr = layer.GetSpatialRef()  # get projected spatial reference
       ### if projection is not SVY21:EPSG 3414 then transform projected spatial reference to SVY21:EPSG3414 
       ### which is singapore projected coordinate syste
       proj_to_svy21=None
       if sr: 
          if sr.GetAttrValue('projcs')!='SVY21':
             outSpatialRef = osr.SpatialReference()
             res=outSpatialRef.ImportFromEPSG(3414)
             if res!=0:
                print('Not able to import EPSG 3414') 
             proj_to_svy21=osr.CoordinateTransformation(sr, outSpatialRef) #define reprojection
          geogr_sr = sr.CloneGeogCS()    # get geographic spatial reference 
          proj_to_geog = osr.CoordinateTransformation(sr, geogr_sr) # define reprojection (this is to convert projected XY to long,lat)
      
       fieldName=[]
       for i in range(layerDefinition.GetFieldCount()):
           fieldName.append(layerDefinition.GetFieldDefn(i).GetName().lower())
      
       rlist=[]
       for feature in layer:        ## row loop over layers (layers are rows)
           clist=[]
           for fldname in fieldName:            ## column loop to collect all columns in the row
               fldvalue=feature.GetField(fldname)
               clist.append(fldvalue)
           area_shape = feature.GetGeometryRef() ## get geometry object ref
           if proj_to_svy21:   ## if projection is not SVY21:EPSG3414 then transform to SVY21:EPSG3414
              area_shape.Transform(proj_to_svy21) #transform projection to SVY21:EPSG3414
           clist.append(area_shape.GetGeometryName()) 
           clist.append(str(area_shape)) ##add geometry object to column
           if sr:
              area_shape.Transform(proj_to_geog) #transform projection to geographic coordainates WGS84
              clist.append(str(area_shape)) ## add geo transformed geometry object to column
           rlist.append(clist) ## add a row to outputlist
       
       fieldName.append('geomtype')    
       fieldName.append('geomobj')
       if sr:
          fieldName.append('geomobj_longlat')
       
       featdf=pd.DataFrame(rlist,columns=fieldName) 
       return featdf
     
if __name__ == '__main__':
   directories=[
                '/Users/pnagula/Downloads/Projects/Govtech_NEA/Data/HDB_UnderConstruct_Blk_P-Jan2015/',
                '/Users/pnagula/Downloads/Projects/Govtech_NEA/Data/Geospace/HDB_DRAIN_OUTLINE/',
                '/Users/pnagula/Downloads/Projects/Govtech_NEA/Data/Geospace/MP14_LAND_USE/',
                '/Users/pnagula/Downloads/Projects/Govtech_NEA/Data/Geospace/PUB_DRAINOUTLINE/',
                '/Users/pnagula/Downloads/Projects/Govtech_NEA/Data/Geospace/PUB_SEW_LINE/',
                '/Users/pnagula/Downloads/Projects/Govtech_NEA/Data/Geospace/PUB_SEW_MANHOLE_CHAMBER/',
                '/Users/pnagula/Downloads/Projects/Govtech_NEA/Data/Geospace/SingaporeMap_Polygon/',
                '/Users/pnagula/Downloads/Projects/Govtech_NEA/Data/Geospace/TC_NParks_greenery/',
                '/Users/pnagula/Downloads/Projects/Govtech_NEA/Data/Geospace/Topographic_Map/',
                '/Users/pnagula/Downloads/Projects/Govtech_NEA/Data/NParks/HistoricalUCdata.shp/',
                '/Users/pnagula/Downloads/Projects/Govtech_NEA/Data/PUB/ConstructionSite_Sewerage/',
                '/Users/pnagula/Downloads/Projects/Govtech_NEA/Data/PUB/CWD_drainage_project/',
                '/Users/pnagula/Downloads/Projects/Govtech_NEA/Data/PUB/'
                
                ]
   tablenames=[
               'hdb_underconstruct_blk_p_jan2015',
               'hdb_drain_outline',
               'mp14_land_use',
               'pub_drainoutline',
               'pub_sew_line',
               'pub_sew_manhole_chamber',
               'singaporemap_polygon',
               'tc_nparks_greenery',
               'topographic_map',
               'nparks_uc_parks_hist',
               'underconstruction_facilities_hist',
               'nparks_uc_pc_hist',
               'construction_sewerage',
               'construction_pumpingmain',
               'cwd_drainage_project',
               'cwd'
               ]    
   engine = create_engine('postgresql://pnagula:work4Pivotal@localhost:5432/neadb')
   dtypes=[
           {"blk_no":String,"st_cod":String,"entityid":Integer,"postal_cod":Integer,"geomtype":String,"geomobj":String},
           {"entityid":Integer,"inc_crc":String,"fmel_upd_d":Date,"geomtype":String,"geomobj":String,"geomobj_longlat":String},
           {"lu_desc":String,"lu_text":String,"gpr":String,"whi_q_mx":Integer,"gpr_b_mn":Integer,"inc_crc":String,"fmel_upd_d":Date,"geomtype":String,"geomobj":String,"geomobj_longlat":String},
           {"inc_crc":String,"fmel_upd_d":Date,"geomtype":String,"geomobj":String,"geomobj_longlat":String},
           {"g3e_fid":Integer,"dia":Integer,"mlen":Float,"state":Integer,"yr_comm":Date,"fty_type":String,"inc_crc":String,"fmel_upd_d":Date,"geomtype":String,"geomobj":String,"geomobj_longlat":String},
           {"g3e_fid":Integer,"state":Integer,"gl":Float,"il":Integer,"fty_type":String,"accu_sta":String,"inc_crc":String,"fmel_upd_d":Date,"geomtype":String,"geomobj":String,"geomobj_longlat":String},
           {"name":String,"folderpath":String,"symbolid":Integer,"inc_crc":String,"fmel_upd_d":Date,"geomtype":String,"geomobj":String,"geomobj_longlat":String},
           {"loc_desc":String,"owner":String,"ma":String,"maname":String,"inc_crc":String,"fmel_upd_d":Date,"geomtype":String,"geomobj":String,"geomobj_longlat":String},
           {"raster":String,"name":String,"shape_length":Float,"shape_area":Float,"geomtype":String,"geomobj":String,"geomobj_longlat":String},
           {"infra_name":String,"infra_desc":String,"developmen":String,"est_start_":Date,"est_end_co":Date,"owner":String,"agency_qsm":String,"contractor":String,"Date_of_la":Date,"infra_type":String,"shape_leng":Float,"shape_area":Float,"geomtype":String,"geomobj":String,"geomobj_longlat":String},
           {"infra_name":String,"infra_desc":String,"developmen":String,"est_start_":Date,"est_end_co":Date,"owner":String,"agency_qsm":String,"contractor":String,"Date_of_la":Date,"infra_type":String,"shape_leng":Float,"shape_area":Float,"geomtype":String,"geomobj":String,"geomobj_longlat":String},
           {"infra_name":String,"infra_desc":String,"developmen":String,"est_start_":Date,"est_end_co":Date,"owner":String,"agency_qsm":String,"contractor":String,"Date_of_la":Date,"infra_type":String,"shape_leng":Float,"geomtype":String,"geomobj":String,"geomobj_longlat":String},
           {"objectid":Integer,"g3e_fid":Integer,"dia":Integer,"mlen":Float,"sn":Integer,"works_cont":String,"contract_n":Integer,"start_Date":Date,"scheduled_":Date,"extended_d":Date,"actual_dat":Date,"shape_leng":Float,"geomtype":String,"geomobj":String,"geomobj_longlat":String},
           {"objectid":Integer,"g3e_fid":Integer,"dia":Integer,"mlen":Float,"enabled":Integer,"sn":Integer,"works_cont":String,"contract_n":Integer,"start_Date":Date,"scheduled_":Date,"extended_d":Date,"actual_dat":Date,"shape_leng":Float,"geomtype":String,"geomobj":String,"geomobj_longlat":String},
           {"objectid":Integer,"proj_title":String,"proj_perio":String,"status":String,"proj_end_d":Date,"proj_start":Date,"proj_bq":String,"globalid":String,"shape_leng":Float,"geomtype":String,"geomobj":String,"geomobj_longlat":String},
           {"proj_title":String,"proj_period":String,"status":String,"proj_end_Date":Date,"proj_start_Date":Date,"proj_bq":String,"globalid":String,"shape_length":Float,"geomtype":String,"geomobj":String,"geomobj_longlat":String}
          ]
   ix=0
   for file in directories:
       for file1 in os.listdir(file): 
              ifname=file+file1
              if file1.endswith('.shp'):
                 print(ifname)
                 featdf=Extract_GEO_Data(ifname,'shp')
                # if ifname.find('hdb_underconstruct_blk')!=-1:
                #    dtyp=dtypes[0] 
                 featdf.to_sql(tablenames[ix], engine,if_exists='replace',index=False,dtype=dtypes[ix])
                 #else:
                 #   dtyp=dtypes[ix] 
                 #   featdf.to_sql(tablenames[ix], engine,if_exists='replace',index=False,dtype=dtyp)
                 ix+=1
              elif file1.endswith('.gdb'):
                 print(ifname)
                 featdf=Extract_GEO_Data(ifname,'gdb')
                 featdf.to_sql(tablenames[ix], engine,if_exists='replace',index=False,dtype=dtypes[ix])
                 ix+=1
          

