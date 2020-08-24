library(tidyverse)
library(RPostgreSQL)
library(dbplyr)
library(readr)
library(lubridate)
library(pool)
library(naniar)
library(rlist)
library(missRanger)
library(feather)
library(readxl)
library(furrr)
library(geosphere)

library(RPostgreSQL)
library(postGIStools)
library(sf)

library(purrr)

library(postmastr)


st_drop_geometry <- function(x) {
  if(inherits(x,"sf")) {
    x <- st_set_geometry(x, NULL)
    class(x) <- 'data.frame'
  }
  return(x)
}



no_cores <- availableCores()
plan(multisession, workers = no_cores-1)



calc_correlation <- function(index,df_left,df_right){
  left_index <- index[1]
  right_index <- index[2]
  value <- cor.test(df_left[[left_index]],df_right[[right_index]],method = 'spearman')
  
  return(data.frame(original_fact_identification=left_index, new_fact_identification =right_index,correlation_value=value$estimate ))
}

append_acs_fn <- function(new_df, base_df, correlation_cutoff){
  
  df <- new_df
  
  new_names <- names(df)[-1]
  
  
  base_names <- names(base_df)[-1]
  
  list <- map(cross2(base_names, new_names), unlist)
  
  
  correlation_df <- furrr::future_map_dfr(.x=list,.f=calc_correlation,df_left=base_df,df_right=df)
  
  columns_remove <- correlation_df %>% filter(abs(correlation_value) >= correlation_cutoff) %>% dplyr::select(new_fact_identification) %>% unique()
  
  columns_remove <- columns_remove %>% mutate_all(as.character)
  
  if(dim(columns_remove)[1] > 0){
    df <- df %>% dplyr::select(-one_of(columns_remove$new_fact_identification))
  }
  
  
  return(df)
  
}





acs_retrieve_fn <- function(fact_identification, startyear, endyear, metadata, shape_id_list,credentials,
                            flag_linear_dependence, summarylevelid){
  
  pool <- dbPool(drv = RPostgreSQL::PostgreSQL(),
                 dbname=credentials['dbname'],
                 host=credentials['host'],
                 port=credentials['port'],
                 user = credentials['user'],
                 password = credentials['password'])
  
  
  fact_identification_val <- fact_identification
  metadata <- metadata %>% filter(fact_identification==fact_identification_val)
  denom_column <- metadata$denominator_column[1]
  data_id <- as.integer(metadata$data_id[1])
  
  
  
  if(is.na(denom_column)) {
    df <- acs_retrieve_no_denominator(fact_identification, pool, startyear, endyear, metadata, shape_id_list,flag_linear_dependence, summarylevelid)
  }
  else{
    df <- acs_retrieve_denominator(fact_identification, pool, startyear, endyear, metadata, shape_id_list,flag_linear_dependence, summarylevelid)
    
  }
  
  
  poolClose(pool)
  
  
  return(df)
}

acs_retrieve_denominator <- function(fact_identification, pool, startyear, endyear, metadata, shape_id_list,
                                     flag_linear_dependence, summarylevelid){
  shapefile_startdate <- paste0(endyear,'-01-01')
  facttable_startdate <- paste0(startyear,'-01-01 00:00:00')
  facttable_enddate <- paste0(endyear,'-12-31 00:00:00')
  shapefile_enddate <- paste0(endyear,'-12-31')
  
  
  
  
  fact_identification_val <- fact_identification
  metadata <- metadata %>% filter(fact_identification==fact_identification_val)
  denom_column <- metadata$denominator_column[1]
  data_id <- as.integer(metadata$data_id[1])
  
  
  
  query <-
    "
  select a.shape_id,
  (jsonb_each(data)).key::VARCHAR as key,
  (jsonb_each(data)).value::VARCHAR as value
  from exposome_pici.shapefile a
  inner join exposome_pici.facttable_acs b on (a.shape_id=b.shape_id)
  where a.summarylevelid = ?summarylevelid
  and a.startdate = ?shapefile_startdate
  and a.enddate = ?shapefile_enddate
  and b.data_id = ?data_id
  and b.startdate = ?facttable_startdate
  and b.enddate = ?facttable_enddate
  "
  
  query_interpolate <- sqlInterpolate(pool,
                                      query,
                                      data_id = data_id,
                                      shapefile_startdate=shapefile_startdate,
                                      shapefile_enddate=shapefile_enddate,
                                      facttable_startdate=facttable_startdate,
                                      facttable_enddate=facttable_enddate,
                                      summarylevelid=summarylevelid)
  
  
  head(query_interpolate)
  
  df <- dbGetQuery(pool, query_interpolate) %>% spread(key,value)
  
  df <- df %>% rename(denominator=!!denom_column)
  df <- df %>% mutate_at(vars(-shape_id), as.numeric)
  df <- df %>%  filter(denominator >0 )
  
  
  if(flag_linear_dependence == 'TRUE'){
    if(dim(df)[2] > 2){
      comboInfo <- findLinearCombos(df)
      
      if(!is.null(comboInfo$remove)){
        df <- df[, -comboInfo$remove]
      }
      
    }
    
  }
  
  
  
  df <- df %>%  mutate_at(vars(-shape_id,-denominator), ~./denominator) %>% dplyr::select(-denominator)
  
  
  
  
  df <- shape_id_list %>% left_join(df, by='shape_id')
  
  return(df)
  
  
}



acs_retrieve_no_denominator <- function(fact_identification, pool, startyear, endyear, metadata, shape_id_list,
                                        flag_linear_dependence, summarylevelid){
  shapefile_startdate <- paste0(endyear,'-01-01')
  facttable_startdate <- paste0(startyear,'-01-01 00:00:00')
  facttable_enddate <- paste0(endyear,'-12-31 00:00:00')
  shapefile_enddate <- paste0(endyear,'-12-31')
  
  
  fact_identification_val <- fact_identification
  metadata <- metadata %>% filter(fact_identification==fact_identification_val)
  denom_column <- metadata$denominator_column[1]
  data_id <- as.integer(metadata$data_id[1])
  
  
  
  query <-
    "
  select a.shape_id,
  (jsonb_each(data)).key::VARCHAR as key,
  (jsonb_each(data)).value::VARCHAR as value
  from exposome_pici.shapefile a
  inner join exposome_pici.facttable_acs b on (a.shape_id=b.shape_id)
  where a.summarylevelid = ?summarylevelid
  and a.startdate = ?shapefile_startdate
  and a.enddate = ?shapefile_enddate
  and b.data_id = ?data_id
  and b.startdate = ?facttable_startdate
  and b.enddate = ?facttable_enddate
  "
  
  query_interpolate <- sqlInterpolate(pool,
                                      query,
                                      data_id = data_id,
                                      shapefile_startdate=shapefile_startdate,
                                      shapefile_enddate=shapefile_enddate,
                                      facttable_startdate=facttable_startdate,
                                      facttable_enddate=facttable_enddate,
                                      summarylevelid=summarylevelid)
  
  
  
  df <- dbGetQuery(pool, query_interpolate) %>% spread(key,value)
  
  df <- df %>% mutate_at(vars(-shape_id), as.numeric)
  
  
  
  if(flag_linear_dependence == 'TRUE'){
    
    if(dim(df)[2] > 2){
      
      comboInfo <- findLinearCombos(df %>% filter(complete.cases(.)))
      
      if(!is.null(comboInfo$remove)){
        df <- df[, -comboInfo$remove]
      }
      
    }
    
  }
  
  df <- shape_id_list %>% left_join(df, by='shape_id')
  
  return(df)
  
  
}


retrieve_non_base_data_acs_list <- function(pool,startyear,endyear,metadata,list_non_base,
                                            credentials_list, flag_linear_dependence, summarylevelid){
  
  shapefile_startdate <- paste0(endyear,'-01-01')
  shapefile_enddate <- paste0(endyear,'-12-31')
  facttable_startdate <- paste0(startyear,'-01-01 00:00:00')
  facttable_enddate <- paste0(endyear,'-12-31 00:00:00')
  
  
  query <-
    "
  select a.shape_id,
  a.geoid,
  st_area(a.geographywkt) as area_sq_km
  from exposome_pici.shapefile a
  where a.summarylevelid = ?summarylevelid
  and a.startdate = ?shapefile_startdate
  and a.enddate=?shapefile_enddate
  "
  
  
  query_interpolate <- sqlInterpolate(pool,
                                      query,
                                      shapefile_startdate=shapefile_startdate,
                                      shapefile_enddate=shapefile_enddate,
                                      summarylevelid=summarylevelid)
  
  
  
  shape_id_list_df_area <- dbGetQuery(pool, query_interpolate)
  
  
  shape_id_list_df <- shape_id_list_df_area %>% dplyr::select(shape_id)
  
  df_other <- furrr::future_map(.x=list_non_base$fact_identification,
                                .f=safely(acs_retrieve_fn),
                                startyear=startyear,
                                endyear=endyear,
                                metadata=metadata,
                                shape_id_list=shape_id_list_df,
                                credentials=credentials_list,
                                flag_linear_dependence=flag_linear_dependence,
                                summarylevelid=summarylevelid,
                                .progress=TRUE)
  
  list_transpose <- transpose(df_other)
  
  return(list_transpose)
  
}



retrieve_base_acs_df <- function(pool,startyear,endyear,metadata,list_non_base, credentials_list,
                                 flag_linear_dependence, summarylevelid){
  shapefile_startdate <- paste0(endyear,'-01-01')
  shapefile_enddate <- paste0(endyear,'-12-31')
  facttable_startdate <- paste0(startyear,'-01-01 00:00:00')
  facttable_enddate <- paste0(endyear,'-12-31 00:00:00')
  
  
  query <-
    "
  select a.shape_id,
  a.geoid,
  st_area(a.geographywkt) / 1000000.0 as area_sq_km
  from exposome_pici.shapefile a
  where a.summarylevelid = ?summarylevelid
  and a.startdate = ?shapefile_startdate
  and a.enddate=?shapefile_enddate
  "
  
  
  query_interpolate <- sqlInterpolate(pool,
                                      query,
                                      shapefile_startdate=shapefile_startdate,
                                      shapefile_enddate=shapefile_enddate,
                                      summarylevelid=summarylevelid)
  
  
  
  shape_id_list_df_area <- dbGetQuery(pool, query_interpolate)
  
  
  shape_id_list_df <- shape_id_list_df_area %>% dplyr::select(shape_id)
  
  acs_questions <- c('B01003','B02001','B03002','B19013','B15003','B17001','B23025',
                     'B25077','B25014','B19057','B19083','B27010','B01001','C17002')
  
  extra <-       c('C24010','B11016','B11017' )
  

  df_list <- furrr::future_map(.x=acs_questions,
                               .f=acs_retrieve_fn,
                               startyear=startyear,
                               endyear=endyear,
                               metadata=metadata,
                               shape_id_list=shape_id_list_df,
                               credentials=credentials_list,
                               flag_linear_dependence=flag_linear_dependence,
                               summarylevelid=summarylevelid,
                               .progress=TRUE)
  
  
  df <- df_list %>% reduce(left_join, by = c('shape_id'))
  
  
  
  
  df_processed <- df %>%
    dplyr::rename(total_population=B01003001) %>%
    rename(white_pct=B03002003,
           black_pct=B03002004,
           native_american_pct=B03002007,
           asian_pct=B03002006,
           hawaiian_pacific_islander_pct=B03002007,
           other_pct = B03002008,
           two_or_more_race_pct=B03002009,
           hispanic_pct=B03002012,
           median_household_income=B19013001
    ) %>%
    dplyr::select(-starts_with('B03003')) %>%
    mutate(college_pct = select(., B15003022:B15003025) %>% apply(1, sum, na.rm=TRUE)) %>%
    mutate(nohs_pct = select(., B15003002:B15003016) %>% apply(1, sum, na.rm=TRUE)) %>%
    dplyr::select(-starts_with('B15003')) %>%
    rename(total_below_poverty=B17001002) %>%
    dplyr::select(-starts_with('B17001')) %>%
    mutate(unemployed_pct=B23025005) %>%
    dplyr::select(-starts_with('B23025')) %>%
    dplyr::rename(median_home_value = B25077001) %>%
    mutate(more_than_one_occupant_per_room_pct= select(., B25014005:B25014007,B25014011:B25014013) %>% apply(1, sum, na.rm=TRUE)) %>%
    mutate(more_than_one_point_five_occupant_per_room_pct= select(., B25014006:B25014007,B25014012:B25014013) %>% apply(1, sum, na.rm=TRUE)) %>%
    mutate(more_than_two_occupant_per_room_pct= select(., B25014007,B25014013) %>% apply(1, sum, na.rm=TRUE)) %>%
    dplyr::select(-starts_with('B25014')) %>%
    rename(public_assistance_income=B19057002) %>%
    rename(gini_index=B19083001) %>%
    rename(pct_uninsured=B27010066) %>%
    mutate(pct_private_insurance = select(.,B27010004:B27010005,B27010011,B27010012,B27010014,B27010020,B27010021,
                                          B27010027,B27010028,B27010030,B27010036,B27010037,B27010043,B27010044,B27010045,B27010047,
                                          B27010053,B27010054,
                                          B27010059:B27010061,B27010063) %>% apply(1, sum, na.rm=TRUE)) %>%
    mutate(pct_medicare_insurance = select(., B27010006,B27010012,B27010013,B27010022,B27010028,B27010029,B27010038,B27010044,B27010045,B27010046,B27010055,B27010060,B27010061,B27010062) %>% apply(1, sum, na.rm=TRUE)) %>%
    mutate(pct_medicaid_insurance = select(., B27010007,B27010013,B27010023,B27010029,B27010039,B27010046,B27010062) %>% apply(1, sum, na.rm=TRUE)) %>%
    mutate(pct_military_va_insurance = select(., B27010008, B27010024, B27010040, B27010056,B27010009, B27010025, B27010041, B27010057 ) %>% apply(1, sum, na.rm=TRUE)) %>%
    mutate(pct_private_and_medicare_insurance = select(., B27010012,B27010028,B27010044,B27010045,B27010060,B27010061) %>% apply(1, sum, na.rm=TRUE)) %>%
    mutate(pct_medicare_and_medicare_insurance = select(., B27010013,B27010029,B27010046,B27010062) %>% apply(1, sum, na.rm=TRUE)) %>%
    dplyr::select(-starts_with('B27010')) %>%
    mutate(over_65_all_pct= select(., B01001020:B01001025,B01001044:B01001049) %>% apply(1, sum, na.rm=TRUE)) %>%
    mutate(over_65_male_pct= select(.,B01001020:B01001025) %>% apply(1, sum, na.rm=TRUE)) %>%
    mutate(over_65_female_pct= select(., B01001044:B01001049) %>% apply(1, sum, na.rm=TRUE)) %>%
    mutate(under_19_all_pct= select(., B01001003:B01001007,B01001027:B01001031) %>% apply(1, sum, na.rm=TRUE)) %>%
    mutate(under_19_male_pct= select(.,B01001003:B01001007) %>% apply(1, sum, na.rm=TRUE)) %>%
    mutate(under_19_female_pct= select(., B01001027:B01001031) %>% apply(1, sum, na.rm=TRUE)) %>%
    dplyr::select(-starts_with('B01001')) %>%
    mutate(poverty_under_100_pct= select(., C17002002:C17002003) %>% apply(1, sum, na.rm=TRUE)) %>%
    mutate(poverty_under_100_150_pct= select(., C17002004:C17002005) %>% apply(1, sum, na.rm=TRUE)) %>%
    mutate(poverty_under_150_200_pct= select(., C17002006:C17002007) %>% apply(1, sum, na.rm=TRUE)) %>%
    mutate(poverty_under_150_pct= select(., C17002002:C17002005) %>% apply(1, sum, na.rm=TRUE)) %>%
    mutate(poverty_under_200_pct= select(., C17002002:C17002007) %>% apply(1, sum, na.rm=TRUE)) %>%
    dplyr::select(-starts_with('C17002')) 
  
  
  
  
  
  
  
  
  
  
  
  
  return(list(base_df=df_processed, shape_id_list_df_area=shape_id_list_df_area))
}



merge_base_non_base_df <- function(acs_base,
                                   list_non_base,
                                   shape_id_list_df_area,
                                   correlation_cutoff,
                                   pct_complete_row){
  
  shape_id_list_df <- shape_id_list_df_area %>% dplyr::select(shape_id)
  
  for (i in list_non_base$result){
    if(!is.null(i))
    {
      subset <- i %>% filter(complete.cases(.))
      num_complete <- dim(subset)[1]
      num_all <- dim(i)[1]
      num_cols <- dim(i)[2]
      if (num_complete/num_all > pct_complete_row & num_cols >= 2){
        print(names(i))
        new_df <- append_acs_fn(i,acs_base, correlation_cutoff )
        acs_base <- acs_base %>% left_join(new_df, by='shape_id')
      }
    }
  }
  
  acs_base <- shape_id_list_df_area %>% left_join(acs_base, by='shape_id')
  
  return(acs_base)
  
}



credentials <- list(dbname = '',
                    host="",
                    port=,
                    user = '',
                    password = '')



pool <- dbPool(drv = RPostgreSQL::PostgreSQL(),
               dbname=credentials['dbname'],
               host=credentials['host'],
               port=credentials['port'],
               user = credentials['user'],
               password = credentials['password'])


con <- dbConnect(PostgreSQL(), 
                 dbname = "", 
                 user = "",
                 host = "",
                 password = "")


query <- "
select data_id,
fact_identification,
factname,
measurement ->> 'universe' as universe,
measurement ->> 'denominator_column' as denominator_column
from exposome_pici.datatable
where datasource = 'ACS' and timeframe_unit = 5;
"


datatable_df = dbGetQuery(pool, query)



acs_base <- retrieve_base_acs_df(pool,
                                 '2014',
                                 '2018',
                                 datatable_df,
                                 list_non_base,
                                 credentials,
                                 'FALSE',
                                 '140')

query <- "
select data_id, 
fact_identification, 
factname, 
measurement ->> 'universe' as universe,
measurement ->> 'denominator_column' as denominator_column
from exposome_pici.datatable
where datasource = 'ACS' and timeframe_unit = 5;
"


datatable_df = dbGetQuery(pool, query)


extra <- data.frame(fact_identification= c('C24010','B11017','B08301','B25035','B25047','B05012','B11016','B25032'))

acs_non_base <- retrieve_non_base_data_acs_list(pool,
                                                '2014',
                                                '2018',
                                                datatable_df,
                                                extra,
                                                credentials,
                                                'FALSE',
                                                '140')


acs_non_base_df <- acs_non_base$result %>% reduce(left_join, by='shape_id')




acs_non_base_df <- acs_non_base_df %>%
  mutate(essential_worker_pct= select(., C24010012,C24010016,C24010020,C24010021:C24010026,C24010030,C24010034,C24010048,C24010052,C24010056,C24010057:C24010062,C24010066,C24010070) %>% apply(1, sum, na.rm=TRUE)) %>%
  mutate(healthcare_worker_pct= select(., C24010016,C24010020,C24010052,C24010056) %>% apply(1, sum, na.rm=TRUE)) %>%
  mutate(bluecollar_worker_pct= select(., C24010019,C24010030,C24010034,C24010055,C24010066,C24010070) %>% apply(1, sum, na.rm=TRUE)) %>%
  mutate(people_facing_worker_pct= select(., C24010012,C24010016,C24010019,C24010033,C24010048,C24010052,C24010055,C24010069) %>% apply(1, sum, na.rm=TRUE)) %>%
  dplyr::select(-starts_with('C24010')) %>%
  mutate(household_2_plus_pct= select(., B11016003:B11016008,B11016011:B11016016) %>% apply(1, sum, na.rm=TRUE)) %>%
  mutate(household_3_plus_pct= select(., B11016004:B11016008,B11016012:B11016016) %>% apply(1, sum, na.rm=TRUE)) %>%
  mutate(household_4_plus_pct= select(., B11016005:B11016008,B11016013:B11016016) %>% apply(1, sum, na.rm=TRUE)) %>%
  mutate(household_5_plus_pct= select(., B11016006:B11016008,B11016014:B11016016) %>% apply(1, sum, na.rm=TRUE)) %>%
  mutate(household_6_plus_pct= select(., B11016007:B11016008,B11016015:B11016016) %>% apply(1, sum, na.rm=TRUE)) %>%
  mutate(household_7_plus_pct= select(., B11016008:B11016008,B11016016:B11016016) %>% apply(1, sum, na.rm=TRUE)) %>%
  dplyr::select(-starts_with('B11016')) %>%
  mutate(commute_vehicle_pct= select(., B08301002,B08301017) %>% apply(1, sum, na.rm=TRUE)) %>%
  rename(commute_public_transportation_pct= B08301010) %>%
  rename(commute_walk_pct= B08301019) %>%
  rename(commute_work_from_home_pct= B08301021) %>%
  dplyr::select(-starts_with('B08301')) %>%
  rename(median_year_house_built= B25035001) %>%
  rename(lack_plumbing_facilities_pct=B25047003) %>%
  dplyr::select(-starts_with('B25047')) %>%
  dplyr::rename(foreign_born_pct=B05012003) %>%
  dplyr::select(-starts_with('B05012')) %>%
  mutate(units_in_structure_1_pct= select(., B25032003:B25032004,B25032014:B25032015) %>% apply(1, sum, na.rm=TRUE)) %>%
  mutate(units_in_structure_2_pct= select(., B25032005,B25032016) %>% apply(1, sum, na.rm=TRUE)) %>%
  mutate(units_in_structure_3_4_pct= select(., B25032006,B25032017) %>% apply(1, sum, na.rm=TRUE)) %>%
  mutate(units_in_structure_5_9_pct= select(., B25032007,B25032018) %>% apply(1, sum, na.rm=TRUE)) %>%
  mutate(units_in_structure_10_19_pct= select(., B25032008,B25032019) %>% apply(1, sum, na.rm=TRUE)) %>%
  mutate(units_in_structure_20_49_pct= select(., B25032009,B25032020) %>% apply(1, sum, na.rm=TRUE)) %>%
  mutate(units_in_structure_2_plus_pct= select(., B25032005,B25032010,B25032016:B25032021) %>% apply(1, sum, na.rm=TRUE)) %>%
  mutate(units_in_structure_3_plus_pct= select(., B25032006,B25032010,B25032017:B25032021) %>% apply(1, sum, na.rm=TRUE)) %>%
  mutate(units_in_structure_5_plus_pct= select(., B25032007,B25032010,B25032018:B25032021) %>% apply(1, sum, na.rm=TRUE)) %>%
  mutate(units_in_structure_10_plus_pct= select(., B25032008,B25032010,B25032019:B25032021) %>% apply(1, sum, na.rm=TRUE)) %>%
  mutate(units_in_structure_20_plus_pct= select(., B25032009,B25032010,B25032020:B25032021) %>% apply(1, sum, na.rm=TRUE)) %>%
  mutate(units_in_structure_50_plus_pct= select(., B25032010,B25032010,B25032021:B25032021) %>% apply(1, sum, na.rm=TRUE)) %>%
  dplyr::select(-starts_with('B25032'))











con <- dbConnect(PostgreSQL(), dbname = "", user = "",
                 host = "",
                 password = "")



postgis_2013_2014_census_tract_sf <- get_postgis_query(con,
"select b.shape_id,
b.latitude,
b.longitude,
b.geoid,
b.statefip,
geometrywkt
from exposome_pici.shapefile b
where summarylevelid = '140'
and b.startdate = '2018-01-01'
and b.enddate = '2018-12-31'", geom_name = "geometrywkt") %>% st_as_sf(.) 


postgis_2013_2014_census_tract_sf <- st_set_crs(postgis_2013_2014_census_tract_sf, 4326) %>%
  mutate(shape_id=as.character(shape_id))

postgis_2013_2014_census_tract_sf_acs_data <- acs_base$base_df %>% 
  inner_join(acs_base$shape_id_list_df_area, by='shape_id') %>% 
  inner_join(acs_non_base_df, by='shape_id') %>% 
  mutate_at(vars(shape_id),as.character)




postgis_2013_2014_census_tract_sf_acs_data <-missRanger(postgis_2013_2014_census_tract_sf_acs_data,
                                                        maxiter = 30,
                                                        pmm.k = 2,
                                                        returnOOB=FALSE,
                                                        num.trees=20,
                                                        verbose=1)



postgis_2013_2014_census_tract_sf_acs <- postgis_2013_2014_census_tract_sf %>% 
  inner_join(postgis_2013_2014_census_tract_sf_acs_data , by='shape_id')



write_rds(postgis_2013_2014_census_tract_sf_acs,'data/postgis_2013_2014_census_tract_sf_acs.rds')


