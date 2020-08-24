
library(tidyverse)
library(RPostgreSQL)
library(dbplyr)
library(readr)
library(lubridate)
library(pool)
library(fst)
library(naniar)

library(missRanger)
library(feather)
library(readxl)
library(furrr)

library(tidycensus)

library(RPostgreSQL)
library(postGIStools)
library(sf)
library(postmastr)
library(furrr)

no_cores <- availableCores()
plan(multisession, workers = no_cores-1)


earth.dist <- function (long1, lat1, long2, lat2)
{
  rad <- pi/180
  a1 <- lat1 * rad
  a2 <- long1 * rad
  b1 <- lat2 * rad
  b2 <- long2 * rad
  dlon <- b2 - a2
  dlat <- b1 - a1
  a <- (sin(dlat/2))^2 + cos(a1) * cos(b1) * (sin(dlon/2))^2
  c <- 2 * atan2(sqrt(a), sqrt(1 - a))
  R <- 6378.145
  d <- R * c
  return(d)
}

st_drop_geometry <- function(x) {
  if(inherits(x,"sf")) {
    x <- st_set_geometry(x, NULL)
    class(x) <- 'data.frame'
  }
  return(x)
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


con <- dbConnect(PostgreSQL(), dbname = "", user = "",
                 host = "",
                 password = "")








address_column_covid_geocoded <- read_csv("data/address_column_covid_geocoded.csv", 
                                          col_types = cols(EMPI = col_character(), 
                                                           lat = col_character(), lon = col_character())) %>%
  mutate(parse_zipcode = str_sub(pm.address,-5,-1))


address_column_covid_geocoded_temporary <- read_csv("data/address_column_covid_temporary_geocoded.csv", 
                                          col_types = cols(EMPI = col_character(), 
                                                           lat = col_character(), lon = col_character())) %>%  mutate(parse_zipcode = str_sub(pm.address,-5,-1))




postgis_zipcode <- get_postgis_query(con,
                                     "select 
b.shape_id,
b.startdate,
b.enddate,
b.geoid,
b.latitude,
b.longitude,
b.statefip,
geometrywkt
from exposome_pici.shapefile b
where summarylevelid = '2000'
and b.startdate = '2013-01-01' and b.enddate = '2013-12-31' and b.statefip in ('25')", geom_name = "geometrywkt") 


postgis_zipcode_sf <- postgis_zipcode %>% st_as_sf() %>% select(geoid,geometry)


postgis_zipcode_sf <- st_set_crs(postgis_zipcode_sf, 4326)





address_column_covid_geocoded_sf = st_as_sf(address_column_covid_geocoded, coords = c("lon", "lat"),  crs = 4326) 


address_column_covid_geocoded_sf_zip <- address_column_covid_geocoded_sf %>% 
  filter(state %in% c('MA')) %>%
  st_join(postgis_zipcode_sf)


df_address_match_zip <- address_column_covid_geocoded_sf_zip %>% 
  filter(parse_zipcode == geoid)

df_address_match_zip_EMPI <- df_address_match_zip %>% dplyr::select(EMPI) 

df_address_match_zip_EMPI <- st_drop_geometry(df_address_match_zip)

df_address_no_match_zip <- address_column_covid_geocoded %>% anti_join(df_address_match_zip_EMPI, by='EMPI') 



EMPI_list <- bind_rows(st_drop_geometry(df_address_match_zip %>% dplyr::select(EMPI) %>% unique()),
                       st_drop_geometry(df_address_no_match_zip_distance %>% dplyr::select(EMPI) %>% unique()))


EMPI_list <- st_drop_geometry(EMPI_list)

address_column_covid_geocoded_subset <- bind_rows(df_address_match_zip,df_address_no_match_zip) %>% unique()

address_column_covid_geocoded_subset_sf <- st_as_sf(address_column_covid_geocoded_subset, coords = c("lon", "lat"),  crs = 4326) 


write_rds(address_column_covid_geocoded_subset_sf,'data/EMPI_address_geocoded.rds')



