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

library(ggmap)
library(postmastr)


st_drop_geometry <- function(x) {
  if(inherits(x,"sf")) {
    x <- st_set_geometry(x, NULL)
    class(x) <- 'data.frame'
  }
  return(x)
}



EMPI_address_geocoded <- read_rds('data/EMPI_address_geocoded.rds') 


postgis_2013_2014_census_tract_sf_acs <- read_rds('data/postgis_2013_2014_census_tract_sf_acs.rds')




EMPI_address_geocoded_merged_acs <- EMPI_address_geocoded %>% st_join(postgis_2013_2014_census_tract_sf_acs)



EMPI_merge_acs <- st_drop_geometry(EMPI_address_geocoded_merged_acs) %>% group_by(EMPI) %>% slice(1) %>% ungroup()



write_rds(EMPI_merge_acs, 'data/EMPI_merge_acs.rds')




