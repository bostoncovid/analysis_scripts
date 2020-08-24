library(tidyverse)


parsed_address_df <- read_rds('data/parsed_address_df.rds') %>% 
  mutate(EMPI=as.character(EMPI)) 



parsed_address_temporary_df <- read_rds('data/parsed_address_temporary_df.rds') %>% 
  mutate(EMPI=as.character(EMPI)) %>% filter(!is.na(complete_address))





## We looked for parsing errors and then tried to correct them


change_address <- function(df,EMPI_val,house_number_val=NULL,road_val=NULL, city_val=NULL,state_val=NULL, postal_val=NULL){
  
  if(!is.null(house_number_val)){
    df <- df %>% mutate(house_number = if_else(EMPI==EMPI_val,tolower(house_number_val),house_number))
  }
  
  if(!is.null(road_val)){
    df <- df %>% mutate(road = if_else(EMPI==EMPI_val,tolower(road_val),road))
  }
  
  if(!is.null(city_val)){
    df <- df %>% mutate(city = if_else(EMPI==EMPI_val,tolower(city_val),city))
  }
  
  if(!is.null(state_val)){
    df <- df %>% mutate(state = if_else(EMPI==EMPI_val,tolower(state_val),state))
  }
  
  
  
  if(!is.null(postal_val)){
    df <- df %>% mutate(postal_code = if_else(EMPI==EMPI_val,tolower(postal_val),postal_code))
  }
  
  
  
  return(df)
}



## Look for errors where state abbreviation was not found
step1 <- parsed_address_df %>% filter(is.na(state))

parsed_address_df <- change_address(parsed_address_df,'',house_number_val='',road_val='', city_val='',state_val='')



## Filter for states within MA, NH, RI, ME, and VT

## Look for states with odd street numbers such as those which start with a letter to see if there was a parsing error
## Look for addresses with a house name to see if those are processed correctly and street address was not thrown into the house name column
## Look with house addresses with no house number and no po box code and check if they were parsed incorrectly
parsed_address_df <- change_address(parsed_address_df,'',house_number_val='',road_val='', city_val='',state_val='')


### Do cleanup of addresses with house information




final_address_list <- parsed_address_df %>% 
  mutate(parsed_address = paste0(house_number,' ',road, ', ', city, ', ', state, ' ', postal_code)) %>%
  dplyr::select(EMPI, pm.address=parsed_address)



final_address_list_temporary <- parsed_address_df %>% 
  mutate(parsed_address = paste0(house_number,' ',road, ', ', city, ', ', state, ' ', postal_code)) %>%
  dplyr::select(EMPI, pm.address=parsed_address)


write_csv(final_address_list,'data/address_column_covid.csv')


write_csv(final_address_list_temporary,'data/address_column_covid_temporary.csv')





## Setup https://github.com/degauss-org/DeGAUSS/wiki/Geocoding-with-DeGAUSS
## Use the DeGauss geocoder to Geocode addresses by specifying the column that was first parsed then merged back together

bsub -Is -R 'rusage[mem=10000]' -n 1 /bin/bash


module load sqlite/3.31.1
module load rubygems/default
module load ruby/default
module load git/default
module load zlib/default
module load gcc/default
module load libyaml/default
module load libffi/default
module load openssl/default
module load make/default
module load bzip2/default
module load autoconf/default
module load automake/default
module load libtool/default
module load bison/default
module load libcurl/default
module load R/3.6.3
module load postgresql/9.4.5
module load geos/default
module load proj/default
module load gdal/default
module load libxml/default
module load protobuf/default


Rscript /PHShome/cl331/geocoder/bin/geocode.R /PHShome/cl331/covid_analysis/data/v2/address_column_covid.csv pm.address

Rscript /PHShome/cl331/geocoder/bin/geocode.R /PHShome/cl331/covid_analysis/data/v2/address_column_covid_temporary.csv pm.address


