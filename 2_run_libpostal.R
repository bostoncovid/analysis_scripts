library(tidyverse)

library(poster)
library(purrr)

parse_address <- function(EMPI,address){
  normalized_address <- normalise_addr(address)
  df_address <- parse_addr(address)
  df_address$EMPI <- EMPI
  df_address$complete_address <- address
  df_address$normalized_address <- normalized_address
  
  
  return(df_address)
  }

df_address <- read_rds('data/patient_dim_v2.rds')



parsed_address_df <- map2_dfr(.x=df_address$PATIENT_NUM, .y=df_address$complete_address, .f=parse_address)

parsed_address_temporary_df <- map2_dfr(.x=df_address$PATIENT_NUM, .y=df_address$complete_address_temp, .f=parse_address)

write_rds(parsed_address_df,'data/parsed_address_df.rds')

