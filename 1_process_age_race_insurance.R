library(tidyverse)
library(glmmTMB)
library(naniar)
library(broom)
library(insight)
library(mgcv)
library(lubridate)
library(parameters)
library(aCRM)
library(performance)
library(missRanger)
library(pheatmap)
library(corrr)
library(car)
library(naniar)
library(ggrepel)
library(leaflet)
library(ggplot2)

library(RODBC)
library(tidyverse)






covid_con1 <- read.delim('COVID_Con 1.txt', header=TRUE, sep='|') %>% mutate_all(as.character)
covid_con2 <- read.delim('COVID_Con 2.txt', header=TRUE, sep='|') %>% mutate_all(as.character)
covid_con3 <- read.delim('COVID_Con 3.txt', header=TRUE, sep='|') %>% mutate_all(as.character)
covid_con4 <- read.delim('COVID_Con 4.txt', header=TRUE, sep='|') %>% mutate_all(as.character)


covid_con <- bind_rows(covid_con1, covid_con2, covid_con3,covid_con4) %>% dplyr::select(EMPI,Insurance_1,Insurance_2,Insurance_3, VIP)
rm(covid_con1)
rm(covid_con2)
rm(covid_con3)
rm(covid_con4)



covid_mrn1 <- read.delim('COVID_MRN 1.txt', header=TRUE, sep='|') %>% mutate_all(as.character)
covid_mrn2 <- read.delim('COVID_MRN 2.txt', header=TRUE, sep='|') %>% mutate_all(as.character)
covid_mrn3 <- read.delim('COVID_MRN 3.txt', header=TRUE, sep='|') %>% mutate_all(as.character)
covid_mrn4 <- read.delim('COVID_MRN 4.txt', header=TRUE, sep='|') %>% mutate_all(as.character)

covid_mrn <-bind_rows(covid_mrn1,covid_mrn2,covid_mrn3,covid_mrn4) 
rm(covid_mrn1)
rm(covid_mrn2)
rm(covid_mrn3)
rm(covid_mrn4)





EMPI_merge_acs <- read_rds('data/EMPI_merge_acs.rds') %>% rename(PATIENT_NUM=EMPI) 



odbc <- odbcConnect()

query <- 
  "
select * from COVID19_Mart.RPDR.PATIENT_DIMENSION a 
FULL OUTER JOIN
COVID19_Mart.Patient.Patient b on (a.PATIENT_NUM=B.PATIENT_NUM)
"
patient_dim <- sqlQuery(odbc, paste(query))


query <- 
  "
select * from COVID19_Mart.Patient.Patient3 a 
"
patient_3_dim <- sqlQuery(odbc, paste(query))



query <- 'select * from COVID19_Mart.Patient.MRN_Mapping'

MRN_mapping <- sqlQuery(odbc, paste(query))






odbcCloseAll()




basic_info <- patient_dim %>% dplyr::select(PATIENT_NUM:INCOME_CD,AddressLine01TXT:ZipCD,BirthDTS:LanguageDSC,
                                            RegistrationStatusDSC,MedicareID,MedicaidID,TemporaryAddressLine01TXT:TemporaryResidenceContactPersonNM,
                                            EmployerID:EmploymentStatusDSC,
                                            CountryOfOriginCD:CountryOfOriginDSC,
                                            PatientStatusCD,PatientStatusDSC) %>%
  rename(Address1=AddressLine01TXT,
         Address2=AddressLine02TXT,
         City=CityNM,
         State=StateDSC,
         Zip=ZipCD,
         Address1_temp=TemporaryAddressLine01TXT,
         Address2_temp=TemporaryAddressLine02TXT,
         City_temp=TemporaryCityNM,
         State_temp=TemporaryStateCD,
         Zip_temp=TemporaryZipCD
  )





basic_info_address <- basic_info 



basic_info_address <- basic_info_address %>%
  mutate(race_normalized = case_when(RACE_CD == 'WHITE' ~ 'white',
                                     RACE_CD == 'BLACK OR AFRICAN AMERICAN' ~ 'black',
                                     RACE_CD == 'OTHER' ~ 'other',
                                     RACE_CD == '@' ~ 'missing',
                                     RACE_CD == 'ASIAN' ~ 'asian',
                                     RACE_CD == 'HISPANIC OR LATINO' ~ 'hispanic',
                                     RACE_CD == 'OTHER@HISPANIC' ~ 'hispanic',
                                     RACE_CD == 'DECLINED' ~ 'missing',
                                     RACE_CD == 'UNKNOWN' ~ 'missing',
                                     RACE_CD == 'WHITE@HISPANIC' ~ 'hispanic',
                                     RACE_CD == 'AMERICAN INDIAN OR ALASKA NATIVE' ~ 'native american',
                                     RACE_CD == 'HISPANIC OR LATINO@HISPANIC' ~ 'hispanic',
                                     RACE_CD == 'NATIVE HAWAIIAN OR OTHER PACIFIC ISLANDER' ~ 'asian',
                                     RACE_CD == 'BLACK OR AFRICAN AMERICAN@HISPANIC' ~ 'hispanic',
                                     RACE_CD == '@HISPANIC' ~ 'hispanic',
                                     RACE_CD == 'DECLINED@HISPANIC' ~ 'hispanic',
                                     RACE_CD == 'SPANISH@HISPANIC' ~ 'hispanic',
                                     RACE_CD == 'UNKNOWN@HISPANIC' ~ 'hispanic',
                                     RACE_CD == 'BLACK' ~ 'black',
                                     RACE_CD == 'DOMINICAN' ~ 'hispanic',
                                     RACE_CD == 'NOT GIVEN' ~ 'missing',
                                     RACE_CD == 'ASIAN@HISPANIC' ~ 'hispanic',
                                     RACE_CD == 'HISPANIC' ~ 'hispanic',
                                     RACE_CD == 'W' ~ 'white',
                                     RACE_CD == 'U' ~ 'missing',
                                     RACE_CD == 'DOMINICAN@HISPANIC' ~ 'hispanic',
                                     RACE_CD == 'H' ~ 'hispanic',
                                     RACE_CD == 'NATIVE HAWAIIAN OR OTHER PACIFIC ISLANDER@HISPANIC' ~ 'asian',
                                     RACE_CD == 'UNAVAILABLE' ~ 'missing',
                                     RACE_CD == 'AMERICAN INDIAN OR ALASKA NATIVE@HISPANIC' ~ 'native american',
                                     RACE_CD == 'ASIAN/AMER IND@HISPANIC' ~ 'hispanic',
                                     RACE_CD == 'EUROPEAN' ~ 'white',
                                     RACE_CD == 'HIS/WHITE' ~ 'hispanic',
                                     RACE_CD == 'NOT REPORTED' ~ 'missing',
                                     RACE_CD == 'ORIENTAL' ~ 'asian',
                                     RACE_CD == 'SPANISH' ~ 'hispanic')) %>%   
  mutate(race_normalized = case_when((race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'Dominican Republic' ~ 'hispanic',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'Puerto Rico' ~ 'hispanic',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'El Salvador' ~ 'hispanic',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'Colombia' ~ 'hispanic',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'Brazil' ~ 'hispanic',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'Guatemala' ~ 'hispanic',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'Honduras' ~ 'hispanic',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'Mexico' ~ 'hispanic',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'Cape Verde' ~ 'black',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'Peru' ~ 'hispanic',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'India' ~ 'hispanic',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'Ecuador' ~ 'hispanic',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'Venezuela' ~ 'hispanic',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'Cuba' ~ 'hispanic',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'Haiti' ~ 'black',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'Trinidad and Tobago' ~ 'black',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'Philippines' ~ 'asian',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'Bolivia' ~ 'hispanic',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'Jamaica' ~ 'black',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'Argentina' ~ 'hispanic',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'Chile' ~ 'hispanic',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'Costa Rica' ~ 'hispanic',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'Nicaragua' ~ 'hispanic',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'Paraguay' ~ 'hispanic',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'Sudan' ~ 'black',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'Nigeria' ~ 'black',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'Pakistan' ~ 'asian',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'Spain' ~ 'white',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'United Kingdom' ~ 'white',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'Angola' ~ 'black',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'Ireland' ~ 'white',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'Somalia' ~ 'black',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'Uganda' ~ 'black',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'Uruguay' ~ 'hispanic',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'Bangladesh' ~ 'asian',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'China' ~ 'asian',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'Germany' ~ 'white',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'Greece' ~ 'white',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'Italy' ~ 'white',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'Panama' ~ 'hispanic',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'Portugal' ~ 'white',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'Russia' ~ 'white',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'Albania' ~ 'white',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'Antigua and Barbuda' ~ 'black',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'Barbados' ~ 'black',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'Bosnia and Herzegovina' ~ 'white',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'Liberia' ~ 'black',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'Eritrea' ~ 'black',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'Ethiopia' ~ 'black',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'Kenya' ~ 'black',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'Nepal' ~ 'asian',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'Andorra' ~ 'white',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'Bhutan' ~ 'asian',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'Cambodia' ~ 'asian',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'Cameroon' ~ 'black',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'Cayman Islands' ~ 'black',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'Congo' ~ 'black',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'Curacao' ~ 'black',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'France' ~ 'white',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'Ghana' ~ 'black',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'Guadeloupe' ~ 'black',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'Guinea' ~ 'black',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'Guinea-Bissau' ~ 'black',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'Hong Kong' ~ 'asian',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'Macao' ~ 'asian',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'Moldova' ~ 'white',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'Poland' ~ 'white',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'South Sudan' ~ 'black',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'Taiwan' ~ 'asian',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'Thailand' ~ 'asian',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'Ukraine' ~ 'white',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'Vietnam' ~ 'asian',
                                     (race_normalized == 'other' | race_normalized == 'missing') & CountryOfOriginDSC == 'Canada' ~ 'white',
                                     TRUE ~ race_normalized)) %>%
  mutate(race_normalized = case_when((race_normalized == 'other' | race_normalized == 'missing') & LANGUAGE_CD == 'SPANISH' ~ 'hispanic',
                                     (race_normalized == 'other' | race_normalized == 'missing') & LANGUAGE_CD == 'PORTUGUESE' ~ 'hispanic',
                                     (race_normalized == 'other' | race_normalized == 'missing') & LANGUAGE_CD == 'SPAN-ENGL' ~ 'hispanic',
                                     (race_normalized == 'other' | race_normalized == 'missing') & LANGUAGE_CD == 'HAITIAN CREOLE' ~ 'black',
                                     (race_normalized == 'other' | race_normalized == 'missing') & LANGUAGE_CD == 'GREEK' ~ 'white',
                                     (race_normalized == 'other' | race_normalized == 'missing') & LANGUAGE_CD == 'RUSSIAN' ~ 'white',
                                     (race_normalized == 'other' | race_normalized == 'missing') & LANGUAGE_CD == 'CHINESE/CANTONESE' ~ 'asian',
                                     (race_normalized == 'other' | race_normalized == 'missing') & LANGUAGE_CD == 'HINDI' ~ 'asian',
                                     (race_normalized == 'other' | race_normalized == 'missing') & LANGUAGE_CD == 'KHMER' ~ 'asian',
                                     (race_normalized == 'other' | race_normalized == 'missing') & LANGUAGE_CD == 'SPAN' ~ 'hispanic',
                                     (race_normalized == 'other' | race_normalized == 'missing') & LANGUAGE_CD == 'ALBANIAN' ~ 'white',
                                     (race_normalized == 'other' | race_normalized == 'missing') & LANGUAGE_CD == 'BENGALI' ~ 'asian',
                                     (race_normalized == 'other' | race_normalized == 'missing') & LANGUAGE_CD == 'CHINESE/MANDARIN' ~ 'asian',
                                     (race_normalized == 'other' | race_normalized == 'missing') & LANGUAGE_CD == 'NEPALI' ~ 'asian',
                                     (race_normalized == 'other' | race_normalized == 'missing') & LANGUAGE_CD == 'PUNJABI' ~ 'asian',
                                     (race_normalized == 'other' | race_normalized == 'missing') & LANGUAGE_CD == 'SOMALI' ~ 'black',
                                     (race_normalized == 'other' | race_normalized == 'missing') & LANGUAGE_CD == 'URDU' ~ 'asian',
                                     (race_normalized == 'other' | race_normalized == 'missing') & LANGUAGE_CD == 'AMHARIC' ~ 'black',
                                     (race_normalized == 'other' | race_normalized == 'missing') & LANGUAGE_CD == 'CAMB' ~ 'asian',
                                     (race_normalized == 'other' | race_normalized == 'missing') & LANGUAGE_CD == 'CREOLE' ~ 'black',
                                     (race_normalized == 'other' | race_normalized == 'missing') & LANGUAGE_CD == 'CROATIAN' ~ 'white',
                                     (race_normalized == 'other' | race_normalized == 'missing') & LANGUAGE_CD == 'BULGARIAN' ~ 'white',
                                     (race_normalized == 'other' | race_normalized == 'missing') & LANGUAGE_CD == 'ERITREAN' ~ 'black',
                                     (race_normalized == 'other' | race_normalized == 'missing') & LANGUAGE_CD == 'GUJARATI' ~ 'asian',
                                     (race_normalized == 'other' | race_normalized == 'missing') & LANGUAGE_CD == 'KANNADA' ~ 'asian',
                                     (race_normalized == 'other' | race_normalized == 'missing') & LANGUAGE_CD == 'LAOTIAN' ~ 'asian',
                                     (race_normalized == 'other' | race_normalized == 'missing') & LANGUAGE_CD == 'ITALIAN' ~ 'white',
                                     (race_normalized == 'other' | race_normalized == 'missing') & LANGUAGE_CD == 'DANISH' ~ 'white',
                                     (race_normalized == 'other' | race_normalized == 'missing') & LANGUAGE_CD == 'SWEDISH' ~ 'white',
                                     (race_normalized == 'other' | race_normalized == 'missing') & LANGUAGE_CD == 'SWAHILI' ~ 'black',
                                     (race_normalized == 'other' | race_normalized == 'missing') & LANGUAGE_CD == 'TAGALOG' ~ 'asian',
                                     (race_normalized == 'other' | race_normalized == 'missing') & LANGUAGE_CD == 'TAGALAG' ~ 'asian',
                                     (race_normalized == 'other' | race_normalized == 'missing') & LANGUAGE_CD == 'THAI' ~ 'asian',
                                     (race_normalized == 'other' | race_normalized == 'missing') & LANGUAGE_CD == 'POPO' ~ 'asian',
                                     (race_normalized == 'other' | race_normalized == 'missing') & LANGUAGE_CD == 'VIETNAMESE' ~ 'asian',
                                     (race_normalized == 'other' | race_normalized == 'missing') & LANGUAGE_CD == 'JAPANESE' ~ 'asian',
                                     (race_normalized == 'other' | race_normalized == 'missing') & LANGUAGE_CD == 'CAPE VERDEAN' ~ 'black',
                                     TRUE ~ race_normalized)) %>%
  mutate(race_normalized = case_when(race_normalized == 'other' & EthnicGroupDSC == 'Yes Hispanic' ~ 'hispanic',
                                     race_normalized == 'missing' & EthnicGroupDSC == 'Yes Hispanic' ~ 'hispanic',
                                     TRUE ~ race_normalized )) %>%
  mutate(language_normalized_1 = case_when(str_detect(LANGUAGE_CD,'Spanish English-SPAN-ENGL') ~ 'spanish',
                                           str_detect(LANGUAGE_CD,'Spanish English-SPEN') ~ 'spanish',
                                           str_detect(LANGUAGE_CD,'SPAN-ENGL') ~ 'spanish',
                                           str_detect(toupper(LANGUAGE_CD),'ENGLISH') ~ 'english',
                                           str_detect(toupper(LANGUAGE_CD),'ENGL') ~ 'english',
                                           str_detect(toupper(LANGUAGE_CD),'SPANISH') ~ 'spanish',
                                           str_detect(toupper(LANGUAGE_CD),'NOT RECORDED') ~ 'unknown',
                                           str_detect(toupper(LANGUAGE_CD),'UNAVAILABLE') ~ 'unknown',
                                           str_detect(toupper(LANGUAGE_CD),'UNKNOWN') ~ 'unknown',
                                           str_detect(LANGUAGE_CD,'Not reported/refused') ~ 'unknown',
                                           str_detect(toupper(LANGUAGE_CD),'DECLINED') ~ 'unknown',
                                           str_detect(toupper(LANGUAGE_CD),'@') ~ 'unknown',
                                           str_detect(toupper(LANGUAGE_CD),'REFUSED TO REPORT') ~ 'unknown',
                                           str_detect(toupper(LANGUAGE_CD),'NOT REPORTED/REFUSED') ~ 'unknown',
                                           is.na(LANGUAGE_CD) ~ 'unknown',
                                           TRUE ~ 'other')) %>%
  mutate(language_normalized = if_else(language_normalized_1 == 'unknown' & race_normalized == 'hispanic','spanish',language_normalized_1)) %>%
  select(-language_normalized_1) %>%
  mutate(race_normalized = if_else(race_normalized == 'native american','other',race_normalized))


basic_info_address_subset <- basic_info_address %>% 
  dplyr::select(PATIENT_NUM,
                VITAL_STATUS_CD,
                BIRTH_DATE,
                DEATH_DATE,
                SEX_CD,
                AGE_IN_YEARS_NUM,
                PatientStatusCD,
                PatientStatusDSC,
                RACE_CD,LANGUAGE_CD,CountryOfOriginDSC,
                race_normalized:language_normalized) %>% 
  mutate(PATIENT_NUM = as.character(PATIENT_NUM))




insurance_data <- MRN_mapping %>% 
  select(PATIENT_NUM=patient_num,EMPI=patient_id_e) %>% mutate_all(as.character) %>% unique() %>%
  left_join(covid_con,by='EMPI')








basic_info_address_subset <-basic_info_address_subset  %>% 
  inner_join(insurance_data,by='PATIENT_NUM') 







ins_champva1 <- grepl('CHAMPVA',basic_info_address_subset$Insurance_1, ignore.case=TRUE)
ins_champva2 <- grepl('CHAMPVA',basic_info_address_subset$Insurance_2, ignore.case=TRUE)
ins_champva3 <- grepl('CHAMPVA',basic_info_address_subset$Insurance_3, ignore.case=TRUE)
basic_info_address_subset$ins_champva<-ifelse(ins_champva1=='TRUE'|ins_champva2=='TRUE'|ins_champva3=='TRUE',1,0)
ins_hne1 <- grepl('HEALTH NEW ENGLAND|Navicare',basic_info_address_subset$Insurance_1, ignore.case=TRUE)
ins_hne2 <- grepl('HEALTH NEW ENGLAND|Navicare',basic_info_address_subset$Insurance_2, ignore.case=TRUE)
ins_hne3 <- grepl('HEALTH NEW ENGLAND|Navicare',basic_info_address_subset$Insurance_3, ignore.case=TRUE)
basic_info_address_subset$ins_hne<-ifelse(ins_hne1=='TRUE'|ins_hne2=='TRUE'|ins_hne3=='TRUE',1,0)
ins_none1<-grepl('Safety net|HSN|Self|Free|COVID19 HRSA UNINSURED|Celticare|For conversion only',basic_info_address_subset$Insurance_1, ignore.case=TRUE)
ins_none2<-grepl('Safety net|HSN|Self|Free|COVID19 HRSA UNINSURED|Celticare|For conversion only',basic_info_address_subset$Insurance_2, ignore.case=TRUE)
ins_none3<-grepl('Safety net|HSN|Self|Free|COVID19 HRSA UNINSURED|Celticare|For conversion only',basic_info_address_subset$Insurance_3, ignore.case=TRUE)
basic_info_address_subset$ins_none<-ifelse(ins_none1=='TRUE'|ins_none2=='TRUE'|ins_none3=='TRUE',1,0)
ins_public1<-grepl('Medicaid|MassHealth|Mass Health|Masshlth|BMC|Government|Healthnet|Health Net|MBHP|Mass Rehab|MCO|CHAMPVA|COUNTY JAIL|STATE PRISONS|Network Hlth Commonwlth', basic_info_address_subset$Insurance_1,ignore.case=TRUE)
ins_public2<-grepl('Medicaid|MassHealth|Mass Health|Masshlth|BMC|Government|Healthnet|Health Net|MBHP|Mass Rehab|MCO|CHAMPVA|COUNTY JAIL|STATE PRISONS|Network Hlth Commonwlth', basic_info_address_subset$Insurance_2,ignore.case=TRUE)
ins_public3<-grepl('Medicaid|MassHealth|Mass Health|Masshlth|BMC|Government|Healthnet|Health Net|MBHP|Mass Rehab|MCO|CHAMPVA|COUNTY JAIL|STATE PRISONS|Network Hlth Commonwlth', basic_info_address_subset$Insurance_3,ignore.case=TRUE)
basic_info_address_subset$ins_public<-ifelse(ins_public1=='TRUE'|ins_public2=='TRUE'|ins_public3=='TRUE',1,0)
ins_medicare1<-grepl('Medicare|Senior|Elder|Commonwealth|AARP',basic_info_address_subset$Insurance_1,ignore.case=TRUE)
ins_medicare2<-grepl('Medicare|Senior|Elder|Commonwealth|AARP',basic_info_address_subset$Insurance_2,ignore.case=TRUE)
ins_medicare3<-grepl('Medicare|Senior|Elder|Commonwealth|AARP',basic_info_address_subset$Insurance_3,ignore.case=TRUE)
basic_info_address_subset$ins_medicare<-ifelse(ins_medicare1=='TRUE'|ins_medicare2=='TRUE'|ins_medicare3=='TRUE',1,0)
ins_private1<-grepl("Employee|Commercial|Amica|Sentry|Tricare|Martin|Network Health|Coventry|Plans, Inc|Guardian|Raytheon|Delta|Unicare|Tufts|Blue|BC|Beacon|Humana|Aetna|Tufts|Fallon|Harvard|HP HMO|Cigna|Partners|Allways|Neighborhood|Anthem|United|HC|Healthcare|workers comp|international embassy|LIBERTY MUTUAL|TRAVELER'S MANAGED CARE|WORKMENS COMP|AIM MUTUAL|COST CARE|NORGUARD|Oxford Health Plans|MASTER HEALTH PLUS|Arcadia|Davis|Cyto|Donor/bone marrow related|Auto Insurance|HP PPO/EPO|Sedgewick|Centers of Excellence|Workmen’s comp|Industrial accident|HOSPICE",basic_info_address_subset$Insurance_1,ignore.case=TRUE)
ins_private2<-grepl("Employee|Commercial|Amica|Sentry|Tricare|Martin|Network Health|Coventry|Plans, Inc|Guardian|Raytheon|Delta|Unicare|Tufts|Blue|BC|Beacon|Humana|Aetna|Tufts|Fallon|Harvard|HP HMO|Cigna|Partners|Allways|Neighborhood|Anthem|United|HC|Healthcare|workers comp|international embassy|LIBERTY MUTUAL|TRAVELER'S MANAGED CARE|WORKMENS COMP|AIM MUTUAL|COST CARE|NORGUARD|Oxford Health Plans|MASTER HEALTH PLUS|Arcadia|Davis|Cyto|Donor/bone marrow related|Auto Insurance|HP PPO/EPO|Sedgewick|Centers of Excellence|Workmen’s comp|Industrial accident|HOSPICE",basic_info_address_subset$Insurance_2,ignore.case=TRUE)
ins_private3<-grepl("Employee|Commercial|Amica|Sentry|Tricare|Martin|Network Health|Coventry|Plans, Inc|Guardian|Raytheon|Delta|Unicare|Tufts|Blue|BC|Beacon|Humana|Aetna|Tufts|Fallon|Harvard|HP HMO|Cigna|Partners|Allways|Neighborhood|Anthem|United|HC|Healthcare|workers comp|international embassy|LIBERTY MUTUAL|TRAVELER'S MANAGED CARE|WORKMENS COMP|AIM MUTUAL|COST CARE|NORGUARD|Oxford Health Plans|MASTER HEALTH PLUS|Arcadia|Davis|Cyto|Donor/bone marrow related|Auto Insurance|HP PPO/EPO|Sedgewick|Centers of Excellence|Workmen’s comp|Industrial accident|HOSPICE",basic_info_address_subset$Insurance_3,ignore.case=TRUE)
basic_info_address_subset$ins_private<-ifelse(ins_private1=='TRUE'|ins_private2=='TRUE'|ins_private3=='TRUE',1,0)


basic_info_address_subset  <- basic_info_address_subset %>%
  mutate(inscat1 = case_when(ins_medicare == 1 ~ 2,
                             ins_hne == 1 & AGE_IN_YEARS_NUM >= 65 ~ 2,
                             ins_champva == 1 & AGE_IN_YEARS_NUM >= 65 ~ 2,
                             ins_private == 1  ~ 3,
                             ins_champva == 1 & AGE_IN_YEARS_NUM < 65 ~ 3,
                             ins_public == 1 ~ 1,
                             ins_hne == 1 & AGE_IN_YEARS_NUM < 65 ~ 1,
                             ins_none == 1 ~ 0,
                             TRUE ~ 0))



write_rds(basic_info_address_subset,'data/patient_dim_v2.rds')

