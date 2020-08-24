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



odbc <- odbcConnect("", "","")


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






query <-
  "
select *
from COVID19_Mart.RPDR.OBSERVATION_FACT_RPDR
where concept_cd in ( 
select concept_cd from COVID19_Mart.RPDR.CONCEPT_DIMENSION
where concept_path like '\\I2B2MetaData\\LabTests\\LAB\\(LLB87) Infectious Disease\\(LLB999) COVID-19\\%'
)
order by patient_num, start_date
"

covid_lab_patient_num <- sqlQuery(odbc, paste(query))


query <- "select * from COVID19_Mart.Analytics.EDWICU"

EDWICU <- sqlQuery(odbc, paste(query))

query <- "select * from COVID19_Mart.Analytics.EDWInpatientAdmit"

EDWInpatientAdmit <- sqlQuery(odbc, paste(query))

query <- "select * from COVID19_Mart.Analytics.EDWMechanicalVentilation"

EDWMechanicalVentilation <- sqlQuery(odbc, paste(query))



query <- "select * from COVID19_Mart.Analytics.EDWConceptAdmitDiagnosis"

EDWMechanicalVentilation <- sqlQuery(odbc, paste(query))



odbcCloseAll()





covid_lab_patient_num <- covid_lab_patient_num %>%
  mutate(pcr_test_pos = case_when(
    TVAL_CHAR == 'NEGATIVE FOR 2019-NOVEL CORONAVIRUS (2019-NCOV) BY PCR.' ~ '0',
    TVAL_CHAR == 'NOT DETECTED' ~ '0',
    TVAL_CHAR == 'SARS-COV-2 NOT DETECTED' ~ '0',
    TVAL_CHAR == 'NEGATIVE' ~ '0',
    TVAL_CHAR == 'POSITIVE FOR 2019-NOVEL CORONAVIRUS (2019-NCOV) BY PCR.' ~ '1',
    TVAL_CHAR == 'DETECTED' ~ '1',
    TVAL_CHAR == 'SARS-COV-2 DETECTED' ~ '1',
    TVAL_CHAR == 'POSITIVE' ~ '1',
    TVAL_CHAR == 'UNDETECTED' ~ '0',
    TVAL_CHAR == 'PRESUMPTIVE POSITIVE' ~ '1',
    TVAL_CHAR == 'NEGATIVE FOR SARS-COV-2 (COVID-19) BY PCR' ~ '0',
    TVAL_CHAR == 'PRESUMPTIVE POSITIVE FOR 2019-NOVEL CORONAVIRUS (2019-NCOV) BY PCR.' ~ '1',
    TVAL_CHAR == 'PRESUMPTIVE NEGATIVE FOR SARS-COV-2 (COVID-19) BY PCR' ~ '0',
    TVAL_CHAR == 'POSITIVE FOR SARS-COV-2 (COVID-19) BY PCR' ~ '1',
    TVAL_CHAR == 'PRESUMPTIVE NEGATIVE' ~ '0',
    TVAL_CHAR == 'NEGATIVE FOR 2019 NOVEL CORONAVIRUS BY PCR' ~ '0',
    TVAL_CHAR == 'THE SPECIMEN IS PRESUMPTIVELY POSITIVE FOR SARS-COV-2, THE CORONAVIRUS ASSOCIATED WITH COVID-19. THIS RESULT WAS UNABLE TO BE CONFIRMED AS POSITIVE BY A SECOND TEST METHOD.' ~ '1',
    TRUE ~ NA_character_
  ))



covid_lab_patient_num_filtered <- covid_lab_patient_num %>% filter(!is.na(pcr_test_pos))




covid_lab_pcr_aggregate <- covid_lab_patient_num_filtered %>% group_by(PATIENT_NUM,pcr_test_pos) %>% 
  summarise(count_tests=n(),max_date=max(START_DATE), min_date=min(START_DATE)) %>%
  pivot_wider(names_from = pcr_test_pos, values_from = c(count_tests, max_date,min_date)) %>%
  replace_na(list(count_tests_0 = 0, count_tests_1 = 0)) %>%
  mutate(any_pcr_pos = if_else(count_tests_1 >= 1,'1','0')) %>%
  mutate(date_first_test_pcr = if_else(any_pcr_pos == '1',min_date_1,min_date_0)) %>%
  mutate(date_last_test_pcr = if_else(any_pcr_pos == '1',max_date_1,max_date_0)) %>%
  dplyr::select(PATIENT_NUM,any_pcr_pos,date_first_test_pcr,date_last_test_pcr) %>% ungroup()





patient_id_positive_covid_lab <- covid_lab_pcr_aggregate %>% filter(any_pcr_pos == '1') %>% dplyr::select(PATIENT_NUM)


covid_lab_pcr_aggregate %>% group_by(any_pcr_pos) %>% tally()






query <- "select * from COVID19_Mart.Analytics.EDWConceptAdmitDiagnosis"

EDWConceptAdmitDiagnosis <- sqlQuery(odbc, paste(query)) %>% 
  inner_join(patient_id_positive_covid_lab, by='PATIENT_NUM')



query <- "select * from COVID19_Mart.Analytics.EDWConceptGeneralDiagnosis"

EDWConceptGeneralDiagnosis <- sqlQuery(odbc, paste(query)) %>% 
  inner_join(patient_id_positive_covid_lab, by='PATIENT_NUM')






odbcCloseAll()



day_buffer <- 10




## Patients with a Positive COVID test
covid_positive_lab_result_patients <- covid_lab_pcr_aggregate %>%
  filter(any_pcr_pos == '1') %>%
  mutate(positive_lab_test_date_interval=interval(date_first_test_pcr,date_last_test_pcr+days(1)))






### Patients from Inpatient List that were positive for COVID
covid_positive_inpatient <- EDWInpatientAdmit %>% 
  inner_join(patient_id_positive_covid_lab, by='PATIENT_NUM') %>%
  mutate_at(vars(HospitalAdmitDTS,HospitalDischargeDTS), as.character) %>%
  mutate(HospitalDischargeDTS = if_else(is.na(HospitalDischargeDTS), '2020-07-15 00:00:00',HospitalDischargeDTS)) %>%
  mutate(admit_date=ymd_hms(HospitalAdmitDTS), discharge_date=ymd_hms(HospitalDischargeDTS)) %>%
  dplyr::select(PATIENT_NUM,PatientEncounterID,admit_date,discharge_date) %>%
  group_by(PATIENT_NUM,PatientEncounterID) %>% summarise(admit_date=min(admit_date), discharge_date=max(discharge_date)) %>%
  ungroup() %>%
  mutate(inpatient_interval=interval(admit_date-days(day_buffer),discharge_date+days(1)))



## Patients with an Encounter that has a COVID ICD code
covid_icd_encounter <- EDWConceptAdmitDiagnosis %>% 
  dplyr::select(PATIENT_NUM,EpicEncounterID,ConceptCD,DiagnosisNM,StartDTS,EndDTS) %>%
  bind_rows(EDWConceptGeneralDiagnosis %>% dplyr::select(PATIENT_NUM,EpicEncounterID,ConceptCD,DiagnosisNM,StartDTS,EndDTS)) %>%
  filter(str_detect(str_to_upper(ConceptCD),'U07.1') |
           str_detect(str_to_upper(ConceptCD),'B97.29') | 
           str_detect(str_to_upper(ConceptCD),'Z03.818') |
           str_detect(str_to_upper(ConceptCD),'Z20.828')
  ) %>% mutate(encounter_covid_icd = '1') %>%
  dplyr::select(PATIENT_NUM,EpicEncounterID,encounter_covid_icd) %>% unique() 



  
## Positive COVID patients with an Encounter with an ICD code
inpatient_covid_positive_inpatient_icd <- covid_positive_inpatient %>% 
  left_join(covid_icd_encounter, by=c('PATIENT_NUM','PatientEncounterID'='EpicEncounterID')) %>%
  filter(encounter_covid_icd == '1') %>% 
  dplyr::select(PATIENT_NUM,PatientEncounterID,encounter_covid_icd) %>% unique()


## Positive COVID patients which had a lab that overlapped with an encounter

lab_patient_positive <- covid_lab_patient_num_filtered %>% 
  filter(pcr_test_pos == '1') %>%
  mutate(test_date = ymd_hms(START_DATE)) %>% dplyr::select(PATIENT_NUM,test_date,pcr_test_pos) %>% unique() %>%
  mutate(test_date_interval = interval(test_date,test_date+days(1)))


inpatient_covid_positive_inpatient_lab <- covid_positive_inpatient %>% 
  left_join(lab_patient_positive, by=c('PATIENT_NUM')) %>%
  mutate(overlap_lab_inpatient = int_overlaps(inpatient_interval,test_date_interval)) %>%
  filter(overlap_lab_inpatient == 'TRUE') %>% dplyr::select(PATIENT_NUM,PatientEncounterID,overlap_lab_inpatient) %>% unique()
  


inpatient_covid_positive_encounter_lab_icd <- covid_positive_inpatient %>%
  left_join(inpatient_covid_positive_inpatient_icd, by=c('PATIENT_NUM','PatientEncounterID')) %>%
  left_join(inpatient_covid_positive_inpatient_lab, by=c('PATIENT_NUM','PatientEncounterID')) %>%
  mutate(inpatient_covid_flag = case_when(encounter_covid_icd == '1' ~ '1',
                                          overlap_lab_inpatient == 'TRUE' ~ '1',
                                          TRUE ~ '0'
                                          )) %>% 
  left_join(EDWMechanicalVentilation %>% select(PATIENT_NUM,PatientEncounterID=EpicEncounterID) %>% unique() %>% mutate(ventilation_flag='1'), 
            by=c('PATIENT_NUM','PatientEncounterID')) %>%
  replace_na(list(ventilation_flag = '0'))





patient_positive_covid_inpatient <- inpatient_covid_positive_encounter_lab_icd %>%  
  filter(inpatient_covid_flag == '1') %>% select(PATIENT_NUM) %>% unique() %>%
  mutate(inpatient_flag = '1')




patient_positive_covid_inpatient_ventilator <- inpatient_covid_positive_encounter_lab_icd %>%  
  filter(inpatient_covid_flag == '1' & ventilation_flag == '1' ) %>% select(PATIENT_NUM) %>% 
  unique() %>% mutate(inpatient_ventilator_flag = '1')





covid_positive_lab_result_inpatient_ventilator <- covid_positive_lab_result_patients %>% 
  left_join(patient_positive_covid_inpatient, by='PATIENT_NUM') %>%
  left_join(patient_positive_covid_inpatient_ventilator, by='PATIENT_NUM') %>%
  replace_na(list(inpatient_flag = '0',inpatient_ventilator_flag = '0'))
  



write_rds(covid_positive_lab_result_inpatient_ventilator, 'data/covid_inpatient.rds')



write_rds(covid_lab_pcr_aggregate, 'data/covid_lab_pcr_aggregate.rds')


