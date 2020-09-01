# Code used for Analysis

This GitHub repository with code used for paper titled *Individual and Census Tract-Level Correlates of Local SARS-CoV-2 Prevalence and Outcomes in Eastern Massachusetts*.  This repository does not include any data or database credentials to reproduce analysis because the data contains personally identifiable information.


1. 1_process_age_race_insurance.R - Code used to extract race, language, insurance, and age variables for all patients in MGB COVID Data Mart
2. 2_run_libpostal.R - Code used to extract permanent and temporary addresses for MGB patients used in analysis.  Code extract appropriate address fields and combines into a single address string which is then parsed by the [libpostal library](https://github.com/openvenues/libpostal).
3. 3_extract_parsed_address.R - Code used to manually check addresses that were parsed incorrectly by the libpostal library and then modify as appropriate.  The code then combines street number, address, city, state, and zipcode to be used by the [DeGauss Geocoder](https://github.com/degauss-org/geocoder).  The end of the R script then shows the command line scripts needed to geocode the addresses.
4. 4_retrieve_acs_data.R - Code used to retrieve data the appropriate data from the American Community Survey through a local data resource (Exposome Data Warehouse).  The code also processes raw counts from the Census tract into a normalized form that can be used for analysis.
5. 5_process_geocoded_addresses.R - Code reads in the addresses and latitude/longitude parsed by the DeGauss geocoder, it checks whether the latitude/longitude intersects with the given zipcode.  The code is then used to manually check for addresses that do not seem to have been geocoded properly so that they can be manually changed to the appropriate latitude/longitude through manual checks.
6. 6_merge_patient_acs.R - Code used to merge the geocoded patients to the appropriate Census tract variables.
7. 7_process_labs_inpatient.R - Code used to extract the 3 SARS-CoV-2 outcomes used in the paper.
8. 8_manuscript_markdown.Rmd - R Markdown file used to merge all data sets and conduct all statistical analysis including the various logistic mixed model analysis used in the paper.
9. 8_manuscript_markdown.html - HTML version of R Markdown file.
