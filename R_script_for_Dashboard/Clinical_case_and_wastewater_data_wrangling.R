library(scales)
library(tidyverse)
library(readxl)
library(dplyr)
library(lubridate)
library(rlang)
library(writexl)

##################################################################################
#preparing clinical case data 
#Clinical case data dump from SDW to CVI server sent as multiple excel spreadsheets
#Since the table format and columns are the same we first use bind_rows to combine 
#the different spreadsheets into one dataframe

cov211<- read_xlsx("./clinical_cases/SARS/covid_21_1.xlsx")
cov212<- read_xlsx("./clinical_cases/SARS/covid_21_2.xlsx")
cov213<- read_xlsx("./clinical_cases/SARS/covid_21_3.xlsx")
cov214<- read_xlsx("./clinical_cases/SARS/covid_21_4.xlsx")
cov215<- read_xlsx("./clinical_cases/SARS/covid_21_5.xlsx")
cov216<- read_xlsx("./clinical_cases/SARS/covid_21_6.xlsx")
cov217<- read_xlsx("./clinical_cases/SARS/covid_21_7.xlsx")
cov218<- read_xlsx("./clinical_cases/SARS/covid_21_8.xlsx")
cov219<- read_xlsx("./clinical_cases/SARS/covid_21_9.xlsx")
cov2110<- read_xlsx("./clinical_cases/SARS/covid_21_10.xlsx")
cov2111<- read_xlsx("./clinical_cases/SARS/covid_21_11.xlsx")
cov221<- read_xlsx("./clinical_cases/SARS/covid_22_1.xlsx")
cov222<- read_xlsx("./clinical_cases/SARS/covid_22_2.xlsx")
cov223<- read_xlsx("./clinical_cases/SARS/covid_22_3.xlsx")
cov23<- read_xlsx("./clinical_cases/SARS/covid_23.xlsx")
cov24<- read_xlsx("./clinical_cases/SARS/covid_24.xlsx")

covcases <- bind_rows(cov211, cov212, cov213, cov214, cov215, cov216, cov217, cov218, cov219, 
                      cov2110, cov2111, cov221, cov222,cov223, cov23, cov24)


#View(cases)

#counting Districts in this dataset 

#table(covcases$District)

#filtering the data by Districts our wwtp are in 

covcases <- covcases %>%
  filter(District == "GP CITY OF JOHANNESBURG METRO" | District == "City of Johannesburg Metro" |
           District == "FS MANGAUNG METRO" | District == "Mangaung Metro" |
           District == "EC NELSON MANDELA BAY METRO" | District == "eThekwini Metro" |
           District == "GP CITY OF TSHWANE METRO" | District == "City of Tshwane Metro" |
           District == "GP EKURHULENI METRO" | District == "Ekurhuleni Metro" |
           District == "KZN ETHEKWINI METRO" | District == "eThekwini Metro" |
           District == "WC CITY OF CAPE TOWN METRO" | District == "City of Cape Town Metro" |
           District == "EC BUFFALO CITY METRO"| District == "Buffalo City Metro" |
           District == "NW BOJANALA PLATINUM"| District == "Bojanala Platinum" | 
           District == "NW DR KENNETH KAUNDA"| District ==  "Dr Kenneth Kaunda" | 
           District == "Ngaka Modiri Molema"| District ==  "NW NGAKA MODIRI MOLEMA" | 
           District == "Ehlanzeni"| District == "MP EHLANZENI"| 
           District == "LP VHEMBE"| District == "Vhembe" | 
           District == "uMkhanyakude"| District == "KZN UMKHANYAKUDE" | 
           District == "NC FRANCES BAARD"| District == "Frances Baard" 
  )


#Setting up epiweeks for x-axis

covcases <- covcases %>% 
  filter(Diagnosis_Method == "Laboratory confirmed")
covcases$newcoldate <- format(as.Date(covcases$Notification_Date, format = "%Y/%m/%d"), "%Y-%m-%d") #changing the format of the date from y/m/d to ymd 
covcases$epiweek <- lubridate::epiweek(ymd( covcases$newcoldate)) #generate epiweek
covcases$year <- strftime(covcases$newcoldate, "%Y") #Creating year column  
covcases$week <- "w" #added column with w
my_cols <- c("year", "week", "epiweek") #new data object with 3 columns combined
covcases$epiweek2 <- do.call(paste, c(covcases[my_cols],sep ="")) #created new variable using concat columns

cases1 <- covcases

#filtering by individual Districts 

joburg_cases <- cases1 %>%
  filter(District == "GP CITY OF JOHANNESBURG METRO" | District == "City of Johannesburg Metro")

mangaung_cases <- cases1 %>%
  filter(District == "FS MANGAUNG METRO" | District == "Mangaung Metro")

nelson_cases <- cases1 %>%
  filter(District == "EC NELSON MANDELA BAY METRO" | District == "eThekwini Metro" )

tshwane_cases <- cases1 %>%
  filter( District == "GP CITY OF TSHWANE METRO" | District == "City of Tshwane Metro")

ekurhuleni_cases <- cases1 %>%
  filter(District == "GP EKURHULENI METRO" | District == "Ekurhuleni Metro" )

ethekwini_cases <- cases1 %>%
  filter(District == "KZN ETHEKWINI METRO" | District == "eThekwini Metro")

capetown_cases <- cases1 %>%
  filter(District == "WC CITY OF CAPE TOWN METRO" | District == "City of Cape Town Metro" )

buffalo_cases <- cases1 %>%
  filter(District == "EC BUFFALO CITY METRO"| District == "Buffalo City Metro" )

rustenburg_cases <- cases1 %>%
  filter(District == "NW BOJANALA PLATINUM"| District == "Bojanala Platinum" )

jbmarks_cases <- cases1 %>%
  filter(District == "Ngaka Modiri Molema"| District ==  "NW NGAKA MODIRI MOLEMA" )

ehlanzeni_cases <- cases1 %>%
  filter(District == "Ehlanzeni"| District == "MP EHLANZENI")

vhembe_cases <- cases1 %>%
  filter(District == "LP VHEMBE"| District == "Vhembe" )

frances_cases <- cases1 %>%
  filter(District == "NC FRANCES BAARD"| District == "Frances Baard" )

umkhanyakude_cases <- cases1 %>%
  filter(District == "uMkhanyakude"| District == "KZN UMKHANYAKUDE")


################################################################################

###############################################################################

#Preparing wastewater levels 
#To obtain the API token, log into RedCap and select API on the left hand side 
#under the Applications tab. Generate an API token and insert into the code below 
#(Ensure you have permissions from your administrator). The RedCap API playground 
#may also be used if you would like to modify the code below to suit your specific requirements.

token <- "insert-redcap-api-token-here"
url <- "insert-redcap-ur-here"
formData <- list("token"=token,
                 content='record',
                 action='export',
                 format='csv',
                 type='flat',
                 csvDelimiter='',
                 rawOrLabel='label',
                 rawOrLabelHeaders='raw',
                 exportCheckboxLabel='false',
                 exportSurveyFields='false',
                 exportDataAccessGroups='false',
                 returnFormat='json'
)
response <- httr::POST(url, body = formData, encode = "form")
result <- httr::content(response)

data <- result

data <- data %>% 
  filter( site_name == "Northern Wastewater Treatment Works (GP)" |
            site_name == "Northern Wastewater Treatment Works (KZN)"| 
            site_name == "Rooiwal Wastewater Treatment Works"| 
            site_name ==  "ERWAT Vlakplaat Wastewater Treatment Works"|
            site_name ==  "Central Wastewater Treatment Works (KZN)"|
            site_name ==  "Goudkoppies Wastewater Treatment Works"|
            site_name ==  "Hartebeesfontein Waterworks"|
            site_name ==  "Daspoort Wastewater Treatment Works" |
            site_name ==  "Zandvleit Wastewater Treatment Works"|
            site_name == "Borcherds Quarry Wastewater Treatment Works"|
            site_name == "Brickfield Pre-treatment Works" |
            site_name == "Sterkwater Wastewater Treatment Works"|
            site_name == "Bloemspruit Wastewater Treatment Works"|
            site_name ==  "Kwanobuhle Wastewater Treatment Works" |
            site_name == "East Bank Wastewater Treatment Works"| 
            site_name ==  "Mdantsane Wastewater Treatment Works" |
            site_name == "Mmabatho Water Treatment Works" |
            site_name == "Musina WWTW (in town)" |
            site_name == "Kingstonvale"|
            site_name == "Boitekong"| 
            site_name == "Rustenburg Wastewater Treatment Works" |
            site_name == "Nancefield"| 
            site_name == "Komatipoort Sewage plant"|
            site_name == "Mahikeng Water TreatmentWorks"|
            site_name == "Homevale Kimberley"|
            site_name == "Silulumanzi Waste Water Treatment Plant"|
            site_name == "Jozini Wastewater Treatment Plant"|
            site_name == "Manguzi Wastewater Treatment Plant"
  )



###############################################################################
#Setting up epiweeks for x-axis 

water1 <- data 
water1 <- water1 %>% arrange(ymd(water1$sam_col_date))
water1$epiweek <- lubridate::epiweek(ymd(water1$sam_col_date)) #generate epiweek
water1$year <- strftime(water1$sam_col_date, "%Y") #Creating year column  
water1$week <- "w" #added column with w
my_cols <- c("year", "week", "epiweek") #new data object with 3 columns combined
water1$epiweek2 <- do.call(paste, c(water1[my_cols],sep ="")) #created new variable using concat columns

#setting up columns
water1$levels <- water1$n_gene_ml
water1$Date <- water1$sam_col_date
water1$Result <-water1$sars_cov_2_pcr_result
water1 <- water1 %>%
  mutate(levels = na_if(levels, levels < 0))
water1 <- water1 %>%
  mutate(levels = if_else(levels<2.34, 2.34, levels)) #replace lower than 2.34 with 2.34 (limit of quantification)
water1$loglevels <- log10(water1$levels)


###############################################################################


#South Africa 

#Tabulate number of samples we've received 

rsa_samples <- cases1 %>%
  group_by(epiweek2)%>%
  count(epiweek2, na.rm=TRUE)

rsa_water <- water1 

rsacopies <- rsa_water %>% 
  group_by(epiweek2)%>%
  summarise(sum_genomes = sum(levels,na.rm = TRUE),
            .groups = 'keep')

rsaww<- rsa_water %>% 
  group_by(epiweek2)%>%
  count(epiweek2, na.rm=TRUE)

rsaww$no_ww_collected <- rsaww$n

rsaww<- rsaww %>% 
  select(epiweek2,no_ww_collected)

rsacopies["sum_genomes"][rsacopies["sum_genomes"] == 0] <- NA 

rsacases_vs_water<- full_join(rsa_samples, rsa_water, by= "epiweek2")
rsacases_vs_water<- full_join(rsacases_vs_water, rsacopies, by= "epiweek2")
rsacases_vs_water<- full_join(rsacases_vs_water, rsaww, by= "epiweek2")


#repeating this just for weeks where no ww samples but clinical cases- otherwise would be blank

rsacases_vs_water <- rsacases_vs_water %>%
  mutate(pcr_type = if_else(year == 2021, "Real-Time", pcr_type)) %>%
  mutate(pcr_type = if_else(year == 2022, "Real-Time", pcr_type)) %>%
  mutate(pcr_type = if_else(year == 2023 & week < 30, "Real-Time", pcr_type)) %>%
  mutate(pcr_type = if_else(year == 2023 & week > 30, "dPCR", pcr_type)) %>%
  mutate(pcr_type = if_else(year == 2024, "dPCR", pcr_type)) %>%
  mutate(pcr_type = if_else(is.na(pcr_type), "Real-Time", pcr_type)) 


rsacases_vs_water$final_result <- rsacases_vs_water$sars_cov_2_pcr_result

rsacases_vs_water <- rsacases_vs_water %>%
  select(epiweek2, n, no_ww_collected, sum_genomes, Date, pcr_type)

rsacases_vs_water$loglevels <- log10(rsacases_vs_water$sum_genomes)

rsacases_vs_water<- rsacases_vs_water %>%
  mutate(tested = case_when( (sum_genomes > 0) ~ -0.3)) %>%
  mutate(n = if_else(is.na(n), 0, n)) %>% #replace na with 0
  mutate(no_ww_collected = if_else(is.na(no_ww_collected), 0, no_ww_collected))


rsacases_vs_water$epiweek3 <- rsacases_vs_water$epiweek2

rsacases_vs_water <- rsacases_vs_water %>%
  separate(epiweek3, sep = "w", into = c("year", "week")) %>%
  mutate(across(c("year", "week"), as.integer)) 

rsacases_vs_water <- rsacases_vs_water %>%
  filter(year != 2020)


rsacases_vs_water <-  rsacases_vs_water[ #ordering by year first then week
  with(rsacases_vs_water, order(year, week)),
]

rsacases_vs_water$epiweek2 <- factor(rsacases_vs_water$epiweek2, levels = unique(rsacases_vs_water$epiweek2), ordered = T)

rsacases_vs_water$Country <- "South African SARS-CoV-2 Wastewater Levels"

rsacases_vs_water$Date <- "w"

#remove duplicated rows 

rsacases_vs_water <- rsacases_vs_water %>% distinct()

rsacases_vs_water$end <- lubridate::parse_date_time(paste(rsacases_vs_water$year, rsacases_vs_water$week,0, sep="-"),'Y-W-w') + days(6)
#lubridate::parse_date_time(year, week, 0= week start on sunday, sep = formwat you want)
#this gives start of epiweek so add 6 days to get end of epiweek


write.csv(rsacases_vs_water,"./output_files/example_rsa_cases_vs_levels.csv",row.names=FALSE)


##############################################################################

#COJ

#Tabulate number of samples we've received 

jhb_samples <- joburg_cases %>%
  group_by(epiweek2)%>%
  count(epiweek2, na.rm=TRUE)

#filter for jhb www 

jhb_water <- water1 %>% 
  filter(district_name == "Johannesburg MM") %>%
  filter( site_name == "Goudkoppies Wastewater Treatment Works" |
            site_name == "Northern Wastewater Treatment Works (GP)")

#merge the two df 

jhbcases_vs_water<- full_join(jhb_samples, jhb_water, by= "epiweek2")
jhbcases_vs_water$final_result <- jhbcases_vs_water$sars_cov_2_pcr_result


#selecting columns I want

jhbcases_vs_water <- jhbcases_vs_water %>% 
  select(epiweek2, n, site_name, 
         site_prov, district_name,n_gene_ml, levels, loglevels, Date, final_result, pcr_type) %>% 
  filter(epiweek2 != "NAwNA")


jhbcases_vs_water<- jhbcases_vs_water %>%
  mutate(tested1 = case_when( (site_name == "Goudkoppies Wastewater Treatment Works" & final_result == "Positive") ~ -0.3, 
                              (site_name == "Goudkoppies Wastewater Treatment Works" & final_result == "Negative") ~ -0.3)) %>%
  mutate(tested2 = case_when( (site_name == "Northern Wastewater Treatment Works (GP)" & final_result == "Positive") ~ -0.1, 
                              (site_name == "Northern Wastewater Treatment Works (GP)" & final_result == "Negative") ~ -0.1))


jhbcases_vs_water$epiweek3 <- jhbcases_vs_water$epiweek2

jhbcases_vs_water <- jhbcases_vs_water %>%
  separate(epiweek3, sep = "w", into = c("year", "week")) %>%
  mutate(across(c("year", "week"), as.integer)) 

jhbcases_vs_water <- jhbcases_vs_water %>%
  filter(year != 2020)


jhbcases_vs_water <-  jhbcases_vs_water[ #ordering by year first then week
  with(jhbcases_vs_water, order(year, week)),
]

jhbcases_vs_water$epiweek2 <- factor(jhbcases_vs_water$epiweek2, levels = unique(jhbcases_vs_water$epiweek2), ordered = T)

jhbcases_vs_water <- jhbcases_vs_water %>%
  mutate(pcr_type = if_else(year == 2021, "Real-Time", pcr_type)) %>%
  mutate(pcr_type = if_else(year == 2022, "Real-Time", pcr_type)) %>%
  mutate(pcr_type = if_else(year == 2023 & week < 30, "Real-Time", pcr_type)) %>%
  mutate(pcr_type = if_else(year == 2023 & week > 30, "dPCR", pcr_type)) %>%
  mutate(pcr_type = if_else(is.na(pcr_type), "Real-Time", pcr_type)) %>%
  mutate(n = if_else(is.na(n), 0, n)) #replace na with 0



#################################################################
#Tshwane

#Tabulate number of samples we've received 

tshwane_samples <- tshwane_cases %>%
  group_by(epiweek2)%>%
  count(epiweek2, na.rm=TRUE)

#filter for wwtp 

tshwane_water <- water1 %>% 
  filter(district_name == "Tshwane MM") %>%
  filter( site_name == "Rooiwal Wastewater Treatment Works" |
            site_name == "Daspoort Wastewater Treatment Works")

#merge the two df 

tshwanecases_vs_water<- full_join(tshwane_samples, tshwane_water, by= "epiweek2")
tshwanecases_vs_water$final_result <- tshwanecases_vs_water$sars_cov_2_pcr_result


#selecting columns I want

tshwanecases_vs_water <- tshwanecases_vs_water %>% 
  select(epiweek2, n, site_name, 
         site_prov, district_name,n_gene_ml, levels, loglevels, Date, final_result, pcr_type) %>% 
  filter(epiweek2 != "NAwNA")


tshwanecases_vs_water<- tshwanecases_vs_water%>%
  mutate(tested1 = case_when( (site_name == "Rooiwal Wastewater Treatment Works" & final_result == "Positive") ~ -0.3, 
                              (site_name == "Rooiwal Wastewater Treatment Works" & final_result == "Negative") ~ -0.3)) %>%
  mutate(tested2 = case_when( (site_name == "Daspoort Wastewater Treatment Works" & final_result == "Positive") ~ -0.1, 
                              (site_name == "Daspoort Wastewater Treatment Works" & final_result == "Negative") ~ -0.1))


tshwanecases_vs_water$epiweek3 <- tshwanecases_vs_water$epiweek2

tshwanecases_vs_water <- tshwanecases_vs_water %>%
  separate(epiweek3, sep = "w", into = c("year", "week")) %>%
  mutate(across(c("year", "week"), as.integer))  %>%
  mutate(n = if_else(is.na(n), 0, n)) #replace na with 0

tshwanecases_vs_water<- tshwanecases_vs_water %>%
  filter(year != 2020)


tshwanecases_vs_water <-  tshwanecases_vs_water[ #ordering by year first then week
  with(tshwanecases_vs_water, order(year, week)),
]

tshwanecases_vs_water$epiweek2 <- factor(tshwanecases_vs_water$epiweek2, levels = unique(tshwanecases_vs_water$epiweek2), ordered = T)

tshwanecases_vs_water <- tshwanecases_vs_water %>%
  mutate(pcr_type = if_else(year == 2021, "Real-Time", pcr_type)) %>%
  mutate(pcr_type = if_else(year == 2022, "Real-Time", pcr_type)) %>%
  mutate(pcr_type = if_else(year == 2023 & week < 30, "Real-Time", pcr_type)) %>%
  mutate(pcr_type = if_else(year == 2023 & week > 30, "dPCR", pcr_type)) %>%
  mutate(pcr_type = if_else(is.na(pcr_type), "Real-Time", pcr_type)) %>%
  mutate(n = if_else(is.na(n), 0, n)) #replace na with 0

#############################################################################

# Ekurhuleni

#Tabulate number of samples we've received 

ekurhuleni_samples <- ekurhuleni_cases %>%
  group_by(epiweek2)%>%
  count(epiweek2, na.rm=TRUE)

#filter for wwtp 

ekurhuleni_water <- water1 %>% 
  filter(district_name == "Ekurhuleni MM") %>%
  filter( site_name == "ERWAT Vlakplaat Wastewater Treatment Works" |
            site_name == "Hartebeesfontein Waterworks")

#merge the two df 

ekurhulencases_vs_water<- full_join(ekurhuleni_samples, ekurhuleni_water, by= "epiweek2")
ekurhulencases_vs_water$final_result <- ekurhulencases_vs_water$sars_cov_2_pcr_result


#selecting columns I want

ekurhulencases_vs_water<- ekurhulencases_vs_water %>% 
  select(epiweek2, n, site_name, 
         site_prov, district_name, n_gene_ml, levels, loglevels, Date, final_result, pcr_type) %>% 
  filter(epiweek2 != "NAwNA") %>%
  mutate(n = if_else(is.na(n), 0, n)) #replace na with 0


ekurhulencases_vs_water<- ekurhulencases_vs_water%>%
  mutate(tested1 = case_when( (site_name == "ERWAT Vlakplaat Wastewater Treatment Works" & final_result == "Positive") ~ -0.3, 
                              (site_name == "ERWAT Vlakplaat Wastewater Treatment Works" & final_result == "Negative") ~ -0.3)) %>%
  mutate(tested2 = case_when( (site_name == "Hartebeesfontein Waterworks" & final_result == "Positive") ~ -0.1, 
                              (site_name == "Hartebeesfontein Waterworks" & final_result == "Negative") ~ -0.1))


ekurhulencases_vs_water$epiweek3 <- ekurhulencases_vs_water$epiweek2

ekurhulencases_vs_water <- ekurhulencases_vs_water %>%
  separate(epiweek3, sep = "w", into = c("year", "week")) %>%
  mutate(across(c("year", "week"), as.integer)) 

ekurhulencases_vs_water<- ekurhulencases_vs_water %>%
  filter(year != 2020)


ekurhulencases_vs_water <-  ekurhulencases_vs_water[ #ordering by year first then week
  with(ekurhulencases_vs_water, order(year, week)),
]

ekurhulencases_vs_water$epiweek2 <- factor(ekurhulencases_vs_water$epiweek2, levels = unique(ekurhulencases_vs_water$epiweek2), ordered = T)

ekurhulencases_vs_water <- ekurhulencases_vs_water %>%
  mutate(pcr_type = if_else(year == 2021, "Real-Time", pcr_type)) %>%
  mutate(pcr_type = if_else(year == 2022, "Real-Time", pcr_type)) %>%
  mutate(pcr_type = if_else(year == 2023 & week < 30, "Real-Time", pcr_type)) %>%
  mutate(pcr_type = if_else(year == 2023 & week > 30, "dPCR", pcr_type)) %>%
  mutate(pcr_type = if_else(year == 2024, "dPCR", pcr_type)) %>%
  mutate(n = if_else(is.na(n), 0, n)) #replace na with 0

#######################################################################
#eThekwini

#Tabulate number of samples we've received 

ethekwini_samples <- ethekwini_cases %>%
  group_by(epiweek2)%>%
  count(epiweek2, na.rm=TRUE)

#filter for wwtp 

ethekwini_water <- water1 %>% 
  filter(district_name == "Ethekwini MM") 



#merge the two df 

ethekwinicases_vs_water<- full_join(ethekwini_samples, ethekwini_water, by= "epiweek2")
ethekwinicases_vs_water$final_result <- ethekwinicases_vs_water$sars_cov_2_pcr_result


#selecting columns I want

ethekwinicases_vs_water<- ethekwinicases_vs_water %>% 
  select(epiweek2, n, site_name, 
         site_prov, district_name, n_gene_ml, levels, loglevels, Date, final_result, pcr_type) %>% 
  filter(epiweek2 != "NAwNA") %>%
  mutate(n = if_else(is.na(n), 0, n)) #replace na with 0


ethekwinicases_vs_water<- ethekwinicases_vs_water%>%
  mutate(tested1 = case_when( (site_name == "Central Wastewater Treatment Works (KZN)" & final_result == "Positive") ~ -0.3, 
                              (site_name == "Central Wastewater Treatment Works (KZN)" & final_result == "Negative") ~ -0.3)) %>%
  mutate(tested2 = case_when( (site_name == "Northern Wastewater Treatment Works (KZN)"& final_result == "Positive") ~ -0.1, 
                              (site_name == "Northern Wastewater Treatment Works (KZN)" & final_result == "Negative") ~ -0.1))


ethekwinicases_vs_water$epiweek3 <- ethekwinicases_vs_water$epiweek2

ethekwinicases_vs_water <- ethekwinicases_vs_water %>%
  separate(epiweek3, sep = "w", into = c("year", "week")) %>%
  mutate(across(c("year", "week"), as.integer)) 

ethekwinicases_vs_water<- ethekwinicases_vs_water%>%
  filter(year != 2020)


ethekwinicases_vs_water <-  ethekwinicases_vs_water[ #ordering by year first then week
  with(ethekwinicases_vs_water, order(year, week)),
]

ethekwinicases_vs_water$epiweek2 <- factor(ethekwinicases_vs_water$epiweek2, levels = unique(ethekwinicases_vs_water$epiweek2), ordered = T)

ethekwinicases_vs_water <- ethekwinicases_vs_water %>%
  mutate(pcr_type = if_else(year == 2021, "Real-Time", pcr_type)) %>%
  mutate(pcr_type = if_else(year == 2022, "Real-Time", pcr_type)) %>%
  mutate(pcr_type = if_else(year == 2023 & week < 30, "Real-Time", pcr_type)) %>%
  mutate(pcr_type = if_else(year == 2023 & week > 30, "dPCR", pcr_type)) %>%
  mutate(pcr_type = if_else(year == 2024, "dPCR", pcr_type)) %>%
  mutate(n = if_else(is.na(n), 0, n)) #replace na with 0

#############################################################################

#Mangaung

#Tabulate number of samples we've received 

mangaung_samples <- mangaung_cases %>%
  group_by(epiweek2)%>%
  count(epiweek2, na.rm=TRUE)

#filter for wwtp 

mangaung_water <- water1 %>% 
  filter(district_name == "Mangaung MM") 


mangaungcases_vs_water<- full_join(mangaung_samples, mangaung_water, by= "epiweek2")
mangaungcases_vs_water$final_result <- mangaungcases_vs_water$sars_cov_2_pcr_result


#selecting columns I want

mangaungcases_vs_water<- mangaungcases_vs_water %>% 
  select(epiweek2, n, site_name, 
         site_prov,district_name,n_gene_ml, levels, loglevels, Date, final_result, pcr_type) %>% 
  filter(epiweek2 != "NAwNA") %>%
  mutate(n = if_else(is.na(n), 0, n)) #replace na with 0


mangaungcases_vs_water<- mangaungcases_vs_water%>%
  mutate(tested1 = case_when( (site_name == "Bloemspruit Wastewater Treatment Works" & final_result == "Positive") ~ -0.3, 
                              (site_name == "Bloemspruit Wastewater Treatment Works" & final_result == "Negative") ~ -0.3)) %>%
  mutate(tested2 = case_when( (site_name == "Sterkwater Wastewater Treatment Works"& final_result == "Positive") ~ -0.1, 
                              (site_name == "Sterkwater Wastewater Treatment Works" & final_result == "Negative") ~ -0.1))


mangaungcases_vs_water$epiweek3 <- mangaungcases_vs_water$epiweek2

mangaungcases_vs_water<- mangaungcases_vs_water %>%
  separate(epiweek3, sep = "w", into = c("year", "week")) %>%
  mutate(across(c("year", "week"), as.integer)) 

mangaungcases_vs_water<- mangaungcases_vs_water%>%
  filter(year != 2020)


mangaungcases_vs_water<-  mangaungcases_vs_water[ #ordering by year first then week
  with(mangaungcases_vs_water, order(year, week)),
]

mangaungcases_vs_water$epiweek2 <- factor(mangaungcases_vs_water$epiweek2, levels = unique(mangaungcases_vs_water$epiweek2), ordered = T)

mangaungcases_vs_water <- mangaungcases_vs_water %>%
  mutate(pcr_type = if_else(year == 2021, "Real-Time", pcr_type)) %>%
  mutate(pcr_type = if_else(year == 2022, "Real-Time", pcr_type)) %>%
  mutate(pcr_type = if_else(year == 2023 & week < 30, "Real-Time", pcr_type)) %>%
  mutate(pcr_type = if_else(year == 2023 & week > 30, "dPCR", pcr_type)) %>%
  mutate(pcr_type = if_else(year == 2024, "dPCR", pcr_type)) %>%
  mutate(n = if_else(is.na(n), 0, n)) #replace na with 0

##########################################################################

#Nelson Mandela 

#Tabulate number of samples we've received 

nelson_samples <- nelson_cases %>%
  group_by(epiweek2)%>%
  count(epiweek2, na.rm=TRUE)

#filter for wwtp 

nelson_water <- water1 %>% 
  filter(district_name == "Nelson Mandela Bay MM") 


#merge the two df 

nelsoncases_vs_water<- full_join(nelson_samples, nelson_water, by= "epiweek2")
nelsoncases_vs_water$final_result <- nelsoncases_vs_water$sars_cov_2_pcr_result


#selecting columns I want

nelsoncases_vs_water<- nelsoncases_vs_water %>% 
  select(epiweek2, n, site_name, 
         site_prov,district_name,n_gene_ml, levels, loglevels, Date, final_result, pcr_type) %>% 
  filter(epiweek2 != "NAwNA") %>%
  mutate(n = if_else(is.na(n), 0, n)) #replace na with 0


nelsoncases_vs_water<- nelsoncases_vs_water%>%
  mutate(tested1 = case_when( (site_name == "Brickfield Pre-treatment Works" & final_result == "Positive") ~ -0.3, 
                              (site_name == "Brickfield Pre-treatment Works" & final_result == "Negative") ~ -0.3)) %>%
  mutate(tested2 = case_when( (site_name == "Kwanobuhle Wastewater Treatment Works"& final_result == "Positive") ~ -0.1, 
                              (site_name == "Kwanobuhle Wastewater Treatment Works" & final_result == "Negative") ~ -0.1))


nelsoncases_vs_water$epiweek3 <- nelsoncases_vs_water$epiweek2

nelsoncases_vs_water<- nelsoncases_vs_water %>%
  separate(epiweek3, sep = "w", into = c("year", "week")) %>%
  mutate(across(c("year", "week"), as.integer)) 

nelsoncases_vs_water<- nelsoncases_vs_water%>%
  filter(year != 2020)


nelsoncases_vs_water<-  nelsoncases_vs_water[ #ordering by year first then week
  with(nelsoncases_vs_water, order(year, week)),
]

nelsoncases_vs_water$epiweek2 <- factor(nelsoncases_vs_water$epiweek2, levels = unique(nelsoncases_vs_water$epiweek2), ordered = T)

nelsoncases_vs_water <- nelsoncases_vs_water %>%
  mutate(pcr_type = if_else(year == 2021, "Real-Time", pcr_type)) %>%
  mutate(pcr_type = if_else(year == 2022, "Real-Time", pcr_type)) %>%
  mutate(pcr_type = if_else(year == 2023 & week < 30, "Real-Time", pcr_type)) %>%
  mutate(pcr_type = if_else(year == 2023 & week > 30, "dPCR", pcr_type)) %>%
  mutate(pcr_type = if_else(year == 2024, "dPCR", pcr_type)) %>%
  mutate(n = if_else(is.na(n), 0, n)) #replace na with 0

############################################################################

#Buffalo City 

#Tabulate number of samples we've received 

buffalo_samples <- buffalo_cases %>%
  group_by(epiweek2)%>%
  count(epiweek2, na.rm=TRUE)

#filter for wwtp 

buffalo_water <- water1 %>% 
  filter(district_name == "Buffalo City MM") 

#merge the two df 

buffalocases_vs_water<- full_join(buffalo_samples, buffalo_water, by= "epiweek2")
buffalocases_vs_water$final_result <- buffalocases_vs_water$sars_cov_2_pcr_result


#selecting columns I want

buffalocases_vs_water<- buffalocases_vs_water %>% 
  select(epiweek2, n, site_name, 
         site_prov,district_name,n_gene_ml, levels, loglevels, Date, final_result, pcr_type) %>% 
  filter(epiweek2 != "NAwNA") %>%
  mutate(n = if_else(is.na(n), 0, n)) #replace na with 0


buffalocases_vs_water<- buffalocases_vs_water%>%
  mutate(tested1 = case_when( (site_name == "East Bank Wastewater Treatment Works" & final_result == "Positive") ~ -0.3, 
                              (site_name == "East Bank Wastewater Treatment Works" & final_result == "Negative") ~ -0.3)) %>%
  mutate(tested2 = case_when( (site_name == "Mdantsane Wastewater Treatment Works"& final_result == "Positive") ~ -0.1, 
                              (site_name == "Mdantsane Wastewater Treatment Works" & final_result == "Negative") ~ -0.1))


buffalocases_vs_water$epiweek3 <- buffalocases_vs_water$epiweek2

buffalocases_vs_water<- buffalocases_vs_water%>%
  separate(epiweek3, sep = "w", into = c("year", "week")) %>%
  mutate(across(c("year", "week"), as.integer)) 

buffalocases_vs_water<- buffalocases_vs_water%>%
  filter(year != 2020)


buffalocases_vs_water<-  buffalocases_vs_water[ #ordering by year first then week
  with(buffalocases_vs_water, order(year, week)),
]

buffalocases_vs_water$epiweek2 <- factor(buffalocases_vs_water$epiweek2, levels = unique(buffalocases_vs_water$epiweek2), ordered = T)

buffalocases_vs_water <- buffalocases_vs_water %>%
  mutate(pcr_type = if_else(year == 2021, "Real-Time", pcr_type)) %>%
  mutate(pcr_type = if_else(year == 2022, "Real-Time", pcr_type)) %>%
  mutate(pcr_type = if_else(year == 2023 & week < 30, "Real-Time", pcr_type)) %>%
  mutate(pcr_type = if_else(year == 2023 & week > 30, "dPCR", pcr_type)) %>%
  mutate(pcr_type = if_else(year == 2024, "dPCR", pcr_type)) %>%
  mutate(n = if_else(is.na(n), 0, n)) #replace na with 0

##############################################################################

#Cape Town

#Tabulate number of samples we've received 

capetown_samples <-capetown_cases %>%
  group_by(epiweek2)%>%
  count(epiweek2, na.rm=TRUE)

#filter for wwtp 

capetown_water <- water1 %>% 
  filter(district_name == "Cape Town MM") 


#merge the two df 


capetown_vs_water<- full_join(capetown_samples, capetown_water, by= "epiweek2")
capetown_vs_water$final_result <- capetown_vs_water$sars_cov_2_pcr_result


#selecting columns I want

capetown_vs_water<- capetown_vs_water %>% 
  select(epiweek2, n, site_name, 
         site_prov,district_name,n_gene_ml, levels, loglevels, Date, final_result, pcr_type) %>% 
  filter(epiweek2 != "NAwNA") %>%
  mutate(n = if_else(is.na(n), 0, n)) #replace na with 0


capetown_vs_water<- capetown_vs_water%>%
  mutate(tested1 = case_when( (site_name == "Borcheds Quarry Wastewater Treatment Works" & final_result == "Positive") ~ -0.3, 
                              (site_name == "Borcheds Quarry Wastewater Treatment Works" & final_result == "Negative") ~ -0.3)) %>%
  mutate(tested2 = case_when( (site_name == "Zandvleit Wastewater Treatment Works"& final_result == "Positive") ~ -0.1, 
                              (site_name == "Zandvleit Wastewater Treatment Works" & final_result == "Negative") ~ -0.1))


capetown_vs_water$epiweek3 <- capetown_vs_water$epiweek2

capetown_vs_water<- capetown_vs_water%>%
  separate(epiweek3, sep = "w", into = c("year", "week")) %>%
  mutate(across(c("year", "week"), as.integer)) 

capetown_vs_water<- capetown_vs_water%>%
  filter(year != 2020)


capetown_vs_water<-  capetown_vs_water[ #ordering by year first then week
  with(capetown_vs_water, order(year, week)),
]

capetown_vs_water$epiweek2 <- factor(capetown_vs_water$epiweek2, levels = unique(capetown_vs_water$epiweek2), ordered = T)

capetown_vs_water <- capetown_vs_water %>%
  mutate(pcr_type = if_else(year == 2021, "Real-Time", pcr_type)) %>%
  mutate(pcr_type = if_else(year == 2022, "Real-Time", pcr_type)) %>%
  mutate(pcr_type = if_else(year == 2023 & week < 30, "Real-Time", pcr_type)) %>%
  mutate(pcr_type = if_else(year == 2023 & week > 30, "dPCR", pcr_type)) %>%
  mutate(pcr_type = if_else(year == 2024, "dPCR", pcr_type)) %>%
  mutate(n = if_else(is.na(n), 0, n)) #replace na with 0

##########################################################################

#Vhembe

#Tabulate number of samples we've received 

vhembe_samples <-vhembe_cases %>%
  group_by(epiweek2)%>%
  count(epiweek2, na.rm=TRUE)

#filter for wwtp 

vhembe_water <- water1 %>% 
  filter(district_name == "Vhembe DM") 


#merge the two df 


vhembe_vs_water<- full_join(vhembe_samples, vhembe_water, by= "epiweek2")
vhembe_vs_water$final_result <- vhembe_vs_water$sars_cov_2_pcr_result


#selecting columns I want

vhembe_vs_water<- vhembe_vs_water %>% 
  select(epiweek2, n, site_name, 
         site_prov,district_name,n_gene_ml, levels, loglevels, Date, final_result, pcr_type) %>% 
  filter(epiweek2 != "NAwNA") %>%
  mutate(n = if_else(is.na(n), 0, n)) #replace na with 0


vhembe_vs_water<- vhembe_vs_water%>%
  mutate(tested1 = case_when( (site_name == "Musina WWTW (in town)" & final_result == "Positive") ~ -0.3, 
                              (site_name == "Musina WWTW (in town)" & final_result == "Negative") ~ -0.3)) %>%
  mutate(tested2 = case_when( (site_name == "Nancefield"& final_result == "Positive") ~ -0.1, 
                              (site_name == "Nancefield" & final_result == "Negative") ~ -0.1))


vhembe_vs_water$epiweek3 <- vhembe_vs_water$epiweek2

vhembe_vs_water<- vhembe_vs_water%>%
  separate(epiweek3, sep = "w", into = c("year", "week")) %>%
  mutate(across(c("year", "week"), as.integer)) 

vhembe_vs_water<- vhembe_vs_water%>%
  filter(year != 2020)


vhembe_vs_water<-  vhembe_vs_water[ #ordering by year first then week
  with(vhembe_vs_water, order(year, week)),
]

vhembe_vs_water$epiweek2 <- factor(vhembe_vs_water$epiweek2, levels = unique(vhembe_vs_water$epiweek2), ordered = T)

vhembe_vs_water <- vhembe_vs_water %>%
  mutate(pcr_type = if_else(year == 2021, "Real-Time", pcr_type)) %>%
  mutate(pcr_type = if_else(year == 2022, "Real-Time", pcr_type)) %>%
  mutate(pcr_type = if_else(year == 2023 & week < 30, "Real-Time", pcr_type)) %>%
  mutate(pcr_type = if_else(year == 2023 & week > 30, "dPCR", pcr_type)) %>%
  mutate(pcr_type = if_else(year == 2024, "dPCR", pcr_type)) %>%
  mutate(n = if_else(is.na(n), 0, n)) #replace na with 0

#################################################################################

#Ehlanzeni

#Tabulate number of samples we've received 

ehlanzeni_samples <-ehlanzeni_cases %>%
  group_by(epiweek2)%>%
  count(epiweek2, na.rm=TRUE)

#filter for wwtp 

ehlanzeni_water <- water1 %>% 
  filter(district_name == "Ehlanzeni DM") 


#merge the two df 


ehlanzeni_vs_water<- full_join(ehlanzeni_samples, ehlanzeni_water, by= "epiweek2")
ehlanzeni_vs_water$final_result <- ehlanzeni_vs_water$sars_cov_2_pcr_result


#selecting columns I want

ehlanzeni_vs_water<- ehlanzeni_vs_water %>% 
  select(epiweek2, n, site_name, 
         site_prov,district_name,n_gene_ml, levels, loglevels, Date, final_result, pcr_type) %>% 
  filter(epiweek2 != "NAwNA") %>%
  mutate(n = if_else(is.na(n), 0, n)) #replace na with 0


ehlanzeni_vs_water<- ehlanzeni_vs_water%>%
  mutate(tested1 = case_when( (site_name == "Komatipoort Sewage plant" & final_result == "Positive") ~ -0.3, 
                              (site_name == "Komatipoort Sewage plant" & final_result == "Negative") ~ -0.3)) %>%
  mutate(tested2 = case_when( (site_name == "Kingstonvale"& final_result == "Positive") ~ -0.1, 
                              (site_name == "Kingstonvale" & final_result == "Negative") ~ -0.1))


ehlanzeni_vs_water$epiweek3 <- ehlanzeni_vs_water$epiweek2

ehlanzeni_vs_water<- ehlanzeni_vs_water%>%
  separate(epiweek3, sep = "w", into = c("year", "week")) %>%
  mutate(across(c("year", "week"), as.integer)) 

ehlanzeni_vs_water<- ehlanzeni_vs_water%>%
  filter(year != 2020)


ehlanzeni_vs_water<-  ehlanzeni_vs_water[ #ordering by year first then week
  with(ehlanzeni_vs_water, order(year, week)),
]

ehlanzeni_vs_water$epiweek2 <- factor(ehlanzeni_vs_water$epiweek2, levels = unique(ehlanzeni_vs_water$epiweek2), ordered = T)

ehlanzeni_vs_water <- ehlanzeni_vs_water %>%
  mutate(pcr_type = if_else(year == 2021, "Real-Time", pcr_type)) %>%
  mutate(pcr_type = if_else(year == 2022, "Real-Time", pcr_type)) %>%
  mutate(pcr_type = if_else(year == 2023 & week < 30, "Real-Time", pcr_type)) %>%
  mutate(pcr_type = if_else(year == 2023 & week > 30, "dPCR", pcr_type)) %>%
  mutate(pcr_type = if_else(year == 2024, "dPCR", pcr_type)) %>%
  mutate(n = if_else(is.na(n), 0, n)) #replace na with 0

##################################################################################

#JB Marks

#Tabulate number of samples we've received 

jbmarks_samples <-jbmarks_cases %>%
  group_by(epiweek2)%>%
  count(epiweek2, na.rm=TRUE)

#filter for wwtp 

jbmarks_water <- water1 %>% 
  filter(district_name == "Ngaka Modiri Molema DM") 


#merge the two df 


jbmarks_vs_water<- full_join(jbmarks_samples, jbmarks_water, by= "epiweek2")
jbmarks_vs_water$final_result <- jbmarks_vs_water$sars_cov_2_pcr_result


#selecting columns I want

jbmarks_vs_water<- jbmarks_vs_water %>% 
  select(epiweek2, n, site_name, 
         site_prov,district_name,n_gene_ml, levels, loglevels, Date, final_result, pcr_type) %>% 
  filter(epiweek2 != "NAwNA") %>%
  mutate(n = if_else(is.na(n), 0, n)) #replace na with 0


jbmarks_vs_water<- jbmarks_vs_water%>%
  mutate(tested1 = case_when( (site_name == "Mahikeng Water TreatmentWorks" & final_result == "Positive") ~ -0.3, 
                              (site_name == "Mahikeng Water TreatmentWorks" & final_result == "Negative") ~ -0.3)) %>%
  mutate(tested2 = case_when( (site_name == "Mmabatho Water TreatmentWorks"& final_result == "Positive") ~ -0.1, 
                              (site_name == "Mmabatho Water TreatmentWorks" & final_result == "Negative") ~ -0.1))


jbmarks_vs_water$epiweek3 <- jbmarks_vs_water$epiweek2

jbmarks_vs_water<- jbmarks_vs_water%>%
  separate(epiweek3, sep = "w", into = c("year", "week")) %>%
  mutate(across(c("year", "week"), as.integer)) 

jbmarks_vs_water<- jbmarks_vs_water%>%
  filter(year != 2020)


jbmarks_vs_water<-  jbmarks_vs_water[ #ordering by year first then week
  with(jbmarks_vs_water, order(year, week)),
]

jbmarks_vs_water$epiweek2 <- factor(jbmarks_vs_water$epiweek2, levels = unique(jbmarks_vs_water$epiweek2), ordered = T)

jbmarks_vs_water <- jbmarks_vs_water %>%
  mutate(pcr_type = if_else(year == 2021, "Real-Time", pcr_type)) %>%
  mutate(pcr_type = if_else(year == 2022, "Real-Time", pcr_type)) %>%
  mutate(pcr_type = if_else(year == 2023 & week < 30, "Real-Time", pcr_type)) %>%
  mutate(pcr_type = if_else(year == 2023 & week > 30, "dPCR", pcr_type)) %>%
  mutate(pcr_type = if_else(year == 2024, "dPCR", pcr_type)) %>%
  mutate(n = if_else(is.na(n), 0, n)) #replace na with 0

################################################################################

#Rustenburg

#Tabulate number of samples we've received 

rustenburg_samples <-rustenburg_cases %>%
  group_by(epiweek2)%>%
  count(epiweek2, na.rm=TRUE)

#filter for wwtp 

rustenburg_water <- water1 %>% 
  filter(district_name == "Bojanala Platinum DM") 


#merge the two df 


rustenburg_vs_water<- full_join(rustenburg_samples, rustenburg_water, by= "epiweek2")
rustenburg_vs_water$final_result <- rustenburg_vs_water$sars_cov_2_pcr_result


#selecting columns I want

rustenburg_vs_water<- rustenburg_vs_water %>% 
  select(epiweek2, n, site_name, 
         site_prov,district_name,n_gene_ml, levels, loglevels, Date, final_result, pcr_type) %>% 
  filter(epiweek2 != "NAwNA") %>%
  mutate(n = if_else(is.na(n), 0, n)) #replace na with 0


rustenburg_vs_water<- rustenburg_vs_water%>%
  mutate(tested1 = case_when( (site_name == "Rustenburg WastewaterTreatment Works" & final_result == "Positive") ~ -0.3, 
                              (site_name == "Rustenburg WastewaterTreatment Works" & final_result == "Negative") ~ -0.3)) %>%
  mutate(tested2 = case_when( (site_name == "Boitekong"& final_result == "Positive") ~ -0.1, 
                              (site_name == "Boitekong" & final_result == "Negative") ~ -0.1))


rustenburg_vs_water$epiweek3 <- rustenburg_vs_water$epiweek2

rustenburg_vs_water<- rustenburg_vs_water%>%
  separate(epiweek3, sep = "w", into = c("year", "week")) %>%
  mutate(across(c("year", "week"), as.integer)) 

rustenburg_vs_water<- rustenburg_vs_water%>%
  filter(year != 2020)


rustenburg_vs_water<-  rustenburg_vs_water[ #ordering by year first then week
  with(rustenburg_vs_water, order(year, week)),
]

rustenburg_vs_water$epiweek2 <- factor(rustenburg_vs_water$epiweek2, levels = unique(rustenburg_vs_water$epiweek2), ordered = T)

rustenburg_vs_water <- rustenburg_vs_water %>%
  mutate(pcr_type = if_else(year == 2021, "Real-Time", pcr_type)) %>%
  mutate(pcr_type = if_else(year == 2022, "Real-Time", pcr_type)) %>%
  mutate(pcr_type = if_else(year == 2023 & week < 30, "Real-Time", pcr_type)) %>%
  mutate(pcr_type = if_else(year == 2023 & week > 30, "dPCR", pcr_type)) %>%
  mutate(pcr_type = if_else(year == 2024, "dPCR", pcr_type)) %>%
  mutate(n = if_else(is.na(n), 0, n)) #replace na with 0

#############################################################################

#Umkhanyakude

#Tabulate number of samples we've received 

umkhanyakude_samples <-umkhanyakude_cases %>%
  group_by(epiweek2)%>%
  count(epiweek2, na.rm=TRUE)

#filter for wwtp 

umkhanyakude_water <- water1 %>% 
  filter(district_name == "Umkhanyakude DM") 


#merge the two df 


umkhanyakude_vs_water<- full_join(umkhanyakude_samples, umkhanyakude_water, by= "epiweek2")
umkhanyakude_vs_water$final_result <- umkhanyakude_vs_water$sars_cov_2_pcr_result


#selecting columns I want

umkhanyakude_vs_water<- umkhanyakude_vs_water %>% 
  select(epiweek2, n, site_name, 
         site_prov,district_name,n_gene_ml, levels, loglevels, Date, final_result, pcr_type) %>% 
  filter(epiweek2 != "NAwNA") %>%
  mutate(n = if_else(is.na(n), 0, n)) #replace na with 0


umkhanyakude_vs_water<- umkhanyakude_vs_water%>%
  mutate(tested1 = case_when( (site_name == "Jozini Wastewater Treatment Plant" & final_result == "Positive") ~ -0.3, 
                              (site_name == "Jozini Wastewater Treatment Plant" & final_result == "Negative") ~ -0.3)) %>%
  mutate(tested2 = case_when( (site_name == "Manguzi Wastewater Treatment Plant"& final_result == "Positive") ~ -0.1, 
                              (site_name == "Manguzi Wastewater Treatment Plant" & final_result == "Negative") ~ -0.1))


umkhanyakude_vs_water$epiweek3 <- umkhanyakude_vs_water$epiweek2

umkhanyakude_vs_water<- umkhanyakude_vs_water%>%
  separate(epiweek3, sep = "w", into = c("year", "week")) %>%
  mutate(across(c("year", "week"), as.integer)) 

umkhanyakude_vs_water<- umkhanyakude_vs_water%>%
  filter(year != 2020)


umkhanyakude_vs_water<-  umkhanyakude_vs_water[ #ordering by year first then week
  with(umkhanyakude_vs_water, order(year, week)),
]

umkhanyakude_vs_water$epiweek2 <- factor(umkhanyakude_vs_water$epiweek2, levels = unique(umkhanyakude_vs_water$epiweek2), ordered = T)

umkhanyakude_vs_water <- umkhanyakude_vs_water %>%
  mutate(pcr_type = if_else(year == 2021, "Real-Time", pcr_type)) %>%
  mutate(pcr_type = if_else(year == 2022, "Real-Time", pcr_type)) %>%
  mutate(pcr_type = if_else(year == 2023 & week < 30, "Real-Time", pcr_type)) %>%
  mutate(pcr_type = if_else(year == 2023 & week > 30, "dPCR", pcr_type)) %>%
  mutate(pcr_type = if_else(year == 2024, "dPCR", pcr_type)) %>%
  mutate(n = if_else(is.na(n), 0, n)) #replace na with 0

#################################################################################
#Frances Baard 

#Tabulate number of samples we've received 

frances_samples <-frances_cases %>%
  group_by(epiweek2)%>%
  count(epiweek2, na.rm=TRUE)

#filter for wwtp 

frances_water <- water1 %>% 
  filter(district_name == "Frances Baard DM") 


#merge the two df 


frances_vs_water<- full_join(frances_samples, frances_water, by= "epiweek2")
frances_vs_water$final_result <- frances_vs_water$sars_cov_2_pcr_result


#selecting columns I want

frances_vs_water<- frances_vs_water %>% 
  select(epiweek2, n, site_name, 
         site_prov,district_name,n_gene_ml, levels, loglevels, Date, final_result, pcr_type) %>% 
  filter(epiweek2 != "NAwNA") %>%
  mutate(n = if_else(is.na(n), 0, n)) #replace na with 0


frances_vs_water<- frances_vs_water%>%
  mutate(tested1 = case_when( (site_name == "Homevale Kimberley" & final_result == "Positive") ~ -0.3, 
                              (site_name == "Homevale Kimberley" & final_result == "Negative") ~ -0.3)) 


frances_vs_water$epiweek3 <- frances_vs_water$epiweek2

frances_vs_water<- frances_vs_water%>%
  separate(epiweek3, sep = "w", into = c("year", "week")) %>%
  mutate(across(c("year", "week"), as.integer)) 

frances_vs_water<- frances_vs_water%>%
  filter(year != 2020)


frances_vs_water<-  frances_vs_water[ #ordering by year first then week
  with(frances_vs_water, order(year, week)),
]

frances_vs_water$epiweek2 <- factor(frances_vs_water$epiweek2, levels = unique(frances_vs_water$epiweek2), ordered = T)

frances_vs_water <- frances_vs_water %>%
  mutate(pcr_type = if_else(year == 2021, "Real-Time", pcr_type)) %>%
  mutate(pcr_type = if_else(year == 2022, "Real-Time", pcr_type)) %>%
  mutate(pcr_type = if_else(year == 2023 & week < 30, "Real-Time", pcr_type)) %>%
  mutate(pcr_type = if_else(year == 2023 & week > 30, "dPCR", pcr_type)) %>%
  mutate(pcr_type = if_else(year == 2024, "dPCR", pcr_type)) %>%
  mutate(n = if_else(is.na(n), 0, n)) #replace na with 0


##############################################################################

#merging all province into 1 dataframe

capetown_vs_water$epiweek2<-as.character(capetown_vs_water$epiweek2)
buffalocases_vs_water$epiweek2<-as.character(buffalocases_vs_water$epiweek2)
nelsoncases_vs_water$epiweek2<-as.character(nelsoncases_vs_water$epiweek2)
mangaungcases_vs_water$epiweek2<-as.character(mangaungcases_vs_water$epiweek2)
ethekwinicases_vs_water$epiweek2<-as.character(ethekwinicases_vs_water$epiweek2)
ekurhulencases_vs_water$epiweek2<-as.character(ekurhulencases_vs_water$epiweek2)
tshwanecases_vs_water$epiweek2<-as.character(tshwanecases_vs_water$epiweek2)
jhbcases_vs_water$epiweek2<-as.character(jhbcases_vs_water$epiweek2)
rustenburg_vs_water$epiweek2<-as.character(rustenburg_vs_water$epiweek2)
jbmarks_vs_water$epiweek2<-as.character(jbmarks_vs_water$epiweek2)
ehlanzeni_vs_water$epiweek2<-as.character(ehlanzeni_vs_water$epiweek2)
vhembe_vs_water$epiweek2<-as.character(vhembe_vs_water$epiweek2)
umkhanyakude_vs_water$epiweek2<-as.character(umkhanyakude_vs_water$epiweek2)
frances_vs_water$epiweek2<-as.character(frances_vs_water$epiweek2)


provincial <- bind_rows(capetown_vs_water, buffalocases_vs_water, nelsoncases_vs_water,
                        mangaungcases_vs_water, ethekwinicases_vs_water,ekurhulencases_vs_water,
                        tshwanecases_vs_water,jhbcases_vs_water, rustenburg_vs_water,
                        ehlanzeni_vs_water,jbmarks_vs_water, vhembe_vs_water, umkhanyakude_vs_water,
                        frances_vs_water) #bind rows is from tidyverse and joins df one under the other

provincial$Site <-provincial$site_name
provincial$Province <-provincial$site_prov
provincial$District <-provincial$district_name
provincial$GC_per_ml <-provincial$n_gene_ml

provincial <- provincial %>%
  select(epiweek2, n, Site, Province, District,GC_per_ml, 
         levels, loglevels, Date, year)

write.csv(provincial,"./output_files/example_provincial_cases_vs_levels.csv",row.names=FALSE)
