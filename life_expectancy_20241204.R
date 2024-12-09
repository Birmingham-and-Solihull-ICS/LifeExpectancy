options(tidyverse.quiet = TRUE)


library(tidyverse, warn.conflicts = FALSE)
library(DBI)        #database connection library
library(IMD)
library(PHEindicatormethods)
library(readxl)

# Suppress summarise info
options(dplyr.summarise.inform = FALSE)

source("HLE_life_expectancy_vBCCwithlimits.R")

########################################################################################################
# Predefined variables
########################################################################################################

IndicatorID = 90810

timeperiod1yrs <- data.frame (
  from = c(2014,2015,2016,2017,2018,2019,2020, 2021,2022,2023),
  to = c(2014,2015,2016,2017,2018,2019,2020,2021,2022,2023)
)


timeperiod3yrs <- data.frame (
  from = c(2014,2015,2016,2017,2018,2019,2020, 2021),
  to = c(2016,2017,2018,2019,2020,2021,2022,2023)
)

timeperiod5yrs <- data.frame (
  from = c(2014,2015,2016,2017,2018,2019),
  to = c(2018,2019,2020,2021,2022,2023)
)


number_of_years = 3

if(number_of_years == 3){
  timeperiodrange <- timeperiod3yrs
} else if (number_of_years == 5){
  timeperiodrange <- timeperiod5yrs
} else {
  timeperiodrange <- timeperiod1yrs
}

########################################################################################################
# create connection
########################################################################################################

con <- dbConnect(odbc::odbc(), .connection_string = "Driver={SQL Server};server=MLCSU-BI-SQL;database=EAT_Reporting_BSOL", timeout = 10)


###################################################################################
# reference tables 

query_agebands <-  paste0("SELECT  [Age]
      ,[AgeBand_5YRS]
	  , min(age) over (partition by [AgeBand_5YRS]) as startage5yrs
      ,[AgeBand_10YRS]
	  , min(age) over (partition by [AgeBand_10YRS]) as startage10yrs
      ,[AgeBand_20YRS]
	  , min(age) over (partition by [AgeBand_20YRS]) as startage20yrs
      ,[PatientGroup]
      ,[AdultOrChild]
      ,[Age_Band_Other1]
      ,[Age_Band_Other2]
      ,[Row_ID]
      ,[Age_UrgentCare]
  FROM [EAT_Reporting_BSOL].[Reference].[tbAge]
")

#query db for data
agebands <- dbGetQuery(con, query_agebands)

agebands <- agebands %>%
  mutate(AgeBand_5YRS = case_when(
    Age >= 90 ~ '90+',
    Age == 0 ~ '0',
    Age < 5  ~ '1-4',
    .default =  AgeBand_5YRS),
    startage5yrs = case_when(
      Age>=90 ~ '90',
      Age==0 ~ '0',
      Age< 5  ~ '1',
      .default =  as.character(startage5yrs))) %>%
  mutate(startage5yrs = as.integer(startage5yrs))

query <- paste0(
  "SELECT *
  FROM [EAT_Reporting_BSOL].[of].[demographic]")

#query db for data
demo <- dbGetQuery(con, query)

query <- paste0(
  "SELECT *
  FROM [EAT_Reporting_BSOL].[of].[aggregation]")

#query db for data
agg <- dbGetQuery(con, query) 

agg <- agg %>%
  filter(!str_detect(AggregationType,'esid') &
           !str_detect(AggregationType,'Const')&
           !str_detect(AggregationType,'Auth'))

query <- paste0(
  "SELECT  [NHSCode]
,[NHSCodeDefinition]
,[LocalGrouping]
,[CensusEthnicGroup]
,[ONSGroup]
FROM [EAT_Reporting_BSOL].[OF].[lkp_ethnic_categories]")

#query db for data
ethgrp <- dbGetQuery(con, query) 


# # Load in the data
HLE_input <- read_excel("example_data.xlsx", sheet = 3)

########################################################################################################
# Get data for indicator from warehouse
# Aggregation is LSOA to map to MSOA and to Wards and to IMD
# Grouped into financial years
# by Ethnic category using NHS 20 groups as letters
########################################################################################################
##extraction query
query <- paste0(
  "
  select 
  count(*) as Numerator
  , [Ethnic_Code] as EthnicCategoryCode 
  , l.[LSOA11CD]
  , l.[LSOA21CD], l.[LSOA21NM]
  , w.[WD22CD], w.[WD22NM]
  , ll.locality as 'Locality'
  , [LAD22CD], [LAD22NM]
  , DEC_AGEC as Age   
  , DEC_AGECUNIT as AgeUnit
  , [DEC_SEX] as Sex
  , [AgeBand_5YRS]
  , startage5yrs
  , [ULA_OF_RESIDENCE_CODE] as 'OSLAUA' 
  ,WARD_OF_RESIDENCE_CODE as 'ward_death'
  ,left([DATE_OF_DEATH],4) as year_of_death
  ,left(REG_DATE,4) as year_of_registration
  from [Other].[VwDeathsRegister]  act
  left join (select [Pseudo_NHS_Number], [Ethnic_Code] from 
             [Demographic].[Ethnicity]
             --where Is_Deceased = 1
  ) d
  on d.[Pseudo_NHS_Number] = act.[PatientId]
  left join (select [LSOA11CD], [LSOA21CD],[LSOA21NM]  
             from [Reference].[LSOA_2011_to_LSOA_2021]
  ) l
  on l.[LSOA11CD] = act.[LSOA_OF_RESIDENCE_CODE]
  left join ( select [LSOA21CD], [WD22CD], [WD22NM], [LAD22CD], [LAD22NM]
              from [Reference].[LSOA_2021_WARD_LAD]
  ) w
  on w.LSOA21CD = l.LSOA21CD
  left join ( select [AreaCode] ,[AreaName] ,[Locality]
              FROM [EAT_Reporting_BSOL].[OF].[lkp_ward_to_locality] ) ll
  on w.WD22CD = ll.AreaCode
  left join (SELECT  [Age]
             ,[AgeBand_5YRS]
             , min(age) over (partition by [AgeBand_5YRS]) as startage5yrs
             ,[AgeBand_10YRS]
             , min(age) over (partition by [AgeBand_10YRS]) as startage10yrs
             ,[AgeBand_20YRS]
             , min(age) over (partition by [AgeBand_20YRS]) as startage20yrs
             FROM [EAT_Reporting_BSOL].[Reference].[tbAge]) a
  on (act.[DEC_AGEC] =  a.[age] and act.DEC_AGECUNIT = 1) or (act.DEC_AGECUNIT <> 1 and a.[age] =0)
  where act.[PatientId] is not null 
  -- and  [WARD_OF_RESIDENCE_CODE] = 'E05011121'
  group by  
  [Ethnic_Code]
  , l.[LSOA11CD]
  , l.[LSOA21CD], l.[LSOA21NM]
  , w.[WD22CD] , w.[WD22NM]
  ,ll.locality
  , [LAD22CD], [LAD22NM]
  , DEC_AGEC
  , [DEC_SEX]
  , DEC_AGECUNIT
  , [AgeBand_5YRS]
  , startage5yrs
  , [ULA_OF_RESIDENCE_CODE] 
  ,left([DATE_OF_DEATH],4)
  ,left(REG_DATE,4)
  ,WARD_OF_RESIDENCE_CODE  
  ") 

#query db for data
indicator <- dbGetQuery(con, query)

indicator <- indicator %>%
  mutate(
    Age = ifelse(AgeUnit > 1, 0 , Age),
    AgeBand_5YRS = case_when(
          AgeUnit > 1 ~ '0',
          Age >= 90 ~ '90+',
          Age == 0 ~ '0',
          Age < 5  ~ '1-4',
          .default =  AgeBand_5YRS),
         startage5yrs = case_when(
           AgeUnit > 1 ~ '0',
           Age>=90 ~ '90',
           Age==0 ~ '0',
           Age< 5  ~ '1',
           .default =  as.character(startage5yrs)),
         year_of_registration = as.integer(year_of_registration)
         ) %>%
filter((OSLAUA == 'E08000025' | OSLAUA == 'E08000029') &
         year_of_registration > 2013) %>%
  mutate(startage5yrs = as.integer(startage5yrs))



########################################################################################################
# Read in reference tables
#
#Lower_Layer_Super_Output_Area_(2021)_to_Ward_(2023)_to_LAD_(2023)_Lookup_in_England_and_Wales.csv
#
#Output_Area_to_Lower_layer_Super_Output_Area_to_Middle_layer_Super_Output_Area_to_Local_Authority_District_(December_2021)_Lookup_in_England_and_Wales_v3.csv
#
#MSOA2021_to_Ward_to_LAD_(May_2023)_Lookup.csv
#
#ward_to_locality.csv
#
#nhs_ethnic_categories.csv
#
#C21pop_msoa_e20_a18.csv
#
#C21pop_ward_e20_a18.csv
########################################################################################################

#read in lsoa ward LAD lookup
## https://geoportal.statistics.gov.uk/datasets/fc3bf6fe8ea949869af0a018205ac952_0/explore

lsoa_ward_lad_map <- read.csv("Lower_Layer_Super_Output_Area_(2021)_to_Ward_(2022)_to_LAD_(2022)_Lookup_in_England_and_Wales_v3.csv", header=TRUE, check.names=FALSE)
#correct column names to be R friendly
names(lsoa_ward_lad_map) <- str_replace_all(names(lsoa_ward_lad_map), c(" " = "_" ,
                                                                        "/" = "_", 
                                                                        "\\(" = ""  , 
                                                                        "\\)" = "" ))
colnames(lsoa_ward_lad_map)[1] <- 'LSOA21CD'

lsoa_ward_lad_map <-  lsoa_ward_lad_map %>%
  filter(LAD22CD == 'E08000025' | LAD22CD == 'E08000029')

#read in ward lookup
ward_locality_map <- read.csv("ward_to_locality.csv", header = TRUE, check.names = FALSE)
#correct column names to be R friendly
names(ward_locality_map) <- str_replace_all(names(ward_locality_map), c(" " = "_" ,
                                                                        "/" = "_", 
                                                                        "\\(" = ""  , 
                                                                        "\\)" = "" ))
colnames(ward_locality_map)[1] <- 'LA'
colnames(ward_locality_map)[3] <- 'WardCode'
colnames(ward_locality_map)[4] <- 'WardName'


indicator <- indicator %>%
  left_join(ethgrp, by=c("EthnicCategoryCode" = "NHSCode")) %>%
  group_by(ONSGroup, year_of_registration, LAD22CD,
           WD22CD, WD22NM, Locality, Sex, startage5yrs, AgeBand_5YRS) %>%
  summarise(Numerator = sum(Numerator, na.rm = TRUE)) %>%
  select(Numerator, ONSGroup, year_of_registration, LAD22CD,
         WD22CD, WD22NM, Locality, Sex, startage5yrs, AgeBand_5YRS) 


#get periods so that geographies with 0 numerators will be populated
periods <- indicator %>%
  group_by(year_of_registration) %>%
  summarise(count = n()) %>%
  select(-count)

#create list of localities from indicator file - this should come from reference file in future
localities <- indicator %>%
  group_by(Locality) %>%
  summarise(count = n()) %>%
  filter(!is.na(Locality)) %>%
  select(-count)

#read in ethnic code translator
ethnic_codes <- read.csv("nhs_ethnic_categories.csv", header = TRUE, check.names = FALSE)
ethnic_codes <- ethnic_codes %>% 
  select(NHSCode, CensusEthnicGroup, NHSCodeDefinition)

#read in population file for wards
popfile_ward <- read.csv("C21_a18_e20_s2_ward.csv", header=TRUE, check.names=FALSE)
#correct column names to be R friendly
names(popfile_ward)<-str_replace_all(names(popfile_ward), c(" " = "_" ,
                                                            "/" = "_",
                                                            "\\(" = ""  ,
                                                            "\\)" = "" ))

#read in population file for wards
popfile_ward90 <- read.csv("c21_a11_e20_s2_ward.csv", header=TRUE, check.names=FALSE)
#correct column names to be R friendly
names(popfile_ward90)<-str_replace_all(names(popfile_ward90), c(" " = "_" ,
                                                            "/" = "_",
                                                            "\\(" = ""  ,
                                                            "\\)" = "" ))

popfile_ward90_insert <- popfile_ward90 %>%
  filter(Age_D_11_categories_Code == 11) %>%
  rename(Age90 = Observation)

popfile_ward <- popfile_ward %>%
  left_join(popfile_ward90_insert) %>%
  mutate(Observation = ifelse(Age_B_18_categories_Code == 18, Observation - Age90, Observation),
         Age_B_18_categories = ifelse(Age_B_18_categories_Code == 18, "Aged 85 to 89 years
", Age_B_18_categories),
  ) %>%
  select(- Age90, -Age_D_11_categories_Code, -Age_D_11_categories) %>%  
  mutate(Age_B_18_categories_Code = ifelse(Age_B_18_categories_Code > 1, Age_B_18_categories_Code +1, Age_B_18_categories_Code )
  )

popfile_ward0 <- popfile_ward %>%
  filter(Age_B_18_categories_Code == 1) %>%
  mutate(Observation = ifelse(Age_B_18_categories_Code == 1, (Observation/5), Observation),
Age_B_18_categories = ifelse(Age_B_18_categories_Code == 1, "Aged 0 years", Age_B_18_categories)
)

popfile_ward1_4 <- popfile_ward %>%
  filter(Age_B_18_categories_Code == 1) %>%
  mutate(Observation = ifelse(Age_B_18_categories_Code == 1, (Observation/5)*4, Observation),
         Age_B_18_categories = ifelse(Age_B_18_categories_Code == 1, "Aged 1 to 4 years", Age_B_18_categories),
         Age_B_18_categories_Code = 2)
  
popfile_ward90_union <- popfile_ward90 %>%
  filter(Age_D_11_categories_Code == 11) %>%
  mutate(Age_B_18_categories_Code = 20,
         Age_B_18_categories = 'Aged 90+ years') %>%
  select(-Age_D_11_categories_Code, -Age_D_11_categories)

popfile_ward <- rbind(popfile_ward, popfile_ward90_union, popfile_ward0, popfile_ward1_4)
########
## change m/f to 1/2
########
popfile_ward <- popfile_ward %>%
  filter(!Age_B_18_categories == 'Aged 4 years and under') %>%
  mutate(Sex_2_categories_Code = ifelse(Sex_2_categories == 'Female',2, 
                                        ifelse(Sex_2_categories == 'Male', 1, NA))
  )

######################################################################################################################
#Add IMD quintiles
# using IMD package

#get IMD score by ward
imd_england_ward <- IMD::imd_england_ward %>%
  select(ward_code,Score) 

#add quintiles to ward
imd_england_ward <- phe_quantile(imd_england_ward, Score, nquantiles = 5L, invert=TRUE)

imd_england_ward <- imd_england_ward %>%
  select(-Score, -nquantiles, -groupvars, -qinverted) 

#add quintile to popfile 

#add quintile to popfile
popfile_ward <- popfile_ward %>%
  left_join(ward_locality_map, by = c("Electoral_wards_and_divisions_Code" = "WardCode")) %>%
  left_join(imd_england_ward, by = c("Electoral_wards_and_divisions_Code" = "ward_code")) %>%
  left_join(ethnic_codes, by = c("Ethnic_group_20_categories_Code" = "CensusEthnicGroup" )) %>%
  mutate(startage5yrs = ifelse(
    Age_B_18_categories_Code == 1, 0,
    ifelse(Age_B_18_categories_Code == 2, 1,
    (Age_B_18_categories_Code -2 ) * 5))
)

#add quintile to indicator
indicator<- indicator %>%
  left_join(imd_england_ward, by = c("WD22CD" = "ward_code")) 

######################################################################################################################
#create population file for each year and each geography
#ward by ethnicity, IMD quintile
pop_ward<- popfile_ward %>%
  mutate(NHSCode = substr(NHSCode,1,1)) %>%
  left_join(ethgrp) %>%
  group_by(Electoral_wards_and_divisions_Code, LA, Locality,
           ONSGroup, quantile, Sex_2_categories_Code, startage5yrs) %>%
  summarise(Denominator = sum(Observation, na.rm = TRUE))

#########################################################################################
#functions
###################################################################

denom_BSol <- function(df, groupping_by, geo){

  index <- which(colnames(df) %in% groupping_by)
  
  df %>% 
    ungroup() %>% 
    group_by_at(index) %>% 
    summarise(Denominator = sum(Denominator, na.rm = TRUE)) %>%
    mutate(Geography = geo)
}  

denom_locality <- function(df, groupping_by){
  
  index <- which(colnames(df) %in% groupping_by)
  
  df %>% 
    ungroup() %>% 
    group_by_at(index) %>% 
    summarise(Denominator = sum(Denominator, na.rm = TRUE)) %>%
    mutate(Geography = Locality)  %>%
    ungroup() %>% 
    select(-Locality)  
}  


denom_brum <- function(df, groupping_by){
  
  index <- which(colnames(df) %in% groupping_by)
  
  df %>% 
    ungroup() %>% 
    filter(LA == 'E08000025') %>%
    group_by_at(index) %>% 
    summarise(Denominator = sum(Denominator, na.rm = TRUE)) %>%
    mutate(Geography = 'Birmingham')  %>% 
    ungroup() %>% 
    select(-LA)  
}  

denom_ward <- function(df, groupping_by){
  
  index <- which(colnames(df) %in% groupping_by)
  
  df %>% 
    ungroup() %>% 
    group_by_at(index) %>% 
  summarise(Denominator = sum(Denominator, na.rm = TRUE)) %>%
  mutate(Geography = Electoral_wards_and_divisions_Code) %>%
    ungroup() %>% 
  select(-Electoral_wards_and_divisions_Code)
}
###############################################################################################
ind_BSol <- function(df, groupping_by){
  
  index <- which(colnames(df) %in% groupping_by)
  df %>%
    ungroup()    %>% 
    group_by_at(index) %>%
    summarise(Numerator = sum(Numerator, na.rm = TRUE)) %>%
    ungroup() %>% 
    mutate(Geography = 'BSol') 
  
}

ind_locality <- function(df, groupping_by){
  
  index <- which(colnames(df) %in% groupping_by)
  df %>%
    ungroup()    %>% 
    group_by_at(index) %>%
    summarise(Numerator = sum(Numerator, na.rm = TRUE)) %>%
    mutate(Geography = Locality) %>%
    ungroup() %>% 
    select(-Locality)  
  
}

ind_LA <- function(df, groupping_by){
  
  index <- which(colnames(df) %in% groupping_by)
  df %>%
     ungroup()    %>%   
    filter(LAD22CD == 'E08000025' ) %>%
    group_by_at(index) %>%
    summarise(Numerator = sum(Numerator, na.rm = TRUE)) %>%
    mutate(Geography = 'Birmingham')  %>% 
    ungroup() %>% 
    select(-LAD22CD)  
}

ind_ward <- function(df, groupping_by){
  
index <- which(colnames(df) %in% groupping_by)

df %>% 
  ungroup()    %>% 
  group_by_at(index) %>% 
  summarise(Numerator = sum(Numerator, na.rm = TRUE)) %>%
  mutate(Geography = WD22CD) %>%
  ungroup() %>% 
  select(-WD22CD)
}


####################################################################################################
sum_numerator <- function(df,df_out, group_by_list ){
  
  index <- which(colnames(df) %in% group_by_list)
  geo <- unique(df$Geography)
  
  out_data <- df_out
  
  for (g in geo){
    for(i in 1:nrow(timeperiodrange)) {       # for-loop over rows
      subgroup_data <- df %>%
        ungroup() %>%
        filter(between(year_of_registration,timeperiodrange$from[i],timeperiodrange$to[i]) &
                 Geography == g)  %>%
        mutate( periodstart = timeperiodrange$from[i],
                periodend =timeperiodrange$to[i] ,
                aggregation  = paste0(number_of_years,'yrs')) %>%
        group_by(periodstart, periodend, aggregation) %>%
        group_by_at(index, .add = TRUE) %>%
        summarise(Numerator = sum(Numerator, na.rm = TRUE)/number_of_years)
      
      out_data <- rbind(out_data,subgroup_data)
    }  
  }
  return(out_data)
}
#######################################################################################################
create_input_data <- function(df, df2, join_by){
  x <- enquo(join_by)
  
  print(x)
input_data <- df %>%
  left_join(df2, by= names(select(., {{x}}))) %>%
  mutate(Di = as.integer(ceiling(Numerator)),
         Pi = as.integer(Denominator),
         xi = as.integer(startage5yrs),
         period = paste0(periodstart," - ",periodend),
         Di = ifelse(is.na(Di),0,Di),
         Pi = ifelse(is.na(Pi),1,Pi)
  ) 
}

#####################################################################################################
output_ref <- data.frame(
  xi = numeric(),
  Pi = numeric(),
  Di= numeric(),
  Life_Expectancy= numeric(),
  Life_Expectancy_lower= numeric(),
  Life_Expectancy_upper= numeric(),
  HLE= numeric(),
  HLE_LowerCI= numeric(),
  HLE_UpperCI= numeric(),
  Sex = numeric(),
  Geography = character(),
  period = character(),
  ethnicity = character(),
  IMD_quintile = numeric()
)


######################################################################################################
le_calculation <- function(df, filter_list){
  
  output <- output_ref
  
  #print(filter_list)
  
  Loop_control <- df  %>%
    mutate( 
      Sex = (if("Sex_2_categories_Code" %in% colnames(.)) Sex_2_categories_Code else NA),
      IMD = (if("quantile" %in% colnames(.)) quantile else NA),
      ethnicity = (if("ONSGroup" %in% colnames(.)) ONSGroup else NA)
    )%>%
    select(Sex, IMD, ethnicity, Geography)

  sexloop <- unique(Loop_control$Sex)
  IMDloop <- unique(Loop_control$IMD)
  ethnicityloop <- unique(Loop_control$ethnicity)
  geo <- unique(Loop_control$Geography)
  
  for (s in sexloop){
    for (q in IMDloop){
      for (e in ethnicityloop){
        for (g in geo){
          for(i in 1:nrow(timeperiodrange)) {       # for-loop over rows
            
            le_data <- df
            
            if('Sex_2_categories_Code' %in% filter_list){
              le_data <- le_data %>% filter(Sex_2_categories_Code == s)
            } 
            
            if('ONSGroup' %in% filter_list){
              le_data <- le_data %>% filter(ONSGroup == e)
            } 
            
            if('quantile' %in% filter_list){
              le_data <- le_data %>% filter(quantile == q)
            } 
            
            le_data <- le_data %>%
              filter(periodstart == timeperiodrange$from[i] & Geography == g ) %>%
              select(xi,Pi,Di)
            
            if (sum(is.na(le_data$Di) == 0 )){
              LE <- life_exp(le_data)
              HLE_out <- healthy_life_exp(LE, HLE_input)
              
              run <- HLE_out %>%
                select(xi,Pi,Di,Life_Expectancy,
                       LE_LowerCI, LE_UpperCI, HLE,  HLE_LowerCI, HLE_UpperCI) %>%
                filter(xi ==0 | xi == 65) %>%
                mutate(Geography = g,
                       period = timeperiodrange$from[i], 
                       Sex =   s,
                       IMD_quintile =   q,
                       ethnicity =    e
                ) %>%
                rename(Life_Expectancy_lower= LE_LowerCI) %>%
                rename(Life_Expectancy_upper= LE_UpperCI) 
              
              output <- rbind(output, run)
            }
          }
        }}}}

  return(output)
}




#########################################################################################
#LE for geographies BSOL, PLACE, Localities, Ward
######################################################################################################################
print("LE for geographies BSOL, PLACE, Localities, Ward")

numeratorAll_data <-  data.frame(
  aggregation  = character(),
  startage5yrs = integer(),
  Sex = numeric(),
  Numerator = integer(),
  periodstart = integer(),
  periodend = integer(),
  Geography = character()
)

Denominator_BSol <- denom_BSol(pop_ward, c('startage5yrs'), 'BSol')
Denominator_Locality <- denom_locality(pop_ward, c('startage5yrs', 'Locality'))
Denominator_LA <- denom_brum(pop_ward, c('startage5yrs', 'LA'))
Denominator_ward <- denom_ward(pop_ward, c('startage5yrs', 'Electoral_wards_and_divisions_Code'))

DenominatorAll <- rbind(Denominator_BSol, Denominator_ward, Denominator_LA, Denominator_Locality)


indicator_BSol <- ind_BSol(indicator, c('startage5yrs', 'year_of_registration'))
indicator_la <- ind_LA(indicator, c('startage5yrs', 'year_of_registration','LAD22CD'))
indicator_Locality <- ind_locality(indicator, c('startage5yrs','Locality', 'year_of_registration'))
indicator_ward <- ind_ward(indicator, c('startage5yrs',  'WD22CD','year_of_registration'))

indicator_le <- rbind(indicator_BSol, indicator_ward, indicator_la, indicator_Locality)

indicator_le <- indicator_le %>%
  filter(!is.na(Geography))

DenominatorAll <- DenominatorAll %>%
  filter(!is.na(Denominator) & Denominator >=0)  %>%
  cross_join(timeperiodrange) %>%
  rename( periodstart = from) %>%
  rename( periodend = to) 

numeratorAll_data <- sum_numerator(indicator_le, numeratorAll_data, c('startage5yrs' ,
                                    'Geography'))

input_data <- create_input_data(DenominatorAll,numeratorAll_data, 
                            c( "startage5yrs", "Geography",'periodstart','periodend'))



output_geo <- le_calculation(input_data, NA)

# Missing <- setdiff(nms, names(output_geo))  # Find names of missing columns
# output_geo[Missing] <- NA  

######################################################################################################################
#LE for geographies BSOL, PLACE, Localities, Ward by sex
########################################################################
print("LE for geographies BSOL, PLACE, Localities, Ward by sex")

numeratorS_data <-  data.frame(
  aggregation  = character(),
  startage5yrs = integer(),
  Sex = numeric(),
  AgeBand_5YRS = character(),
  Numerator = integer(),
  periodstart = integer(),
  periodend = integer(),
  period = character(),
  Geography = character()
)

Denominator_BSol <- denom_BSol(pop_ward, c('startage5yrs', 'Sex_2_categories_Code'), 'BSol')
Denominator_Locality <- denom_locality(pop_ward, c('startage5yrs', 'Locality', 'Sex_2_categories_Code'))
Denominator_LA <- denom_brum(pop_ward, c('startage5yrs', 'LA', 'Sex_2_categories_Code'))
Denominator_ward <- denom_ward(pop_ward, c('startage5yrs', 'Electoral_wards_and_divisions_Code', 'Sex_2_categories_Code'))

DenominatorS <- rbind(Denominator_BSol, Denominator_ward, Denominator_LA, Denominator_Locality)

indicator_BSol <- ind_BSol(indicator, c('startage5yrs','Sex', 'year_of_registration'))
indicator_LA <- ind_LA(indicator, c('startage5yrs', 'Sex','year_of_registration','LAD22CD'))
indicator_Locality <- ind_locality(indicator, c('startage5yrs','Sex','Locality', 'year_of_registration'))
indicator_ward <- ind_ward(indicator, c('startage5yrs', 'Sex', 'WD22CD','year_of_registration'))

indicatorS <- rbind(indicator_BSol, indicator_ward, indicator_LA, 
                    indicator_Locality) 

indicatorS <- indicatorS %>%
  filter(!is.na(Geography) & !Sex == 3) %>%
 rename( Sex_2_categories_Code = Sex) 

DenominatorS <- DenominatorS %>%
  filter(!is.na(Denominator) & Denominator >=0)  %>%
  cross_join(timeperiodrange) %>%
  rename( periodstart = from) %>%
  rename( periodend = to) 

numeratorS_data <- sum_numerator(indicatorS,numeratorS_data, c('startage5yrs' ,
                                                 'Geography', 'Sex_2_categories_Code'))

input_data <- create_input_data(DenominatorS, numeratorS_data, 
                                c( "startage5yrs", 'Sex_2_categories_Code', "Geography",'periodstart','periodend'))


output_S <- le_calculation(input_data, c( "Sex_2_categories_Code"))



######################################################################################################################
#LE for geographies and ethnicity BSOL/LA/Localites
################################################################
print("LE for geographies and ethnicity BSOL/LA/Localites")

numeratorE_data <-  data.frame(
  aggregation  = character(),
  startage5yrs = integer(),
  Sex = numeric(),
  AgeBand_5YRS = character(),
  Numerator = integer(),
  periodstart = integer(),
  periodend = integer(),
  period = character(),
  Ethnicity = character()
)

Denominator_BSol <- denom_BSol(pop_ward, c('startage5yrs', 'ONSGroup'), 'BSol')
Denominator_Locality <- denom_locality(pop_ward, c('startage5yrs', 'Locality', 'ONSGroup'))
Denominator_LA <- denom_brum(pop_ward, c('startage5yrs', 'LA', 'ONSGroup'))
Denominator_ward <- denom_ward(pop_ward, c('startage5yrs', 'Electoral_wards_and_divisions_Code', 'ONSGroup'))

DenominatorE <- rbind(Denominator_BSol, Denominator_LA, Denominator_Locality, Denominator_ward)
DenominatorE <- DenominatorE %>%
  filter( !is.na(ONSGroup) ) %>%
  cross_join(timeperiodrange) %>%
  rename( periodstart = from) %>%
  rename( periodend = to) 

indicator_BSol <- ind_BSol(indicator, c('startage5yrs','year_of_registration','ONSGroup'))
indicator_la <- ind_LA(indicator, c('startage5yrs', 'year_of_registration','LAD22CD','ONSGroup'))
indicator_Locality <- ind_locality(indicator, c('startage5yrs','Locality', 'year_of_registration','ONSGroup'))
indicator_ward <- ind_ward(indicator, c('startage5yrs', 'WD22CD','year_of_registration','ONSGroup'))

indicatorE <- rbind(indicator_BSol, indicator_ward, indicator_la, indicator_Locality)

indicatorE <- indicatorE %>%
  filter(!is.na(Geography) & !is.na(ONSGroup)) 

numeratorE_data <- sum_numerator(indicatorE, numeratorE_data, c('startage5yrs' ,
                                               'Geography', 'ONSGroup'))

testout <- indicatorE %>% 
  filter(Geography == 'Birmingham') %>%
  group_by(ONSGroup, year_of_registration) %>%
  summarise(num = sum(Numerator, na.rm = TRUE))


input_data <- create_input_data(DenominatorE, numeratorE_data, 
                                c( "startage5yrs", 'ONSGroup', "Geography",'periodstart','periodend'))

output_E <- le_calculation(input_data, c('ONSGroup'))

######################################################################################################################
#LE for geographies and ethnicity BSOL/LA/Localites by sex
################################################################################
print("LE for geographies and ethnicity BSOL/LA/Localites by sex")

numeratorSE_data <-  data.frame(
  aggregation  = character(),
  startage5yrs = integer(),
  Sex = numeric(),
  AgeBand_5YRS = character(),
  Numerator = integer(),
  periodstart = integer(),
  periodend = integer(),
  period = character(),
  Ethnicity = character()
)

Denominator_BSol <- denom_BSol(pop_ward, c('startage5yrs', 'Sex_2_categories_Code', 'ONSGroup'), 'BSol')
Denominator_Locality <- denom_locality(pop_ward, c('startage5yrs', 'Locality','Sex_2_categories_Code', 'ONSGroup'))
Denominator_LA <- denom_brum(pop_ward, c('startage5yrs','Sex_2_categories_Code', 'LA', 'ONSGroup'))
#Denominator_ward <- denom_ward(pop_ward, c('startage5yrs','Sex_2_categories_Code', 'Electoral_wards_and_divisions_Code', 'ONSGroup'))

DenominatorSE <- rbind(Denominator_BSol, Denominator_LA, Denominator_Locality)
DenominatorSE <- DenominatorSE %>%
  filter(!is.na(Denominator) & Denominator >=0 & !is.na(ONSGroup) ) %>%
  cross_join(timeperiodrange) %>%
  rename( periodstart = from) %>%
  rename( periodend = to) 

indicator_BSol <- ind_BSol(indicator, c('startage5yrs','Sex','year_of_registration','ONSGroup'))
indicator_la <- ind_LA(indicator, c('startage5yrs','Sex', 'year_of_registration','LAD22CD','ONSGroup'))
indicator_Locality <- ind_locality(indicator, c('startage5yrs','Sex','Locality', 'year_of_registration','ONSGroup'))
#indicator_ward <- ind_ward(indicator, c('startage5yrs','Sex', 'WD22CD','year_of_registration','ONSGroup'))

indicatorSE <- rbind(indicator_BSol, indicator_la, indicator_Locality)

indicatorSE <- indicatorSE %>%
  filter(!is.na(Geography) & !is.na(ONSGroup) & !Sex == 3) %>%
  rename( Sex_2_categories_Code = Sex)

numeratorSE_data <- sum_numerator(indicatorSE,numeratorSE_data, c('startage5yrs' ,
                                               'Geography', 'Sex_2_categories_Code','ONSGroup'))

input_data <- create_input_data(DenominatorSE, numeratorSE_data, 
                                c( "startage5yrs", 'Sex_2_categories_Code','ONSGroup',
                                   "Geography",'periodstart','periodend'))

output_SE <- le_calculation(input_data, c( 'Sex_2_categories_Code','ONSGroup'))

######################################################################################################################
#LE for geographies and IMD BSOL/LA/Localites
################################################################
print("LE for geographies and IMD BSOL/LA/Localites")

numeratorI_data <-  data.frame(
  aggregation  = character(),
  startage5yrs = integer(),
  Sex = numeric(),
  AgeBand_5YRS = character(),
  Numerator = integer(),
  periodstart = integer(),
  periodend = integer(),
  period = character(),
  IMD = numeric()
)
Denominator_BSol <- denom_BSol(pop_ward, c('startage5yrs', 'quantile'), 'BSol')
Denominator_Locality <- denom_locality(pop_ward, c('startage5yrs','quantile', 'Locality'))
Denominator_LA <- denom_brum(pop_ward, c('startage5yrs','quantile', 'LA'))
Denominator_ward <- denom_ward(pop_ward, c('startage5yrs','quantile', 'Electoral_wards_and_divisions_Code'))

DenominatorI <- rbind(Denominator_BSol, Denominator_LA, Denominator_Locality)
DenominatorI <- DenominatorI %>%
  filter(!is.na(Denominator) & Denominator >=0 & !is.na(quantile) ) %>%
  cross_join(timeperiodrange) %>%
  rename( periodstart = from) %>%
  rename( periodend = to) 

indicator_BSol <- ind_BSol(indicator, c('startage5yrs','quantile','year_of_registration'))
indicator_la <- ind_LA(indicator, c('startage5yrs','quantile', 'year_of_registration','LAD22CD'))
indicator_Locality <- ind_locality(indicator, c('startage5yrs','quantile','Locality', 'year_of_registration'))
indicator_ward <- ind_ward(indicator, c('startage5yrs','quantile', 'WD22CD','year_of_registration'))

indicatorI <- rbind(indicator_BSol, indicator_ward, indicator_la, indicator_Locality)

indicatorI <- indicatorI %>%
  filter(!is.na(Geography) & !is.na(quantile) ) 

numeratorI_data <- sum_numerator(indicatorI,numeratorI_data,  c('startage5yrs' ,
                                                 'Geography', 'quantile'))

input_data <- create_input_data(DenominatorI, numeratorI_data, 
                                c( "startage5yrs", 'quantile', "Geography",'periodstart','periodend'))

output_I <- le_calculation(input_data,  c( 'quantile'))

######################################################################################################################
#LE for geographies and IMD BSOL/LA/Localites by sex
###########################################################
print("LE for geographies and IMD BSOL/LA/Localites by sex")

numeratorSI_data <-  data.frame(
  aggregation  = character(),
  startage5yrs = integer(),
  Sex = numeric(),
  AgeBand_5YRS = character(),
  Numerator = integer(),
  periodstart = integer(),
  periodend = integer(),
  period = character(),
  IMD = numeric()
)

Denominator_BSol <- denom_BSol(pop_ward, c('startage5yrs', 'Sex_2_categories_Code', 'quantile'), 'BSol')
Denominator_Locality <- denom_locality(pop_ward, c('startage5yrs','quantile','Sex_2_categories_Code', 'Locality'))
Denominator_LA <- denom_brum(pop_ward, c('startage5yrs','quantile','Sex_2_categories_Code', 'LA'))
#Denominator_ward <- denom_ward(pop_ward, c('startage5yrs','quantile','Sex_2_categories_Code', 'Electoral_wards_and_divisions_Code'))

DenominatorSI <- rbind(Denominator_BSol, Denominator_LA, Denominator_Locality)
DenominatorSI <- DenominatorSI %>%
  filter(!is.na(Denominator) & Denominator >=0 & !is.na(quantile) ) %>%
  cross_join(timeperiodrange) %>%
  rename( periodstart = from) %>%
  rename( periodend = to) 

indicator_BSol <- ind_BSol(indicator, c('startage5yrs','quantile','Sex', 'year_of_registration'))
indicator_la <- ind_LA(indicator, c('startage5yrs','quantile', 'Sex','year_of_registration','LAD22CD'))
indicator_Locality <- ind_locality(indicator, c('startage5yrs','Sex','quantile','Locality', 'year_of_registration'))
#indicator_ward <- ind_ward(indicator, c('startage5yrs','quantile','Sex', 'WD22CD','year_of_registration'))

indicatorSI <- rbind(indicator_BSol,  indicator_la, indicator_Locality)

indicatorSI <- indicatorSI %>% 
  filter(!is.na(Geography) & !is.na(quantile) & !is.na(Sex ) & !Sex == 3) %>%
rename( Sex_2_categories_Code = Sex) 

numeratorSI_data <- sum_numerator(indicatorSI, numeratorSI_data, c('startage5yrs' ,
                                               'Geography', 'quantile','Sex_2_categories_Code'))

input_data <- create_input_data(DenominatorSI, numeratorSI_data, 
                                c( "startage5yrs", 'quantile','Sex_2_categories_Code',
                                   "Geography",'periodstart','periodend'))
####fail
output_SI <- le_calculation(input_data, c('quantile','Sex_2_categories_Code'))


######################################################################################################################
#LE for geographies and IMD and ethnicity BSOL/LA/Localites
###########################################################
print("LE for geographies and IMD and ethnicity BSOL/LA/Localites")
numeratorEI_data <-  data.frame(
  aggregation  = character(),
  startage5yrs = integer(),
  Sex = numeric(),
  AgeBand_5YRS = character(),
  Numerator = integer(),
  periodstart = integer(),
  periodend = integer(),
  period = character(),
  EIMD = numeric()
)

Denominator_BSol <- denom_BSol(pop_ward, c('startage5yrs', 'ONSGroup', 'quantile'), 'BSol')
Denominator_Locality <- denom_locality(pop_ward, c('startage5yrs','ONSGroup','quantile','Locality'))
Denominator_LA <- denom_brum(pop_ward, c('startage5yrs','quantile','ONSGroup', 'LA'))
#Denominator_ward <- denom_ward(pop_ward, c('startage5yrs','quantile','ONSGroup','Electoral_wards_and_divisions_Code'))

DenominatorEI <- rbind(Denominator_BSol, Denominator_LA, Denominator_Locality)
DenominatorEI <- DenominatorEI %>%
  filter(!is.na(Denominator) & Denominator >=0 & !is.na(quantile) & !is.na(ONSGroup)  ) %>%
  cross_join(timeperiodrange) %>%
  rename( periodstart = from) %>%
  rename( periodend = to) 

indicator_BSol <- ind_BSol(indicator, c('startage5yrs','quantile','ONSGroup', 'year_of_registration'))
indicator_la <- ind_LA(indicator, c('startage5yrs','quantile', 'ONSGroup','year_of_registration','LAD22CD'))
indicator_Locality <- ind_locality(indicator, c('startage5yrs','ONSGroup','quantile','Locality', 'year_of_registration'))
#indicator_ward <- ind_ward(indicator, c('startage5yrs','quantile','ONSGroup', 'WD22CD','year_of_registration'))

indicatorEI <- rbind(indicator_BSol, indicator_la, indicator_Locality)

indicatorEI <- indicatorEI %>%
  filter(!is.na(Geography) & !is.na(quantile) & !is.na(ONSGroup) )

numeratorEI_data <- sum_numerator(indicatorEI,numeratorEI_data,  c('startage5yrs' ,
                                                 'Geography', 'quantile','ONSGroup'))

input_data <- create_input_data(DenominatorEI, numeratorEI_data, 
                                c( "startage5yrs", 'quantile','ONSGroup', "Geography",'periodstart','periodend'))

output_EI <- le_calculation(input_data,  c('quantile','ONSGroup'))


######################################################################################################################
#LE for geographies and IMD and ethnicity BSOL/LA/Localites by sex
#################################################################
print("LE for geographies and IMD and ethnicity BSOL/LA/Localites by sex")
numeratorEIS_data <-  data.frame(
  aggregation  = character(),
  startage5yrs = integer(),
  Sex = numeric(),
  AgeBand_5YRS = character(),
  Numerator = integer(),
  periodstart = integer(),
  periodend = integer(),
  period = character(),
  EIMD = numeric()
)

Denominator_BSol <- denom_BSol(pop_ward, c('startage5yrs','Sex_2_categories_Code', 'ONSGroup', 'quantile'), 'BSol')
Denominator_Locality <- denom_locality(pop_ward, c('startage5yrs','quantile','ONSGroup','Sex_2_categories_Code',  'Locality'))
Denominator_LA <- denom_brum(pop_ward, c('startage5yrs','quantile','ONSGroup','Sex_2_categories_Code',   'LA'))
#Denominator_ward <- denom_ward(pop_ward, c('startage5yrs','quantile','ONSGroup','Sex_2_categories_Code',  'Electoral_wards_and_divisions_Code'))

DenominatorEIS <- rbind(Denominator_BSol, Denominator_LA, Denominator_Locality)
DenominatorEIS <- DenominatorEIS %>%
  filter(!is.na(Denominator) & Denominator >=0 & !is.na(quantile) & !is.na(ONSGroup)  ) %>%
  cross_join(timeperiodrange) %>%
  rename( periodstart = from) %>%
  rename( periodend = to) 

indicator_BSol <- ind_BSol(indicator, c('startage5yrs','quantile','ONSGroup','Sex', 'year_of_registration'))
indicator_la <- ind_LA(indicator, c('startage5yrs','quantile', 'ONSGroup','Sex', 'year_of_registration','LAD22CD'))
indicator_Locality <- ind_locality(indicator, c('startage5yrs','ONSGroup','Sex', 'quantile','Locality', 'year_of_registration'))
#indicator_ward <- ind_ward(indicator, c('startage5yrs','quantile','ONSGroup','Sex',  'WD22CD','year_of_registration'))

indicatorEIS <- rbind(indicator_BSol, indicator_la, indicator_Locality)

indicatorEIS <- indicatorEIS %>%
  filter(!is.na(Geography) & !is.na(quantile) & !is.na(ONSGroup) & !Sex == 3) %>%
  rename( Sex_2_categories_Code = Sex) 

numeratorEIS_data <- sum_numerator(indicatorEIS,numeratorEIS_data, c('startage5yrs' ,
                                                 'Geography', 'quantile', 'Sex_2_categories_Code','ONSGroup'))

input_data <- create_input_data(DenominatorEIS, numeratorEIS_data, 
                                c( "startage5yrs", 'quantile','ONSGroup', 'Sex_2_categories_Code',"Geography",'periodstart','periodend'))

output_EIS <- le_calculation(input_data, c( 'quantile', 'Sex_2_categories_Code','ONSGroup'))


##############################################
# output
##############################################




outfile <- rbind(output_geo, output_E, output_I, output_EI,
                 output_S, output_SE, output_SI, output_EIS)

HLEoutfile <- outfile %>%
  filter(xi == 65) %>%
  rename(IndicatorValue = HLE) %>%
  left_join(agg, by= c("Geography" = "AggregationCode")) %>%
  mutate(
    DemographicLabel = case_when(
      is.na(IMD_quintile) & is.na(ethnicity) & is.na(Sex) ~ paste0('Persons: 65+ yrs'),
      is.na(IMD_quintile) & !is.na(ethnicity) & is.na(Sex) ~ paste0('Persons: 65+ yrs: ',ethnicity),
      !is.na(IMD_quintile) & is.na(ethnicity) & is.na(Sex) ~ paste0('Persons: 65+ yrs: IMD Quintile',IMD_quintile),
      !is.na(IMD_quintile) & !is.na(ethnicity)& is.na(Sex)  ~ paste0('Persons: 65+ yrs: ',ethnicity,': IMD Quintile',IMD_quintile)      , 
      is.na(IMD_quintile) & is.na(ethnicity) & Sex == 1 ~ paste0('Male: 65+ yrs'),
      is.na(IMD_quintile) & !is.na(ethnicity) & Sex == 1  ~ paste0('Male: 65+ yrs: ',ethnicity),
      !is.na(IMD_quintile) & is.na(ethnicity) & Sex == 1  ~ paste0('Male: 65+ yrs: IMD Quintile',IMD_quintile),
      !is.na(IMD_quintile) & !is.na(ethnicity)& Sex == 1   ~ 
        paste0('Male: 65+ yrs: ',ethnicity,': IMD Quintile',IMD_quintile)      , 
      is.na(IMD_quintile) & is.na(ethnicity) & Sex == 2 ~ paste0('Female: 65+ yrs'),
      is.na(IMD_quintile) & !is.na(ethnicity) & Sex == 2  ~ paste0('Female: 65+ yrs: ',ethnicity),
      !is.na(IMD_quintile) & is.na(ethnicity) & Sex == 2  ~ paste0('Female: 65+ yrs: IMD Quintile',IMD_quintile),
      !is.na(IMD_quintile) & !is.na(ethnicity)& Sex == 2   ~ 
        paste0('Female: 65+ yrs: ',ethnicity,': IMD Quintile',IMD_quintile) , 
      .default = 'NA'),
    AggregationID = case_when(
      Geography  == 'Birmingham' ~ "147",
      Geography  == 'BSol' ~ "148",
      Geography  == 'Solihull' ~ "140",
      Geography  == 'North' ~ "142",
      Geography  == 'West' ~ "145",
      Geography  == 'Central' ~ "141",
      Geography  == 'South' ~ "144",
      Geography  == 'East' ~ "143",
      .default = as.character(AggregationID)    ),
    IndicatorID = ifelse(Sex == 1,40, ifelse(Sex == 2,41,NA)),
    ReferenceID = 93505,
    indicator_title = 'HLE',
    TimePeriodDesc = paste0(number_of_years,' years'),
    TimePeriod = period, 
    IndicatorStartDate = paste0(period,'-01-01'),
    IndicatorEndDate = paste0(period+number_of_years-1,'-12-31'),
    lowerCI95 = HLE_LowerCI,
    upperCI95 = HLE_UpperCI,
    numerator = NA,
    denominator = NA,
    DataQualityID = 1,
InsertDate = Sys.Date()  ) %>%
  left_join(demo, by=c("DemographicLabel" = "DemographicLabel"))  %>%
    select(IndicatorID, ReferenceID,
         IndicatorStartDate,IndicatorEndDate,
         numerator, denominator, IndicatorValue, lowerCI95, upperCI95, 
         AggregationID, DemographicID,DataQualityID,InsertDate) %>%
filter(!is.na(IndicatorID) & !is.na(IndicatorValue) & IndicatorValue != 'Inf' & IndicatorValue != '-Inf')

solihullLA <- HLEoutfile %>%
  filter(AggregationID == "140") %>%
  mutate(AggregationID = "146")


HLEoutfile <- rbind(HLEoutfile, solihullLA)


LE_outfile <- outfile %>%
 # filter(xi == 0)
  left_join(agg, by= c("Geography" = "AggregationCode")) %>%
  rename(IndicatorValue = Life_Expectancy) %>%
  mutate(
    DemographicLabel = case_when(
      is.na(IMD_quintile) & is.na(ethnicity) & is.na(Sex) & xi == 65 ~ paste0('Persons: 65+ yrs'),
      is.na(IMD_quintile) & !is.na(ethnicity) & is.na(Sex) & xi == 65 ~ paste0('Persons: 65+ yrs: ',ethnicity),
      !is.na(IMD_quintile) & is.na(ethnicity) & is.na(Sex) & xi == 65 ~ paste0('Persons: 65+ yrs: IMD Quintile',IMD_quintile),
      !is.na(IMD_quintile) & !is.na(ethnicity)& is.na(Sex) & xi == 65  ~ paste0('Persons: 65+ yrs: ',ethnicity,': IMD Quintile',IMD_quintile)      , 
      is.na(IMD_quintile) & is.na(ethnicity) & Sex == 1 & xi == 65 ~ paste0('Male: 65+ yrs'),
      is.na(IMD_quintile) & !is.na(ethnicity) & Sex == 1  & xi == 65 ~ paste0('Male: 65+ yrs: ',ethnicity),
      !is.na(IMD_quintile) & is.na(ethnicity) & Sex == 1  & xi == 65 ~ paste0('Male: 65+ yrs: IMD Quintile',IMD_quintile),
      !is.na(IMD_quintile) & !is.na(ethnicity)& Sex == 1   & xi == 65 ~ 
        paste0('Male: 65+ yrs: ',ethnicity,': IMD Quintile',IMD_quintile)      , 
      is.na(IMD_quintile) & is.na(ethnicity) & Sex == 2  & xi == 65~ paste0('Female: 65+ yrs'),
      is.na(IMD_quintile) & !is.na(ethnicity) & Sex == 2  & xi == 65 ~ paste0('Female: 65+ yrs: ',ethnicity),
      !is.na(IMD_quintile) & is.na(ethnicity) & Sex == 2   & xi == 65~ paste0('Female: 65+ yrs: IMD Quintile',IMD_quintile),
      !is.na(IMD_quintile) & !is.na(ethnicity)& Sex == 2  & xi == 65  ~ 
        paste0('Female: 65+ yrs: ',ethnicity,': IMD Quintile',IMD_quintile) , 
      is.na(IMD_quintile) & is.na(ethnicity) & is.na(Sex) & xi == 0 ~ paste0('Persons: All ages'),
      is.na(IMD_quintile) & !is.na(ethnicity) & is.na(Sex) & xi == 0 ~ paste0('Persons: All ages: ',ethnicity),
      !is.na(IMD_quintile) & is.na(ethnicity) & is.na(Sex) & xi == 0 ~ paste0('Persons: All ages: IMD Quintile',IMD_quintile),
      !is.na(IMD_quintile) & !is.na(ethnicity)& is.na(Sex) & xi == 0  ~ paste0('Persons: All ages: ',ethnicity,': IMD Quintile',IMD_quintile)      , 
      is.na(IMD_quintile) & is.na(ethnicity) & Sex == 1 & xi == 0 ~ paste0('Male: All ages'),
      is.na(IMD_quintile) & !is.na(ethnicity) & Sex == 1  & xi == 0 ~ paste0('Male: All ages: ',ethnicity),
      !is.na(IMD_quintile) & is.na(ethnicity) & Sex == 1  & xi == 0 ~ paste0('Male: All ages: IMD Quintile',IMD_quintile),
      !is.na(IMD_quintile) & !is.na(ethnicity)& Sex == 1   & xi == 0 ~ 
        paste0('Male: 65+ yrs: ',ethnicity,': IMD Quintile',IMD_quintile)      , 
      is.na(IMD_quintile) & is.na(ethnicity) & Sex == 2  & xi == 0~ paste0('Female: All ages'),
      is.na(IMD_quintile) & !is.na(ethnicity) & Sex == 2  & xi == 0 ~ paste0('Female: All ages: ',ethnicity),
      !is.na(IMD_quintile) & is.na(ethnicity) & Sex == 2   & xi == 0~ paste0('Female: All ages: IMD Quintile',IMD_quintile),
      !is.na(IMD_quintile) & !is.na(ethnicity)& Sex == 2  & xi == 0  ~ 
        paste0('Female: 65+ yrs: ',ethnicity,': IMD Quintile',IMD_quintile) ,     .default = 'NA'),
    IndicatorStartDate = paste0(period,'-01-01'),
    IndicatorEndDate = paste0(period+number_of_years-1,'-12-31'),  
    AggregationID = case_when(
      Geography  == 'Birmingham' ~ "147",
      Geography  == 'BSol' ~ "148",
      Geography  == 'Solihull' ~ "140",
      Geography  == 'North' ~ "142",
      Geography  == 'West' ~ "145",
      Geography  == 'Central' ~ "141",
      Geography  == 'South' ~ "144",
      Geography  == 'East' ~ "143",
      .default = as.character(AggregationID)    ),
    IndicatorID = ifelse(Sex == 1,901, ifelse(Sex == 2,902,NA)),
    ReferenceID = 93505,
    indicator_title = 'LE',
    TimePeriodDesc = paste0(number_of_years,' years'),
    TimePeriod = period, 
    lowerCI95 = Life_Expectancy_lower,
    upperCI95 = Life_Expectancy_upper,
    numerator = NA,
    denominator = NA,
    DataQualityID = 1,
    InsertDate = Sys.Date()  )  %>%
  left_join(demo, by=c("DemographicLabel" = "DemographicLabel"))  %>%
  select(IndicatorID, ReferenceID,
         IndicatorStartDate,IndicatorEndDate,
         numerator, denominator, IndicatorValue, lowerCI95, upperCI95, 
         AggregationID, DemographicID,DataQualityID,InsertDate) %>%
  filter(!is.na(IndicatorID) & !is.na(IndicatorValue) & 
           IndicatorValue != 'Inf' & IndicatorValue != '-Inf')

solihullLA <- LE_outfile %>%
  filter(AggregationID == "140") %>%
  mutate(AggregationID = "146")

LE_outfile <- rbind(LE_outfile, solihullLA)

write.csv(LE_outfile,'le_all.csv')

write.csv(outfile,'le_all_basic.csv')

write.csv(HLEoutfile, 'HealthylifeexpectancyOF40_41.csv')


