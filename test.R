library(googlesheets4)
library(tidyverse)
library(dplyr)
library(reshape2)
library(lubridate)
library(hms)
library(reshape2)
library(readr)
library(readODS)


#google form authorisation
#gs4_auth()

#options(googleAuthR.verbose = FALSE)
#gs4_auth(
#  path = "client_secret_497263995273-ronbqboohkusd1dekbcg0nei8roc1rr3.apps.googleusercontent.com.json",
#  scopes = "https://www.googleapis.com/auth/spreadsheets",
#  cache = "gargle::gargle_oauth_cache()"
#)

#created a spreadsheet for tracker
#spreadsheet <- gs4_create("pb_pilot_tracker")
#options(googleAuthR.verbose = FALSE)

#reading in already created tracker sheet
spreadsheet <- gs4_get("https://docs.google.com/spreadsheets/d/1JtPY3js0SMZHpVgJUB3nn9zbTbtLW_eWgBvSJn1aWbk/edit?usp=sharing")

#pushbutton syrveycto
#sheet <- gs4_get("https://docs.google.com/spreadsheets/d/1sPRbvFZVDG2KNlhYf7i2JKh9kGxCLDipk6HUMiIzcnY/edit?usp=sharing")
#reading in surveycto published to google sheets
sheet <- gs4_get("https://docs.google.com/spreadsheets/d/13jqI4ZZyZFLkONdQBZpDf0D3ZrUL-v8JcJcYctPRZfc/edit?usp=sharing")

data <- read_sheet(sheet)
#data <- data %>%
#  mutate(across(starts_with("timestamp"), lubridate::ymd_hms))



#data$tokenno_1 <- as.character(data$tokenno_1)
#data$tokenno_2 <- as.character(data$tokenno_2)
#making all data as character
data <- mutate_all(data, as.character)


#choosing columns which end with _digit
columns_to_transform <- grep("_\\d+$", names(data), value = TRUE)

#reshaping wide to long for the chosen columns
long <- data %>%
  pivot_longer(cols = all_of(columns_to_transform), 
               names_to = c(".value", "suffix"), 
               names_pattern = "(.*)_(\\d+)")

#choosing relevant columns to put in google sheet
rel_cols <- long[,c("enumerator_name", "date", "time", "device_id", "calculate_tknno", "calculate_name", "attendance", "pieces_prod", "target_pieces", "remark")]

prod <- data.frame(rel_cols)

#cleaning date and time
prod$date <- ymd(prod$date)
prod$time <- as_hms(prod$time)
#prod_clean$time <- as.character(prod_clean$time)


prod_clean <- prod[complete.cases(prod$time), ]
prod_clean <- prod_clean[complete.cases(prod_clean$calculate_tknno), ]
prod_clean$time <- as.character(prod_clean$time)


#arranging according to descending order
prod_clean <- prod_clean %>% 
  arrange(desc(date))

#reading into pb_tracker
sheet_name <- "prod_cto"  # Name of the sheet to create or overwrite
write_sheet(prod_clean, spreadsheet, sheet = sheet_name)




#HFCs


#operations surveycto
#sheet1 <- gs4_get("https://docs.google.com/spreadsheets/d/13hXImRY3NwhFW80jwb0A_AFMmQ7PmUW_qUeQ5kqP6ZU/edit?usp=sharing")
sheet1 <- gs4_get("https://docs.google.com/spreadsheets/d/13F2csihfSADqXO-cj9yiMlZU07fbn3jWEP0SzQ_Wc08/edit?usp=sharing")

data1 <- read_sheet(sheet1)
columns_to_transform1 <- grep("_\\d+$", names(data1), value = TRUE)

data1 <- mutate_all(data1, as.character)


long1 <- data1 %>%
  pivot_longer(cols = all_of(columns_to_transform1), 
               names_to = c(".value", "suffix"), 
               names_pattern = "(.*)_(\\d+)")

rel_cols1 <- long1[,c("enumerator_name", "date", "time", "device_id", "calculate_tknno", "calculate_name", "attendance", "workstation_change", "line_number", "operation_name", "remark")]

opd <- data.frame(rel_cols1)
opd$date <- ymd(opd$date)
#opd$date <- format(opd$date, "%d%b")
opd$time <- as_hms(opd$time)

opd_clean <- opd[complete.cases(opd$time), ]
opd_clean$time <- as.character(opd_clean$time)
opd_clean <- opd_clean %>% 
  arrange(desc(date))

sheet_name1 <- "opd_cto"  # Name of the sheet to create or overwrite
write_sheet(opd_clean, spreadsheet, sheet = sheet_name1)

#reading in sample: don't run now. will add later for analysis
#sheet_name2 <- "sample"

#sample <- read_sheet(spreadsheet, sheet = sheet_name2)

#opd_monitor <- sample$device_id
#opd_monitor <- data.frame(device_id = opd_monitor)
#unique_dates <- unique(format(opd$date, "%d%b"))

#opd_monitor[, unique_dates] <- NA



#HFCs

#Match with line data
#Match with OB operation SAMs


#importing Sipmon for item code
#sipmon <- read_csv("Dropbox (Good Business Lab)/SipmonLite/Pushbutton/Pushbutton Pilot 2023/Sipmon/SipmonReport.csv", 
#                                 col_types = cols(`SCHEDULE DATE` = col_date(format = "%Y-%m-%d")), 
#                                 skip = 2)

sipmon <- read_csv("Sipmon/SipmonReport.csv", 
                   col_types = cols(`SCHEDULE DATE` = col_date(format = "%Y-%m-%d")), 
                   skip = 2)


#renaming sipmon columns
sipmon <- sipmon %>%
  rename_all(~gsub(" ", "_", tolower(.))) %>%
  filter(unit_code == "UNIT-31.") %>%
  rename(date = schedule_date)

#keeping only one item code per day which has highest accepted qty
sipmon <- sipmon %>%
  group_by(date, line_number) %>%
  arrange(desc(accepted_quantity)) %>%
  slice_head(n = 1) %>%
  ungroup()

#getting date line item-code to match with opd_clean
sipmon_line <- sipmon %>%
  select(date, line_number, item_code) %>%
  mutate(line_number = substr(line_number, nchar(line_number) - 1, nchar(line_number))) %>%
  mutate(line_number = as.numeric(line_number)) %>%
  #mutate(day = as.numeric(difftime(date, as.Date("2023-06-11"), units = "days"))) %>%
  select(date, line_number, item_code) #%>%
  #filter(day > 0)

#reading in IE_Line_Item
line_item <- gs4_get("https://docs.google.com/spreadsheets/d/1YfYi8ulPrilHRFK85nqnjIp4l6Y0330TvOoxJCO0_Z8/edit?usp=sharing")
line_item <- read_sheet(line_item)

line_item <- line_item %>% 
  filter(username == "gbl")

columns_to_transforml <- grep("_\\d+$", names(line_item), value = TRUE)

datal <- mutate_all(line_item, as.character)


longl <- datal %>%
  pivot_longer(cols = all_of(columns_to_transforml), 
               names_to = c(".value", "suffix"), 
               names_pattern = "(.*)_(\\d+)")

line_item_f <- longl %>%
  select(line_number, item_code, item_code_other) %>%
  filter(line_number > 0)

item_map <- data.frame(item_code = 1:11, name = c("148K", "268K", "518K", "019J", "671J", "017J", "768J", "668J", "641K","696L", "025L"))
item_map$item_code <- as.character(item_map$item_code)
line_item_f1 <- left_join(line_item_f, item_map, by = "item_code")

line_item_f1 <- line_item_f1 %>%
  mutate(name = ifelse(is.na(name), item_code_other, name)) %>%
  filter(line_number != 13) %>%
  select(line_number, name) %>%
  rename(item_code = name)


#defining date-day map
start_date <- as.Date("2023-06-21")
days <- 13
date_day_map <- data.frame(date = seq(start_date, by = "day", length.out = days),
                           day = 0:(days - 1))




#merging sipmon item code with opd scto
#opd_clean <- merge(opd_clean, sipmon_line, by = c("date", "line_number"))

opd_clean <- merge(opd_clean, line_item_f1, by = c("line_number"))




#sort data to plug into IE scto
opd_clean_1 <- opd_clean %>% 
  distinct(date, device_id, .keep_all = TRUE) %>% #keeps only the first observation out of duplicates
  filter(attendance == 1) %>%
  mutate(day = as.numeric(difftime(date, start_date, units = "days"))) %>% #day 1 is 12th Monday
  distinct(date, device_id, .keep_all = TRUE) %>%
  filter(attendance == 1) %>%
  filter(day > 0)

#wide operation_name
wide_data <- opd_clean_1 %>% 
  select (device_id, day, operation_name) %>%
  mutate(day = paste0("operation_name_", day)) %>% 
  spread(day, operation_name)

#wide line_number
wide_data1 <- opd_clean_1 %>% 
  select (device_id, day, line_number) %>%
  mutate(day = paste0("line_number_", day)) %>% 
  spread(day, line_number)

#wide item_code
wide_data2 <- opd_clean_1 %>% 
  select (device_id, day, item_code) %>%
  mutate(day = paste0("item_code_", day)) %>% 
  spread(day, item_code)

#matching the wide datapoints
wide_data <- left_join(wide_data, wide_data1, by = "device_id")
wide_data <- left_join(wide_data, wide_data2, by = "device_id")






#correct: flag checks with one previous operation
wide_data$flag_1 <- 1
n <- as.integer((ncol(wide_data)-1)/3)
for (i in 2:n) {
  wide_data <- wide_data %>%
    mutate(!!paste0("flag_", i) := ifelse(!is.na(!!sym(paste0("operation_name_", i))) & is.na(!!sym(paste0("operation_name_", i-1))), 1, 
                                          ifelse(!!sym(paste0("operation_name_", i)) != (!!sym(paste0("operation_name_", i-1))), 1, 0)))
}

#creating first columns for surveycto
wide_data <- wide_data %>%
  mutate(operation_name = !!sym(paste0("operation_name_", n)), 
         line_number = !!sym(paste0("line_number_", n)), 
         item_code = !!sym(paste0("item_code_", n)), 
         flag = !!sym(paste0("flag_", n)))


#reordering columns
wide_data <- wide_data %>% 
  select(order(as.numeric(sub("\\D+", "", names(wide_data))), sub("\\d+", "", names(wide_data)), decreasing = TRUE)) %>%
  select(device_id, operation_name, line_number, item_code, flag, everything())

##################################
##################################
wide_data <- opd_clean_1 %>%
  select(device_id, operation_name, line_number, item_code)
wide_data$flag <- 1





##################################
##################################


#writing into tracker sheet
sheet_name2 <- "to_ie_cto"  # Name of the sheet to create or overwrite
#write_sheet(wide_data, spreadsheet, sheet = sheet_name2)
write_sheet(temp1, spreadsheet, sheet = sheet_name2)


#writing date_day_map into tracker sheet
sheet_name3 <- "date_day_map"  # Name of the sheet to create or overwrite
write_sheet(date_day_map, spreadsheet, sheet = sheet_name3)


#operation names
OB <- read_sheet("https://docs.google.com/spreadsheets/d/1m4KpkdG1DtcmUfpJi9JbjTr0PrjO0u7AaLcvhL2XBlo/edit?usp=sharing", sheet = "operation_names" )

op_names <- melt(OB, id.vars = NULL)
op_names <- op_names[complete.cases(op_names), ]
op_names$value <- paste0(op_names$value, " ", op_names$variable)
op_names$serial_number <- seq.int(nrow(op_names))
op_names <- op_names %>%
  select(serial_number, value) %>%
  rename(op_name = value)

sheet_name4 <- "op_names"
to_OpName <- gs4_get("https://docs.google.com/spreadsheets/d/10oVsUkEcIIDuNMyF5ddxKi0lEantjWbTvQi9bVPv3_A/edit?usp=sharing")
write_sheet(op_names, to_OpName, sheet = sheet_name4)


ob_names <- gs4_get("https://docs.google.com/spreadsheets/d/1zDk3iGOYOheFaNORma6JiNq4abtBmd70rOadXr0o1lU/edit?usp=sharing")

ob_names <- read_sheet(ob_names)

ob_names <- mutate_all(ob_names, as.character)


#choosing columns which end with _digit
columns_to_transform_ob <- grep("_\\d+$", names(ob_names), value = TRUE)

#reshaping wide to long for the chosen columns
ob_names_long <- ob_names %>%
  pivot_longer(cols = all_of(columns_to_transform_ob), 
               names_to = c(".value", "suffix"), 
               names_pattern = "(.*)_(\\d+)")

#choosing relevant columns to put in dataframe
rel_cols_ob <- ob_names_long[,c("enumerator_name", "date", "time", "device_id", "calculate_tknno", "calculate_name", "calculate_line_number", "calculate_flag", "calculate_item_code", "calculate_operation_name", "op_name")]

ob_names <- data.frame(rel_cols_ob)

ob_names <- ob_names %>%
  rename(serial_number = op_name)
ob_names <- merge(ob_names, op_names, by = c("serial_number"))

#cleaning date and time
ob_names$date <- ymd(ob_names$date)
ob_names$time <- as_hms(ob_names$time)
#correct time format
ob_names$time <- format(as.POSIXct(ob_names$time, format = "%H:%M:%S"), format = "%H:%M:%S")

sheet_name4 <- "ob_mapped_22June"  # Name of the sheet to create or overwrite
write_sheet(ob_names, spreadsheet, sheet = sheet_name4)
ob_mapped_22June <- ob_names

ob_mapped_22June <- ob_mapped_22June %>%
  rename(line_number = calculate_line_number,
         flag = calculate_flag,
         item_code = calculate_item_code,
         operation_name = calculate_operation_name)
ob_mapped_22June$flag <- as.numeric(ob_mapped_22June$flag)

temp <- wide_data %>%
  anti_join(ob_mapped_22June)
temp1 <- temp %>%
  distinct(device_id, .keep_all = TRUE)


