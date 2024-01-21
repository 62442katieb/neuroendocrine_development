require(dplyr)

DATA_DIR = "/Volumes/projects_herting/LABDOCS/Personnel/Katie/kangaroo_aim1/data"
OUTP_DIR = "/Volumes/projects_herting/LABDOCS/Personnel/Katie/kangaroo_aim1/output"

df1 <- read.csv('/Volumes/projects_herting/LABDOCS/PROJECTS/ABCD/Data/release5.0/core/physical-health/ph_y_sal_horm.csv')
temp <- read.csv('/Volumes/projects_herting/LABDOCS/PROJECTS/ABCD/Data/release5.0/core/abcd-general/abcd_p_demo.csv')
temp <- temp %>% filter(eventname=="baseline_year_1_arm_1")
df1 <- df1 %>% filter(eventname=="baseline_year_1_arm_1")
ppts <- rownames(df1)
df <- cbind(df1, temp[ppts,])

##Check to see if 'gender' and 'hormone_sal_sex' data matches.
#21 Female with misclassified Male tubes. 12 Male with misclassified Female tubes. 81 either had issues at saliva collection or had NA gender values.
#Let's get rid of them. We go down from 11875 -> 11761 (-114)
table(df$demo_sex_v2, df$hormone_sal_sex)
df  <- df[-c(which(df$demo_sex_v2 == "M" & df$hormone_sal_sex == 1), # "Pink/Purple (female)" = 1
             which(df$demo_sex_v2 =="F" & df$hormone_sal_sex == 2), # "Blue (male)" = 2
             which(df$hormone_sal_sex == 3), # "Participant unable to complete"
             which(df$hormone_sal_sex == 4), # "Participant/Parent refused"
             which(df$hormone_sal_sex == 5), # "Not collected (other)"
             which(df$hormone_sal_sex == 6), # "Institution not authorized to collect samples due to COVID"
             which(df$hormone_sal_sex == 7), # "Staff discomfort due to COVID"
             which(df$hormone_sal_sex == 8), # "Exceed 15min of < 6ft distance from youth (COVID)"
             which(df$hormone_sal_sex == 9), # "Not approved by ABCD for collection"
             which(is.na(df$demo_sex_v2)),
             which(is.na(df$hormone_sal_sex))),]


#Let's filter the data
#The filter scheme is to check records for any RA saliva collection notes. If true, then flag the record. 
#Then check flagged records and see if the Salimetrics value is out of range per hormone.
#If yes, then change value to NA, else keep the existing values for each replicate.
#Finally, average the two replicates into a new field.
df$hormone_notes_ss <- as.numeric(df$hormon_sal_notes_y___2) + 
                       as.numeric(df$hormon_sal_notes_y___3) +
                       as.numeric(df$hormon_sal_notes_y___4) + 
                       as.numeric(df$hormon_sal_notes_y___5) + 
                       as.numeric(df$hormon_sal_notes_y___6)
rownums <- which(df$hormone_notes_ss > 5)

#DHEA
df$filtered_dhea <- NA
df$filtered_dhea_rep1 <- df$hormone_scr_dhea_rep1
df$filtered_dhea_rep2 <- df$hormone_scr_dhea_rep2
df$filtered_dhea[which(df$hormone_scr_dhea_rep1_nd == 1)] <- 0
df$filtered_dhea[which(df$hormone_scr_dhea_rep2_nd == 1)] <- 0
rownums_rep1 <- which(df$hormone_scr_dhea_rep1 < 5 | df$hormone_scr_dhea_rep1 > 1000)
rownums_rep2 <- which(df$hormone_scr_dhea_rep2 < 5 | df$hormone_scr_dhea_rep2 > 1000)
df$filtered_dhea_rep1[rownums[which(rownums %in% rownums_rep1)]] <- NA
df$filtered_dhea_rep2[rownums[which(rownums %in% rownums_rep2)]] <- NA
temp1 = df[, c("filtered_dhea_rep1", "filtered_dhea_rep2")]
temp =  lapply(temp1,as.numeric)
temp <- as.data.frame(temp)
df$filtered_dhea <- rowMeans(temp,na.rm=TRUE) #apply(df[, c("filtered_dhea_rep1", "filtered_dhea_rep2")], 1, function(x) mean(x, na.rm=T))


#Testosterone
df$filtered_testosterone <- NA
df$filtered_testosterone_rep1 <- df$hormone_scr_ert_rep1
df$filtered_testosterone_rep2 <- df$hormone_scr_ert_rep2
df$filtered_testosterone[which(df$hormone_scr_ert_rep1_nd == 1)] <- 0
df$filtered_testosterone[which(df$hormone_scr_ert_rep2_nd == 1)] <- 0
rownums_rep1 <- which(df$hormone_scr_ert_rep1 < 1 | df$hormone_scr_ert_rep1 > 600)
rownums_rep2 <- which(df$hormone_scr_ert_rep2 < 1 | df$hormone_scr_ert_rep2 > 600)
df$filtered_testosterone_rep1[rownums[which(rownums %in% rownums_rep1)]] <- NA
df$filtered_testosterone_rep2[rownums[which(rownums %in% rownums_rep2)]] <- NA
temp1 = df[, c("filtered_testosterone_rep1", "filtered_testosterone_rep2")]
temp =  lapply(temp1,as.numeric)
temp <- as.data.frame(temp)
df$filtered_testosterone <- rowMeans(temp,na.rm=TRUE)


#Estradiol
df$filtered_estradiol <- NA
df$filtered_estradiol_rep1 <- df$hormone_scr_hse_rep1
df$filtered_estradiol_rep2 <- df$hormone_scr_hse_rep2
df$filtered_estradiol[which(df$hormone_scr_hse_rep1_nd == 1)] <- 0
df$filtered_estradiol[which(df$hormone_scr_hse_rep2_nd == 1)] <- 0
rownums_rep1 <- which(df$hormone_scr_hse_rep1 < 0.1 | df$hormone_scr_hse_rep1 > 32)
rownums_rep2 <- which(df$hormone_scr_hse_rep2 < 0.1 | df$hormone_scr_hse_rep2 > 32)
df$filtered_estradiol_rep1[rownums[which(rownums %in% rownums_rep1)]] <- NA
df$filtered_estradiol_rep2[rownums[which(rownums %in% rownums_rep2)]] <- NA
temp1 = df[, c("filtered_estradiol_rep1", "filtered_estradiol_rep2")]
temp =  lapply(temp1,as.numeric)
temp <- as.data.frame(temp)
df$filtered_estradiol <- rowMeans(temp,na.rm=TRUE)



df <- df %>% select("src_subject_id", "eventname", "filtered_dhea", "filtered_estradiol", "filtered_testosterone", 
                    "hormone_sal_sex", "demo_sex_v2")
saveRDS(df,paste(DATA_DIR, "SalimetricsClean.RDS", sep = "/"))
write.csv(df,paste(DATA_DIR, "SalimetricsClean.csv", sep = "/"))