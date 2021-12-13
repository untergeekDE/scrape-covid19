impftage_df <- bl_tbl %>%
  # nur Hessen
  filter(id == "06") %>% 
  # Woche errechnen; Jahr mitnehmen
  # mit isoweek/isoyear, das den Versatz 2020/2021 in Betracht zieht
  group_by(Datum) %>% 
  pivot_wider(names_from=Impfserie,values_from=Anzahl) %>%
  summarize(erstgeimpft=sum(`1`,na.rm = TRUE),
            zweitgeimpft=sum(`2`,na.rm = TRUE),
            geboostert=sum(`3`,na.rm = TRUE),
            p=0) %>% 
  mutate(woche = paste0(isoyear(Datum),"_",
                        ifelse(isoweek(Datum)<10,"0",""),isoweek(Datum))) %>% 
  group_by(woche) %>% 
  mutate(erst_wk = sum(erstgeimpft),
         zweit_wk = sum(zweitgeimpft),
         booster_wk = sum(geboostert)) %>% 
  ungroup()

write.xlsx(impftage_df,"daten/meine_impfwochen.xlsx",overwrite=T)
  
