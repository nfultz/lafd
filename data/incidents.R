
incidents <- local({

  timeToSecs <- function(x) {
    vapply(strsplit(x, ":"), FUN.VALUE = 0.0, function(x){
      if(identical(x, NA_character_)) NA else crossprod(60**(2:0), as.numeric(x))
    })
  }




  main <- read.csv(pipe("unrar p -inul 'Data Angels.rar' 'Data - Main Table.csv'"), stringsAsFactors=FALSE)
  for(i in colnames(main)) {
    main[main[[i]] %in% c('', ' '), i] <- NA
  }

  incidents <- read.csv(pipe("unrar p -inul 'Data Angels.rar' 'Data - Incident Ref.csv'"), stringsAsFactors=FALSE)
  for(i in colnames(incidents)) {
    incidents[incidents[[i]] %in% c('', ' '), i] <- NA
  }

  incidents <- merge(main, incidents, by="INCIDENT_TYPE_CD", all.x=TRUE)

  # Feature engineering
  incidents <- within(incidents, {
      ssm_dispatch  <- timeToSecs(TIME_DISPATCHED_UTC)
      ssm_enroute   <- timeToSecs(TIME_ENROUTE_UTC)
      ssm_onscene   <- timeToSecs(TIME_ON_SCENE_UTC)
      ssm_transport <- timeToSecs(TIME_OF_TRANSPORT_UTC)
      ssm_hospital  <- timeToSecs(TIME_AT_HOSPITAL_UTC)
      ssm_ended     <- timeToSecs(TIME_ENDED_UTC)


      secs_pickup  <- ssm_onscene - ssm_enroute
      secs_dropoff <- ssm_hospital - ssm_transport

      secs_pickup  <- secs_pickup  + ifelse(secs_pickup  < 0, 24*60*60 , 0)
      secs_dropoff <- secs_dropoff + ifelse(secs_dropoff < 0, 24*60*60 , 0)

      secs_pickup  <- ifelse(secs_pickup  > 15000, NA , secs_pickup)
      secs_dropoff <- ifelse(secs_dropoff > 15000, NA , secs_dropoff)

      battalion_to_analyze <- HOME_BATTALION %in% c(10,12,14,15, 17)    &
        INCIDENT_BATTALION_NBR %in% c(10,12,14,15, 17) & !duplicated(RANDOMIZED_INCIDENT_NBR)

      HOME_STATION <- as.factor(HOME_STATION)
      HOME_BATTALION <- as.factor(HOME_BATTALION)

      INCIDENT_FIRST_IN_DISTRICT <- ifelse(is.na(INCIDENT_FIRST_IN_DISTRICT), HOME_STATION, INCIDENT_FIRST_IN_DISTRICT)
      INCIDENT_FIRST_IN_DISTRICT <- as.factor(INCIDENT_FIRST_IN_DISTRICT)

      INCIDENT_BATTALION_NBR <- as.factor(INCIDENT_BATTALION_NBR)

      yyyy <- as.factor(year)
      qq   <- as.factor(quarter)
      dow  <- factor(trimws(DAY_OF_WEEK), c("MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY", "SATURDAY", "SUNDAY"))

      emergency_dispatch <- +(EMERGENCY_DISPATCH_IND %in% "EMERGENCY")

      rand <- RANDOMIZED_INCIDENT_NBR %% 999999


      borrowed <- as.character(INCIDENT_FIRST_IN_DISTRICT) != as.character(HOME_STATION)

      fold <- rand %% 13 + 1
      flip <- (borrowed) &
        (seq_along(fold) ==
                 ave(seq_along(fold), FUN = min, battalion_to_analyze,
                     borrowed,
                    INCIDENT_FIRST_IN_DISTRICT, yyyy, qq, dow, fold, drop=TRUE))

  })

  tmp <- tempfile()
  system(sprintf("unrar p -inul 'Data Angels.rar' 'Data Dictionary.xlsx' > %s", tmp))
  dictionary_incident <- readxl::read_xlsx(tmp, "Incident Ref")
  dictionary_main <- readxl::read_xlsx(tmp, "Main Data")
  unlink(tmp)

  dictionary_incident <- dictionary_incident[dictionary_incident$COLUMN_NAME != 'INCIDENT_TYPE_CD',] # join key in both tables
  dictionary <- as.data.frame(rbind(dictionary_main, dictionary_incident))
  row.names(dictionary) <- dictionary$COLUMN_NAME


  for(i in colnames(incidents)) {
    attr(incidents[[i]], "description") <- dictionary[i, "DESCRIPTION"]
  }

  incidents
})
