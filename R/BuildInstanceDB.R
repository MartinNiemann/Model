# ******************************************************************************
# Intraday Overreaction - BuildInstanceDB.R                                    *
# ******************************************************************************
# This script reads overreaction score files per stock and generates one       *
# combined instance database for a minimum event cut-off value for analysis    *
# Process: loop through all score files, and                                   *
# 1) Identify events for desired cut-off value and return definitions,         *
#    creating eventDBs for all [up/down], [r/ar], [abs], [rel] combinations    *
# 2) Calculate event properties for each eventDB                               *
# 3) Save event properties in new dataframe ("master eventDB")                 *  
# ******************************************************************************

# ----- PREP -------------------------------------------------------------------
# path & file info
sf.files <- list.files (path = data.path.sf, pattern = "*.sco")

symbol.info <- 
  read.csv ("D:/Doktorarbeit/40 Data/Stocks/Taipan/symbol_info.csv", 
            header=TRUE, stringsAsFactors=FALSE, comment.char="", 
            col.names=c("Symbol", "shares_out", "Name", 
                        "Sector", "Industry", "Country"), 
            colClasses=c("character", "numeric", rep("character",4)))
rownames(symbol.info) <- symbol.info$Symbol

# ----- CORE LOOP: PROCESS SCORE FILES -----------------------------------------
param.filt.abs.rel <- list (c(2,6), c(2.5,6.5), c(3,7), c(3.5,7.5), c(4,8))

# special loop through abs/rel combinations
for (L in seq_along(param.filt.abs.rel)[1]) {
  param.filt.abs <- param.filt.abs.rel[[L]][1]
  param.filt.rel <- param.filt.abs.rel[[L]][2]
  print (c(param.filt.abs, param.filt.rel))
for (i in 1:length(sf.files)) {
  # 1) Load current score file & HFT data --------------------------------------
  # 1a) score file
  load (paste(data.path.sf, sf.files[i], sep=""))
  cur.symbol <- as.character (strsplit(sf.files[i],".sco")[1]) # [1] = filename
  print (paste("[BuildInstanceDB.R ", format(Sys.time(), "%H:%M:%S"), 
               "] Processing ", cur.symbol, sep=""))
  
  # 1b) hft data
  cur.hft.x <- Load1minHFTDataXTS (paste(data.path.HFT,
                                   paste(cur.symbol,".csv", sep=""), sep="/"))
  # create aligned HFT series (simplifies all event-bar based lookups)
  cur.hft.x <- cur.hft.x[index(cur.hft.x) %in% index(cur.x.aligned)]
  cur.hft.x.aligned <- merge (cur.hft.x, index(cur.x.aligned), fill=0)
  
  # create daily HFT series'
  cur.hft.flags.d.x <- period.apply (cur.hft.x.aligned$Events, 
                                     endpoints(cur.hft.x.aligned, "days"), sum)
  cur.hft.mins.d.x <- period.apply (cur.hft.x.aligned$Events, 
                                    endpoints(cur.hft.x.aligned, "days"),
                                    FUN=function(x) sum(x>0))
  cur.hft.quotes.d.x <- period.apply (cur.hft.x.aligned$Quotes, 
                                     endpoints(cur.hft.x.aligned, "days"), sum)
  # remove day-times from daily HFT series' timestamps
  index(cur.hft.flags.d.x) <- as.Date (index(cur.hft.flags.d.x))
  index(cur.hft.mins.d.x) <- as.Date (index(cur.hft.mins.d.x))
  index(cur.hft.quotes.d.x) <- as.Date (index(cur.hft.quotes.d.x))
  
  # calculate daily volume "participation rates"
  cur.hft.part.flags.d.x <- cur.hft.flags.d.x / cur.d.x.aligned$Volume
  cur.hft.part.quotes.d.x <- cur.hft.quotes.d.x / cur.d.x.aligned$Volume
  
  # calculate dta overlay curves for total HFT series
  cur.hft.daily <- array (coredata(cur.hft.x.aligned$Events), 
                          dim=c(daybars, nrow(cur.d.x.aligned)))
  cur.hft.pf.daily <- array (coredata(cur.hft.x.aligned$Events/cur.x.aligned$Volume), 
                             dim=c(daybars, nrow(cur.d.x.aligned)))
  cur.hft.pq.daily <- array (coredata(cur.hft.x.aligned$Quotes/cur.x.aligned$Volume), 
                             dim=c(daybars, nrow(cur.d.x.aligned)))
  # catch cases where both values == 0
  cur.hft.pf.daily[is.nan(cur.hft.pf.daily)] <- 0
  cur.hft.pq.daily[is.nan(cur.hft.pq.daily)] <- 0
  # catch cases where HFT > 0 and volume == 0 --> set to double of max
  cur.hft.pf.daily[!is.finite(cur.hft.pf.daily)] <-
    max(cur.hft.pf.daily[is.finite(cur.hft.pf.daily)])
  cur.hft.pq.daily[!is.finite(cur.hft.pq.daily)] <-
    max(cur.hft.pq.daily[is.finite(cur.hft.pq.daily)])
  
  # flags: unit = % of seconds with flags
  cur.hft.flags.dta <- array (NA, dim=c(daybars))
  cur.hft.flags.dta[1:daybars] <- 
    sapply (1:daybars, function(x) mean(cur.hft.daily[x,])/60)
  # mins: unit = % of minutes with flags
  cur.hft.mins.dta <- array (NA, dim=c(daybars))
  cur.hft.mins.dta[1:daybars] <-
    sapply (1:daybars, function(x) mean(ifelse(cur.hft.daily[x,]>0,1,0)))
  # flag & quote rates / volume: unit = % of volume
  cur.hft.pf.dta <- array (NA, dim=c(daybars))
  cur.hft.pq.dta <- array (NA, dim=c(daybars))
  cur.hft.pf.dta[1:daybars] <- sapply (1:daybars, function(x) mean(cur.hft.pf.daily[x,]))
  cur.hft.pq.dta[1:daybars] <- sapply (1:daybars, function(x) mean(cur.hft.pq.daily[x,]))
  # smooth dta estimates
  cur.hft.flags.dta <- array (lowess (cur.hft.flags.dta, f=.05)$y)
  cur.hft.mins.dta <- array (lowess (cur.hft.mins.dta, f=.05)$y)
  cur.hft.pf.dta <- array (lowess (cur.hft.pf.dta, f=.05)$y)
  cur.hft.pq.dta <- array (lowess (cur.hft.pq.dta, f=.05)$y)
  
  # enable time-based lookup in dta variables
  # TODO: veeerry dirty code to generate time sequence... (str-cut)
  rownames(cur.hft.flags.dta) <- 
    substr (seq(from=as.POSIXct(paste("2012-01-01 ", day.start.time, sep="")), 
                to=as.POSIXct(paste("2012-01-01 ", day.end.time, sep="")), 
                by="min"), 12, 19)
  rownames(cur.hft.mins.dta) <- 
    substr (seq(from=as.POSIXct(paste("2012-01-01 ", day.start.time, sep="")), 
                to=as.POSIXct(paste("2012-01-01 ", day.end.time, sep="")), 
                by="min"), 12, 19)  
  rownames(cur.hft.pf.dta) <- 
    substr (seq(from=as.POSIXct(paste("2012-01-01 ", day.start.time, sep="")), 
                to=as.POSIXct(paste("2012-01-01 ", day.end.time, sep="")), 
                by="min"), 12, 19)
  rownames(cur.hft.pq.dta) <- 
    substr (seq(from=as.POSIXct(paste("2012-01-01 ", day.start.time, sep="")), 
                to=as.POSIXct(paste("2012-01-01 ", day.end.time, sep="")), 
                by="min"), 12, 19)  
  
  # build daily sum-of-flags variable for HFT activity (across all stocks)
  if (i == 1) {
    hft.flags.sum.x <- cur.hft.flags.d.x
    hft.mins.sum.x <- cur.hft.mins.d.x
    active.stocks.sum.x <- xts (order.by=index(cur.d.x.aligned), 
                                x=rep(1, nrow(cur.d.x.aligned)))
  } else {
    temp.x <- merge(cur.hft.flags.d.x, hft.flags.sum.x, all=TRUE, fill = 0)
    hft.flags.sum.x <- xts(temp.x$Events + temp.x$Events.1, 
                           order.by=index(temp.x))
    temp.x <- merge(cur.hft.mins.d.x, hft.mins.sum.x, all=TRUE, fill = 0)
    hft.mins.sum.x <- xts(temp.x$Events + temp.x$Events.1, 
                           order.by=index(temp.x))
    active.stocks.sum.x <- merge (active.stocks.sum.x, index(cur.d.x.aligned), 
                                  fill = 0, all = TRUE)
    active.stocks.sum.x[index(cur.d.x.aligned)] <- 
      active.stocks.sum.x[index(cur.d.x.aligned)] + 1
    colnames(active.stocks.sum.x) <- "#stocks"
  }
  # end of 1) Load current score file & HFT data
  
  # ***** loaded vars - reference ******************************
  # cur.x.aligned, cur.d.x.aligned, sp.x.aligned, sp.d.x.aligned,
  # cur.rbetas.d.x.shifted, id.dsbmom, id.score,
  # id.volume.dta, id.rv.dta, id.rv.dta.sc, id.rv.1min.dta, sp.id.rv.dta.sc,
  
  # ATTENTION: vola definitions
  # - id.rv.dta: day-start-bounded 60-minute rolling intraday vola
  # - id.rv.dta.sc: id.rv.dta, sqrt(x)-scaled with x 1:60 ramp-up at day-start
  # - id.rv.1min.dta: lowess-smoothed day-time average 1-min vola
  # Uses
  # - id.rv.dta.sc: for comparisons of dsbmom with dta-vola of same length
  # - id.rv.1min.dta: for comparisons of 1-min returns with dta-vola
  
  # ***** Definitions of time periods/windows ******************
  # pre-event period (PREP) = period.days prior to event
  # pre-event window (PREW) = event formation window
  # post-event window (POEW)
  # full-event window (FEW) = pre- + post event window
  # event-continuation phase (ECP) = event to t(reversal start)-1
  # event-reversal phase (ERP) = reversal start to end of FEW
  
  # 2) Identify events for desired cut-off values ------------------------------
  #    and apply 1-event/day and date/daytime filters
  # raw return events  
  id.events.idx.up <- GetEventIdx (id.score, id.dsbmom,
                                   param.filt.rel, param.filt.abs, up=TRUE)
  id.events.idx.dn <- GetEventIdx (id.score, id.dsbmom,
                                   param.filt.rel, param.filt.abs, up=FALSE)
  events.up.df <- BuildEventDf (id.events.idx.up)
  events.dn.df <- BuildEventDf (id.events.idx.dn)  
    
  # abnormal return events
#   id.events.idx.up.a <- GetEventIdx (id.score.a, id.dsbmom.a,
#                                      param.filt.rel, param.filt.abs, up=TRUE)
#   id.events.idx.dn.a <- GetEventIdx (id.score.a, id.dsbmom.a,
#                                      param.filt.rel, param.filt.abs, up=FALSE)
  # *** FOR ANALYSIS IN ABNORMAL RETURN SPACE, simply calculate events.xx.df ***
  # *** using BuildEventDf on id.events.idx.xx.a                             ***
  
  # end of 2) Identify events for desired cut-off values


  # 3) Build symbol's data frame with event properties -------------------------
  # build data frame with event time stamp as index
  
  # 3a) generic company properties ---------------------------------------------
  # return variables for generic company properties
  cur.d.x.aligned.ret <- diff (log(cur.d.x.aligned$Close), 1) * 100
  sp.d.x.aligned.ret <- diff (log(sp.d.x.aligned$Close), 1) * 100
  cur.d.x.aligned.aret <- cur.d.x.aligned.ret - 
                          cur.rbetas.d.x.shifted * sp.d.x.aligned.ret
  # up
  if (length(id.events.idx.up) > 0) {
    # beta, last close and market cap
    events.up.df$rbeta <- 
      as.vector (cur.rbetas.d.x.shifted[as.Date(events.up.df$timestamp)])
    cur.d.x.events.up.idx <- which (index(cur.d.x.aligned) %in%
      index(cur.d.x.aligned[as.character(as.Date(events.up.df$timestamp))]))
    events.up.df$price.c1 <- as.vector (
      cur.d.x.aligned$Close[cur.d.x.events.up.idx - 1])
    events.up.df$market.cap <- events.up.df$price.c1 * 
                                 symbol.info[cur.symbol,'shares_out']
    # special case market cap adjustment for diluted finance companies
    if (cur.symbol == "AIG.N") {
      events.up.df$market.cap[as.Date(events.up.df$timestamp) < "2009-01-01"] <-
        events.up.df$market.cap[as.Date(events.up.df$timestamp) < "2009-01-01"] / 10
    }
    if (cur.symbol == "BAC.N") {
      events.up.df$market.cap[as.Date(events.up.df$timestamp) < "2009-01-01"] <-
        events.up.df$market.cap[as.Date(events.up.df$timestamp) < "2009-01-01"] / 5
    }
    if (cur.symbol == "ABV.N") {
      events.up.df$market.cap[as.Date(events.up.df$timestamp) < "2011-01-01"] <-
        events.up.df$market.cap[as.Date(events.up.df$timestamp) < "2011-01-01"] / 5
    }
    if (cur.symbol == "C.N") {
      events.up.df$market.cap[as.Date(events.up.df$timestamp) < "2009-01-01"] <-
        events.up.df$market.cap[as.Date(events.up.df$timestamp) < "2009-01-01"] / 10
    }
  }
  # dn
  if (length(id.events.idx.dn) > 0) {
    # beta, last close and market cap
    events.dn.df$rbeta <- 
      as.vector (cur.rbetas.d.x.shifted[as.Date(events.dn.df$timestamp)])
    cur.d.x.events.dn.idx <- which (index(cur.d.x.aligned) %in%
                                      index(cur.d.x.aligned[as.character(as.Date(events.dn.df$timestamp))]))
    events.dn.df$price.c1 <- as.vector (
      cur.d.x.aligned$Close[cur.d.x.events.dn.idx - 1])
    events.dn.df$market.cap <- events.dn.df$price.c1 * 
      symbol.info[cur.symbol,'shares_out']
    # special case market cap adjustment for diluted finance companies
    if (cur.symbol == "AIG.N") {
      events.dn.df$market.cap[as.Date(events.dn.df$timestamp) < "2009-01-01"] <-
        events.dn.df$market.cap[as.Date(events.dn.df$timestamp) < "2009-01-01"] / 10
    }
    if (cur.symbol == "BAC.N") {
      events.dn.df$market.cap[as.Date(events.dn.df$timestamp) < "2009-01-01"] <-
        events.dn.df$market.cap[as.Date(events.dn.df$timestamp) < "2009-01-01"] / 5
    }
    if (cur.symbol == "ABV.N") {
      events.dn.df$market.cap[as.Date(events.dn.df$timestamp) < "2011-01-01"] <-
        events.dn.df$market.cap[as.Date(events.dn.df$timestamp) < "2011-01-01"] / 5
    }
    if (cur.symbol == "C.N") {
      events.dn.df$market.cap[as.Date(events.dn.df$timestamp) < "2009-01-01"] <-
        events.dn.df$market.cap[as.Date(events.dn.df$timestamp) < "2009-01-01"] / 10
    }
  }  
  # end of 3a) generic company properties
  
  # 3b) generic stock characteristics in PREP ----------------------------------
  if (length(id.events.idx.up) > 0) {
    # generate overall index matrices for data subsets
    events.up.PREP.idx <- sapply (cur.d.x.events.up.idx, 
                                  function(x) (x-period.days):(x-1))
    # vola, liquidity and returns
    events.up.df$PREP.vola <- ApplyToMat(as.vector(cur.d.x.aligned.ret), 
                                         events.up.PREP.idx, sd)
    events.up.df$PREP.vola.ids <- ApplyToMat(as.vector(cur.d.x.aligned.aret), 
                                         events.up.PREP.idx, sd)
    events.up.df$PREP.vol.avg <- ApplyToMat(as.vector(cur.d.x.aligned$Volume), 
                                            events.up.PREP.idx, mean)
    events.up.df$PREP.liq.avg <- ApplyToMat(as.vector(cur.d.x.aligned$Volume) *
                                              as.vector(cur.d.x.aligned$Close), 
                                            events.up.PREP.idx, mean)
    events.up.df$PREP.r.cum <- ApplyToMat(as.vector(cur.d.x.aligned.ret), 
                                          events.up.PREP.idx, sum)
    events.up.df$PREP.ar.cum <- ApplyToMat(as.vector(cur.d.x.aligned.aret), 
                                           events.up.PREP.idx, sum)
  }
  if (length(id.events.idx.dn) > 0) {
    # generate overall index matrices for data subsets
    events.dn.PREP.idx <- sapply (cur.d.x.events.dn.idx, 
                                  function(x) (x-period.days):(x-1))
    # vola, liquidity and returns
    events.dn.df$PREP.vola <- ApplyToMat(as.vector(cur.d.x.aligned.ret), 
                                         events.dn.PREP.idx, sd)
    events.dn.df$PREP.vola.ids <- ApplyToMat(as.vector(cur.d.x.aligned.aret), 
                                         events.dn.PREP.idx, sd)
    events.dn.df$PREP.vol.avg <- ApplyToMat(as.vector(cur.d.x.aligned$Volume), 
                                            events.dn.PREP.idx, mean)
    events.dn.df$PREP.liq.avg <- ApplyToMat(as.vector(cur.d.x.aligned$Volume) *
                                              as.vector(cur.d.x.aligned$Close), 
                                            events.dn.PREP.idx, mean)
    events.dn.df$PREP.r.cum <- ApplyToMat(as.vector(cur.d.x.aligned.ret), 
                                          events.dn.PREP.idx, sum)
    events.dn.df$PREP.ar.cum <- ApplyToMat(as.vector(cur.d.x.aligned.aret), 
                                           events.dn.PREP.idx, sum)
  }
  # end of 3b) generic stock characteristics in PREP
    
  # 3c.1x) special: build daily dataframe/symbol matrix ------------------------
  #                 for key selection criteria (price, size, liq, vola)
  if (i == 1) {
    # build dataframe with date as key
    stocks.prices.df <- data.frame (cur.d.x.aligned$Close, 
                                    row.names=index(cur.d.x.aligned))
    stocks.sizes.df <- data.frame (cur.d.x.aligned$Close * 
                                     symbol.info[cur.symbol,'shares_out'], 
                                   row.names=index(cur.d.x.aligned))
    stocks.liqs.df <-  data.frame (
      MovingAvg (cur.d.x.aligned$Close * cur.d.x.aligned$Volume, period.days), 
                                   row.names=index(cur.d.x.aligned))
    stocks.volas.df <- data.frame (sapply (seq_along(cur.d.x.aligned.ret), 
                               function(i) if (i < period.days) NA else 
                                 sd(cur.d.x.aligned.ret[i:(i-period.days+1)])),
                                   row.names=index(cur.d.x.aligned))
    stocks.betas.df <- data.frame (cur.rbetas.d.x.shifted, 
                                   row.names=index(cur.d.x.aligned))
    stocks.HFTflags.df <- data.frame (cur.hft.flags.d.x$Events, 
                                      row.names=index(cur.hft.flags.d.x))
    stocks.HFTmins.df <- data.frame (cur.hft.mins.d.x$Events, 
                                     row.names=index(cur.hft.mins.d.x))
    stocks.HFTpflags.df <- data.frame (cur.hft.part.flags.d.x, 
                                       row.names=index(cur.hft.flags.d.x))
    stocks.HFTpquotes.df <- data.frame (cur.hft.part.quotes.d.x, 
                                        row.names=index(cur.hft.mins.d.x))
    
    names(stocks.prices.df)[1] <- cur.symbol
    names(stocks.sizes.df)[1] <- cur.symbol
    names(stocks.liqs.df)[1] <- cur.symbol
    names(stocks.volas.df)[1] <- cur.symbol
    names(stocks.betas.df)[1] <- cur.symbol
    names(stocks.HFTflags.df)[1] <- cur.symbol
    names(stocks.HFTmins.df)[1] <- cur.symbol
    names(stocks.HFTpflags.df)[1] <- cur.symbol
    names(stocks.HFTpquotes.df)[1] <- cur.symbol
  } else {
    # create temporary dataframes for current symbol
    cur.prices.df <- data.frame (cur.d.x.aligned$Close, 
                                 row.names=index(cur.d.x.aligned))
    cur.sizes.df <- data.frame (cur.d.x.aligned$Close * 
                                  symbol.info[cur.symbol,'shares_out'], 
                                row.names=index(cur.d.x.aligned))
    cur.liqs.df <-  data.frame (
      MovingAvg (cur.d.x.aligned$Close * cur.d.x.aligned$Volume, period.days), 
      row.names=index(cur.d.x.aligned))
    cur.volas.df <- data.frame (sapply (seq_along(cur.d.x.aligned.ret), 
      function(i) if (i < period.days) NA else 
        sd(cur.d.x.aligned.ret[i:(i-period.days+1)])),
                                row.names=index(cur.d.x.aligned))
    cur.betas.df <- data.frame (cur.rbetas.d.x.shifted, 
                                   row.names=index(cur.d.x.aligned))
    cur.HFTflags.df <- data.frame (cur.hft.flags.d.x$Events, 
                                   row.names=index(cur.hft.flags.d.x))
    cur.HFTmins.df <- data.frame (cur.hft.mins.d.x$Events, 
                                  row.names=index(cur.hft.mins.d.x))
    cur.HFTpflags.df <- data.frame (cur.hft.part.flags.d.x, 
                                    row.names=index(cur.hft.flags.d.x))
    cur.HFTpquotes.df <- data.frame (cur.hft.part.quotes.d.x, 
                                     row.names=index(cur.hft.mins.d.x))
    
    names(cur.prices.df)[1] <- cur.symbol
    names(cur.sizes.df)[1] <- cur.symbol
    names(cur.liqs.df)[1] <- cur.symbol
    names(cur.volas.df)[1] <- cur.symbol
    names(cur.betas.df)[1] <- cur.symbol
    names(cur.HFTflags.df)[1] <- cur.symbol
    names(cur.HFTmins.df)[1] <- cur.symbol
    names(cur.HFTpflags.df)[1] <- cur.symbol
    names(cur.HFTpquotes.df)[1] <- cur.symbol
    
    # merge temporary dataframes into overall data frames
    # (by=0 is equal to by=row.names)
    test.df <- merge (stocks.prices.df, cur.prices.df, by=0, all=TRUE, fill=0)
    stocks.prices.df <- data.frame (test.df[,2:ncol(test.df)], 
                                    row.names=test.df[,1])
    test.df <- merge (stocks.sizes.df, cur.sizes.df, by=0, all=TRUE, fill=0)
    stocks.sizes.df <- data.frame (test.df[,2:ncol(test.df)], 
                                   row.names=test.df[,1])
    test.df <- merge (stocks.liqs.df, cur.liqs.df, by=0, all=TRUE, fill=0)
    stocks.liqs.df <- data.frame (test.df[,2:ncol(test.df)], 
                                  row.names=test.df[,1])
    test.df <- merge (stocks.volas.df, cur.volas.df, by=0, all=TRUE, fill=0)
    stocks.volas.df <- data.frame (test.df[,2:ncol(test.df)], 
                                   row.names=test.df[,1])
    test.df <- merge (stocks.betas.df, cur.betas.df, by=0, all=TRUE, fill=0)
    stocks.betas.df <- data.frame (test.df[,2:ncol(test.df)], 
                                   row.names=test.df[,1])
    test.df <- merge (stocks.HFTflags.df, cur.HFTflags.df, by=0, all=TRUE, fill=0)
    stocks.HFTflags.df <- data.frame (test.df[,2:ncol(test.df)], 
                                      row.names=test.df[,1])
    test.df <- merge (stocks.HFTmins.df, cur.HFTmins.df, by=0, all=TRUE, fill=0)
    stocks.HFTmins.df <- data.frame (test.df[,2:ncol(test.df)], 
                                     row.names=test.df[,1])
    test.df <- merge (stocks.HFTpflags.df, cur.HFTpflags.df, by=0, all=TRUE, fill=0)
    stocks.HFTpflags.df <- data.frame (test.df[,2:ncol(test.df)], 
                                       row.names=test.df[,1])
    test.df <- merge (stocks.HFTpquotes.df, cur.HFTpquotes.df, by=0, all=TRUE, fill=0)
    stocks.HFTpquotes.df <- data.frame (test.df[,2:ncol(test.df)], 
                                        row.names=test.df[,1])
  }
  # end of 3c.1x) special: build daily dataframe/symbol matrix
  
  # 3c.2) generic HFT activity in PREP -----------------------------------------
  if (length(id.events.idx.up) > 0) {
    # build vector of date ranges [mm-dd-yyyy/mm-dd-yyyy] for date-based lookup
    events.up.PREP.dateranges <- 
      paste (index(cur.d.x.aligned)[cur.d.x.events.up.idx - period.days], 
             index(cur.d.x.aligned)[cur.d.x.events.up.idx - 1], sep='/' )
    # extract list of vectors from HFT data series
    events.up.PREP.HFT.flags.list <- lapply (events.up.PREP.dateranges, 
                                             function(x) cur.hft.flags.d.x[x])
    events.up.PREP.HFT.mins.list <- lapply (events.up.PREP.dateranges, 
                                            function(x) cur.hft.mins.d.x[x])
    events.up.PREP.HFT.pf.list <- lapply (events.up.PREP.dateranges, 
                                          function(x) cur.hft.part.flags.d.x[x])
    events.up.PREP.HFT.pq.list <- lapply (events.up.PREP.dateranges, 
                                          function(x) cur.hft.part.quotes.d.x[x])
    # calculate HFT rates (/100 achieves percent-scaled variable)
    events.up.df$PREP.HFT.f.rate <- sapply (events.up.PREP.HFT.flags.list, sum) / 
                                      (period.days * daybars * 60 / 100)
    events.up.df$PREP.HFT.m.rate <- sapply (events.up.PREP.HFT.mins.list, sum) / 
                                      (period.days * daybars / 100)
    events.up.df$PREP.HFT.pf.rate <- sapply (events.up.PREP.HFT.pf.list, mean)
    events.up.df$PREP.HFT.pq.rate <- sapply (events.up.PREP.HFT.pq.list, mean)

  }
  if (length(id.events.idx.dn) > 0) {
    # build vector of date ranges [mm-dd-yyyy/mm-dd-yyyy] for date-based lookup
    events.dn.PREP.dateranges <- 
      paste (index(cur.d.x.aligned)[cur.d.x.events.dn.idx - period.days], 
             index(cur.d.x.aligned)[cur.d.x.events.dn.idx - 1], sep='/' )
    # extract list of vectors from HFT data series
    events.dn.PREP.HFT.flags.list <- lapply (events.dn.PREP.dateranges, 
                                             function(x) cur.hft.flags.d.x[x])
    events.dn.PREP.HFT.mins.list <- lapply (events.dn.PREP.dateranges, 
                                            function(x) cur.hft.mins.d.x[x])
    events.dn.PREP.HFT.pf.list <- lapply (events.dn.PREP.dateranges, 
                                          function(x) cur.hft.part.flags.d.x[x])
    events.dn.PREP.HFT.pq.list <- lapply (events.dn.PREP.dateranges, 
                                          function(x) cur.hft.part.quotes.d.x[x])
    # calculate HFT rates (/100 achieves percent-scaled variable)
    events.dn.df$PREP.HFT.f.rate <- sapply (events.dn.PREP.HFT.flags.list, sum) / 
                                      (period.days * daybars * 60 / 100)
    events.dn.df$PREP.HFT.m.rate <- sapply (events.dn.PREP.HFT.mins.list, sum) / 
                                      (period.days * daybars / 100)
    events.dn.df$PREP.HFT.pf.rate <- sapply (events.dn.PREP.HFT.pf.list, mean)
    events.dn.df$PREP.HFT.pq.rate <- sapply (events.dn.PREP.HFT.pq.list, mean)    
  }
  # end of 3c.2) generic HFT activity in PREP
  
  # 3d) minute-measures x(t): r, ar, v, rv, arv, hft, hftpf, hftpq -------------
  # up
  if (length(id.events.idx.up) > 0) {
    for (t in (-period.id.window):(param.post.event.window)) {
      # 3d.1) r_t: returns
      events.up.df[paste("r_", t , sep="")] <- 
        ifelse (as.Date(index(cur.x.aligned[events.up.df$bar+t])) == 
                  as.Date(index(cur.x.aligned[events.up.df$bar])), 
                100 * log (coredata(cur.x.aligned[events.up.df$bar+t]$Close) /
                             coredata(cur.x.aligned[events.up.df$bar]$Close))
                , NA )
    }
    for (t in (-period.id.window):(param.post.event.window)) {
      # 3d.2) ar_t: abnormal returns
      events.up.df[paste("ar_", t , sep="")] <- 
        ifelse (as.Date(index(cur.x.aligned[events.up.df$bar+t])) == 
                  as.Date(index(cur.x.aligned[events.up.df$bar])), 
                100 * ( log (coredata(cur.x.aligned[events.up.df$bar+t]$Close) /
                               coredata(cur.x.aligned[events.up.df$bar]$Close)) 
                        - events.up.df$rbeta * 
                          log (coredata(sp.x.aligned[events.up.df$bar+t]$Close) /
                                 coredata(sp.x.aligned[events.up.df$bar]$Close)) )
                , NA )    
    }
    for (t in (-period.id.window):(param.post.event.window)) {
      # 3d.3) v_t: volume ratio vs. dta volume
      events.up.df[paste("v_", t , sep="")] <- 
        ifelse (as.Date(index(cur.x.aligned[events.up.df$bar+t])) == 
                  as.Date(index(cur.x.aligned[events.up.df$bar])),
                pmin(c(100), coredata(cur.x.aligned[events.up.df$bar+t]$Volume) / 
                       id.volume.dta[events.up.df$bar+t])
                , NA )    
    }
    for (t in (-period.id.window):(param.post.event.window)) {
      # 3d.4) rv_t: realized vola ratio vs. dta vola (at respective daytime)
      events.up.df[paste("rv_", t , sep="")] <- 
        ifelse (as.Date(index(cur.x.aligned[events.up.df$bar+t])) == 
                  as.Date(index(cur.x.aligned[events.up.df$bar-1])), 
                abs(100 * log(coredata(cur.x.aligned[events.up.df$bar+t]$Close) / 
                          coredata(cur.x.aligned[events.up.df$bar+t-1]$Close))) /
                  id.rv.1min.dta[events.up.df$bar+t]
                , NA )    
    }
    for (t in (-period.id.window):(param.post.event.window)) {
      # 3d.5) arv_t: ar realized vola ratio vs. dta vola (at respective daytime)
      events.up.df[paste("arv_", t , sep="")] <- 
        ifelse (as.Date(index(cur.x.aligned[events.up.df$bar+t])) == 
                  as.Date(index(cur.x.aligned[events.up.df$bar-1])), 
                abs (100 * (log (coredata(cur.x.aligned[events.up.df$bar+t]$Close) /
                                 coredata(cur.x.aligned[events.up.df$bar+t-1]$Close)) 
                            - events.up.df$rbeta * 
                            log (coredata(sp.x.aligned[events.up.df$bar+t]$Close) /
                                 coredata(sp.x.aligned[events.up.df$bar+t-1]$Close)) )
                    ) / id.rv.1min.dta[events.up.df$bar+t]
                , NA )    
    }
    for (t in (-period.id.window):(param.post.event.window)) {
      # 3d.6) hft_t: HFT flags / minute (original data set)
      events.up.df[paste("hft_", t , sep="")] <- 
        ifelse (as.Date(index(cur.x.aligned[events.up.df$bar+t])) == 
                  as.Date(index(cur.x.aligned[events.up.df$bar])), 
                coredata(cur.hft.x.aligned[events.up.df$bar+t]$Events)
                , NA )
    }
    for (t in (-period.id.window):(param.post.event.window)) {
      # 3d.7) hftpf_t: HFT flags / volume / minute
      events.up.df[paste("hftpf_", t , sep="")] <- 
        ifelse (as.Date(index(cur.x.aligned[events.up.df$bar+t])) == 
                  as.Date(index(cur.x.aligned[events.up.df$bar])), 
                coredata(cur.hft.x.aligned[events.up.df$bar+t]$Events) / 
                  coredata(cur.x.aligned[events.up.df$bar+t]$Volume)
                , NA )
    }
    for (t in (-period.id.window):(param.post.event.window)) {
      # 3d.8) hftpq_t: HFT quotes / volume / minute
      events.up.df[paste("hftpq_", t , sep="")] <- 
        ifelse (as.Date(index(cur.x.aligned[events.up.df$bar+t])) == 
                  as.Date(index(cur.x.aligned[events.up.df$bar])), 
                coredata(cur.hft.x.aligned[events.up.df$bar+t]$Quotes) / 
                  coredata(cur.x.aligned[events.up.df$bar+t]$Volume)
                , NA )
    }
  }
  # dn
  if (length(id.events.idx.dn) > 0) {
    for (t in (-period.id.window):(param.post.event.window)) {
      # 3d.1) r_t: returns
      events.dn.df[paste("r_", t , sep="")] <- 
        ifelse (as.Date(index(cur.x.aligned[events.dn.df$bar+t])) == 
                  as.Date(index(cur.x.aligned[events.dn.df$bar])), 
                100 * log (coredata(cur.x.aligned[events.dn.df$bar+t]$Close) /
                             coredata(cur.x.aligned[events.dn.df$bar]$Close))
                , NA )
    }
    for (t in (-period.id.window):(param.post.event.window)) {
      # 3d.2) ar_t: abnormal returns
      events.dn.df[paste("ar_", t , sep="")] <- 
        ifelse (as.Date(index(cur.x.aligned[events.dn.df$bar+t])) == 
                  as.Date(index(cur.x.aligned[events.dn.df$bar])), 
                100 * ( log (coredata(cur.x.aligned[events.dn.df$bar+t]$Close) /
                               coredata(cur.x.aligned[events.dn.df$bar]$Close)) 
                        - events.dn.df$rbeta * 
                          log (coredata(sp.x.aligned[events.dn.df$bar+t]$Close) /
                                 coredata(sp.x.aligned[events.dn.df$bar]$Close)) )
                , NA )    
    }
    for (t in (-period.id.window):(param.post.event.window)) {
      # 3d.3) v_t: volume ratio vs. dta volume
      events.dn.df[paste("v_", t , sep="")] <- 
        ifelse (as.Date(index(cur.x.aligned[events.dn.df$bar+t])) == 
                  as.Date(index(cur.x.aligned[events.dn.df$bar])),
                pmin(c(100), coredata(cur.x.aligned[events.dn.df$bar+t]$Volume) / 
                       id.volume.dta[events.dn.df$bar+t])
                , NA )    
    }
    for (t in (-period.id.window):(param.post.event.window)) {
      # 3d.4) rv_t: realized vola ratio vs. dta vola (at respective daytime)
      events.dn.df[paste("rv_", t , sep="")] <- 
        ifelse (as.Date(index(cur.x.aligned[events.dn.df$bar+t])) == 
                  as.Date(index(cur.x.aligned[events.dn.df$bar-1])), 
                abs(100 * log(coredata(cur.x.aligned[events.dn.df$bar+t]$Close) / 
                          coredata(cur.x.aligned[events.dn.df$bar+t-1]$Close))) /
                  id.rv.1min.dta[events.dn.df$bar+t]
                , NA )    
    }
    for (t in (-period.id.window):(param.post.event.window)) {
      # 3d.5) arv_t: ar realized vola ratio vs. dta vola (at respective daytime)
      events.dn.df[paste("arv_", t , sep="")] <- 
        ifelse (as.Date(index(cur.x.aligned[events.dn.df$bar+t])) == 
                  as.Date(index(cur.x.aligned[events.dn.df$bar-1])), 
                abs (100 * (log (coredata(cur.x.aligned[events.dn.df$bar+t]$Close) /
                                 coredata(cur.x.aligned[events.dn.df$bar+t-1]$Close)) 
                            - events.dn.df$rbeta * 
                            log (coredata(sp.x.aligned[events.dn.df$bar+t]$Close) /
                                 coredata(sp.x.aligned[events.dn.df$bar+t-1]$Close)) )
                    ) / id.rv.1min.dta[events.dn.df$bar+t]
                , NA )    
    }
    for (t in (-period.id.window):(param.post.event.window)) {
      # 3d.6) hft_t: HFT flags / minute (original data set)
      events.dn.df[paste("hft_", t , sep="")] <- 
        ifelse (as.Date(index(cur.x.aligned[events.dn.df$bar+t])) == 
                  as.Date(index(cur.x.aligned[events.dn.df$bar])), 
                coredata(cur.hft.x.aligned[events.dn.df$bar+t]$Events)
                , NA )
    }
    for (t in (-period.id.window):(param.post.event.window)) {
      # 3d.7) hftpf_t: HFT flags / volume / minute
      events.dn.df[paste("hftpf_", t , sep="")] <- 
        ifelse (as.Date(index(cur.x.aligned[events.dn.df$bar+t])) == 
                  as.Date(index(cur.x.aligned[events.dn.df$bar])), 
                coredata(cur.hft.x.aligned[events.dn.df$bar+t]$Events) / 
                  coredata(cur.x.aligned[events.dn.df$bar+t]$Volume)
                , NA )
    }
    for (t in (-period.id.window):(param.post.event.window)) {
      # 3d.8) hftpq_t: HFT quotes / volume / minute
      events.dn.df[paste("hftpq_", t , sep="")] <- 
        ifelse (as.Date(index(cur.x.aligned[events.dn.df$bar+t])) == 
                  as.Date(index(cur.x.aligned[events.dn.df$bar])), 
                coredata(cur.hft.x.aligned[events.dn.df$bar+t]$Quotes) / 
                  coredata(cur.x.aligned[events.dn.df$bar+t]$Volume)
                , NA )
    }
  }
  # catch NAs and NaNs for hftpx variables
  # catch cases where both values == 0
  
  
 
  
  
  # end of 3d) minute-measures x(t): r, ar, v, rv, arv, hft, hftpf, hftpq
  
  # 3e.1) add PREW behavior - price --------------------------------------------
  if (length(id.events.idx.up) > 0) {
    # build vector of bar indices [start to end] for bar-based lookup in array
    events.up.PREW.idx <- lapply (events.up.df$bar, 
                                  function(x) (x-period.id.window):(x-1))
    # volume ratio vs. dta
    events.up.df$PREW60.rel.vol <- vGetTseriesAvg (events.up.df, 
      c(1:nrow(events.up.df)), "v", t.start=-period.id.window, t.end=-1)
    events.up.df$PREW30.rel.vol <- vGetTseriesAvg (events.up.df, 
      c(1:nrow(events.up.df)), "v", t.start=-30, t.end=-1)
    events.up.df$PREW15.rel.vol <- vGetTseriesAvg (events.up.df, 
      c(1:nrow(events.up.df)), "v", t.start=-15, t.end=-1)
    events.up.df$PREW10.rel.vol <- vGetTseriesAvg (events.up.df, 
      c(1:nrow(events.up.df)), "v", t.start=-10, t.end=-1)
    events.up.df$PREW05.rel.vol <- vGetTseriesAvg (events.up.df, 
      c(1:nrow(events.up.df)), "v", t.start=-5, t.end=-1)

    # vola ratio vs. dta
    events.up.df$PREW60.rel.vola <- vGetTseriesSqRootAvg (events.up.df, 
      c(1:nrow(events.up.df)), "rv", t.start=-period.id.window, t.end=-1)
    events.up.df$PREW30.rel.vola <- vGetTseriesSqRootAvg (events.up.df, 
      c(1:nrow(events.up.df)), "rv", t.start=-30, t.end=-1)
    events.up.df$PREW15.rel.vola <- vGetTseriesSqRootAvg (events.up.df, 
      c(1:nrow(events.up.df)), "rv", t.start=-15, t.end=-1)
    events.up.df$PREW10.rel.vola <- vGetTseriesSqRootAvg (events.up.df, 
      c(1:nrow(events.up.df)), "rv", t.start=-10, t.end=-1)
    events.up.df$PREW05.rel.vola <- vGetTseriesSqRootAvg (events.up.df, 
      c(1:nrow(events.up.df)), "rv", t.start=-5, t.end=-1)

    # ids vola ratio vs. dta (of regular vola, but that does not matter...)
    events.up.df$PREW60.rel.vola.ids <- vGetTseriesSqRootAvg (events.up.df, 
      c(1:nrow(events.up.df)), "arv", t.start=-period.id.window, t.end=-1)
    events.up.df$PREW30.rel.vola.ids <- vGetTseriesSqRootAvg (events.up.df, 
      c(1:nrow(events.up.df)), "arv", t.start=-30, t.end=-1)
    events.up.df$PREW15.rel.vola.ids <- vGetTseriesSqRootAvg (events.up.df, 
      c(1:nrow(events.up.df)), "arv", t.start=-15, t.end=-1)
    events.up.df$PREW10.rel.vola.ids <- vGetTseriesSqRootAvg (events.up.df, 
      c(1:nrow(events.up.df)), "arv", t.start=-10, t.end=-1)
    events.up.df$PREW05.rel.vola.ids <- vGetTseriesSqRootAvg (events.up.df, 
      c(1:nrow(events.up.df)), "arv", t.start=-5, t.end=-1)

    # event return vs. dta-vola
    events.up.df$PREW60.rel.r <- as.vector ( as.matrix (
      -events.up.df[paste("r_", -period.id.window, sep="")] / 
        sapply (events.up.PREW.idx, function(x) sqrt(period.id.window) *
                mean(id.rv.1min.dta[x[(1):period.id.window]], na.rm=T)) )) 
    events.up.df$PREW30.rel.r <- as.vector ( as.matrix (
      -events.up.df[paste("r_", -30, sep="")] / 
        sapply (events.up.PREW.idx, function(x) sqrt(period.id.window) *
          mean(id.rv.1min.dta[x[(period.id.window+1-30):period.id.window]], na.rm=T)))) 
    events.up.df$PREW15.rel.r <- as.vector ( as.matrix (
      -events.up.df[paste("r_", -15, sep="")] / 
        sapply (events.up.PREW.idx, function(x) sqrt(period.id.window) *
          mean(id.rv.1min.dta[x[(period.id.window+1-15):period.id.window]], na.rm=T)))) 
    events.up.df$PREW10.rel.r <- as.vector ( as.matrix (
      -events.up.df[paste("r_", -10, sep="")] / 
        sapply (events.up.PREW.idx, function(x) sqrt(period.id.window) *
          mean(id.rv.1min.dta[x[(period.id.window+1-10):period.id.window]], na.rm=T)))) 
    events.up.df$PREW05.rel.r <- as.vector ( as.matrix (
      -events.up.df[paste("r_", -5, sep="")] / 
        sapply (events.up.PREW.idx, function(x) sqrt(period.id.window) *
          mean(id.rv.1min.dta[x[(period.id.window+1-5):period.id.window]], na.rm=T))))
    
    # event abnormal return vs. dta-vola
    events.up.df$PREW60.rel.ar <- as.vector ( as.matrix (
      -events.up.df[paste("ar_", -period.id.window, sep="")] / 
        sapply (events.up.PREW.idx, function(x) sqrt(period.id.window) *
                mean(id.rv.1min.dta[x[(1):period.id.window]], na.rm=T)) ))
    events.up.df$PREW30.rel.ar <- as.vector ( as.matrix (
      -events.up.df[paste("ar_", -30, sep="")] / 
        sapply (events.up.PREW.idx, function(x) sqrt(period.id.window) *
          mean(id.rv.1min.dta[x[(period.id.window+1-30):period.id.window]], na.rm=T))))  
    events.up.df$PREW15.rel.ar <- as.vector ( as.matrix (
      -events.up.df[paste("ar_", -15, sep="")] / 
        sapply (events.up.PREW.idx, function(x) sqrt(period.id.window) *
          mean(id.rv.1min.dta[x[(period.id.window+1-15):period.id.window]], na.rm=T)))) 
    events.up.df$PREW10.rel.ar <- as.vector ( as.matrix (
      -events.up.df[paste("ar_", -10, sep="")] / 
        sapply (events.up.PREW.idx, function(x) sqrt(period.id.window) *
          mean(id.rv.1min.dta[x[(period.id.window+1-10):period.id.window]], na.rm=T)))) 
    events.up.df$PREW05.rel.ar <- as.vector ( as.matrix (
      -events.up.df[paste("ar_", -5, sep="")] / 
        sapply (events.up.PREW.idx, function(x) sqrt(period.id.window) *
          mean(id.rv.1min.dta[x[(period.id.window+1-5):period.id.window]], na.rm=T)))) 
  }
  if (length(id.events.idx.dn) > 0) {
    # build vector of bar indices [start to end] for bar-based lookup in array
    events.dn.PREW.idx <- lapply (events.dn.df$bar, 
                                  function(x) (x-period.id.window):(x-1))
    # volume ratio vs. dta
    events.dn.df$PREW60.rel.vol <- vGetTseriesAvg (events.dn.df, 
      c(1:nrow(events.dn.df)), "v", t.start=-period.id.window, t.end=-1)
    events.dn.df$PREW30.rel.vol <- vGetTseriesAvg (events.dn.df, 
      c(1:nrow(events.dn.df)), "v", t.start=-30, t.end=-1)
    events.dn.df$PREW15.rel.vol <- vGetTseriesAvg (events.dn.df, 
      c(1:nrow(events.dn.df)), "v", t.start=-15, t.end=-1)
    events.dn.df$PREW10.rel.vol <- vGetTseriesAvg (events.dn.df, 
      c(1:nrow(events.dn.df)), "v", t.start=-10, t.end=-1)
    events.dn.df$PREW05.rel.vol <- vGetTseriesAvg (events.dn.df, 
      c(1:nrow(events.dn.df)), "v", t.start=-5, t.end=-1)

    # vola ratio vs. dta
    events.dn.df$PREW60.rel.vola <- vGetTseriesSqRootAvg (events.dn.df, 
      c(1:nrow(events.dn.df)), "rv", t.start=-period.id.window, t.end=-1)
    events.dn.df$PREW30.rel.vola <- vGetTseriesSqRootAvg (events.dn.df, 
      c(1:nrow(events.dn.df)), "rv", t.start=-30, t.end=-1)
    events.dn.df$PREW15.rel.vola <- vGetTseriesSqRootAvg (events.dn.df, 
      c(1:nrow(events.dn.df)), "rv", t.start=-15, t.end=-1)
    events.dn.df$PREW10.rel.vola <- vGetTseriesSqRootAvg (events.dn.df, 
      c(1:nrow(events.dn.df)), "rv", t.start=-10, t.end=-1)
    events.dn.df$PREW05.rel.vola <- vGetTseriesSqRootAvg (events.dn.df, 
      c(1:nrow(events.dn.df)), "rv", t.start=-5, t.end=-1)

    # ids vola ratio vs. dta (of regular vola, but that does not matter...)
    events.dn.df$PREW60.rel.vola.ids <- vGetTseriesSqRootAvg (events.dn.df, 
      c(1:nrow(events.dn.df)), "arv", t.start=-period.id.window, t.end=-1)
    events.dn.df$PREW30.rel.vola.ids <- vGetTseriesSqRootAvg (events.dn.df, 
      c(1:nrow(events.dn.df)), "arv", t.start=-30, t.end=-1)
    events.dn.df$PREW15.rel.vola.ids <- vGetTseriesSqRootAvg (events.dn.df, 
      c(1:nrow(events.dn.df)), "arv", t.start=-15, t.end=-1)
    events.dn.df$PREW10.rel.vola.ids <- vGetTseriesSqRootAvg (events.dn.df, 
      c(1:nrow(events.dn.df)), "arv", t.start=-10, t.end=-1)
    events.dn.df$PREW05.rel.vola.ids <- vGetTseriesSqRootAvg (events.dn.df, 
      c(1:nrow(events.dn.df)), "arv", t.start=-5, t.end=-1)

    # event return vs. dta-vola
    events.dn.df$PREW60.rel.r <- as.vector ( as.matrix (
      -events.dn.df[paste("r_", -period.id.window, sep="")] / 
        sapply (events.dn.PREW.idx, function(x) sqrt(period.id.window) *
                mean(id.rv.1min.dta[x[(1):period.id.window]], na.rm=T)) )) 
    events.dn.df$PREW30.rel.r <- as.vector ( as.matrix (
      -events.dn.df[paste("r_", -30, sep="")] / 
        sapply (events.dn.PREW.idx, function(x) sqrt(period.id.window) *
          mean(id.rv.1min.dta[x[(period.id.window+1-30):period.id.window]], na.rm=T)))) 
    events.dn.df$PREW15.rel.r <- as.vector ( as.matrix (
      -events.dn.df[paste("r_", -15, sep="")] / 
        sapply (events.dn.PREW.idx, function(x) sqrt(period.id.window) *
          mean(id.rv.1min.dta[x[(period.id.window+1-15):period.id.window]], na.rm=T)))) 
    events.dn.df$PREW10.rel.r <- as.vector ( as.matrix (
      -events.dn.df[paste("r_", -10, sep="")] / 
        sapply (events.dn.PREW.idx, function(x) sqrt(period.id.window) *
          mean(id.rv.1min.dta[x[(period.id.window+1-10):period.id.window]], na.rm=T)))) 
    events.dn.df$PREW05.rel.r <- as.vector ( as.matrix (
      -events.dn.df[paste("r_", -5, sep="")] / 
        sapply (events.dn.PREW.idx, function(x) sqrt(period.id.window) *
          mean(id.rv.1min.dta[x[(period.id.window+1-5):period.id.window]], na.rm=T))))
    
    # event abnormal return vs. dta-vola
    events.dn.df$PREW60.rel.ar <- as.vector ( as.matrix (
      -events.dn.df[paste("ar_", -period.id.window, sep="")] / 
        sapply (events.dn.PREW.idx, function(x) sqrt(period.id.window) *
                mean(id.rv.1min.dta[x[(1):period.id.window]], na.rm=T)) ))
    events.dn.df$PREW30.rel.ar <- as.vector ( as.matrix (
      -events.dn.df[paste("ar_", -30, sep="")] / 
        sapply (events.dn.PREW.idx, function(x) sqrt(period.id.window) *
          mean(id.rv.1min.dta[x[(period.id.window+1-30):period.id.window]], na.rm=T))))  
    events.dn.df$PREW15.rel.ar <- as.vector ( as.matrix (
      -events.dn.df[paste("ar_", -15, sep="")] / 
        sapply (events.dn.PREW.idx, function(x) sqrt(period.id.window) *
          mean(id.rv.1min.dta[x[(period.id.window+1-15):period.id.window]], na.rm=T)))) 
    events.dn.df$PREW10.rel.ar <- as.vector ( as.matrix (
      -events.dn.df[paste("ar_", -10, sep="")] / 
        sapply (events.dn.PREW.idx, function(x) sqrt(period.id.window) *
          mean(id.rv.1min.dta[x[(period.id.window+1-10):period.id.window]], na.rm=T)))) 
    events.dn.df$PREW05.rel.ar <- as.vector ( as.matrix (
      -events.dn.df[paste("ar_", -5, sep="")] / 
        sapply (events.dn.PREW.idx, function(x) sqrt(period.id.window) *
          mean(id.rv.1min.dta[x[(period.id.window+1-5):period.id.window]], na.rm=T)))) 
  }
  # end of 3e.1) add PREW behavior - price
  
  # 3e.2) add PREW behavior - HFT ----------------------------------------------
  # HFT activity abs = means of HFT activity in PREPxx
  if (length(id.events.idx.up) > 0) {
    # HFT activity abs - PREW60
# old code backup
#     events.up.df$PREW60.HFT.f.rate <- 100 * sapply (1:length(events.up.PREW.idx), 
#       function(x) mean(cur.hft.x.aligned$Events[events.up.PREW.idx[[x]]
#                                                 [1:period.id.window]]))/60
#     events.up.df$PREW60.HFT.m.rate <- 100 * sapply (1:length(events.up.PREW.idx), 
#       function(x) mean(cur.hft.x.aligned$Events[events.up.PREW.idx[[x]]
#                                                 [1:period.id.window]]>0))
    events.up.df$PREW60.HFT.f.rate <- 100 * vGetTseriesAvg (events.up.df, 
      c(1:nrow(events.up.df)), "hft", t.start=-period.id.window, t.end=-1) / 60    
    events.up.df$PREW60.HFT.m.rate <- 100 * vGetTseriesAvg.01 (events.up.df, 
      c(1:nrow(events.up.df)), "hft", t.start=-period.id.window, t.end=-1)
    # HFT activity abs - PREW30
    events.up.df$PREW30.HFT.f.rate <- 100 * vGetTseriesAvg (events.up.df, 
      c(1:nrow(events.up.df)), "hft", t.start=-30, t.end=-1) / 60    
    events.up.df$PREW30.HFT.m.rate <- 100 * vGetTseriesAvg.01 (events.up.df, 
      c(1:nrow(events.up.df)), "hft", t.start=-30, t.end=-1)
    # HFT activity abs - PREW15
    events.up.df$PREW15.HFT.f.rate <- 100 * vGetTseriesAvg (events.up.df, 
      c(1:nrow(events.up.df)), "hft", t.start=-15, t.end=-1) / 60    
    events.up.df$PREW15.HFT.m.rate <- 100 * vGetTseriesAvg.01 (events.up.df, 
      c(1:nrow(events.up.df)), "hft", t.start=-15, t.end=-1)
    # HFT activity abs - PREW10
    events.up.df$PREW10.HFT.f.rate <- 100 * vGetTseriesAvg (events.up.df, 
      c(1:nrow(events.up.df)), "hft", t.start=-10, t.end=-1) / 60    
    events.up.df$PREW10.HFT.m.rate <- 100 * vGetTseriesAvg.01 (events.up.df, 
      c(1:nrow(events.up.df)), "hft", t.start=-10, t.end=-1)
    # HFT activity abs - PREW05
    events.up.df$PREW05.HFT.f.rate <- 100 * vGetTseriesAvg (events.up.df, 
      c(1:nrow(events.up.df)), "hft", t.start=-5, t.end=-1) / 60    
    events.up.df$PREW05.HFT.m.rate <- 100 * vGetTseriesAvg.01 (events.up.df, 
      c(1:nrow(events.up.df)), "hft", t.start=-5, t.end=-1)
    
    # HFT rates vs. volume - PREW60
    events.up.df$PREW60.HFT.pf.rate <- vGetTseriesAvg (events.up.df, 
      c(1:nrow(events.up.df)), "hftpf", t.start=-period.id.window, t.end=-1) 
    events.up.df$PREW60.HFT.pq.rate <- vGetTseriesAvg (events.up.df, 
      c(1:nrow(events.up.df)), "hftpq", t.start=-period.id.window, t.end=-1)
    # HFT rates vs. volume - PREW30
    events.up.df$PREW30.HFT.pf.rate <- vGetTseriesAvg (events.up.df, 
      c(1:nrow(events.up.df)), "hftpf", t.start=-30, t.end=-1)
    events.up.df$PREW30.HFT.pq.rate <- vGetTseriesAvg (events.up.df, 
      c(1:nrow(events.up.df)), "hftpq", t.start=-30, t.end=-1)
    # HFT rates vs. volume - PREW15
    events.up.df$PREW15.HFT.pf.rate <- vGetTseriesAvg (events.up.df, 
      c(1:nrow(events.up.df)), "hftpf", t.start=-15, t.end=-1)
    events.up.df$PREW15.HFT.pq.rate <- vGetTseriesAvg (events.up.df, 
      c(1:nrow(events.up.df)), "hftpq", t.start=-15, t.end=-1)
    # HFT rates vs. volume - PREW10
    events.up.df$PREW10.HFT.pf.rate <- vGetTseriesAvg (events.up.df, 
      c(1:nrow(events.up.df)), "hftpf", t.start=-10, t.end=-1)
    events.up.df$PREW10.HFT.pq.rate <- vGetTseriesAvg (events.up.df, 
      c(1:nrow(events.up.df)), "hftpq", t.start=-10, t.end=-1)
    # HFT rates vs. volume - PREW05
    events.up.df$PREW05.HFT.pf.rate <- vGetTseriesAvg (events.up.df, 
      c(1:nrow(events.up.df)), "hftpf", t.start=-5, t.end=-1)
    events.up.df$PREW05.HFT.pq.rate <- vGetTseriesAvg (events.up.df, 
      c(1:nrow(events.up.df)), "hftpq", t.start=-5, t.end=-1)

    # HFT activity rel vs. dta
    # divide abs. rates by: PREP HFT * mean(dta profile[PREWxx])
    # HFT activity rel - PREW60
#     events.up.df$PREW60.HFT.rel.f.rate <- na.fill(events.up.df$PREW60.HFT.f.rate / 
#       (events.up.df$PREP.HFT.f.rate * sapply( 1:nrow(events.up.df), function(x) 
#         mean(cur.hft.flags.dta[substr(index(cur.x.aligned)[events.up.PREW.idx[[x]]
#           [(1):period.id.window]], 12, 19)] ) /
#           mean(cur.hft.flags.dta) )),0)
#     events.up.df$PREW60.HFT.rel.m.rate <- na.fill(events.up.df$PREW60.HFT.m.rate / 
#       (events.up.df$PREP.HFT.m.rate * sapply( 1:nrow(events.up.df), function(x) 
#         mean(cur.hft.mins.dta[substr(index(cur.x.aligned)[events.up.PREW.idx[[x]]
#           [(1):period.id.window]], 12, 19)] ) /
#           mean(cur.hft.mins.dta) )),0)
    events.up.df$PREW60.HFT.rel.f.rate <- na.fill(events.up.df$PREW60.HFT.f.rate / 
      (events.up.df$PREP.HFT.f.rate * sapply( 1:nrow(events.up.df), function(x) 
      mean(cur.hft.flags.dta[DayBoundedBarSeq(events.up.df$timestamp[x], 
        t.start=-60, t.end=-1)])) / mean(cur.hft.flags.dta) ), 0)
    events.up.df$PREW60.HFT.rel.m.rate <- na.fill(events.up.df$PREW60.HFT.m.rate / 
      (events.up.df$PREP.HFT.m.rate * sapply( 1:nrow(events.up.df), function(x) 
      mean(cur.hft.mins.dta[DayBoundedBarSeq(events.up.df$timestamp[x], 
        t.start=-60, t.end=-1)])) / mean(cur.hft.mins.dta) ), 0)
    # HFT activity rel - PREW30
    events.up.df$PREW30.HFT.rel.f.rate <- na.fill(events.up.df$PREW30.HFT.f.rate / 
      (events.up.df$PREP.HFT.f.rate * sapply( 1:nrow(events.up.df), function(x) 
      mean(cur.hft.flags.dta[DayBoundedBarSeq(events.up.df$timestamp[x], 
        t.start=-30, t.end=-1)])) / mean(cur.hft.flags.dta) ), 0)
    events.up.df$PREW30.HFT.rel.m.rate <- na.fill(events.up.df$PREW30.HFT.m.rate / 
      (events.up.df$PREP.HFT.m.rate * sapply( 1:nrow(events.up.df), function(x) 
      mean(cur.hft.mins.dta[DayBoundedBarSeq(events.up.df$timestamp[x], 
        t.start=-30, t.end=-1)])) / mean(cur.hft.mins.dta) ), 0)
    # HFT activity rel - PREW15
    events.up.df$PREW15.HFT.rel.f.rate <- na.fill(events.up.df$PREW15.HFT.f.rate / 
      (events.up.df$PREP.HFT.f.rate * sapply( 1:nrow(events.up.df), function(x) 
      mean(cur.hft.flags.dta[DayBoundedBarSeq(events.up.df$timestamp[x], 
        t.start=-15, t.end=-1)])) / mean(cur.hft.flags.dta) ), 0)
    events.up.df$PREW15.HFT.rel.m.rate <- na.fill(events.up.df$PREW15.HFT.m.rate / 
      (events.up.df$PREP.HFT.m.rate * sapply( 1:nrow(events.up.df), function(x) 
      mean(cur.hft.mins.dta[DayBoundedBarSeq(events.up.df$timestamp[x], 
        t.start=-15, t.end=-1)])) / mean(cur.hft.mins.dta) ), 0)
    # HFT activity rel - PREW10
    events.up.df$PREW10.HFT.rel.f.rate <- na.fill(events.up.df$PREW10.HFT.f.rate / 
      (events.up.df$PREP.HFT.f.rate * sapply( 1:nrow(events.up.df), function(x) 
      mean(cur.hft.flags.dta[DayBoundedBarSeq(events.up.df$timestamp[x], 
        t.start=-10, t.end=-1)])) / mean(cur.hft.flags.dta) ), 0)
    events.up.df$PREW10.HFT.rel.m.rate <- na.fill(events.up.df$PREW10.HFT.m.rate / 
      (events.up.df$PREP.HFT.m.rate * sapply( 1:nrow(events.up.df), function(x) 
      mean(cur.hft.mins.dta[DayBoundedBarSeq(events.up.df$timestamp[x], 
        t.start=-10, t.end=-1)])) / mean(cur.hft.mins.dta) ), 0)
    # HFT activity rel - PREW05
    events.up.df$PREW05.HFT.rel.f.rate <- na.fill(events.up.df$PREW05.HFT.f.rate / 
      (events.up.df$PREP.HFT.f.rate * sapply( 1:nrow(events.up.df), function(x) 
      mean(cur.hft.flags.dta[DayBoundedBarSeq(events.up.df$timestamp[x], 
        t.start=-5, t.end=-1)])) / mean(cur.hft.flags.dta) ), 0)
    events.up.df$PREW05.HFT.rel.m.rate <- na.fill(events.up.df$PREW05.HFT.m.rate / 
      (events.up.df$PREP.HFT.m.rate * sapply( 1:nrow(events.up.df), function(x) 
      mean(cur.hft.mins.dta[DayBoundedBarSeq(events.up.df$timestamp[x], 
        t.start=-5, t.end=-1)])) / mean(cur.hft.mins.dta) ), 0)
    
    # HFT rates vs. volume rel - PREW60    
    events.up.df$PREW60.HFT.rel.pf.rate <- na.fill(events.up.df$PREW60.HFT.pf.rate / 
      (events.up.df$PREP.HFT.pf.rate * sapply( 1:nrow(events.up.df), function(x) 
      mean(cur.hft.pf.dta[DayBoundedBarSeq(events.up.df$timestamp[x], 
        t.start=-60, t.end=-1)])) / mean(cur.hft.pf.dta) ), 0)
    events.up.df$PREW60.HFT.rel.pq.rate <- na.fill(events.up.df$PREW60.HFT.pq.rate / 
      (events.up.df$PREP.HFT.pq.rate * sapply( 1:nrow(events.up.df), function(x) 
      mean(cur.hft.pq.dta[DayBoundedBarSeq(events.up.df$timestamp[x], 
        t.start=-60, t.end=-1)])) / mean(cur.hft.pq.dta) ), 0)
    # HFT rates vs. volume rel - PREW30
    events.up.df$PREW30.HFT.rel.pf.rate <- na.fill(events.up.df$PREW30.HFT.pf.rate / 
      (events.up.df$PREP.HFT.pf.rate * sapply( 1:nrow(events.up.df), function(x) 
      mean(cur.hft.pf.dta[DayBoundedBarSeq(events.up.df$timestamp[x], 
        t.start=-30, t.end=-1)])) / mean(cur.hft.pf.dta) ), 0)
    events.up.df$PREW30.HFT.rel.pq.rate <- na.fill(events.up.df$PREW30.HFT.pq.rate / 
      (events.up.df$PREP.HFT.pq.rate * sapply( 1:nrow(events.up.df), function(x) 
      mean(cur.hft.pq.dta[DayBoundedBarSeq(events.up.df$timestamp[x], 
        t.start=-30, t.end=-1)])) / mean(cur.hft.pq.dta) ), 0)
    # HFT rates vs. volume rel - PREW15
    events.up.df$PREW15.HFT.rel.pf.rate <- na.fill(events.up.df$PREW15.HFT.pf.rate / 
      (events.up.df$PREP.HFT.pf.rate * sapply( 1:nrow(events.up.df), function(x) 
      mean(cur.hft.pf.dta[DayBoundedBarSeq(events.up.df$timestamp[x], 
        t.start=-15, t.end=-1)])) / mean(cur.hft.pf.dta) ), 0)
    events.up.df$PREW15.HFT.rel.pq.rate <- na.fill(events.up.df$PREW15.HFT.pq.rate / 
      (events.up.df$PREP.HFT.pq.rate * sapply( 1:nrow(events.up.df), function(x) 
      mean(cur.hft.pq.dta[DayBoundedBarSeq(events.up.df$timestamp[x], 
        t.start=-15, t.end=-1)])) / mean(cur.hft.pq.dta) ), 0)
    # HFT rates vs. volume rel - PREW10
    events.up.df$PREW10.HFT.rel.pf.rate <- na.fill(events.up.df$PREW10.HFT.pf.rate / 
      (events.up.df$PREP.HFT.pf.rate * sapply( 1:nrow(events.up.df), function(x) 
      mean(cur.hft.pf.dta[DayBoundedBarSeq(events.up.df$timestamp[x], 
        t.start=-10, t.end=-1)])) / mean(cur.hft.pf.dta) ), 0)
    events.up.df$PREW10.HFT.rel.pq.rate <- na.fill(events.up.df$PREW10.HFT.pq.rate / 
      (events.up.df$PREP.HFT.pq.rate * sapply( 1:nrow(events.up.df), function(x) 
      mean(cur.hft.pq.dta[DayBoundedBarSeq(events.up.df$timestamp[x], 
        t.start=-10, t.end=-1)])) / mean(cur.hft.pq.dta) ), 0)
    # HFT rates vs. volume rel - PREW05
    events.up.df$PREW05.HFT.rel.pf.rate <- na.fill(events.up.df$PREW05.HFT.pf.rate / 
      (events.up.df$PREP.HFT.pf.rate * sapply( 1:nrow(events.up.df), function(x) 
      mean(cur.hft.pf.dta[DayBoundedBarSeq(events.up.df$timestamp[x], 
        t.start=-5, t.end=-1)])) / mean(cur.hft.pf.dta) ), 0)
    events.up.df$PREW05.HFT.rel.pq.rate <- na.fill(events.up.df$PREW05.HFT.pq.rate / 
      (events.up.df$PREP.HFT.pq.rate * sapply( 1:nrow(events.up.df), function(x) 
      mean(cur.hft.pq.dta[DayBoundedBarSeq(events.up.df$timestamp[x], 
        t.start=-5, t.end=-1)])) / mean(cur.hft.pq.dta) ), 0)
  }
  if (length(id.events.idx.dn) > 0) {
    events.dn.df$PREW60.HFT.f.rate <- 100 * vGetTseriesAvg (events.dn.df, 
      c(1:nrow(events.dn.df)), "hft", t.start=-period.id.window, t.end=-1) / 60    
    events.dn.df$PREW60.HFT.m.rate <- 100 * vGetTseriesAvg.01 (events.dn.df, 
      c(1:nrow(events.dn.df)), "hft", t.start=-period.id.window, t.end=-1)
    # HFT activity abs - PREW30
    events.dn.df$PREW30.HFT.f.rate <- 100 * vGetTseriesAvg (events.dn.df, 
      c(1:nrow(events.dn.df)), "hft", t.start=-30, t.end=-1) / 60    
    events.dn.df$PREW30.HFT.m.rate <- 100 * vGetTseriesAvg.01 (events.dn.df, 
      c(1:nrow(events.dn.df)), "hft", t.start=-30, t.end=-1)
    # HFT activity abs - PREW15
    events.dn.df$PREW15.HFT.f.rate <- 100 * vGetTseriesAvg (events.dn.df, 
      c(1:nrow(events.dn.df)), "hft", t.start=-15, t.end=-1) / 60    
    events.dn.df$PREW15.HFT.m.rate <- 100 * vGetTseriesAvg.01 (events.dn.df, 
      c(1:nrow(events.dn.df)), "hft", t.start=-15, t.end=-1)
    # HFT activity abs - PREW10
    events.dn.df$PREW10.HFT.f.rate <- 100 * vGetTseriesAvg (events.dn.df, 
      c(1:nrow(events.dn.df)), "hft", t.start=-10, t.end=-1) / 60    
    events.dn.df$PREW10.HFT.m.rate <- 100 * vGetTseriesAvg.01 (events.dn.df, 
      c(1:nrow(events.dn.df)), "hft", t.start=-10, t.end=-1)
    # HFT activity abs - PREW05
    events.dn.df$PREW05.HFT.f.rate <- 100 * vGetTseriesAvg (events.dn.df, 
      c(1:nrow(events.dn.df)), "hft", t.start=-5, t.end=-1) / 60    
    events.dn.df$PREW05.HFT.m.rate <- 100 * vGetTseriesAvg.01 (events.dn.df, 
      c(1:nrow(events.dn.df)), "hft", t.start=-5, t.end=-1)
      
    # HFT rates vs. volume - PREW60
    events.dn.df$PREW60.HFT.pf.rate <- vGetTseriesAvg (events.dn.df, 
      c(1:nrow(events.dn.df)), "hftpf", t.start=-period.id.window, t.end=-1) 
    events.dn.df$PREW60.HFT.pq.rate <- vGetTseriesAvg (events.dn.df, 
      c(1:nrow(events.dn.df)), "hftpq", t.start=-period.id.window, t.end=-1)
    # HFT rates vs. volume - PREW30
    events.dn.df$PREW30.HFT.pf.rate <- vGetTseriesAvg (events.dn.df, 
      c(1:nrow(events.dn.df)), "hftpf", t.start=-30, t.end=-1)
    events.dn.df$PREW30.HFT.pq.rate <- vGetTseriesAvg (events.dn.df, 
      c(1:nrow(events.dn.df)), "hftpq", t.start=-30, t.end=-1)
    # HFT rates vs. volume - PREW15
    events.dn.df$PREW15.HFT.pf.rate <- vGetTseriesAvg (events.dn.df, 
      c(1:nrow(events.dn.df)), "hftpf", t.start=-15, t.end=-1)
    events.dn.df$PREW15.HFT.pq.rate <- vGetTseriesAvg (events.dn.df, 
      c(1:nrow(events.dn.df)), "hftpq", t.start=-15, t.end=-1)
    # HFT rates vs. volume - PREW10
    events.dn.df$PREW10.HFT.pf.rate <- vGetTseriesAvg (events.dn.df, 
      c(1:nrow(events.dn.df)), "hftpf", t.start=-10, t.end=-1)
    events.dn.df$PREW10.HFT.pq.rate <- vGetTseriesAvg (events.dn.df, 
      c(1:nrow(events.dn.df)), "hftpq", t.start=-10, t.end=-1)
    # HFT rates vs. volume - PREW05
    events.dn.df$PREW05.HFT.pf.rate <- vGetTseriesAvg (events.dn.df, 
      c(1:nrow(events.dn.df)), "hftpf", t.start=-5, t.end=-1)
    events.dn.df$PREW05.HFT.pq.rate <- vGetTseriesAvg (events.dn.df, 
      c(1:nrow(events.dn.df)), "hftpq", t.start=-5, t.end=-1)      
      
    # HFT activity rel vs. dta
    # divide abs. rates by: PREP HFT * mean(dta profile[PREWxx])
    # HFT activity rel - PREW60
    events.dn.df$PREW60.HFT.rel.f.rate <- na.fill(events.dn.df$PREW60.HFT.f.rate / 
      (events.dn.df$PREP.HFT.f.rate * sapply( 1:nrow(events.dn.df), function(x) 
      mean(cur.hft.flags.dta[DayBoundedBarSeq(events.dn.df$timestamp[x], 
        t.start=-60, t.end=-1)])) / mean(cur.hft.flags.dta) ), 0)
    events.dn.df$PREW60.HFT.rel.m.rate <- na.fill(events.dn.df$PREW60.HFT.m.rate / 
      (events.dn.df$PREP.HFT.m.rate * sapply( 1:nrow(events.dn.df), function(x) 
      mean(cur.hft.mins.dta[DayBoundedBarSeq(events.dn.df$timestamp[x], 
        t.start=-60, t.end=-1)])) / mean(cur.hft.mins.dta) ), 0)
    # HFT activity rel - PREW30
    events.dn.df$PREW30.HFT.rel.f.rate <- na.fill(events.dn.df$PREW30.HFT.f.rate / 
      (events.dn.df$PREP.HFT.f.rate * sapply( 1:nrow(events.dn.df), function(x) 
      mean(cur.hft.flags.dta[DayBoundedBarSeq(events.dn.df$timestamp[x], 
        t.start=-30, t.end=-1)])) / mean(cur.hft.flags.dta) ), 0)
    events.dn.df$PREW30.HFT.rel.m.rate <- na.fill(events.dn.df$PREW30.HFT.m.rate / 
      (events.dn.df$PREP.HFT.m.rate * sapply( 1:nrow(events.dn.df), function(x) 
      mean(cur.hft.mins.dta[DayBoundedBarSeq(events.dn.df$timestamp[x], 
        t.start=-30, t.end=-1)])) / mean(cur.hft.mins.dta) ), 0)
    # HFT activity rel - PREW15
    events.dn.df$PREW15.HFT.rel.f.rate <- na.fill(events.dn.df$PREW15.HFT.f.rate / 
      (events.dn.df$PREP.HFT.f.rate * sapply( 1:nrow(events.dn.df), function(x) 
      mean(cur.hft.flags.dta[DayBoundedBarSeq(events.dn.df$timestamp[x], 
        t.start=-15, t.end=-1)])) / mean(cur.hft.flags.dta) ), 0)
    events.dn.df$PREW15.HFT.rel.m.rate <- na.fill(events.dn.df$PREW15.HFT.m.rate / 
      (events.dn.df$PREP.HFT.m.rate * sapply( 1:nrow(events.dn.df), function(x) 
      mean(cur.hft.mins.dta[DayBoundedBarSeq(events.dn.df$timestamp[x], 
        t.start=-15, t.end=-1)])) / mean(cur.hft.mins.dta) ), 0)
    # HFT activity rel - PREW10
    events.dn.df$PREW10.HFT.rel.f.rate <- na.fill(events.dn.df$PREW10.HFT.f.rate / 
      (events.dn.df$PREP.HFT.f.rate * sapply( 1:nrow(events.dn.df), function(x) 
      mean(cur.hft.flags.dta[DayBoundedBarSeq(events.dn.df$timestamp[x], 
        t.start=-10, t.end=-1)])) / mean(cur.hft.flags.dta) ), 0)
    events.dn.df$PREW10.HFT.rel.m.rate <- na.fill(events.dn.df$PREW10.HFT.m.rate / 
      (events.dn.df$PREP.HFT.m.rate * sapply( 1:nrow(events.dn.df), function(x) 
      mean(cur.hft.mins.dta[DayBoundedBarSeq(events.dn.df$timestamp[x], 
        t.start=-10, t.end=-1)])) / mean(cur.hft.mins.dta) ), 0)
    # HFT activity rel - PREW05
    events.dn.df$PREW05.HFT.rel.f.rate <- na.fill(events.dn.df$PREW05.HFT.f.rate / 
      (events.dn.df$PREP.HFT.f.rate * sapply( 1:nrow(events.dn.df), function(x) 
      mean(cur.hft.flags.dta[DayBoundedBarSeq(events.dn.df$timestamp[x], 
        t.start=-5, t.end=-1)])) / mean(cur.hft.flags.dta) ), 0)
    events.dn.df$PREW05.HFT.rel.m.rate <- na.fill(events.dn.df$PREW05.HFT.m.rate / 
      (events.dn.df$PREP.HFT.m.rate * sapply( 1:nrow(events.dn.df), function(x) 
      mean(cur.hft.mins.dta[DayBoundedBarSeq(events.dn.df$timestamp[x], 
        t.start=-5, t.end=-1)])) / mean(cur.hft.mins.dta) ), 0)
        
    # HFT rates vs. volume rel - PREW60    
    events.dn.df$PREW60.HFT.rel.pf.rate <- na.fill(events.dn.df$PREW60.HFT.pf.rate / 
      (events.dn.df$PREP.HFT.pf.rate * sapply( 1:nrow(events.dn.df), function(x) 
      mean(cur.hft.pf.dta[DayBoundedBarSeq(events.dn.df$timestamp[x], 
        t.start=-60, t.end=-1)])) / mean(cur.hft.pf.dta) ), 0)
    events.dn.df$PREW60.HFT.rel.pq.rate <- na.fill(events.dn.df$PREW60.HFT.pq.rate / 
      (events.dn.df$PREP.HFT.pq.rate * sapply( 1:nrow(events.dn.df), function(x) 
      mean(cur.hft.pq.dta[DayBoundedBarSeq(events.dn.df$timestamp[x], 
        t.start=-60, t.end=-1)])) / mean(cur.hft.pq.dta) ), 0)
    # HFT rates vs. volume rel - PREW30
    events.dn.df$PREW30.HFT.rel.pf.rate <- na.fill(events.dn.df$PREW30.HFT.pf.rate / 
      (events.dn.df$PREP.HFT.pf.rate * sapply( 1:nrow(events.dn.df), function(x) 
      mean(cur.hft.pf.dta[DayBoundedBarSeq(events.dn.df$timestamp[x], 
        t.start=-30, t.end=-1)])) / mean(cur.hft.pf.dta) ), 0)
    events.dn.df$PREW30.HFT.rel.pq.rate <- na.fill(events.dn.df$PREW30.HFT.pq.rate / 
      (events.dn.df$PREP.HFT.pq.rate * sapply( 1:nrow(events.dn.df), function(x) 
      mean(cur.hft.pq.dta[DayBoundedBarSeq(events.dn.df$timestamp[x], 
        t.start=-30, t.end=-1)])) / mean(cur.hft.pq.dta) ), 0)
    # HFT rates vs. volume rel - PREW15
    events.dn.df$PREW15.HFT.rel.pf.rate <- na.fill(events.dn.df$PREW15.HFT.pf.rate / 
      (events.dn.df$PREP.HFT.pf.rate * sapply( 1:nrow(events.dn.df), function(x) 
      mean(cur.hft.pf.dta[DayBoundedBarSeq(events.dn.df$timestamp[x], 
        t.start=-15, t.end=-1)])) / mean(cur.hft.pf.dta) ), 0)
    events.dn.df$PREW15.HFT.rel.pq.rate <- na.fill(events.dn.df$PREW15.HFT.pq.rate / 
      (events.dn.df$PREP.HFT.pq.rate * sapply( 1:nrow(events.dn.df), function(x) 
      mean(cur.hft.pq.dta[DayBoundedBarSeq(events.dn.df$timestamp[x], 
        t.start=-15, t.end=-1)])) / mean(cur.hft.pq.dta) ), 0)
    # HFT rates vs. volume rel - PREW10
    events.dn.df$PREW10.HFT.rel.pf.rate <- na.fill(events.dn.df$PREW10.HFT.pf.rate / 
      (events.dn.df$PREP.HFT.pf.rate * sapply( 1:nrow(events.dn.df), function(x) 
      mean(cur.hft.pf.dta[DayBoundedBarSeq(events.dn.df$timestamp[x], 
        t.start=-10, t.end=-1)])) / mean(cur.hft.pf.dta) ), 0)
    events.dn.df$PREW10.HFT.rel.pq.rate <- na.fill(events.dn.df$PREW10.HFT.pq.rate / 
      (events.dn.df$PREP.HFT.pq.rate * sapply( 1:nrow(events.dn.df), function(x) 
      mean(cur.hft.pq.dta[DayBoundedBarSeq(events.dn.df$timestamp[x], 
        t.start=-10, t.end=-1)])) / mean(cur.hft.pq.dta) ), 0)
    # HFT rates vs. volume rel - PREW05
    events.dn.df$PREW05.HFT.rel.pf.rate <- na.fill(events.dn.df$PREW05.HFT.pf.rate / 
      (events.dn.df$PREP.HFT.pf.rate * sapply( 1:nrow(events.dn.df), function(x) 
      mean(cur.hft.pf.dta[DayBoundedBarSeq(events.dn.df$timestamp[x], 
        t.start=-5, t.end=-1)])) / mean(cur.hft.pf.dta) ), 0)
    events.dn.df$PREW05.HFT.rel.pq.rate <- na.fill(events.dn.df$PREW05.HFT.pq.rate / 
      (events.dn.df$PREP.HFT.pq.rate * sapply( 1:nrow(events.dn.df), function(x) 
      mean(cur.hft.pq.dta[DayBoundedBarSeq(events.dn.df$timestamp[x], 
        t.start=-5, t.end=-1)])) / mean(cur.hft.pq.dta) ), 0)        
  }
  # end of 3e.2) add PREW behavior - HFT

  # 3f) price-pattern: POEW (ECP, ERP, TUP) ------------------------------------
  #   * max.cont  : max move continuation in % after event trigger
  #   * max.cont_t: time of continuation vs. event trigger time
  #   * reversal  : max move reversal in % after max.cont (always >= 0!)
  #   * reversal_t: time of reversal maximum extent
  if (length(id.events.idx.up) > 0) {
    events.up.df$ECP.max.cont <- 
      vGetTseriesMax (events.up.df, c(1:nrow(events.up.df)), "r")
    events.up.df$ECP.max.cont_t <-
      vGetTseriesMax_t (events.up.df, c(1:nrow(events.up.df)), "r")-1
    events.up.df$ERP.reversal <- vGetTseriesReversalFromHigh (
      events.up.df, c(1:nrow(events.up.df)), varname="r", getT=FALSE)
    events.up.df$ERP.reversal_t <- vGetTseriesReversalFromHigh (
      events.up.df, c(1:nrow(events.up.df)), varname="r", getT=TRUE)
      
    # simplifing variables: t.TUP (turning point) & T.ERP (end of reversal)
    events.up.t.TUP <- events.up.df$ECP.max.cont_t
    events.up.t.ERP <- events.up.df$ECP.max.cont_t + events.up.df$ERP.reversal_t
  }
  if (length(id.events.idx.dn) > 0) {
    events.dn.df$ECP.max.cont <- 
      vGetTseriesMin (events.dn.df, c(1:nrow(events.dn.df)), "r")
    events.dn.df$ECP.max.cont_t <-
      vGetTseriesMin_t (events.dn.df, c(1:nrow(events.dn.df)), "r")-1
    events.dn.df$ERP.reversal <- vGetTseriesReversalFromLow (
      events.dn.df, c(1:nrow(events.dn.df)), varname="r", getT=FALSE)
    events.dn.df$ERP.reversal_t <- vGetTseriesReversalFromLow (
      events.dn.df, c(1:nrow(events.dn.df)), varname="r", getT=TRUE)
      
    # simplifing variables: t.TUP (turning point) & T.ERP (end of reversal)
    events.dn.t.TUP <- events.dn.df$ECP.max.cont_t
    events.dn.t.ERP <- events.dn.df$ECP.max.cont_t + events.dn.df$ERP.reversal_t
  }
  # end of 3f) start: build simplifying variables

  # 3f.0) build lists of custom lookup indexes for all windows -----------------
  if (length(id.events.idx.up) > 0) {
    # ECP
    events.up.ECP.idx <- mapply (FUN=function(x,y) x:(x+y), x=events.up.df$bar, 
                                 y=events.up.t.TUP, SIMPLIFY = FALSE)
    # ERP
    events.up.ERP.idx <- mapply (FUN=function(x,y) x:y, 
                                 x=events.up.df$bar + events.up.t.TUP, 
                                 y=events.up.df$bar + events.up.t.ERP,
                                 SIMPLIFY = FALSE)
    # TUP.X
    events.up.TUP.C5.idx <- mapply (FUN=function(x,y) x:y, 
                                    x=events.up.df$bar + events.up.t.TUP - 5, 
                                    y=events.up.df$bar + events.up.t.TUP + 5,
                                    SIMPLIFY=FALSE)
    events.up.TUP.C2.idx <- mapply (FUN=function(x,y) x:y, 
                                    x=events.up.df$bar + events.up.t.TUP - 2, 
                                    y=events.up.df$bar + events.up.t.TUP + 2,
                                    SIMPLIFY=FALSE)
    events.up.TUP.L60.idx <- mapply (FUN=function(x,y) x:y, 
                                     x=events.up.df$bar + events.up.t.TUP - 60, 
                                     y=events.up.df$bar + events.up.t.TUP - 1,
                                     SIMPLIFY=FALSE)
    events.up.TUP.L30.idx <- mapply (FUN=function(x,y) x:y, 
                                     x=events.up.df$bar + events.up.t.TUP - 30, 
                                     y=events.up.df$bar + events.up.t.TUP - 1,
                                     SIMPLIFY=FALSE)
    events.up.TUP.L15.idx <- mapply (FUN=function(x,y) x:y, 
                                     x=events.up.df$bar + events.up.t.TUP - 15, 
                                     y=events.up.df$bar + events.up.t.TUP - 1,
                                     SIMPLIFY=FALSE)
    events.up.TUP.L10.idx <- mapply (FUN=function(x,y) x:y, 
                                     x=events.up.df$bar + events.up.t.TUP - 10, 
                                     y=events.up.df$bar + events.up.t.TUP - 1,
                                     SIMPLIFY=FALSE)
    events.up.TUP.L05.idx <- mapply (FUN=function(x,y) x:y, 
                                     x=events.up.df$bar + events.up.t.TUP - 5, 
                                     y=events.up.df$bar + events.up.t.TUP - 1,
                                     SIMPLIFY=FALSE)
    events.up.TUP.R05.idx <- mapply (FUN=function(x,y) x:y, 
                                     x=events.up.df$bar + events.up.t.TUP + 1, 
                                     y=events.up.df$bar + events.up.t.TUP + 5,
                                     SIMPLIFY=FALSE)
    events.up.TUP.R10.idx <- mapply (FUN=function(x,y) x:y, 
                                     x=events.up.df$bar + events.up.t.TUP + 1, 
                                     y=events.up.df$bar + events.up.t.TUP + 10,
                                     SIMPLIFY=FALSE)
    events.up.TUP.R15.idx <- mapply (FUN=function(x,y) x:y, 
                                     x=events.up.df$bar + events.up.t.TUP + 1, 
                                     y=events.up.df$bar + events.up.t.TUP + 15,
                                     SIMPLIFY=FALSE)
  }
  if (length(id.events.idx.dn) > 0) {
    # ECP
    events.dn.ECP.idx <- mapply (FUN=function(x,y) x:(x+y), x=events.dn.df$bar, 
                                 y=events.dn.t.TUP, SIMPLIFY = FALSE)
    # ERP
    events.dn.ERP.idx <- mapply (FUN=function(x,y) x:y, 
                                 x=events.dn.df$bar + events.dn.t.TUP, 
                                 y=events.dn.df$bar + events.dn.t.ERP,
                                 SIMPLIFY = FALSE)
    # TUP.X
    events.dn.TUP.C5.idx <- mapply (FUN=function(x,y) x:y, 
                                    x=events.dn.df$bar + events.dn.t.TUP - 5, 
                                    y=events.dn.df$bar + events.dn.t.TUP + 5,
                                    SIMPLIFY=FALSE)
    events.dn.TUP.C2.idx <- mapply (FUN=function(x,y) x:y, 
                                    x=events.dn.df$bar + events.dn.t.TUP - 2, 
                                    y=events.dn.df$bar + events.dn.t.TUP + 2,
                                    SIMPLIFY=FALSE)
    events.dn.TUP.L60.idx <- mapply (FUN=function(x,y) x:y, 
                                     x=events.dn.df$bar + events.dn.t.TUP - 60, 
                                     y=events.dn.df$bar + events.dn.t.TUP - 1,
                                     SIMPLIFY=FALSE)
    events.dn.TUP.L30.idx <- mapply (FUN=function(x,y) x:y, 
                                     x=events.dn.df$bar + events.dn.t.TUP - 30, 
                                     y=events.dn.df$bar + events.dn.t.TUP - 1,
                                     SIMPLIFY=FALSE)
    events.dn.TUP.L15.idx <- mapply (FUN=function(x,y) x:y, 
                                     x=events.dn.df$bar + events.dn.t.TUP - 15, 
                                     y=events.dn.df$bar + events.dn.t.TUP - 1,
                                     SIMPLIFY=FALSE)
    events.dn.TUP.L10.idx <- mapply (FUN=function(x,y) x:y, 
                                     x=events.dn.df$bar + events.dn.t.TUP - 10, 
                                     y=events.dn.df$bar + events.dn.t.TUP - 1,
                                     SIMPLIFY=FALSE)
    events.dn.TUP.L05.idx <- mapply (FUN=function(x,y) x:y, 
                                     x=events.dn.df$bar + events.dn.t.TUP - 5, 
                                     y=events.dn.df$bar + events.dn.t.TUP - 1,
                                     SIMPLIFY=FALSE)
    events.dn.TUP.R05.idx <- mapply (FUN=function(x,y) x:y, 
                                     x=events.dn.df$bar + events.dn.t.TUP + 1, 
                                     y=events.dn.df$bar + events.dn.t.TUP + 5,
                                     SIMPLIFY=FALSE)
    events.dn.TUP.R10.idx <- mapply (FUN=function(x,y) x:y, 
                                     x=events.dn.df$bar + events.dn.t.TUP + 1, 
                                     y=events.dn.df$bar + events.dn.t.TUP + 10,
                                     SIMPLIFY=FALSE)
    events.dn.TUP.R15.idx <- mapply (FUN=function(x,y) x:y, 
                                     x=events.dn.df$bar + events.dn.t.TUP + 1, 
                                     y=events.dn.df$bar + events.dn.t.TUP + 15,
                                     SIMPLIFY=FALSE)
  }
  # end of 3f.0) build lists of custom lookup indexes

                                   
  # 3f.1) add POEW behavior - price --------------------------------------------
  # 3f.1a) avg. relative volume (vs. dta) --------------------------------------
  if (length(id.events.idx.up) > 0) {
    # ECP
    events.up.df$ECP.rel.vol <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.up.df)), t.end=events.up.t.TUP,          # vec args
      MoreArgs = list (df=events.up.df, varname="v", t.start=0))   # other args
    # ERP
    events.up.df$ERP.rel.vol <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP, t.end=events.up.t.ERP,          
      MoreArgs = list (df=events.up.df, varname="v"))   
    # TUP.X
    events.up.df$TUP.C5.rel.vol <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP-5, 
                                   t.end=events.up.t.TUP+5, 
      MoreArgs = list (df=events.up.df, varname="v"))   
    events.up.df$TUP.C2.rel.vol <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP-2, 
                                   t.end=events.up.t.TUP+2, 
      MoreArgs = list (df=events.up.df, varname="v"))   
    events.up.df$TUP.L60.rel.vol <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP-60, 
                                   t.end=events.up.t.TUP-1, 
      MoreArgs = list (df=events.up.df, varname="v"))   
    events.up.df$TUP.L30.rel.vol <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP-30, 
                                   t.end=events.up.t.TUP-1, 
      MoreArgs = list (df=events.up.df, varname="v"))   
    events.up.df$TUP.L15.rel.vol <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP-15, 
                                   t.end=events.up.t.TUP-1, 
      MoreArgs = list (df=events.up.df, varname="v"))   
    events.up.df$TUP.L10.rel.vol <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP-10, 
                                   t.end=events.up.t.TUP-1, 
      MoreArgs = list (df=events.up.df, varname="v"))   
    events.up.df$TUP.L05.rel.vol <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP-5, 
                                   t.end=events.up.t.TUP-1, 
      MoreArgs = list (df=events.up.df, varname="v"))   
    events.up.df$TUP.R05.rel.vol <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP+1, 
                                   t.end=events.up.t.TUP+5, 
      MoreArgs = list (df=events.up.df, varname="v"))   
    events.up.df$TUP.R10.rel.vol <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP+1, 
                                   t.end=events.up.t.TUP+10, 
      MoreArgs = list (df=events.up.df, varname="v"))   
    events.up.df$TUP.R15.rel.vol <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP+1, 
                                   t.end=events.up.t.TUP+15, 
      MoreArgs = list (df=events.up.df, varname="v"))
  }
  if (length(id.events.idx.dn) > 0) {
    # ECP
    events.dn.df$ECP.rel.vol <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.dn.df)), t.end=events.dn.t.TUP,          # vec args
      MoreArgs = list (df=events.dn.df, varname="v", t.start=0))   # other args
    # ERP
    events.dn.df$ERP.rel.vol <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP, t.end=events.dn.t.ERP,          
      MoreArgs = list (df=events.dn.df, varname="v"))   
    # TUP.X
    events.dn.df$TUP.C5.rel.vol <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP-5, 
                                   t.end=events.dn.t.TUP+5, 
      MoreArgs = list (df=events.dn.df, varname="v"))   
    events.dn.df$TUP.C2.rel.vol <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP-2, 
                                   t.end=events.dn.t.TUP+2, 
      MoreArgs = list (df=events.dn.df, varname="v"))   
    events.dn.df$TUP.L60.rel.vol <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP-60, 
                                   t.end=events.dn.t.TUP-1, 
      MoreArgs = list (df=events.dn.df, varname="v"))   
    events.dn.df$TUP.L30.rel.vol <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP-30, 
                                   t.end=events.dn.t.TUP-1, 
      MoreArgs = list (df=events.dn.df, varname="v"))   
    events.dn.df$TUP.L15.rel.vol <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP-15, 
                                   t.end=events.dn.t.TUP-1, 
      MoreArgs = list (df=events.dn.df, varname="v"))   
    events.dn.df$TUP.L10.rel.vol <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP-10, 
                                   t.end=events.dn.t.TUP-1, 
      MoreArgs = list (df=events.dn.df, varname="v"))   
    events.dn.df$TUP.L05.rel.vol <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP-5, 
                                   t.end=events.dn.t.TUP-1, 
      MoreArgs = list (df=events.dn.df, varname="v"))   
    events.dn.df$TUP.R05.rel.vol <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP+1, 
                                   t.end=events.dn.t.TUP+5, 
      MoreArgs = list (df=events.dn.df, varname="v"))   
    events.dn.df$TUP.R10.rel.vol <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP+1, 
                                   t.end=events.dn.t.TUP+10, 
      MoreArgs = list (df=events.dn.df, varname="v"))   
    events.dn.df$TUP.R15.rel.vol <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP+1, 
                                   t.end=events.dn.t.TUP+15, 
      MoreArgs = list (df=events.dn.df, varname="v"))
  }
  # end of 3f.1a) avg. relative volume (vs. dta)

    
  # 3f.1b) vola ratio vs. dta --------------------------------------------------
  if (length(id.events.idx.up) > 0) {
    # ECP
    events.up.df$ECP.rel.vola <- mapply (FUN=GetTseriesSqRootAvg, 
      row=c(1:nrow(events.up.df)), t.end=events.up.t.TUP,
      MoreArgs = list (df=events.up.df, varname="rv", t.start=0))
    # ERP
    events.up.df$ERP.rel.vola <- mapply (FUN=GetTseriesSqRootAvg, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP, t.end=events.up.t.ERP,          
      MoreArgs = list (df=events.up.df, varname="rv"))   
    # TUP.X
    events.up.df$TUP.C5.rel.vola <- mapply (FUN=GetTseriesSqRootAvg, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP-5, 
                                   t.end=events.up.t.TUP+5, 
      MoreArgs = list (df=events.up.df, varname="rv"))   
    events.up.df$TUP.C2.rel.vola <- mapply (FUN=GetTseriesSqRootAvg, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP-2, 
                                   t.end=events.up.t.TUP+2, 
      MoreArgs = list (df=events.up.df, varname="rv"))   
    events.up.df$TUP.L60.rel.vola <- mapply (FUN=GetTseriesSqRootAvg, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP-60, 
                                   t.end=events.up.t.TUP-1, 
      MoreArgs = list (df=events.up.df, varname="rv"))   
    events.up.df$TUP.L30.rel.vola <- mapply (FUN=GetTseriesSqRootAvg, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP-30, 
                                   t.end=events.up.t.TUP-1, 
      MoreArgs = list (df=events.up.df, varname="rv"))   
    events.up.df$TUP.L15.rel.vola <- mapply (FUN=GetTseriesSqRootAvg, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP-15, 
                                   t.end=events.up.t.TUP-1, 
      MoreArgs = list (df=events.up.df, varname="rv"))   
    events.up.df$TUP.L10.rel.vola <- mapply (FUN=GetTseriesSqRootAvg, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP-10, 
                                   t.end=events.up.t.TUP-1, 
      MoreArgs = list (df=events.up.df, varname="rv"))   
    events.up.df$TUP.L05.rel.vola <- mapply (FUN=GetTseriesSqRootAvg, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP-5, 
                                   t.end=events.up.t.TUP-1, 
      MoreArgs = list (df=events.up.df, varname="rv"))   
    events.up.df$TUP.R05.rel.vola <- mapply (FUN=GetTseriesSqRootAvg, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP+1, 
                                   t.end=events.up.t.TUP+5, 
      MoreArgs = list (df=events.up.df, varname="rv"))   
    events.up.df$TUP.R10.rel.vola <- mapply (FUN=GetTseriesSqRootAvg, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP+1, 
                                   t.end=events.up.t.TUP+10, 
      MoreArgs = list (df=events.up.df, varname="rv"))   
    events.up.df$TUP.R15.rel.vola <- mapply (FUN=GetTseriesSqRootAvg, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP+1, 
                                   t.end=events.up.t.TUP+15, 
      MoreArgs = list (df=events.up.df, varname="rv"))  
  }
  if (length(id.events.idx.dn) > 0) {
    # ECP
    events.dn.df$ECP.rel.vola <- mapply (FUN=GetTseriesSqRootAvg, 
      row=c(1:nrow(events.dn.df)), t.end=events.dn.t.TUP,
      MoreArgs = list (df=events.dn.df, varname="rv", t.start=0))
    # ERP
    events.dn.df$ERP.rel.vola <- mapply (FUN=GetTseriesSqRootAvg, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP, t.end=events.dn.t.ERP,          
      MoreArgs = list (df=events.dn.df, varname="rv"))   
    # TUP.X
    events.dn.df$TUP.C5.rel.vola <- mapply (FUN=GetTseriesSqRootAvg, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP-5, 
                                   t.end=events.dn.t.TUP+5, 
      MoreArgs = list (df=events.dn.df, varname="rv"))   
    events.dn.df$TUP.C2.rel.vola <- mapply (FUN=GetTseriesSqRootAvg, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP-2, 
                                   t.end=events.dn.t.TUP+2, 
      MoreArgs = list (df=events.dn.df, varname="rv"))   
    events.dn.df$TUP.L60.rel.vola <- mapply (FUN=GetTseriesSqRootAvg, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP-60, 
                                   t.end=events.dn.t.TUP-1, 
      MoreArgs = list (df=events.dn.df, varname="rv"))   
    events.dn.df$TUP.L30.rel.vola <- mapply (FUN=GetTseriesSqRootAvg, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP-30, 
                                   t.end=events.dn.t.TUP-1, 
      MoreArgs = list (df=events.dn.df, varname="rv"))   
    events.dn.df$TUP.L15.rel.vola <- mapply (FUN=GetTseriesSqRootAvg, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP-15, 
                                   t.end=events.dn.t.TUP-1, 
      MoreArgs = list (df=events.dn.df, varname="rv"))   
    events.dn.df$TUP.L10.rel.vola <- mapply (FUN=GetTseriesSqRootAvg, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP-10, 
                                   t.end=events.dn.t.TUP-1, 
      MoreArgs = list (df=events.dn.df, varname="rv"))   
    events.dn.df$TUP.L05.rel.vola <- mapply (FUN=GetTseriesSqRootAvg, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP-5, 
                                   t.end=events.dn.t.TUP-1, 
      MoreArgs = list (df=events.dn.df, varname="rv"))   
    events.dn.df$TUP.R05.rel.vola <- mapply (FUN=GetTseriesSqRootAvg, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP+1, 
                                   t.end=events.dn.t.TUP+5, 
      MoreArgs = list (df=events.dn.df, varname="rv"))   
    events.dn.df$TUP.R10.rel.vola <- mapply (FUN=GetTseriesSqRootAvg, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP+1, 
                                   t.end=events.dn.t.TUP+10, 
      MoreArgs = list (df=events.dn.df, varname="rv"))   
    events.dn.df$TUP.R15.rel.vola <- mapply (FUN=GetTseriesSqRootAvg, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP+1, 
                                   t.end=events.dn.t.TUP+15, 
      MoreArgs = list (df=events.dn.df, varname="rv"))  
  }
  # end of 3f.1b) vola ratio vs. dta

	
  # 3f.1c) ids vola ratio vs. dta (of regular vola, ...) -----------------------
  if (length(id.events.idx.up) > 0) {
    # ECP
    events.up.df$ECP.rel.vola.ids <- mapply (FUN=GetTseriesSqRootAvg, 
      row=c(1:nrow(events.up.df)), t.end=events.up.t.TUP,
      MoreArgs = list (df=events.up.df, varname="arv", t.start=0))
    # ERP
    events.up.df$ERP.rel.vola.ids <- mapply (FUN=GetTseriesSqRootAvg, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP, t.end=events.up.t.ERP,          
      MoreArgs = list (df=events.up.df, varname="arv"))   
    # TUP.X
    events.up.df$TUP.C5.rel.vola.ids <- mapply (FUN=GetTseriesSqRootAvg, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP-5, 
                                   t.end=events.up.t.TUP+5, 
      MoreArgs = list (df=events.up.df, varname="arv"))   
    events.up.df$TUP.C2.rel.vola.ids <- mapply (FUN=GetTseriesSqRootAvg, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP-2, 
                                   t.end=events.up.t.TUP+2, 
      MoreArgs = list (df=events.up.df, varname="arv"))   
    events.up.df$TUP.L60.rel.vola.ids <- mapply (FUN=GetTseriesSqRootAvg, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP-60, 
                                   t.end=events.up.t.TUP-1, 
      MoreArgs = list (df=events.up.df, varname="arv"))   
    events.up.df$TUP.L30.rel.vola.ids <- mapply (FUN=GetTseriesSqRootAvg, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP-30, 
                                   t.end=events.up.t.TUP-1, 
      MoreArgs = list (df=events.up.df, varname="arv"))   
    events.up.df$TUP.L15.rel.vola.ids <- mapply (FUN=GetTseriesSqRootAvg, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP-15, 
                                   t.end=events.up.t.TUP-1, 
      MoreArgs = list (df=events.up.df, varname="arv"))   
    events.up.df$TUP.L10.rel.vola.ids <- mapply (FUN=GetTseriesSqRootAvg, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP-10, 
                                   t.end=events.up.t.TUP-1, 
      MoreArgs = list (df=events.up.df, varname="arv"))   
    events.up.df$TUP.L05.rel.vola.ids <- mapply (FUN=GetTseriesSqRootAvg, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP-5, 
                                   t.end=events.up.t.TUP-1, 
      MoreArgs = list (df=events.up.df, varname="arv"))   
    events.up.df$TUP.R05.rel.vola.ids <- mapply (FUN=GetTseriesSqRootAvg, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP+1, 
                                   t.end=events.up.t.TUP+5, 
      MoreArgs = list (df=events.up.df, varname="arv"))   
    events.up.df$TUP.R10.rel.vola.ids <- mapply (FUN=GetTseriesSqRootAvg, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP+1, 
                                   t.end=events.up.t.TUP+10, 
      MoreArgs = list (df=events.up.df, varname="arv"))   
    events.up.df$TUP.R15.rel.vola.ids <- mapply (FUN=GetTseriesSqRootAvg, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP+1, 
                                   t.end=events.up.t.TUP+15, 
      MoreArgs = list (df=events.up.df, varname="arv"))   
  }
  if (length(id.events.idx.dn) > 0) {
    # ECP
    events.dn.df$ECP.rel.vola.ids <- mapply (FUN=GetTseriesSqRootAvg, 
      row=c(1:nrow(events.dn.df)), t.end=events.dn.t.TUP,
      MoreArgs = list (df=events.dn.df, varname="arv", t.start=0))
    # ERP
    events.dn.df$ERP.rel.vola.ids <- mapply (FUN=GetTseriesSqRootAvg, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP, t.end=events.dn.t.ERP,          
      MoreArgs = list (df=events.dn.df, varname="arv"))   
    # TUP.X
    events.dn.df$TUP.C5.rel.vola.ids <- mapply (FUN=GetTseriesSqRootAvg, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP-5, 
                                   t.end=events.dn.t.TUP+5, 
      MoreArgs = list (df=events.dn.df, varname="arv"))   
    events.dn.df$TUP.C2.rel.vola.ids <- mapply (FUN=GetTseriesSqRootAvg, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP-2, 
                                   t.end=events.dn.t.TUP+2, 
      MoreArgs = list (df=events.dn.df, varname="arv"))   
    events.dn.df$TUP.L60.rel.vola.ids <- mapply (FUN=GetTseriesSqRootAvg, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP-60, 
                                   t.end=events.dn.t.TUP-1, 
      MoreArgs = list (df=events.dn.df, varname="arv"))   
    events.dn.df$TUP.L30.rel.vola.ids <- mapply (FUN=GetTseriesSqRootAvg, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP-30, 
                                   t.end=events.dn.t.TUP-1, 
      MoreArgs = list (df=events.dn.df, varname="arv"))   
    events.dn.df$TUP.L15.rel.vola.ids <- mapply (FUN=GetTseriesSqRootAvg, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP-15, 
                                   t.end=events.dn.t.TUP-1, 
      MoreArgs = list (df=events.dn.df, varname="arv"))   
    events.dn.df$TUP.L10.rel.vola.ids <- mapply (FUN=GetTseriesSqRootAvg, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP-10, 
                                   t.end=events.dn.t.TUP-1, 
      MoreArgs = list (df=events.dn.df, varname="arv"))   
    events.dn.df$TUP.L05.rel.vola.ids <- mapply (FUN=GetTseriesSqRootAvg, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP-5, 
                                   t.end=events.dn.t.TUP-1, 
      MoreArgs = list (df=events.dn.df, varname="arv"))   
    events.dn.df$TUP.R05.rel.vola.ids <- mapply (FUN=GetTseriesSqRootAvg, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP+1, 
                                   t.end=events.dn.t.TUP+5, 
      MoreArgs = list (df=events.dn.df, varname="arv"))   
    events.dn.df$TUP.R10.rel.vola.ids <- mapply (FUN=GetTseriesSqRootAvg, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP+1, 
                                   t.end=events.dn.t.TUP+10, 
      MoreArgs = list (df=events.dn.df, varname="arv"))   
    events.dn.df$TUP.R15.rel.vola.ids <- mapply (FUN=GetTseriesSqRootAvg, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP+1, 
                                   t.end=events.dn.t.TUP+15, 
      MoreArgs = list (df=events.dn.df, varname="arv"))   
  }
  # end of 3f.1c) ids vola ratio vs. dta
    
  # 3f.1d) event return vs. dta-vola -------------------------------------------
  # Attention:  sapply (events.up.xxx.idx, function(x) sqrt(period.id.window) *
  #               mean(id.rv.dta[x], na.rm=T))
  #             leads to wrong windows taken into account for dta normalization,
  #             but these cases turn into NA if r_t is NA
  if (length(id.events.idx.up) > 0) {
    # ECP
    events.up.df$ECP.rel.r <- sapply (1:nrow(events.up.df), function(x) 
        events.up.df[x,paste("r_", events.up.t.TUP[x], sep="")] ) / 
      sapply (events.up.ECP.idx, function(x) sqrt(period.id.window) *
                mean(id.rv.1min.dta[x], na.rm=T))
    # ERP
    events.up.df$ERP.rel.r <- sapply (1:nrow(events.up.df), function(x) 
      events.up.df[x, paste("r_", events.up.t.ERP[x], sep="")] -
      events.up.df[x, paste("r_", events.up.t.TUP[x], sep="")] ) / 
      sapply (events.up.ERP.idx, function(x) sqrt(period.id.window) *
                mean(id.rv.1min.dta[x], na.rm=T))
    # TUP.X
    events.up.df$TUP.C5.rel.r <- sapply (1:nrow(events.up.df), function(x)
      ifelse (events.up.t.TUP[x]+5 > param.post.event.window, NA, 
        events.up.df[x, paste("r_", events.up.t.TUP[x]+5, sep="")] -
        events.up.df[x, paste("r_", events.up.t.TUP[x]-5, sep="")]) )  / 
      sapply (events.up.TUP.C5.idx, function(x) sqrt(period.id.window) *
                mean(id.rv.1min.dta[x], na.rm=T))
    events.up.df$TUP.C2.rel.r <- sapply (1:nrow(events.up.df), function(x)
      ifelse (events.up.t.TUP[x]+2 > param.post.event.window, NA, 
        events.up.df[x, paste("r_", events.up.t.TUP[x]+2, sep="")] -
        events.up.df[x, paste("r_", events.up.t.TUP[x]-2, sep="")]) )  / 
      sapply (events.up.TUP.C2.idx, function(x) sqrt(period.id.window) *
                mean(id.rv.1min.dta[x], na.rm=T))
    events.up.df$TUP.L60.rel.r <- sapply (1:nrow(events.up.df), function(x)
      ifelse (MinSinceDaystart(events.up.df$timestamp[x]) + 
                events.up.t.TUP[x] - 60 < 1, NA, 
        events.up.df[x, paste("r_", events.up.t.TUP[x]-1, sep="")] -
        events.up.df[x, paste("r_", events.up.t.TUP[x]-60, sep="")] ))  / 
      sapply (events.up.TUP.L60.idx, function(x) sqrt(period.id.window) *
                mean(id.rv.1min.dta[x], na.rm=T))
    events.up.df$TUP.L30.rel.r <- sapply (1:nrow(events.up.df), function(x)
      ifelse (MinSinceDaystart(events.up.df$timestamp[x]) + 
                events.up.t.TUP[x] - 30 < 1, NA, 
        events.up.df[x, paste("r_", events.up.t.TUP[x]-1, sep="")] -
        events.up.df[x, paste("r_", events.up.t.TUP[x]-30, sep="")] ))  / 
      sapply (events.up.TUP.L30.idx, function(x) sqrt(period.id.window) *
                mean(id.rv.1min.dta[x], na.rm=T))
    events.up.df$TUP.L15.rel.r <- sapply (1:nrow(events.up.df), function(x)
      ifelse (MinSinceDaystart(events.up.df$timestamp[x]) + 
                events.up.t.TUP[x] - 15 < 1, NA, 
        events.up.df[x, paste("r_", events.up.t.TUP[x]-1, sep="")] -
        events.up.df[x, paste("r_", events.up.t.TUP[x]-15, sep="")] ))  / 
      sapply (events.up.TUP.L15.idx, function(x) sqrt(period.id.window) *
                mean(id.rv.1min.dta[x], na.rm=T))
    events.up.df$TUP.L10.rel.r <- sapply (1:nrow(events.up.df), function(x)
      ifelse (MinSinceDaystart(events.up.df$timestamp[x]) + 
                events.up.t.TUP[x] - 10 < 1, NA,       
        events.up.df[x, paste("r_", events.up.t.TUP[x]-1, sep="")] -
        events.up.df[x, paste("r_", events.up.t.TUP[x]-10, sep="")] ))  / 
      sapply (events.up.TUP.L10.idx, function(x) sqrt(period.id.window) *
                mean(id.rv.1min.dta[x], na.rm=T))
    events.up.df$TUP.L05.rel.r <- sapply (1:nrow(events.up.df), function(x)
      ifelse (MinSinceDaystart(events.up.df$timestamp[x]) + 
                events.up.t.TUP[x] - 5 < 1, NA,       
        events.up.df[x, paste("r_", events.up.t.TUP[x]-1, sep="")] -
        events.up.df[x, paste("r_", events.up.t.TUP[x]-5, sep="")] ))  / 
      sapply (events.up.TUP.L05.idx, function(x) sqrt(period.id.window) *
                mean(id.rv.1min.dta[x], na.rm=T))
    events.up.df$TUP.R05.rel.r <- sapply (1:nrow(events.up.df), function(x)
    ifelse (events.up.t.TUP[x]+5 > param.post.event.window, NA, 
        events.up.df[x, paste("r_", events.up.t.TUP[x]+5, sep="")] -
        events.up.df[x, paste("r_", events.up.t.TUP[x]+1, sep="")]) )  / 
      sapply (events.up.TUP.R05.idx, function(x) sqrt(period.id.window) *
                mean(id.rv.1min.dta[x], na.rm=T))
    events.up.df$TUP.R10.rel.r <- sapply (1:nrow(events.up.df), function(x)
    ifelse (events.up.t.TUP[x]+10 > param.post.event.window, NA, 
        events.up.df[x, paste("r_", events.up.t.TUP[x]+10, sep="")] -
        events.up.df[x, paste("r_", events.up.t.TUP[x]+1, sep="")]) )  / 
      sapply (events.up.TUP.R10.idx, function(x) sqrt(period.id.window) *
                mean(id.rv.1min.dta[x], na.rm=T))
    events.up.df$TUP.R15.rel.r <- sapply (1:nrow(events.up.df), function(x)
    ifelse (events.up.t.TUP[x]+15 > param.post.event.window, NA, 
        events.up.df[x, paste("r_", events.up.t.TUP[x]+15, sep="")] -
        events.up.df[x, paste("r_", events.up.t.TUP[x]+1, sep="")]) )  / 
      sapply (events.up.TUP.R15.idx, function(x) sqrt(period.id.window) *
                mean(id.rv.1min.dta[x], na.rm=T))
  }  
  if (length(id.events.idx.dn) > 0) {
    # ECP
    events.dn.df$ECP.rel.r <- sapply (1:nrow(events.dn.df), function(x) 
        events.dn.df[x,paste("r_", events.dn.t.TUP[x], sep="")] ) / 
      sapply (events.dn.ECP.idx, function(x) sqrt(period.id.window) *
                mean(id.rv.1min.dta[x], na.rm=T))
    # ERP
    events.dn.df$ERP.rel.r <- sapply (1:nrow(events.dn.df), function(x) 
      events.dn.df[x, paste("r_", events.dn.t.ERP[x], sep="")] -
      events.dn.df[x, paste("r_", events.dn.t.TUP[x], sep="")] ) / 
      sapply (events.dn.ERP.idx, function(x) sqrt(period.id.window) *
                mean(id.rv.1min.dta[x], na.rm=T))
    # TUP.X
    events.dn.df$TUP.C5.rel.r <- sapply (1:nrow(events.dn.df), function(x)
      ifelse (events.dn.t.TUP[x]+5 > param.post.event.window, NA, 
        events.dn.df[x, paste("r_", events.dn.t.TUP[x]+5, sep="")] -
        events.dn.df[x, paste("r_", events.dn.t.TUP[x]-5, sep="")]) )  / 
      sapply (events.dn.TUP.C5.idx, function(x) sqrt(period.id.window) *
                mean(id.rv.1min.dta[x], na.rm=T))
    events.dn.df$TUP.C2.rel.r <- sapply (1:nrow(events.dn.df), function(x)
      ifelse (events.dn.t.TUP[x]+2 > param.post.event.window, NA, 
        events.dn.df[x, paste("r_", events.dn.t.TUP[x]+2, sep="")] -
        events.dn.df[x, paste("r_", events.dn.t.TUP[x]-2, sep="")]) )  / 
      sapply (events.dn.TUP.C2.idx, function(x) sqrt(period.id.window) *
                mean(id.rv.1min.dta[x], na.rm=T))
    events.dn.df$TUP.L60.rel.r <- sapply (1:nrow(events.dn.df), function(x)
      ifelse (MinSinceDaystart(events.dn.df$timestamp[x]) + 
                events.dn.t.TUP[x] - 60 < 1, NA, 
        events.dn.df[x, paste("r_", events.dn.t.TUP[x]-1, sep="")] -
        events.dn.df[x, paste("r_", events.dn.t.TUP[x]-60, sep="")] ))  / 
      sapply (events.dn.TUP.L60.idx, function(x) sqrt(period.id.window) *
                mean(id.rv.1min.dta[x], na.rm=T))
    events.dn.df$TUP.L30.rel.r <- sapply (1:nrow(events.dn.df), function(x)
      ifelse (MinSinceDaystart(events.dn.df$timestamp[x]) + 
                events.dn.t.TUP[x] - 30 < 1, NA, 
        events.dn.df[x, paste("r_", events.dn.t.TUP[x]-1, sep="")] -
        events.dn.df[x, paste("r_", events.dn.t.TUP[x]-30, sep="")] ))  / 
      sapply (events.dn.TUP.L30.idx, function(x) sqrt(period.id.window) *
                mean(id.rv.1min.dta[x], na.rm=T))
    events.dn.df$TUP.L15.rel.r <- sapply (1:nrow(events.dn.df), function(x)
      ifelse (MinSinceDaystart(events.dn.df$timestamp[x]) + 
                events.dn.t.TUP[x] - 15 < 1, NA, 
        events.dn.df[x, paste("r_", events.dn.t.TUP[x]-1, sep="")] -
        events.dn.df[x, paste("r_", events.dn.t.TUP[x]-15, sep="")] ))  / 
      sapply (events.dn.TUP.L15.idx, function(x) sqrt(period.id.window) *
                mean(id.rv.1min.dta[x], na.rm=T))
    events.dn.df$TUP.L10.rel.r <- sapply (1:nrow(events.dn.df), function(x)
      ifelse (MinSinceDaystart(events.dn.df$timestamp[x]) + 
                events.dn.t.TUP[x] - 10 < 1, NA,       
        events.dn.df[x, paste("r_", events.dn.t.TUP[x]-1, sep="")] -
        events.dn.df[x, paste("r_", events.dn.t.TUP[x]-10, sep="")] ))  / 
      sapply (events.dn.TUP.L10.idx, function(x) sqrt(period.id.window) *
                mean(id.rv.1min.dta[x], na.rm=T))
    events.dn.df$TUP.L05.rel.r <- sapply (1:nrow(events.dn.df), function(x)
      ifelse (MinSinceDaystart(events.dn.df$timestamp[x]) + 
                events.dn.t.TUP[x] - 5 < 1, NA,       
        events.dn.df[x, paste("r_", events.dn.t.TUP[x]-1, sep="")] -
        events.dn.df[x, paste("r_", events.dn.t.TUP[x]-5, sep="")] ))  / 
      sapply (events.dn.TUP.L05.idx, function(x) sqrt(period.id.window) *
                mean(id.rv.1min.dta[x], na.rm=T))
    events.dn.df$TUP.R05.rel.r <- sapply (1:nrow(events.dn.df), function(x)
    ifelse (events.dn.t.TUP[x]+5 > param.post.event.window, NA, 
        events.dn.df[x, paste("r_", events.dn.t.TUP[x]+5, sep="")] -
        events.dn.df[x, paste("r_", events.dn.t.TUP[x]+1, sep="")]) )  / 
      sapply (events.dn.TUP.R05.idx, function(x) sqrt(period.id.window) *
                mean(id.rv.1min.dta[x], na.rm=T))
    events.dn.df$TUP.R10.rel.r <- sapply (1:nrow(events.dn.df), function(x)
    ifelse (events.dn.t.TUP[x]+10 > param.post.event.window, NA, 
        events.dn.df[x, paste("r_", events.dn.t.TUP[x]+10, sep="")] -
        events.dn.df[x, paste("r_", events.dn.t.TUP[x]+1, sep="")]) )  / 
      sapply (events.dn.TUP.R10.idx, function(x) sqrt(period.id.window) *
                mean(id.rv.1min.dta[x], na.rm=T))
    events.dn.df$TUP.R15.rel.r <- sapply (1:nrow(events.dn.df), function(x)
    ifelse (events.dn.t.TUP[x]+15 > param.post.event.window, NA, 
        events.dn.df[x, paste("r_", events.dn.t.TUP[x]+15, sep="")] -
        events.dn.df[x, paste("r_", events.dn.t.TUP[x]+1, sep="")]) )  / 
      sapply (events.dn.TUP.R15.idx, function(x) sqrt(period.id.window) *
                mean(id.rv.1min.dta[x], na.rm=T))
  }
  # end of 3f.1d) event return vs. dta-vola
  
  # 3f.1e) event abnormal return vs. dta-vola ----------------------------------
  if (length(id.events.idx.up) > 0) {
    # ECP
    events.up.df$ECP.rel.ar <- sapply (1:nrow(events.up.df), function(x) 
        events.up.df[x,paste("ar_", events.up.t.TUP[x], sep="")] ) / 
      sapply (events.up.ECP.idx, function(x) sqrt(period.id.window) *
                mean(id.rv.1min.dta[x], na.rm=T))
    # ERP
    events.up.df$ERP.rel.ar <- sapply (1:nrow(events.up.df), function(x) 
      events.up.df[x, paste("ar_", events.up.t.ERP[x], sep="")] -
      events.up.df[x, paste("ar_", events.up.t.TUP[x], sep="")] ) / 
      sapply (events.up.ERP.idx, function(x) sqrt(period.id.window) *
                mean(id.rv.1min.dta[x], na.rm=T))
    # TUP.X
    events.up.df$TUP.C5.rel.ar <- sapply (1:nrow(events.up.df), function(x)
      ifelse (events.up.t.TUP[x]+5 > param.post.event.window, NA, 
        events.up.df[x, paste("ar_", events.up.t.TUP[x]+5, sep="")] -
        events.up.df[x, paste("ar_", events.up.t.TUP[x]-5, sep="")]) )  / 
      sapply (events.up.TUP.C5.idx, function(x) sqrt(period.id.window) *
                mean(id.rv.1min.dta[x], na.rm=T))
    events.up.df$TUP.C2.rel.ar <- sapply (1:nrow(events.up.df), function(x)
      ifelse (events.up.t.TUP[x]+2 > param.post.event.window, NA, 
        events.up.df[x, paste("ar_", events.up.t.TUP[x]+2, sep="")] -
        events.up.df[x, paste("ar_", events.up.t.TUP[x]-2, sep="")]) )  / 
      sapply (events.up.TUP.C2.idx, function(x) sqrt(period.id.window) *
                mean(id.rv.1min.dta[x], na.rm=T))
    events.up.df$TUP.L60.rel.ar <- sapply (1:nrow(events.up.df), function(x)
      ifelse (MinSinceDaystart(events.up.df$timestamp[x]) + 
                events.up.t.TUP[x] - 60 < 1, NA, 
        events.up.df[x, paste("ar_", events.up.t.TUP[x]-1, sep="")] -
        events.up.df[x, paste("ar_", events.up.t.TUP[x]-60, sep="")] ))  / 
      sapply (events.up.TUP.L60.idx, function(x) sqrt(period.id.window) *
                mean(id.rv.1min.dta[x], na.rm=T))
    events.up.df$TUP.L30.rel.ar <- sapply (1:nrow(events.up.df), function(x)
      ifelse (MinSinceDaystart(events.up.df$timestamp[x]) + 
                events.up.t.TUP[x] - 30 < 1, NA, 
        events.up.df[x, paste("ar_", events.up.t.TUP[x]-1, sep="")] -
        events.up.df[x, paste("ar_", events.up.t.TUP[x]-30, sep="")] ))  / 
      sapply (events.up.TUP.L30.idx, function(x) sqrt(period.id.window) *
                mean(id.rv.1min.dta[x], na.rm=T))
    events.up.df$TUP.L15.rel.ar <- sapply (1:nrow(events.up.df), function(x)
      ifelse (MinSinceDaystart(events.up.df$timestamp[x]) + 
                events.up.t.TUP[x] - 15 < 1, NA, 
        events.up.df[x, paste("ar_", events.up.t.TUP[x]-1, sep="")] -
        events.up.df[x, paste("ar_", events.up.t.TUP[x]-15, sep="")] ))  / 
      sapply (events.up.TUP.L15.idx, function(x) sqrt(period.id.window) *
                mean(id.rv.1min.dta[x], na.rm=T))
    events.up.df$TUP.L10.rel.ar <- sapply (1:nrow(events.up.df), function(x)
      ifelse (MinSinceDaystart(events.up.df$timestamp[x]) + 
                events.up.t.TUP[x] - 10 < 1, NA,       
        events.up.df[x, paste("ar_", events.up.t.TUP[x]-1, sep="")] -
        events.up.df[x, paste("ar_", events.up.t.TUP[x]-10, sep="")] ))  / 
      sapply (events.up.TUP.L10.idx, function(x) sqrt(period.id.window) *
                mean(id.rv.1min.dta[x], na.rm=T))
    events.up.df$TUP.L05.rel.ar <- sapply (1:nrow(events.up.df), function(x)
      ifelse (MinSinceDaystart(events.up.df$timestamp[x]) + 
                events.up.t.TUP[x] - 5 < 1, NA,       
        events.up.df[x, paste("ar_", events.up.t.TUP[x]-1, sep="")] -
        events.up.df[x, paste("ar_", events.up.t.TUP[x]-5, sep="")] ))  / 
      sapply (events.up.TUP.L05.idx, function(x) sqrt(period.id.window) *
                mean(id.rv.1min.dta[x], na.rm=T))
    events.up.df$TUP.R05.rel.ar <- sapply (1:nrow(events.up.df), function(x)
    ifelse (events.up.t.TUP[x]+5 > param.post.event.window, NA, 
        events.up.df[x, paste("ar_", events.up.t.TUP[x]+5, sep="")] -
        events.up.df[x, paste("ar_", events.up.t.TUP[x]+1, sep="")]) )  / 
      sapply (events.up.TUP.R05.idx, function(x) sqrt(period.id.window) *
                mean(id.rv.1min.dta[x], na.rm=T))
    events.up.df$TUP.R10.rel.ar <- sapply (1:nrow(events.up.df), function(x)
    ifelse (events.up.t.TUP[x]+10 > param.post.event.window, NA, 
        events.up.df[x, paste("ar_", events.up.t.TUP[x]+10, sep="")] -
        events.up.df[x, paste("ar_", events.up.t.TUP[x]+1, sep="")]) )  / 
      sapply (events.up.TUP.R10.idx, function(x) sqrt(period.id.window) *
                mean(id.rv.1min.dta[x], na.rm=T))
    events.up.df$TUP.R15.rel.ar <- sapply (1:nrow(events.up.df), function(x)
    ifelse (events.up.t.TUP[x]+15 > param.post.event.window, NA, 
        events.up.df[x, paste("ar_", events.up.t.TUP[x]+15, sep="")] -
        events.up.df[x, paste("ar_", events.up.t.TUP[x]+1, sep="")]) )  / 
      sapply (events.up.TUP.R15.idx, function(x) sqrt(period.id.window) *
                mean(id.rv.1min.dta[x], na.rm=T))
  }
  if (length(id.events.idx.dn) > 0) {
    # ECP
    events.dn.df$ECP.rel.ar <- sapply (1:nrow(events.dn.df), function(x) 
        events.dn.df[x,paste("ar_", events.dn.t.TUP[x], sep="")] ) / 
      sapply (events.dn.ECP.idx, function(x) sqrt(period.id.window) *
                mean(id.rv.1min.dta[x], na.rm=T))
    # ERP
    events.dn.df$ERP.rel.ar <- sapply (1:nrow(events.dn.df), function(x) 
      events.dn.df[x, paste("ar_", events.dn.t.ERP[x], sep="")] -
      events.dn.df[x, paste("ar_", events.dn.t.TUP[x], sep="")] ) / 
      sapply (events.dn.ERP.idx, function(x) sqrt(period.id.window) *
                mean(id.rv.1min.dta[x], na.rm=T))
    # TUP.X
    events.dn.df$TUP.C5.rel.ar <- sapply (1:nrow(events.dn.df), function(x)
      ifelse (events.dn.t.TUP[x]+5 > param.post.event.window, NA, 
        events.dn.df[x, paste("ar_", events.dn.t.TUP[x]+5, sep="")] -
        events.dn.df[x, paste("ar_", events.dn.t.TUP[x]-5, sep="")]) )  / 
      sapply (events.dn.TUP.C5.idx, function(x) sqrt(period.id.window) *
                mean(id.rv.1min.dta[x], na.rm=T))
    events.dn.df$TUP.C2.rel.ar <- sapply (1:nrow(events.dn.df), function(x)
      ifelse (events.dn.t.TUP[x]+2 > param.post.event.window, NA, 
        events.dn.df[x, paste("ar_", events.dn.t.TUP[x]+2, sep="")] -
        events.dn.df[x, paste("ar_", events.dn.t.TUP[x]-2, sep="")]) )  / 
      sapply (events.dn.TUP.C2.idx, function(x) sqrt(period.id.window) *
                mean(id.rv.1min.dta[x], na.rm=T))
    events.dn.df$TUP.L60.rel.ar <- sapply (1:nrow(events.dn.df), function(x)
      ifelse (MinSinceDaystart(events.dn.df$timestamp[x]) + 
                events.dn.t.TUP[x] - 60 < 1, NA, 
        events.dn.df[x, paste("ar_", events.dn.t.TUP[x]-1, sep="")] -
        events.dn.df[x, paste("ar_", events.dn.t.TUP[x]-60, sep="")] ))  / 
      sapply (events.dn.TUP.L60.idx, function(x) sqrt(period.id.window) *
                mean(id.rv.1min.dta[x], na.rm=T))
    events.dn.df$TUP.L30.rel.ar <- sapply (1:nrow(events.dn.df), function(x)
      ifelse (MinSinceDaystart(events.dn.df$timestamp[x]) + 
                events.dn.t.TUP[x] - 30 < 1, NA, 
        events.dn.df[x, paste("ar_", events.dn.t.TUP[x]-1, sep="")] -
        events.dn.df[x, paste("ar_", events.dn.t.TUP[x]-30, sep="")] ))  / 
      sapply (events.dn.TUP.L30.idx, function(x) sqrt(period.id.window) *
                mean(id.rv.1min.dta[x], na.rm=T))
    events.dn.df$TUP.L15.rel.ar <- sapply (1:nrow(events.dn.df), function(x)
      ifelse (MinSinceDaystart(events.dn.df$timestamp[x]) + 
                events.dn.t.TUP[x] - 15 < 1, NA, 
        events.dn.df[x, paste("ar_", events.dn.t.TUP[x]-1, sep="")] -
        events.dn.df[x, paste("ar_", events.dn.t.TUP[x]-15, sep="")] ))  / 
      sapply (events.dn.TUP.L15.idx, function(x) sqrt(period.id.window) *
                mean(id.rv.1min.dta[x], na.rm=T))
    events.dn.df$TUP.L10.rel.ar <- sapply (1:nrow(events.dn.df), function(x)
      ifelse (MinSinceDaystart(events.dn.df$timestamp[x]) + 
                events.dn.t.TUP[x] - 10 < 1, NA,       
        events.dn.df[x, paste("ar_", events.dn.t.TUP[x]-1, sep="")] -
        events.dn.df[x, paste("ar_", events.dn.t.TUP[x]-10, sep="")] ))  / 
      sapply (events.dn.TUP.L10.idx, function(x) sqrt(period.id.window) *
                mean(id.rv.1min.dta[x], na.rm=T))
    events.dn.df$TUP.L05.rel.ar <- sapply (1:nrow(events.dn.df), function(x)
      ifelse (MinSinceDaystart(events.dn.df$timestamp[x]) + 
                events.dn.t.TUP[x] - 5 < 1, NA,       
        events.dn.df[x, paste("ar_", events.dn.t.TUP[x]-1, sep="")] -
        events.dn.df[x, paste("ar_", events.dn.t.TUP[x]-5, sep="")] ))  / 
      sapply (events.dn.TUP.L05.idx, function(x) sqrt(period.id.window) *
                mean(id.rv.1min.dta[x], na.rm=T))
    events.dn.df$TUP.R05.rel.ar <- sapply (1:nrow(events.dn.df), function(x)
    ifelse (events.dn.t.TUP[x]+5 > param.post.event.window, NA, 
        events.dn.df[x, paste("ar_", events.dn.t.TUP[x]+5, sep="")] -
        events.dn.df[x, paste("ar_", events.dn.t.TUP[x]+1, sep="")]) )  / 
      sapply (events.dn.TUP.R05.idx, function(x) sqrt(period.id.window) *
                mean(id.rv.1min.dta[x], na.rm=T))
    events.dn.df$TUP.R10.rel.ar <- sapply (1:nrow(events.dn.df), function(x)
    ifelse (events.dn.t.TUP[x]+10 > param.post.event.window, NA, 
        events.dn.df[x, paste("ar_", events.dn.t.TUP[x]+10, sep="")] -
        events.dn.df[x, paste("ar_", events.dn.t.TUP[x]+1, sep="")]) )  / 
      sapply (events.dn.TUP.R10.idx, function(x) sqrt(period.id.window) *
                mean(id.rv.1min.dta[x], na.rm=T))
    events.dn.df$TUP.R15.rel.ar <- sapply (1:nrow(events.dn.df), function(x)
    ifelse (events.dn.t.TUP[x]+15 > param.post.event.window, NA, 
        events.dn.df[x, paste("ar_", events.dn.t.TUP[x]+15, sep="")] -
        events.dn.df[x, paste("ar_", events.dn.t.TUP[x]+1, sep="")]) )  / 
      sapply (events.dn.TUP.R15.idx, function(x) sqrt(period.id.window) *
                mean(id.rv.1min.dta[x], na.rm=T))
  }
  # end of 3f.1e) event abnormal return vs. dta-vola
  
  # 3f.1f) raw event return ----------------------------------------------------
  if (length(id.events.idx.up) > 0) {
    # ECP
    events.up.df$ECP.r <- sapply (1:nrow(events.up.df), function(x) 
        events.up.df[x,paste("r_", events.up.t.TUP[x], sep="")] )
    # ERP
    events.up.df$ERP.r <- sapply (1:nrow(events.up.df), function(x) 
      events.up.df[x, paste("r_", events.up.t.ERP[x], sep="")] -
      events.up.df[x, paste("r_", events.up.t.TUP[x], sep="")] )
    # TUP.X
    events.up.df$TUP.C5.r <- sapply (1:nrow(events.up.df), function(x)
      ifelse (events.up.t.TUP[x]+5 > param.post.event.window, NA, 
        events.up.df[x, paste("r_", events.up.t.TUP[x]+5, sep="")] -
        events.up.df[x, paste("r_", events.up.t.TUP[x]-5, sep="")]) )
    events.up.df$TUP.C2.r <- sapply (1:nrow(events.up.df), function(x)
      ifelse (events.up.t.TUP[x]+2 > param.post.event.window, NA, 
        events.up.df[x, paste("r_", events.up.t.TUP[x]+2, sep="")] -
        events.up.df[x, paste("r_", events.up.t.TUP[x]-2, sep="")]) )
    events.up.df$TUP.L60.r <- sapply (1:nrow(events.up.df), function(x)
        events.up.df[x, paste("r_", events.up.t.TUP[x]-1, sep="")] -
        events.up.df[x, paste("r_", events.up.t.TUP[x]-60, sep="")] )
    events.up.df$TUP.L30.r <- sapply (1:nrow(events.up.df), function(x)
        events.up.df[x, paste("r_", events.up.t.TUP[x]-1, sep="")] -
        events.up.df[x, paste("r_", events.up.t.TUP[x]-30, sep="")] )
    events.up.df$TUP.L15.r <- sapply (1:nrow(events.up.df), function(x)
        events.up.df[x, paste("r_", events.up.t.TUP[x]-1, sep="")] -
        events.up.df[x, paste("r_", events.up.t.TUP[x]-15, sep="")] )
    events.up.df$TUP.L10.r <- sapply (1:nrow(events.up.df), function(x)
        events.up.df[x, paste("r_", events.up.t.TUP[x]-1, sep="")] -
        events.up.df[x, paste("r_", events.up.t.TUP[x]-10, sep="")] )
    events.up.df$TUP.L05.r <- sapply (1:nrow(events.up.df), function(x)
        events.up.df[x, paste("r_", events.up.t.TUP[x]-1, sep="")] -
        events.up.df[x, paste("r_", events.up.t.TUP[x]-5, sep="")] )
    events.up.df$TUP.R05.r <- sapply (1:nrow(events.up.df), function(x)
      ifelse (events.up.t.TUP[x]+5 > param.post.event.window, NA, 
        events.up.df[x, paste("r_", events.up.t.TUP[x]+5, sep="")] -
        events.up.df[x, paste("r_", events.up.t.TUP[x]+1, sep="")]) )
    events.up.df$TUP.R10.r <- sapply (1:nrow(events.up.df), function(x)
      ifelse (events.up.t.TUP[x]+10 > param.post.event.window, NA, 
        events.up.df[x, paste("r_", events.up.t.TUP[x]+10, sep="")] -
        events.up.df[x, paste("r_", events.up.t.TUP[x]+1, sep="")]) )
    events.up.df$TUP.R15.r <- sapply (1:nrow(events.up.df), function(x)
      ifelse (events.up.t.TUP[x]+15 > param.post.event.window, NA, 
        events.up.df[x, paste("r_", events.up.t.TUP[x]+15, sep="")] -
        events.up.df[x, paste("r_", events.up.t.TUP[x]+1, sep="")]) )
  }
  if (length(id.events.idx.dn) > 0) {
    # ECP
    events.dn.df$ECP.r <- sapply (1:nrow(events.dn.df), function(x) 
        events.dn.df[x,paste("r_", events.dn.t.TUP[x], sep="")] )
    # ERP
    events.dn.df$ERP.r <- sapply (1:nrow(events.dn.df), function(x) 
      events.dn.df[x, paste("r_", events.dn.t.ERP[x], sep="")] -
      events.dn.df[x, paste("r_", events.dn.t.TUP[x], sep="")] )
    # TUP.X
    events.dn.df$TUP.C5.r <- sapply (1:nrow(events.dn.df), function(x)
      ifelse (events.dn.t.TUP[x]+5 > param.post.event.window, NA, 
        events.dn.df[x, paste("r_", events.dn.t.TUP[x]+5, sep="")] -
        events.dn.df[x, paste("r_", events.dn.t.TUP[x]-5, sep="")]) )
    events.dn.df$TUP.C2.r <- sapply (1:nrow(events.dn.df), function(x)
      ifelse (events.dn.t.TUP[x]+2 > param.post.event.window, NA, 
        events.dn.df[x, paste("r_", events.dn.t.TUP[x]+2, sep="")] -
        events.dn.df[x, paste("r_", events.dn.t.TUP[x]-2, sep="")]) )
    events.dn.df$TUP.L60.r <- sapply (1:nrow(events.dn.df), function(x)
        events.dn.df[x, paste("r_", events.dn.t.TUP[x]-1, sep="")] -
        events.dn.df[x, paste("r_", events.dn.t.TUP[x]-60, sep="")] )
    events.dn.df$TUP.L30.r <- sapply (1:nrow(events.dn.df), function(x)
        events.dn.df[x, paste("r_", events.dn.t.TUP[x]-1, sep="")] -
        events.dn.df[x, paste("r_", events.dn.t.TUP[x]-30, sep="")] )
    events.dn.df$TUP.L15.r <- sapply (1:nrow(events.dn.df), function(x)
        events.dn.df[x, paste("r_", events.dn.t.TUP[x]-1, sep="")] -
        events.dn.df[x, paste("r_", events.dn.t.TUP[x]-15, sep="")] )
    events.dn.df$TUP.L10.r <- sapply (1:nrow(events.dn.df), function(x)
        events.dn.df[x, paste("r_", events.dn.t.TUP[x]-1, sep="")] -
        events.dn.df[x, paste("r_", events.dn.t.TUP[x]-10, sep="")] )
    events.dn.df$TUP.L05.r <- sapply (1:nrow(events.dn.df), function(x)
        events.dn.df[x, paste("r_", events.dn.t.TUP[x]-1, sep="")] -
        events.dn.df[x, paste("r_", events.dn.t.TUP[x]-5, sep="")] )
    events.dn.df$TUP.R05.r <- sapply (1:nrow(events.dn.df), function(x)
      ifelse (events.dn.t.TUP[x]+5 > param.post.event.window, NA, 
        events.dn.df[x, paste("r_", events.dn.t.TUP[x]+5, sep="")] -
        events.dn.df[x, paste("r_", events.dn.t.TUP[x]+1, sep="")]) )
    events.dn.df$TUP.R10.r <- sapply (1:nrow(events.dn.df), function(x)
      ifelse (events.dn.t.TUP[x]+10 > param.post.event.window, NA, 
        events.dn.df[x, paste("r_", events.dn.t.TUP[x]+10, sep="")] -
        events.dn.df[x, paste("r_", events.dn.t.TUP[x]+1, sep="")]) )
    events.dn.df$TUP.R15.r <- sapply (1:nrow(events.dn.df), function(x)
      ifelse (events.dn.t.TUP[x]+15 > param.post.event.window, NA, 
        events.dn.df[x, paste("r_", events.dn.t.TUP[x]+15, sep="")] -
        events.dn.df[x, paste("r_", events.dn.t.TUP[x]+1, sep="")]) )
  }
  # end of 3f.1f) raw event return
	  
  # 3f.1g) abnormal event return -----------------------------------------------
  if (length(id.events.idx.up) > 0) {
    # ECP
    events.up.df$ECP.ar <- sapply (1:nrow(events.up.df), function(x) 
        events.up.df[x,paste("ar_", events.up.t.TUP[x], sep="")] )
    # ERP
    events.up.df$ERP.ar <- sapply (1:nrow(events.up.df), function(x) 
      events.up.df[x, paste("ar_", events.up.t.ERP[x], sep="")] -
      events.up.df[x, paste("ar_", events.up.t.TUP[x], sep="")] )
    # TUP.X
    events.up.df$TUP.C5.ar <- sapply (1:nrow(events.up.df), function(x)
      ifelse (events.up.t.TUP[x]+5 > param.post.event.window, NA, 
        events.up.df[x, paste("ar_", events.up.t.TUP[x]+5, sep="")] -
        events.up.df[x, paste("ar_", events.up.t.TUP[x]-5, sep="")]) )
    events.up.df$TUP.C2.ar <- sapply (1:nrow(events.up.df), function(x)
      ifelse (events.up.t.TUP[x]+2 > param.post.event.window, NA, 
        events.up.df[x, paste("ar_", events.up.t.TUP[x]+2, sep="")] -
        events.up.df[x, paste("ar_", events.up.t.TUP[x]-2, sep="")]) )
    events.up.df$TUP.L60.ar <- sapply (1:nrow(events.up.df), function(x)
        events.up.df[x, paste("ar_", events.up.t.TUP[x]-1, sep="")] -
        events.up.df[x, paste("ar_", events.up.t.TUP[x]-60, sep="")] )
    events.up.df$TUP.L30.ar <- sapply (1:nrow(events.up.df), function(x)
        events.up.df[x, paste("ar_", events.up.t.TUP[x]-1, sep="")] -
        events.up.df[x, paste("ar_", events.up.t.TUP[x]-30, sep="")] )
    events.up.df$TUP.L15.ar <- sapply (1:nrow(events.up.df), function(x)
        events.up.df[x, paste("ar_", events.up.t.TUP[x]-1, sep="")] -
        events.up.df[x, paste("ar_", events.up.t.TUP[x]-15, sep="")] )
    events.up.df$TUP.L10.ar <- sapply (1:nrow(events.up.df), function(x)
        events.up.df[x, paste("ar_", events.up.t.TUP[x]-1, sep="")] -
        events.up.df[x, paste("ar_", events.up.t.TUP[x]-10, sep="")] )
    events.up.df$TUP.L05.ar <- sapply (1:nrow(events.up.df), function(x)
        events.up.df[x, paste("ar_", events.up.t.TUP[x]-1, sep="")] -
        events.up.df[x, paste("ar_", events.up.t.TUP[x]-5, sep="")] )
    events.up.df$TUP.R05.ar <- sapply (1:nrow(events.up.df), function(x)
      ifelse (events.up.t.TUP[x]+5 > param.post.event.window, NA, 
        events.up.df[x, paste("ar_", events.up.t.TUP[x]+5, sep="")] -
        events.up.df[x, paste("ar_", events.up.t.TUP[x]+1, sep="")]) )
    events.up.df$TUP.R10.ar <- sapply (1:nrow(events.up.df), function(x)
      ifelse (events.up.t.TUP[x]+10 > param.post.event.window, NA, 
        events.up.df[x, paste("ar_", events.up.t.TUP[x]+10, sep="")] -
        events.up.df[x, paste("ar_", events.up.t.TUP[x]+1, sep="")]) )
    events.up.df$TUP.R15.ar <- sapply (1:nrow(events.up.df), function(x)
      ifelse (events.up.t.TUP[x]+15 > param.post.event.window, NA, 
        events.up.df[x, paste("ar_", events.up.t.TUP[x]+15, sep="")] -
        events.up.df[x, paste("ar_", events.up.t.TUP[x]+1, sep="")]) )
  }
  if (length(id.events.idx.dn) > 0) {
    # ECP
    events.dn.df$ECP.ar <- sapply (1:nrow(events.dn.df), function(x) 
        events.dn.df[x,paste("ar_", events.dn.t.TUP[x], sep="")] )
    # ERP
    events.dn.df$ERP.ar <- sapply (1:nrow(events.dn.df), function(x) 
      events.dn.df[x, paste("ar_", events.dn.t.ERP[x], sep="")] -
      events.dn.df[x, paste("ar_", events.dn.t.TUP[x], sep="")] )
    # TUP.X
    events.dn.df$TUP.C5.ar <- sapply (1:nrow(events.dn.df), function(x)
      ifelse (events.dn.t.TUP[x]+5 > param.post.event.window, NA, 
        events.dn.df[x, paste("ar_", events.dn.t.TUP[x]+5, sep="")] -
        events.dn.df[x, paste("ar_", events.dn.t.TUP[x]-5, sep="")]) )
    events.dn.df$TUP.C2.ar <- sapply (1:nrow(events.dn.df), function(x)
      ifelse (events.dn.t.TUP[x]+2 > param.post.event.window, NA, 
        events.dn.df[x, paste("ar_", events.dn.t.TUP[x]+2, sep="")] -
        events.dn.df[x, paste("ar_", events.dn.t.TUP[x]-2, sep="")]) )
    events.dn.df$TUP.L60.ar <- sapply (1:nrow(events.dn.df), function(x)
        events.dn.df[x, paste("ar_", events.dn.t.TUP[x]-1, sep="")] -
        events.dn.df[x, paste("ar_", events.dn.t.TUP[x]-60, sep="")] )
    events.dn.df$TUP.L30.ar <- sapply (1:nrow(events.dn.df), function(x)
        events.dn.df[x, paste("ar_", events.dn.t.TUP[x]-1, sep="")] -
        events.dn.df[x, paste("ar_", events.dn.t.TUP[x]-30, sep="")] )
    events.dn.df$TUP.L15.ar <- sapply (1:nrow(events.dn.df), function(x)
        events.dn.df[x, paste("ar_", events.dn.t.TUP[x]-1, sep="")] -
        events.dn.df[x, paste("ar_", events.dn.t.TUP[x]-15, sep="")] )
    events.dn.df$TUP.L10.ar <- sapply (1:nrow(events.dn.df), function(x)
        events.dn.df[x, paste("ar_", events.dn.t.TUP[x]-1, sep="")] -
        events.dn.df[x, paste("ar_", events.dn.t.TUP[x]-10, sep="")] )
    events.dn.df$TUP.L05.ar <- sapply (1:nrow(events.dn.df), function(x)
        events.dn.df[x, paste("ar_", events.dn.t.TUP[x]-1, sep="")] -
        events.dn.df[x, paste("ar_", events.dn.t.TUP[x]-5, sep="")] )
    events.dn.df$TUP.R05.ar <- sapply (1:nrow(events.dn.df), function(x)
      ifelse (events.dn.t.TUP[x]+5 > param.post.event.window, NA, 
        events.dn.df[x, paste("ar_", events.dn.t.TUP[x]+5, sep="")] -
        events.dn.df[x, paste("ar_", events.dn.t.TUP[x]+1, sep="")]) )
    events.dn.df$TUP.R10.ar <- sapply (1:nrow(events.dn.df), function(x)
      ifelse (events.dn.t.TUP[x]+10 > param.post.event.window, NA, 
        events.dn.df[x, paste("ar_", events.dn.t.TUP[x]+10, sep="")] -
        events.dn.df[x, paste("ar_", events.dn.t.TUP[x]+1, sep="")]) )
    events.dn.df$TUP.R15.ar <- sapply (1:nrow(events.dn.df), function(x)
      ifelse (events.dn.t.TUP[x]+15 > param.post.event.window, NA, 
        events.dn.df[x, paste("ar_", events.dn.t.TUP[x]+15, sep="")] -
        events.dn.df[x, paste("ar_", events.dn.t.TUP[x]+1, sep="")]) )
  }
  # end of 3f.1g) abnormal event return
    
  # 3f.2) add POEW behavior - HFT activity -------------------------------------
  # a) does HFT drive the final crash until reversal (overreaction climax)?
  #    --> covered by analysis of ECP and TUP-X
  #    --> price based variables: return, vola, volume (TUP-X)
  # b) does HFT drive the reversal itself, or its extent, or its speed?
  #    --> covered by analysis of ERP and TUP+X
  #    --> price based variables: return, vola, volume (TUP+X)
  # c) how does HFT activity - at event time, in climax, and at reversal -
  #    change over time?
  #    --> plot averages (for selected stocks with equal characteristis)
  #        over time
  
  # 3f.2a) HFT activity abs --------------------------------------------------
  if (length(id.events.idx.up) > 0) {
    # ECP
#     events.up.df$ECP.HFT.f.rate <- 100 * sapply (1:length(events.up.ECP.idx),
#       function(x) mean (cur.hft.x.aligned$Events[events.up.ECP.idx[[x]]]))/60
#     events.up.df$ECP.HFT.m.rate <- 100 * sapply (1:length(events.up.ECP.idx),
#       function(x) mean (cur.hft.x.aligned$Events[events.up.ECP.idx[[x]]]>0))    
    events.up.df$ECP.HFT.f.rate <- 100 * mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.up.df)), t.end=events.up.t.TUP,
      MoreArgs = list (df=events.up.df, varname="hft", t.start=0)) / 60
    events.up.df$ECP.HFT.m.rate <- 100 * mapply (FUN=GetTseriesAvg.01, 
      row=c(1:nrow(events.up.df)), t.end=events.up.t.TUP,
      MoreArgs = list (df=events.up.df, varname="hft", t.start=0))    
    # ERP
    events.up.df$ERP.HFT.f.rate <- 100 * mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP, t.end=events.up.t.ERP,
      MoreArgs = list (df=events.up.df, varname="hft")) / 60
    events.up.df$ERP.HFT.m.rate <- 100 * mapply (FUN=GetTseriesAvg.01, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP, t.end=events.up.t.ERP,
      MoreArgs = list (df=events.up.df, varname="hft"))        
    # TUP.X
#     events.up.df$TUP.C5.HFT.f.rate <- 100 * sapply (1:length(events.up.TUP.C5.idx),
#       function(x) ifelse (events.up.t.TUP[x]+5 > param.post.event.window, NA, 
#       mean (cur.hft.x.aligned$Events[events.up.TUP.C5.idx[[x]]])))/60
#     events.up.df$TUP.C5.HFT.m.rate <- 100 * sapply (1:length(events.up.TUP.C5.idx),
#       function(x) ifelse (events.up.t.TUP[x]+5 > param.post.event.window, NA, 
#       mean (cur.hft.x.aligned$Events[events.up.TUP.C5.idx[[x]]]>0)))    
    events.up.df$TUP.C5.HFT.f.rate <- 100 * mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP-5, t.end=events.up.t.TUP+5,
      MoreArgs = list (df=events.up.df, varname="hft")) / 60
    events.up.df$TUP.C5.HFT.m.rate <- 100 * mapply (FUN=GetTseriesAvg.01, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP-5, t.end=events.up.t.TUP+5,
      MoreArgs = list (df=events.up.df, varname="hft"))
    events.up.df$TUP.C2.HFT.f.rate <- 100 * mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP-2, t.end=events.up.t.TUP+2,
      MoreArgs = list (df=events.up.df, varname="hft")) / 60
    events.up.df$TUP.C2.HFT.m.rate <- 100 * mapply (FUN=GetTseriesAvg.01, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP-2, t.end=events.up.t.TUP+2,
      MoreArgs = list (df=events.up.df, varname="hft"))
    events.up.df$TUP.L60.HFT.f.rate <- 100 * mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP-60, t.end=events.up.t.TUP-1,
      MoreArgs = list (df=events.up.df, varname="hft")) / 60
    events.up.df$TUP.L60.HFT.m.rate <- 100 * mapply (FUN=GetTseriesAvg.01, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP-60, t.end=events.up.t.TUP-1,
      MoreArgs = list (df=events.up.df, varname="hft"))
    events.up.df$TUP.L30.HFT.f.rate <- 100 * mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP-30, t.end=events.up.t.TUP-1,
      MoreArgs = list (df=events.up.df, varname="hft")) / 60
    events.up.df$TUP.L30.HFT.m.rate <- 100 * mapply (FUN=GetTseriesAvg.01, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP-30, t.end=events.up.t.TUP-1,
      MoreArgs = list (df=events.up.df, varname="hft"))
    events.up.df$TUP.L15.HFT.f.rate <- 100 * mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP-15, t.end=events.up.t.TUP-1,
      MoreArgs = list (df=events.up.df, varname="hft")) / 60
    events.up.df$TUP.L15.HFT.m.rate <- 100 * mapply (FUN=GetTseriesAvg.01, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP-15, t.end=events.up.t.TUP-1,
      MoreArgs = list (df=events.up.df, varname="hft"))
    events.up.df$TUP.L10.HFT.f.rate <- 100 * mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP-10, t.end=events.up.t.TUP-1,
      MoreArgs = list (df=events.up.df, varname="hft")) / 60
    events.up.df$TUP.L10.HFT.m.rate <- 100 * mapply (FUN=GetTseriesAvg.01, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP-10, t.end=events.up.t.TUP-1,
      MoreArgs = list (df=events.up.df, varname="hft"))
    events.up.df$TUP.L05.HFT.f.rate <- 100 * mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP-5, t.end=events.up.t.TUP-1,
      MoreArgs = list (df=events.up.df, varname="hft")) / 60
    events.up.df$TUP.L05.HFT.m.rate <- 100 * mapply (FUN=GetTseriesAvg.01, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP-5, t.end=events.up.t.TUP-1,
      MoreArgs = list (df=events.up.df, varname="hft"))
    events.up.df$TUP.R05.HFT.f.rate <- 100 * mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP+1, t.end=events.up.t.TUP+5,
      MoreArgs = list (df=events.up.df, varname="hft")) / 60
    events.up.df$TUP.R05.HFT.m.rate <- 100 * mapply (FUN=GetTseriesAvg.01, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP+1, t.end=events.up.t.TUP+5,
      MoreArgs = list (df=events.up.df, varname="hft"))
    events.up.df$TUP.R10.HFT.f.rate <- 100 * mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP+1, t.end=events.up.t.TUP+10,
      MoreArgs = list (df=events.up.df, varname="hft")) / 60
    events.up.df$TUP.R10.HFT.m.rate <- 100 * mapply (FUN=GetTseriesAvg.01, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP+1, t.end=events.up.t.TUP+10,
      MoreArgs = list (df=events.up.df, varname="hft"))
    events.up.df$TUP.R15.HFT.f.rate <- 100 * mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP+1, t.end=events.up.t.TUP+15,
      MoreArgs = list (df=events.up.df, varname="hft")) / 60
    events.up.df$TUP.R15.HFT.m.rate <- 100 * mapply (FUN=GetTseriesAvg.01, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP+1, t.end=events.up.t.TUP+15,
      MoreArgs = list (df=events.up.df, varname="hft"))
  }
  if (length(id.events.idx.dn) > 0) {
    # ECP
    events.dn.df$ECP.HFT.f.rate <- 100 * mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.dn.df)), t.end=events.dn.t.TUP,
      MoreArgs = list (df=events.dn.df, varname="hft", t.start=0)) / 60
    events.dn.df$ECP.HFT.m.rate <- 100 * mapply (FUN=GetTseriesAvg.01, 
      row=c(1:nrow(events.dn.df)), t.end=events.dn.t.TUP,
      MoreArgs = list (df=events.dn.df, varname="hft", t.start=0))    
    # ERP
    events.dn.df$ERP.HFT.f.rate <- 100 * mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP, t.end=events.dn.t.ERP,
      MoreArgs = list (df=events.dn.df, varname="hft")) / 60
    events.dn.df$ERP.HFT.m.rate <- 100 * mapply (FUN=GetTseriesAvg.01, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP, t.end=events.dn.t.ERP,
      MoreArgs = list (df=events.dn.df, varname="hft"))        
    # TUP.X
    events.dn.df$TUP.C5.HFT.f.rate <- 100 * mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP-5, t.end=events.dn.t.TUP+5,
      MoreArgs = list (df=events.dn.df, varname="hft")) / 60
    events.dn.df$TUP.C5.HFT.m.rate <- 100 * mapply (FUN=GetTseriesAvg.01, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP-5, t.end=events.dn.t.TUP+5,
      MoreArgs = list (df=events.dn.df, varname="hft"))
    events.dn.df$TUP.C2.HFT.f.rate <- 100 * mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP-2, t.end=events.dn.t.TUP+2,
      MoreArgs = list (df=events.dn.df, varname="hft")) / 60
    events.dn.df$TUP.C2.HFT.m.rate <- 100 * mapply (FUN=GetTseriesAvg.01, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP-2, t.end=events.dn.t.TUP+2,
      MoreArgs = list (df=events.dn.df, varname="hft"))
    events.dn.df$TUP.L60.HFT.f.rate <- 100 * mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP-60, t.end=events.dn.t.TUP-1,
      MoreArgs = list (df=events.dn.df, varname="hft")) / 60
    events.dn.df$TUP.L60.HFT.m.rate <- 100 * mapply (FUN=GetTseriesAvg.01, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP-60, t.end=events.dn.t.TUP-1,
      MoreArgs = list (df=events.dn.df, varname="hft"))
    events.dn.df$TUP.L30.HFT.f.rate <- 100 * mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP-30, t.end=events.dn.t.TUP-1,
      MoreArgs = list (df=events.dn.df, varname="hft")) / 60
    events.dn.df$TUP.L30.HFT.m.rate <- 100 * mapply (FUN=GetTseriesAvg.01, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP-30, t.end=events.dn.t.TUP-1,
      MoreArgs = list (df=events.dn.df, varname="hft"))
    events.dn.df$TUP.L15.HFT.f.rate <- 100 * mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP-15, t.end=events.dn.t.TUP-1,
      MoreArgs = list (df=events.dn.df, varname="hft")) / 60
    events.dn.df$TUP.L15.HFT.m.rate <- 100 * mapply (FUN=GetTseriesAvg.01, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP-15, t.end=events.dn.t.TUP-1,
      MoreArgs = list (df=events.dn.df, varname="hft"))
    events.dn.df$TUP.L10.HFT.f.rate <- 100 * mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP-10, t.end=events.dn.t.TUP-1,
      MoreArgs = list (df=events.dn.df, varname="hft")) / 60
    events.dn.df$TUP.L10.HFT.m.rate <- 100 * mapply (FUN=GetTseriesAvg.01, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP-10, t.end=events.dn.t.TUP-1,
      MoreArgs = list (df=events.dn.df, varname="hft"))
    events.dn.df$TUP.L05.HFT.f.rate <- 100 * mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP-5, t.end=events.dn.t.TUP-1,
      MoreArgs = list (df=events.dn.df, varname="hft")) / 60
    events.dn.df$TUP.L05.HFT.m.rate <- 100 * mapply (FUN=GetTseriesAvg.01, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP-5, t.end=events.dn.t.TUP-1,
      MoreArgs = list (df=events.dn.df, varname="hft"))
    events.dn.df$TUP.R05.HFT.f.rate <- 100 * mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP+1, t.end=events.dn.t.TUP+5,
      MoreArgs = list (df=events.dn.df, varname="hft")) / 60
    events.dn.df$TUP.R05.HFT.m.rate <- 100 * mapply (FUN=GetTseriesAvg.01, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP+1, t.end=events.dn.t.TUP+5,
      MoreArgs = list (df=events.dn.df, varname="hft"))
    events.dn.df$TUP.R10.HFT.f.rate <- 100 * mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP+1, t.end=events.dn.t.TUP+10,
      MoreArgs = list (df=events.dn.df, varname="hft")) / 60
    events.dn.df$TUP.R10.HFT.m.rate <- 100 * mapply (FUN=GetTseriesAvg.01, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP+1, t.end=events.dn.t.TUP+10,
      MoreArgs = list (df=events.dn.df, varname="hft"))
    events.dn.df$TUP.R15.HFT.f.rate <- 100 * mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP+1, t.end=events.dn.t.TUP+15,
      MoreArgs = list (df=events.dn.df, varname="hft")) / 60
    events.dn.df$TUP.R15.HFT.m.rate <- 100 * mapply (FUN=GetTseriesAvg.01, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP+1, t.end=events.dn.t.TUP+15,
      MoreArgs = list (df=events.dn.df, varname="hft"))
  }
  # end of 3f.2a) HFT activity abs
  
  # 3f.2b) HFT activity rel vs. dta --------------------------------------------
  # divide abs. rates by: PREP HFT * mean(dta profile[PREWxx])
  if (length(id.events.idx.up) > 0) {
    # ECP
#     events.up.df$ECP.HFT.rel.f.rate <- events.up.df$ECP.HFT.f.rate / 
#       (events.up.df$PREP.HFT.f.rate * sapply( 1:nrow(events.up.df), function(x) 
#         mean(cur.hft.flags.dta[substr(index(cur.x.aligned)[events.up.ECP.idx[[x]]
#                       ], 12, 19)] ) / mean(cur.hft.flags.dta) ))
#     events.up.df$ECP.HFT.rel.m.rate <- events.up.df$ECP.HFT.m.rate / 
#       (events.up.df$PREP.HFT.m.rate * sapply( 1:nrow(events.up.df), function(x) 
#         mean(cur.hft.mins.dta[substr(index(cur.x.aligned)[events.up.ECP.idx[[x]]
#                       ], 12, 19)] ) / mean(cur.hft.mins.dta) ))
    events.up.df$ECP.HFT.rel.f.rate <- na.fill (events.up.df$ECP.HFT.f.rate / 
      (events.up.df$PREP.HFT.f.rate * sapply( 1:nrow(events.up.df), function(x) 
        mean(cur.hft.flags.dta[DayBoundedBarSeq(events.up.df$timestamp[x], 
             t.start=0, t.end=events.up.t.TUP[x])])) / mean(cur.hft.flags.dta) ), 0)
    events.up.df$ECP.HFT.rel.m.rate <- na.fill (events.up.df$ECP.HFT.m.rate / 
      (events.up.df$PREP.HFT.m.rate * sapply( 1:nrow(events.up.df), function(x) 
        mean(cur.hft.mins.dta[DayBoundedBarSeq(events.up.df$timestamp[x], 
             t.start=0, t.end=events.up.t.TUP[x])])) / mean(cur.hft.mins.dta) ), 0)
    # ERP
    events.up.df$ERP.HFT.rel.f.rate <- na.fill (events.up.df$ERP.HFT.f.rate / 
      (events.up.df$PREP.HFT.f.rate * sapply( 1:nrow(events.up.df), function(x) 
      mean(cur.hft.flags.dta[DayBoundedBarSeq(events.up.df$timestamp[x], 
        t.start=events.up.t.TUP[x], t.end=events.up.t.ERP[x])])) / mean(cur.hft.flags.dta) ), 0)
    events.up.df$ERP.HFT.rel.m.rate <- na.fill (events.up.df$ERP.HFT.m.rate / 
      (events.up.df$PREP.HFT.m.rate * sapply( 1:nrow(events.up.df), function(x) 
      mean(cur.hft.mins.dta[DayBoundedBarSeq(events.up.df$timestamp[x], 
        t.start=events.up.t.TUP[x], t.end=events.up.t.ERP[x])])) / mean(cur.hft.mins.dta) ), 0)
    # TUP.X
    events.up.df$TUP.C5.HFT.rel.f.rate <- na.fill (events.up.df$TUP.C5.HFT.f.rate / 
      (events.up.df$PREP.HFT.f.rate * sapply( 1:nrow(events.up.df), function(x) 
      mean(cur.hft.flags.dta[DayBoundedBarSeq(events.up.df$timestamp[x], 
        t.start=events.up.t.TUP[x]-5, t.end=events.up.t.TUP[x]+5)])) / mean(cur.hft.flags.dta) ), 0)
    events.up.df$TUP.C5.HFT.rel.m.rate <- na.fill (events.up.df$TUP.C5.HFT.m.rate / 
      (events.up.df$PREP.HFT.m.rate * sapply( 1:nrow(events.up.df), function(x) 
      mean(cur.hft.mins.dta[DayBoundedBarSeq(events.up.df$timestamp[x], 
        t.start=events.up.t.TUP[x]-5, t.end=events.up.t.TUP[x]+5)])) / mean(cur.hft.mins.dta) ), 0)
    events.up.df$TUP.C2.HFT.rel.f.rate <- na.fill (events.up.df$TUP.C2.HFT.f.rate / 
      (events.up.df$PREP.HFT.f.rate * sapply( 1:nrow(events.up.df), function(x) 
      mean(cur.hft.flags.dta[DayBoundedBarSeq(events.up.df$timestamp[x], 
        t.start=events.up.t.TUP[x]-2, t.end=events.up.t.TUP[x]+2)])) / mean(cur.hft.flags.dta) ), 0)
    events.up.df$TUP.C2.HFT.rel.m.rate <- na.fill (events.up.df$TUP.C2.HFT.m.rate / 
      (events.up.df$PREP.HFT.m.rate * sapply( 1:nrow(events.up.df), function(x) 
      mean(cur.hft.mins.dta[DayBoundedBarSeq(events.up.df$timestamp[x], 
        t.start=events.up.t.TUP[x]-2, t.end=events.up.t.TUP[x]+2)])) / mean(cur.hft.mins.dta) ), 0)
    events.up.df$TUP.L60.HFT.rel.f.rate <- na.fill (events.up.df$TUP.L60.HFT.f.rate / 
      (events.up.df$PREP.HFT.f.rate * sapply( 1:nrow(events.up.df), function(x) 
      mean(cur.hft.flags.dta[DayBoundedBarSeq(events.up.df$timestamp[x], 
        t.start=events.up.t.TUP[x]-60, t.end=events.up.t.TUP[x]-1)])) / mean(cur.hft.flags.dta) ), 0)
    events.up.df$TUP.L60.HFT.rel.m.rate <- na.fill (events.up.df$TUP.L60.HFT.m.rate / 
      (events.up.df$PREP.HFT.m.rate * sapply( 1:nrow(events.up.df), function(x) 
      mean(cur.hft.mins.dta[DayBoundedBarSeq(events.up.df$timestamp[x], 
        t.start=events.up.t.TUP[x]-60, t.end=events.up.t.TUP[x]-1)])) / mean(cur.hft.mins.dta) ), 0)
    events.up.df$TUP.L30.HFT.rel.f.rate <- na.fill (events.up.df$TUP.L30.HFT.f.rate / 
      (events.up.df$PREP.HFT.f.rate * sapply( 1:nrow(events.up.df), function(x) 
      mean(cur.hft.flags.dta[DayBoundedBarSeq(events.up.df$timestamp[x], 
        t.start=events.up.t.TUP[x]-30, t.end=events.up.t.TUP[x]-1)])) / mean(cur.hft.flags.dta) ), 0)
    events.up.df$TUP.L30.HFT.rel.m.rate <- na.fill (events.up.df$TUP.L30.HFT.m.rate / 
      (events.up.df$PREP.HFT.m.rate * sapply( 1:nrow(events.up.df), function(x) 
      mean(cur.hft.mins.dta[DayBoundedBarSeq(events.up.df$timestamp[x], 
        t.start=events.up.t.TUP[x]-30, t.end=events.up.t.TUP[x]-1)])) / mean(cur.hft.mins.dta) ), 0)
    events.up.df$TUP.L15.HFT.rel.f.rate <- na.fill (events.up.df$TUP.L15.HFT.f.rate / 
      (events.up.df$PREP.HFT.f.rate * sapply( 1:nrow(events.up.df), function(x) 
      mean(cur.hft.flags.dta[DayBoundedBarSeq(events.up.df$timestamp[x], 
        t.start=events.up.t.TUP[x]-15, t.end=events.up.t.TUP[x]-1)])) / mean(cur.hft.flags.dta) ), 0)
    events.up.df$TUP.L15.HFT.rel.m.rate <- na.fill (events.up.df$TUP.L15.HFT.m.rate / 
      (events.up.df$PREP.HFT.m.rate * sapply( 1:nrow(events.up.df), function(x) 
      mean(cur.hft.mins.dta[DayBoundedBarSeq(events.up.df$timestamp[x], 
        t.start=events.up.t.TUP[x]-15, t.end=events.up.t.TUP[x]-1)])) / mean(cur.hft.mins.dta) ), 0)
    events.up.df$TUP.L10.HFT.rel.f.rate <- na.fill (events.up.df$TUP.L10.HFT.f.rate / 
      (events.up.df$PREP.HFT.f.rate * sapply( 1:nrow(events.up.df), function(x) 
      mean(cur.hft.flags.dta[DayBoundedBarSeq(events.up.df$timestamp[x], 
        t.start=events.up.t.TUP[x]-10, t.end=events.up.t.TUP[x]-1)])) / mean(cur.hft.flags.dta) ), 0)
    events.up.df$TUP.L10.HFT.rel.m.rate <- na.fill (events.up.df$TUP.L10.HFT.m.rate / 
      (events.up.df$PREP.HFT.m.rate * sapply( 1:nrow(events.up.df), function(x) 
      mean(cur.hft.mins.dta[DayBoundedBarSeq(events.up.df$timestamp[x], 
        t.start=events.up.t.TUP[x]-10, t.end=events.up.t.TUP[x]-1)])) / mean(cur.hft.mins.dta) ), 0)
    events.up.df$TUP.L05.HFT.rel.f.rate <- na.fill (events.up.df$TUP.L05.HFT.f.rate / 
      (events.up.df$PREP.HFT.f.rate * sapply( 1:nrow(events.up.df), function(x) 
      mean(cur.hft.flags.dta[DayBoundedBarSeq(events.up.df$timestamp[x], 
        t.start=events.up.t.TUP[x]-5, t.end=events.up.t.TUP[x]-1)])) / mean(cur.hft.flags.dta) ), 0)
    events.up.df$TUP.L05.HFT.rel.m.rate <- na.fill (events.up.df$TUP.L05.HFT.m.rate / 
      (events.up.df$PREP.HFT.m.rate * sapply( 1:nrow(events.up.df), function(x) 
      mean(cur.hft.mins.dta[DayBoundedBarSeq(events.up.df$timestamp[x], 
        t.start=events.up.t.TUP[x]-5, t.end=events.up.t.TUP[x]-1)])) / mean(cur.hft.mins.dta) ), 0)
    events.up.df$TUP.R05.HFT.rel.f.rate <- na.fill (events.up.df$TUP.R05.HFT.f.rate / 
      (events.up.df$PREP.HFT.f.rate * sapply( 1:nrow(events.up.df), function(x) 
      mean(cur.hft.flags.dta[DayBoundedBarSeq(events.up.df$timestamp[x], 
        t.start=events.up.t.TUP[x]+1, t.end=events.up.t.TUP[x]+5)])) / mean(cur.hft.flags.dta) ), 0)
    events.up.df$TUP.R05.HFT.rel.m.rate <- na.fill (events.up.df$TUP.R05.HFT.m.rate / 
      (events.up.df$PREP.HFT.m.rate * sapply( 1:nrow(events.up.df), function(x) 
      mean(cur.hft.mins.dta[DayBoundedBarSeq(events.up.df$timestamp[x], 
        t.start=events.up.t.TUP[x]+1, t.end=events.up.t.TUP[x]+5)])) / mean(cur.hft.mins.dta) ), 0)
    events.up.df$TUP.R10.HFT.rel.f.rate <- na.fill (events.up.df$TUP.R10.HFT.f.rate / 
      (events.up.df$PREP.HFT.f.rate * sapply( 1:nrow(events.up.df), function(x) 
      mean(cur.hft.flags.dta[DayBoundedBarSeq(events.up.df$timestamp[x], 
        t.start=events.up.t.TUP[x]+1, t.end=events.up.t.TUP[x]+10)])) / mean(cur.hft.flags.dta) ), 0)
    events.up.df$TUP.R10.HFT.rel.m.rate <- na.fill (events.up.df$TUP.R10.HFT.m.rate / 
      (events.up.df$PREP.HFT.m.rate * sapply( 1:nrow(events.up.df), function(x) 
      mean(cur.hft.mins.dta[DayBoundedBarSeq(events.up.df$timestamp[x], 
        t.start=events.up.t.TUP[x]+1, t.end=events.up.t.TUP[x]+10)])) / mean(cur.hft.mins.dta) ), 0)
    events.up.df$TUP.R15.HFT.rel.f.rate <- na.fill (events.up.df$TUP.R15.HFT.f.rate / 
      (events.up.df$PREP.HFT.f.rate * sapply( 1:nrow(events.up.df), function(x) 
      mean(cur.hft.flags.dta[DayBoundedBarSeq(events.up.df$timestamp[x], 
        t.start=events.up.t.TUP[x]+1, t.end=events.up.t.TUP[x]+15)])) / mean(cur.hft.flags.dta) ), 0)
    events.up.df$TUP.R15.HFT.rel.m.rate <- na.fill (events.up.df$TUP.R15.HFT.m.rate / 
      (events.up.df$PREP.HFT.m.rate * sapply( 1:nrow(events.up.df), function(x) 
      mean(cur.hft.mins.dta[DayBoundedBarSeq(events.up.df$timestamp[x], 
        t.start=events.up.t.TUP[x]+1, t.end=events.up.t.TUP[x]+15)])) / mean(cur.hft.mins.dta) ), 0)
  }
  if (length(id.events.idx.dn) > 0) {
    # ECP
    events.dn.df$ECP.HFT.rel.f.rate <- na.fill (events.dn.df$ECP.HFT.f.rate / 
      (events.dn.df$PREP.HFT.f.rate * sapply( 1:nrow(events.dn.df), function(x) 
        mean(cur.hft.flags.dta[DayBoundedBarSeq(events.dn.df$timestamp[x], 
             t.start=0, t.end=events.dn.t.TUP[x])])) / mean(cur.hft.flags.dta) ), 0)
    events.dn.df$ECP.HFT.rel.m.rate <- na.fill (events.dn.df$ECP.HFT.m.rate / 
      (events.dn.df$PREP.HFT.m.rate * sapply( 1:nrow(events.dn.df), function(x) 
        mean(cur.hft.mins.dta[DayBoundedBarSeq(events.dn.df$timestamp[x], 
             t.start=0, t.end=events.dn.t.TUP[x])])) / mean(cur.hft.mins.dta) ), 0)
    # ERP
    events.dn.df$ECP.HFT.rel.f.rate <- na.fill (events.dn.df$ECP.HFT.f.rate / 
      (events.dn.df$PREP.HFT.f.rate * sapply( 1:nrow(events.dn.df), function(x) 
      mean(cur.hft.flags.dta[DayBoundedBarSeq(events.dn.df$timestamp[x], 
        t.start=events.dn.t.TUP[x], t.end=events.dn.t.ERP[x])])) / mean(cur.hft.flags.dta) ), 0)
    events.dn.df$ECP.HFT.rel.m.rate <- na.fill (events.dn.df$ECP.HFT.m.rate / 
      (events.dn.df$PREP.HFT.m.rate * sapply( 1:nrow(events.dn.df), function(x) 
      mean(cur.hft.mins.dta[DayBoundedBarSeq(events.dn.df$timestamp[x], 
        t.start=events.dn.t.TUP[x], t.end=events.dn.t.ERP[x])])) / mean(cur.hft.mins.dta) ), 0)
    # TUP.X
    events.dn.df$TUP.C5.HFT.rel.f.rate <- na.fill (events.dn.df$TUP.C5.HFT.f.rate / 
      (events.dn.df$PREP.HFT.f.rate * sapply( 1:nrow(events.dn.df), function(x) 
      mean(cur.hft.flags.dta[DayBoundedBarSeq(events.dn.df$timestamp[x], 
        t.start=events.dn.t.TUP[x]-5, t.end=events.dn.t.TUP[x]+5)])) / mean(cur.hft.flags.dta) ), 0)
    events.dn.df$TUP.C5.HFT.rel.m.rate <- na.fill (events.dn.df$TUP.C5.HFT.m.rate / 
      (events.dn.df$PREP.HFT.m.rate * sapply( 1:nrow(events.dn.df), function(x) 
      mean(cur.hft.mins.dta[DayBoundedBarSeq(events.dn.df$timestamp[x], 
        t.start=events.dn.t.TUP[x]-5, t.end=events.dn.t.TUP[x]+5)])) / mean(cur.hft.mins.dta) ), 0)
    events.dn.df$TUP.C2.HFT.rel.f.rate <- na.fill (events.dn.df$TUP.C2.HFT.f.rate / 
      (events.dn.df$PREP.HFT.f.rate * sapply( 1:nrow(events.dn.df), function(x) 
      mean(cur.hft.flags.dta[DayBoundedBarSeq(events.dn.df$timestamp[x], 
        t.start=events.dn.t.TUP[x]-2, t.end=events.dn.t.TUP[x]+2)])) / mean(cur.hft.flags.dta) ), 0)
    events.dn.df$TUP.C2.HFT.rel.m.rate <- na.fill (events.dn.df$TUP.C2.HFT.m.rate / 
      (events.dn.df$PREP.HFT.m.rate * sapply( 1:nrow(events.dn.df), function(x) 
      mean(cur.hft.mins.dta[DayBoundedBarSeq(events.dn.df$timestamp[x], 
        t.start=events.dn.t.TUP[x]-2, t.end=events.dn.t.TUP[x]+2)])) / mean(cur.hft.mins.dta) ), 0)
    events.dn.df$TUP.L60.HFT.rel.f.rate <- na.fill (events.dn.df$TUP.L60.HFT.f.rate / 
      (events.dn.df$PREP.HFT.f.rate * sapply( 1:nrow(events.dn.df), function(x) 
      mean(cur.hft.flags.dta[DayBoundedBarSeq(events.dn.df$timestamp[x], 
        t.start=events.dn.t.TUP[x]-60, t.end=events.dn.t.TUP[x]-1)])) / mean(cur.hft.flags.dta) ), 0)
    events.dn.df$TUP.L60.HFT.rel.m.rate <- na.fill (events.dn.df$TUP.L60.HFT.m.rate / 
      (events.dn.df$PREP.HFT.m.rate * sapply( 1:nrow(events.dn.df), function(x) 
      mean(cur.hft.mins.dta[DayBoundedBarSeq(events.dn.df$timestamp[x], 
        t.start=events.dn.t.TUP[x]-60, t.end=events.dn.t.TUP[x]-1)])) / mean(cur.hft.mins.dta) ), 0)
    events.dn.df$TUP.L30.HFT.rel.f.rate <- na.fill (events.dn.df$TUP.L30.HFT.f.rate / 
      (events.dn.df$PREP.HFT.f.rate * sapply( 1:nrow(events.dn.df), function(x) 
      mean(cur.hft.flags.dta[DayBoundedBarSeq(events.dn.df$timestamp[x], 
        t.start=events.dn.t.TUP[x]-30, t.end=events.dn.t.TUP[x]-1)])) / mean(cur.hft.flags.dta) ), 0)
    events.dn.df$TUP.L30.HFT.rel.m.rate <- na.fill (events.dn.df$TUP.L30.HFT.m.rate / 
      (events.dn.df$PREP.HFT.m.rate * sapply( 1:nrow(events.dn.df), function(x) 
      mean(cur.hft.mins.dta[DayBoundedBarSeq(events.dn.df$timestamp[x], 
        t.start=events.dn.t.TUP[x]-30, t.end=events.dn.t.TUP[x]-1)])) / mean(cur.hft.mins.dta) ), 0)
    events.dn.df$TUP.L15.HFT.rel.f.rate <- na.fill (events.dn.df$TUP.L15.HFT.f.rate / 
      (events.dn.df$PREP.HFT.f.rate * sapply( 1:nrow(events.dn.df), function(x) 
      mean(cur.hft.flags.dta[DayBoundedBarSeq(events.dn.df$timestamp[x], 
        t.start=events.dn.t.TUP[x]-15, t.end=events.dn.t.TUP[x]-1)])) / mean(cur.hft.flags.dta) ), 0)
    events.dn.df$TUP.L15.HFT.rel.m.rate <- na.fill (events.dn.df$TUP.L15.HFT.m.rate / 
      (events.dn.df$PREP.HFT.m.rate * sapply( 1:nrow(events.dn.df), function(x) 
      mean(cur.hft.mins.dta[DayBoundedBarSeq(events.dn.df$timestamp[x], 
        t.start=events.dn.t.TUP[x]-15, t.end=events.dn.t.TUP[x]-1)])) / mean(cur.hft.mins.dta) ), 0)
    events.dn.df$TUP.L10.HFT.rel.f.rate <- na.fill (events.dn.df$TUP.L10.HFT.f.rate / 
      (events.dn.df$PREP.HFT.f.rate * sapply( 1:nrow(events.dn.df), function(x) 
      mean(cur.hft.flags.dta[DayBoundedBarSeq(events.dn.df$timestamp[x], 
        t.start=events.dn.t.TUP[x]-10, t.end=events.dn.t.TUP[x]-1)])) / mean(cur.hft.flags.dta) ), 0)
    events.dn.df$TUP.L10.HFT.rel.m.rate <- na.fill (events.dn.df$TUP.L10.HFT.m.rate / 
      (events.dn.df$PREP.HFT.m.rate * sapply( 1:nrow(events.dn.df), function(x) 
      mean(cur.hft.mins.dta[DayBoundedBarSeq(events.dn.df$timestamp[x], 
        t.start=events.dn.t.TUP[x]-10, t.end=events.dn.t.TUP[x]-1)])) / mean(cur.hft.mins.dta) ), 0)
    events.dn.df$TUP.L05.HFT.rel.f.rate <- na.fill (events.dn.df$TUP.L05.HFT.f.rate / 
      (events.dn.df$PREP.HFT.f.rate * sapply( 1:nrow(events.dn.df), function(x) 
      mean(cur.hft.flags.dta[DayBoundedBarSeq(events.dn.df$timestamp[x], 
        t.start=events.dn.t.TUP[x]-5, t.end=events.dn.t.TUP[x]-1)])) / mean(cur.hft.flags.dta) ), 0)
    events.dn.df$TUP.L05.HFT.rel.m.rate <- na.fill (events.dn.df$TUP.L05.HFT.m.rate / 
      (events.dn.df$PREP.HFT.m.rate * sapply( 1:nrow(events.dn.df), function(x) 
      mean(cur.hft.mins.dta[DayBoundedBarSeq(events.dn.df$timestamp[x], 
        t.start=events.dn.t.TUP[x]-5, t.end=events.dn.t.TUP[x]-1)])) / mean(cur.hft.mins.dta) ), 0)
    events.dn.df$TUP.R05.HFT.rel.f.rate <- na.fill (events.dn.df$TUP.R05.HFT.f.rate / 
      (events.dn.df$PREP.HFT.f.rate * sapply( 1:nrow(events.dn.df), function(x) 
      mean(cur.hft.flags.dta[DayBoundedBarSeq(events.dn.df$timestamp[x], 
        t.start=events.dn.t.TUP[x]+1, t.end=events.dn.t.TUP[x]+5)])) / mean(cur.hft.flags.dta) ), 0)
    events.dn.df$TUP.R05.HFT.rel.m.rate <- na.fill (events.dn.df$TUP.R05.HFT.m.rate / 
      (events.dn.df$PREP.HFT.m.rate * sapply( 1:nrow(events.dn.df), function(x) 
      mean(cur.hft.mins.dta[DayBoundedBarSeq(events.dn.df$timestamp[x], 
        t.start=events.dn.t.TUP[x]+1, t.end=events.dn.t.TUP[x]+5)])) / mean(cur.hft.mins.dta) ), 0)
    events.dn.df$TUP.R10.HFT.rel.f.rate <- na.fill (events.dn.df$TUP.R10.HFT.f.rate / 
      (events.dn.df$PREP.HFT.f.rate * sapply( 1:nrow(events.dn.df), function(x) 
      mean(cur.hft.flags.dta[DayBoundedBarSeq(events.dn.df$timestamp[x], 
        t.start=events.dn.t.TUP[x]+1, t.end=events.dn.t.TUP[x]+10)])) / mean(cur.hft.flags.dta) ), 0)
    events.dn.df$TUP.R10.HFT.rel.m.rate <- na.fill (events.dn.df$TUP.R10.HFT.m.rate / 
      (events.dn.df$PREP.HFT.m.rate * sapply( 1:nrow(events.dn.df), function(x) 
      mean(cur.hft.mins.dta[DayBoundedBarSeq(events.dn.df$timestamp[x], 
        t.start=events.dn.t.TUP[x]+1, t.end=events.dn.t.TUP[x]+10)])) / mean(cur.hft.mins.dta) ), 0)
    events.dn.df$TUP.R15.HFT.rel.f.rate <- na.fill (events.dn.df$TUP.R15.HFT.f.rate / 
      (events.dn.df$PREP.HFT.f.rate * sapply( 1:nrow(events.dn.df), function(x) 
      mean(cur.hft.flags.dta[DayBoundedBarSeq(events.dn.df$timestamp[x], 
        t.start=events.dn.t.TUP[x]+1, t.end=events.dn.t.TUP[x]+15)])) / mean(cur.hft.flags.dta) ), 0)
    events.dn.df$TUP.R15.HFT.rel.m.rate <- na.fill (events.dn.df$TUP.R15.HFT.m.rate / 
      (events.dn.df$PREP.HFT.m.rate * sapply( 1:nrow(events.dn.df), function(x) 
      mean(cur.hft.mins.dta[DayBoundedBarSeq(events.dn.df$timestamp[x], 
        t.start=events.dn.t.TUP[x]+1, t.end=events.dn.t.TUP[x]+15)])) / mean(cur.hft.mins.dta) ), 0)
  }
  # end of 3f.2b) HFT activity rel vs. dta
  
  # 3f.2c) HFT rates vs. volume abs ------------------------------------------
  if (length(id.events.idx.up) > 0) {
    events.up.df$ECP.HFT.pf.rate <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.up.df)), t.end=events.up.t.TUP,
      MoreArgs = list (df=events.up.df, varname="hftpf", t.start=0))
    events.up.df$ECP.HFT.pq.rate <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.up.df)), t.end=events.up.t.TUP,
      MoreArgs = list (df=events.up.df, varname="hftpq", t.start=0))    
    # ERP
    events.up.df$ERP.HFT.pf.rate <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP, t.end=events.up.t.ERP,
      MoreArgs = list (df=events.up.df, varname="hftpf"))
    events.up.df$ERP.HFT.pq.rate <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP, t.end=events.up.t.ERP,
      MoreArgs = list (df=events.up.df, varname="hftpq"))        
    # TUP.X
    events.up.df$TUP.C5.HFT.pf.rate <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP-5, t.end=events.up.t.TUP+5,
      MoreArgs = list (df=events.up.df, varname="hftpf"))
    events.up.df$TUP.C5.HFT.pq.rate <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP-5, t.end=events.up.t.TUP+5,
      MoreArgs = list (df=events.up.df, varname="hftpq"))
    events.up.df$TUP.C2.HFT.pf.rate <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP-2, t.end=events.up.t.TUP+2,
      MoreArgs = list (df=events.up.df, varname="hftpf"))
    events.up.df$TUP.C2.HFT.pq.rate <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP-2, t.end=events.up.t.TUP+2,
      MoreArgs = list (df=events.up.df, varname="hftpq"))
    events.up.df$TUP.L60.HFT.pf.rate <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP-60, t.end=events.up.t.TUP-1,
      MoreArgs = list (df=events.up.df, varname="hftpf"))
    events.up.df$TUP.L60.HFT.pq.rate <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP-60, t.end=events.up.t.TUP-1,
      MoreArgs = list (df=events.up.df, varname="hftpq"))
    events.up.df$TUP.L30.HFT.pf.rate <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP-30, t.end=events.up.t.TUP-1,
      MoreArgs = list (df=events.up.df, varname="hftpf"))
    events.up.df$TUP.L30.HFT.pq.rate <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP-30, t.end=events.up.t.TUP-1,
      MoreArgs = list (df=events.up.df, varname="hftpq"))
    events.up.df$TUP.L15.HFT.pf.rate <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP-15, t.end=events.up.t.TUP-1,
      MoreArgs = list (df=events.up.df, varname="hftpf"))
    events.up.df$TUP.L15.HFT.pq.rate <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP-15, t.end=events.up.t.TUP-1,
      MoreArgs = list (df=events.up.df, varname="hftpq"))
    events.up.df$TUP.L10.HFT.pf.rate <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP-10, t.end=events.up.t.TUP-1,
      MoreArgs = list (df=events.up.df, varname="hftpf"))
    events.up.df$TUP.L10.HFT.pq.rate <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP-10, t.end=events.up.t.TUP-1,
      MoreArgs = list (df=events.up.df, varname="hftpq"))
    events.up.df$TUP.L05.HFT.pf.rate <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP-5, t.end=events.up.t.TUP-1,
      MoreArgs = list (df=events.up.df, varname="hftpf"))
    events.up.df$TUP.L05.HFT.pq.rate <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP-5, t.end=events.up.t.TUP-1,
      MoreArgs = list (df=events.up.df, varname="hftpq"))
    events.up.df$TUP.R05.HFT.pf.rate <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP+1, t.end=events.up.t.TUP+5,
      MoreArgs = list (df=events.up.df, varname="hftpf"))
    events.up.df$TUP.R05.HFT.pq.rate <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP+1, t.end=events.up.t.TUP+5,
      MoreArgs = list (df=events.up.df, varname="hftpq"))
    events.up.df$TUP.R10.HFT.pf.rate <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP+1, t.end=events.up.t.TUP+10,
      MoreArgs = list (df=events.up.df, varname="hftpf"))
    events.up.df$TUP.R10.HFT.pq.rate <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP+1, t.end=events.up.t.TUP+10,
      MoreArgs = list (df=events.up.df, varname="hftpq"))
    events.up.df$TUP.R15.HFT.pf.rate <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP+1, t.end=events.up.t.TUP+15,
      MoreArgs = list (df=events.up.df, varname="hftpf"))
    events.up.df$TUP.R15.HFT.pq.rate <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.up.df)), t.start=events.up.t.TUP+1, t.end=events.up.t.TUP+15,
      MoreArgs = list (df=events.up.df, varname="hftpq"))
  }
  if (length(id.events.idx.dn) > 0) {
    events.dn.df$ECP.HFT.pf.rate <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.dn.df)), t.end=events.dn.t.TUP,
      MoreArgs = list (df=events.dn.df, varname="hftpf", t.start=0))
    events.dn.df$ECP.HFT.pq.rate <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.dn.df)), t.end=events.dn.t.TUP,
      MoreArgs = list (df=events.dn.df, varname="hftpq", t.start=0))    
    # ERP
    events.dn.df$ERP.HFT.pf.rate <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP, t.end=events.dn.t.ERP,
      MoreArgs = list (df=events.dn.df, varname="hftpf"))
    events.dn.df$ERP.HFT.pq.rate <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP, t.end=events.dn.t.ERP,
      MoreArgs = list (df=events.dn.df, varname="hftpq"))        
    # TUP.X
    events.dn.df$TUP.C5.HFT.pf.rate <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP-5, t.end=events.dn.t.TUP+5,
      MoreArgs = list (df=events.dn.df, varname="hftpf"))
    events.dn.df$TUP.C5.HFT.pq.rate <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP-5, t.end=events.dn.t.TUP+5,
      MoreArgs = list (df=events.dn.df, varname="hftpq"))
    events.dn.df$TUP.C2.HFT.pf.rate <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP-2, t.end=events.dn.t.TUP+2,
      MoreArgs = list (df=events.dn.df, varname="hftpf"))
    events.dn.df$TUP.C2.HFT.pq.rate <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP-2, t.end=events.dn.t.TUP+2,
      MoreArgs = list (df=events.dn.df, varname="hftpq"))
    events.dn.df$TUP.L60.HFT.pf.rate <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP-60, t.end=events.dn.t.TUP-1,
      MoreArgs = list (df=events.dn.df, varname="hftpf"))
    events.dn.df$TUP.L60.HFT.pq.rate <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP-60, t.end=events.dn.t.TUP-1,
      MoreArgs = list (df=events.dn.df, varname="hftpq"))
    events.dn.df$TUP.L30.HFT.pf.rate <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP-30, t.end=events.dn.t.TUP-1,
      MoreArgs = list (df=events.dn.df, varname="hftpf"))
    events.dn.df$TUP.L30.HFT.pq.rate <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP-30, t.end=events.dn.t.TUP-1,
      MoreArgs = list (df=events.dn.df, varname="hftpq"))
    events.dn.df$TUP.L15.HFT.pf.rate <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP-15, t.end=events.dn.t.TUP-1,
      MoreArgs = list (df=events.dn.df, varname="hftpf"))
    events.dn.df$TUP.L15.HFT.pq.rate <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP-15, t.end=events.dn.t.TUP-1,
      MoreArgs = list (df=events.dn.df, varname="hftpq"))
    events.dn.df$TUP.L10.HFT.pf.rate <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP-10, t.end=events.dn.t.TUP-1,
      MoreArgs = list (df=events.dn.df, varname="hftpf"))
    events.dn.df$TUP.L10.HFT.pq.rate <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP-10, t.end=events.dn.t.TUP-1,
      MoreArgs = list (df=events.dn.df, varname="hftpq"))
    events.dn.df$TUP.L05.HFT.pf.rate <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP-5, t.end=events.dn.t.TUP-1,
      MoreArgs = list (df=events.dn.df, varname="hftpf"))
    events.dn.df$TUP.L05.HFT.pq.rate <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP-5, t.end=events.dn.t.TUP-1,
      MoreArgs = list (df=events.dn.df, varname="hftpq"))
    events.dn.df$TUP.R05.HFT.pf.rate <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP+1, t.end=events.dn.t.TUP+5,
      MoreArgs = list (df=events.dn.df, varname="hftpf"))
    events.dn.df$TUP.R05.HFT.pq.rate <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP+1, t.end=events.dn.t.TUP+5,
      MoreArgs = list (df=events.dn.df, varname="hftpq"))
    events.dn.df$TUP.R10.HFT.pf.rate <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP+1, t.end=events.dn.t.TUP+10,
      MoreArgs = list (df=events.dn.df, varname="hftpf"))
    events.dn.df$TUP.R10.HFT.pq.rate <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP+1, t.end=events.dn.t.TUP+10,
      MoreArgs = list (df=events.dn.df, varname="hftpq"))
    events.dn.df$TUP.R15.HFT.pf.rate <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP+1, t.end=events.dn.t.TUP+15,
      MoreArgs = list (df=events.dn.df, varname="hftpf"))
    events.dn.df$TUP.R15.HFT.pq.rate <- mapply (FUN=GetTseriesAvg, 
      row=c(1:nrow(events.dn.df)), t.start=events.dn.t.TUP+1, t.end=events.dn.t.TUP+15,
      MoreArgs = list (df=events.dn.df, varname="hftpq"))
  }
  # end of 3f.2c) HFT rates vs. volume abs
  
  # 3f.2d) HFT rates vs. volume rel vs. dta ------------------------------------
  # divide abs. rates by: PREP HFT * mean(dta profile[PREWxx])
  if (length(id.events.idx.up) > 0) {
    # ECP
    events.up.df$ECP.HFT.rel.pf.rate <- na.fill (events.up.df$ECP.HFT.pf.rate / 
      (events.up.df$PREP.HFT.pf.rate * sapply( 1:nrow(events.up.df), function(x) 
        mean(cur.hft.pf.dta[DayBoundedBarSeq(events.up.df$timestamp[x], 
             t.start=0, t.end=events.up.t.TUP[x])])) / mean(cur.hft.pf.dta) ), 0)
    events.up.df$ECP.HFT.rel.pq.rate <- na.fill (events.up.df$ECP.HFT.pq.rate / 
      (events.up.df$PREP.HFT.pq.rate * sapply( 1:nrow(events.up.df), function(x) 
        mean(cur.hft.pq.dta[DayBoundedBarSeq(events.up.df$timestamp[x], 
             t.start=0, t.end=events.up.t.TUP[x])])) / mean(cur.hft.pq.dta) ), 0)
    # ERP
    events.up.df$ERP.HFT.rel.pf.rate <- na.fill (events.up.df$ERP.HFT.pf.rate / 
      (events.up.df$PREP.HFT.pf.rate * sapply( 1:nrow(events.up.df), function(x) 
      mean(cur.hft.pf.dta[DayBoundedBarSeq(events.up.df$timestamp[x], 
        t.start=events.up.t.TUP[x], t.end=events.up.t.ERP[x])])) / mean(cur.hft.pf.dta) ), 0)
    events.up.df$ERP.HFT.rel.pq.rate <- na.fill (events.up.df$ERP.HFT.pq.rate / 
      (events.up.df$PREP.HFT.pq.rate * sapply( 1:nrow(events.up.df), function(x) 
      mean(cur.hft.pq.dta[DayBoundedBarSeq(events.up.df$timestamp[x], 
        t.start=events.up.t.TUP[x], t.end=events.up.t.ERP[x])])) / mean(cur.hft.pq.dta) ), 0)
    # TUP.X
    events.up.df$TUP.C5.HFT.rel.pf.rate <- na.fill (events.up.df$TUP.C5.HFT.pf.rate / 
      (events.up.df$PREP.HFT.pf.rate * sapply( 1:nrow(events.up.df), function(x) 
      mean(cur.hft.pf.dta[DayBoundedBarSeq(events.up.df$timestamp[x], 
        t.start=events.up.t.TUP[x]-5, t.end=events.up.t.TUP[x]+5)])) / mean(cur.hft.pf.dta) ), 0)
    events.up.df$TUP.C5.HFT.rel.pq.rate <- na.fill (events.up.df$TUP.C5.HFT.pq.rate / 
      (events.up.df$PREP.HFT.pq.rate * sapply( 1:nrow(events.up.df), function(x) 
      mean(cur.hft.pq.dta[DayBoundedBarSeq(events.up.df$timestamp[x], 
        t.start=events.up.t.TUP[x]-5, t.end=events.up.t.TUP[x]+5)])) / mean(cur.hft.pq.dta) ), 0)
    events.up.df$TUP.C2.HFT.rel.pf.rate <- na.fill (events.up.df$TUP.C2.HFT.pf.rate / 
      (events.up.df$PREP.HFT.pf.rate * sapply( 1:nrow(events.up.df), function(x) 
      mean(cur.hft.pf.dta[DayBoundedBarSeq(events.up.df$timestamp[x], 
        t.start=events.up.t.TUP[x]-2, t.end=events.up.t.TUP[x]+2)])) / mean(cur.hft.pf.dta) ), 0)
    events.up.df$TUP.C2.HFT.rel.pq.rate <- na.fill (events.up.df$TUP.C2.HFT.pq.rate / 
      (events.up.df$PREP.HFT.pq.rate * sapply( 1:nrow(events.up.df), function(x) 
      mean(cur.hft.pq.dta[DayBoundedBarSeq(events.up.df$timestamp[x], 
        t.start=events.up.t.TUP[x]-2, t.end=events.up.t.TUP[x]+2)])) / mean(cur.hft.pq.dta) ), 0)
    events.up.df$TUP.L60.HFT.rel.pf.rate <- na.fill (events.up.df$TUP.L60.HFT.pf.rate / 
      (events.up.df$PREP.HFT.pf.rate * sapply( 1:nrow(events.up.df), function(x) 
      mean(cur.hft.pf.dta[DayBoundedBarSeq(events.up.df$timestamp[x], 
        t.start=events.up.t.TUP[x]-60, t.end=events.up.t.TUP[x]-1)])) / mean(cur.hft.pf.dta) ), 0)
    events.up.df$TUP.L60.HFT.rel.pq.rate <- na.fill (events.up.df$TUP.L60.HFT.pq.rate / 
      (events.up.df$PREP.HFT.pq.rate * sapply( 1:nrow(events.up.df), function(x) 
      mean(cur.hft.pq.dta[DayBoundedBarSeq(events.up.df$timestamp[x], 
        t.start=events.up.t.TUP[x]-60, t.end=events.up.t.TUP[x]-1)])) / mean(cur.hft.pq.dta) ), 0)
    events.up.df$TUP.L30.HFT.rel.pf.rate <- na.fill (events.up.df$TUP.L30.HFT.pf.rate / 
      (events.up.df$PREP.HFT.pf.rate * sapply( 1:nrow(events.up.df), function(x) 
      mean(cur.hft.pf.dta[DayBoundedBarSeq(events.up.df$timestamp[x], 
        t.start=events.up.t.TUP[x]-30, t.end=events.up.t.TUP[x]-1)])) / mean(cur.hft.pf.dta) ), 0)
    events.up.df$TUP.L30.HFT.rel.pq.rate <- na.fill (events.up.df$TUP.L30.HFT.pq.rate / 
      (events.up.df$PREP.HFT.pq.rate * sapply( 1:nrow(events.up.df), function(x) 
      mean(cur.hft.pq.dta[DayBoundedBarSeq(events.up.df$timestamp[x], 
        t.start=events.up.t.TUP[x]-30, t.end=events.up.t.TUP[x]-1)])) / mean(cur.hft.pq.dta) ), 0)
    events.up.df$TUP.L15.HFT.rel.pf.rate <- na.fill (events.up.df$TUP.L15.HFT.pf.rate / 
      (events.up.df$PREP.HFT.pf.rate * sapply( 1:nrow(events.up.df), function(x) 
      mean(cur.hft.pf.dta[DayBoundedBarSeq(events.up.df$timestamp[x], 
        t.start=events.up.t.TUP[x]-15, t.end=events.up.t.TUP[x]-1)])) / mean(cur.hft.pf.dta) ), 0)
    events.up.df$TUP.L15.HFT.rel.pq.rate <- na.fill (events.up.df$TUP.L15.HFT.pq.rate / 
      (events.up.df$PREP.HFT.pq.rate * sapply( 1:nrow(events.up.df), function(x) 
      mean(cur.hft.pq.dta[DayBoundedBarSeq(events.up.df$timestamp[x], 
        t.start=events.up.t.TUP[x]-15, t.end=events.up.t.TUP[x]-1)])) / mean(cur.hft.pq.dta) ), 0)
    events.up.df$TUP.L10.HFT.rel.pf.rate <- na.fill (events.up.df$TUP.L10.HFT.pf.rate / 
      (events.up.df$PREP.HFT.pf.rate * sapply( 1:nrow(events.up.df), function(x) 
      mean(cur.hft.pf.dta[DayBoundedBarSeq(events.up.df$timestamp[x], 
        t.start=events.up.t.TUP[x]-10, t.end=events.up.t.TUP[x]-1)])) / mean(cur.hft.pf.dta) ), 0)
    events.up.df$TUP.L10.HFT.rel.pq.rate <- na.fill (events.up.df$TUP.L10.HFT.pq.rate / 
      (events.up.df$PREP.HFT.pq.rate * sapply( 1:nrow(events.up.df), function(x) 
      mean(cur.hft.pq.dta[DayBoundedBarSeq(events.up.df$timestamp[x], 
        t.start=events.up.t.TUP[x]-10, t.end=events.up.t.TUP[x]-1)])) / mean(cur.hft.pq.dta) ), 0)
    events.up.df$TUP.L05.HFT.rel.pf.rate <- na.fill (events.up.df$TUP.L05.HFT.pf.rate / 
      (events.up.df$PREP.HFT.pf.rate * sapply( 1:nrow(events.up.df), function(x) 
      mean(cur.hft.pf.dta[DayBoundedBarSeq(events.up.df$timestamp[x], 
        t.start=events.up.t.TUP[x]-5, t.end=events.up.t.TUP[x]-1)])) / mean(cur.hft.pf.dta) ), 0)
    events.up.df$TUP.L05.HFT.rel.pq.rate <- na.fill (events.up.df$TUP.L05.HFT.pq.rate / 
      (events.up.df$PREP.HFT.pq.rate * sapply( 1:nrow(events.up.df), function(x) 
      mean(cur.hft.pq.dta[DayBoundedBarSeq(events.up.df$timestamp[x], 
        t.start=events.up.t.TUP[x]-5, t.end=events.up.t.TUP[x]-1)])) / mean(cur.hft.pq.dta) ), 0)
    events.up.df$TUP.R05.HFT.rel.pf.rate <- na.fill (events.up.df$TUP.R05.HFT.pf.rate / 
      (events.up.df$PREP.HFT.pf.rate * sapply( 1:nrow(events.up.df), function(x) 
      mean(cur.hft.pf.dta[DayBoundedBarSeq(events.up.df$timestamp[x], 
        t.start=events.up.t.TUP[x]+1, t.end=events.up.t.TUP[x]+5)])) / mean(cur.hft.pf.dta) ), 0)
    events.up.df$TUP.R05.HFT.rel.pq.rate <- na.fill (events.up.df$TUP.R05.HFT.pq.rate / 
      (events.up.df$PREP.HFT.pq.rate * sapply( 1:nrow(events.up.df), function(x) 
      mean(cur.hft.pq.dta[DayBoundedBarSeq(events.up.df$timestamp[x], 
        t.start=events.up.t.TUP[x]+1, t.end=events.up.t.TUP[x]+5)])) / mean(cur.hft.pq.dta) ), 0)
    events.up.df$TUP.R10.HFT.rel.pf.rate <- na.fill (events.up.df$TUP.R10.HFT.pf.rate / 
      (events.up.df$PREP.HFT.pf.rate * sapply( 1:nrow(events.up.df), function(x) 
      mean(cur.hft.pf.dta[DayBoundedBarSeq(events.up.df$timestamp[x], 
        t.start=events.up.t.TUP[x]+1, t.end=events.up.t.TUP[x]+10)])) / mean(cur.hft.pf.dta) ), 0)
    events.up.df$TUP.R10.HFT.rel.pq.rate <- na.fill (events.up.df$TUP.R10.HFT.pq.rate / 
      (events.up.df$PREP.HFT.pq.rate * sapply( 1:nrow(events.up.df), function(x) 
      mean(cur.hft.pq.dta[DayBoundedBarSeq(events.up.df$timestamp[x], 
        t.start=events.up.t.TUP[x]+1, t.end=events.up.t.TUP[x]+10)])) / mean(cur.hft.pq.dta) ), 0)
    events.up.df$TUP.R15.HFT.rel.pf.rate <- na.fill (events.up.df$TUP.R15.HFT.pf.rate / 
      (events.up.df$PREP.HFT.pf.rate * sapply( 1:nrow(events.up.df), function(x) 
      mean(cur.hft.pf.dta[DayBoundedBarSeq(events.up.df$timestamp[x], 
        t.start=events.up.t.TUP[x]+1, t.end=events.up.t.TUP[x]+15)])) / mean(cur.hft.pf.dta) ), 0)
    events.up.df$TUP.R15.HFT.rel.pq.rate <- na.fill (events.up.df$TUP.R15.HFT.pq.rate / 
      (events.up.df$PREP.HFT.pq.rate * sapply( 1:nrow(events.up.df), function(x) 
      mean(cur.hft.pq.dta[DayBoundedBarSeq(events.up.df$timestamp[x], 
        t.start=events.up.t.TUP[x]+1, t.end=events.up.t.TUP[x]+15)])) / mean(cur.hft.pq.dta) ), 0)
  }  
  if (length(id.events.idx.dn) > 0) {
    # ECP
    events.dn.df$ECP.HFT.rel.pf.rate <- na.fill (events.dn.df$ECP.HFT.pf.rate / 
      (events.dn.df$PREP.HFT.pf.rate * sapply( 1:nrow(events.dn.df), function(x) 
        mean(cur.hft.pf.dta[DayBoundedBarSeq(events.dn.df$timestamp[x], 
             t.start=0, t.end=events.dn.t.TUP[x])])) / mean(cur.hft.pf.dta) ), 0)
    events.dn.df$ECP.HFT.rel.pq.rate <- na.fill (events.dn.df$ECP.HFT.pq.rate / 
      (events.dn.df$PREP.HFT.pq.rate * sapply( 1:nrow(events.dn.df), function(x) 
        mean(cur.hft.pq.dta[DayBoundedBarSeq(events.dn.df$timestamp[x], 
             t.start=0, t.end=events.dn.t.TUP[x])])) / mean(cur.hft.pq.dta) ), 0)
    # ERP
    events.dn.df$ECP.HFT.rel.pf.rate <- na.fill (events.dn.df$ECP.HFT.pf.rate / 
      (events.dn.df$PREP.HFT.pf.rate * sapply( 1:nrow(events.dn.df), function(x) 
      mean(cur.hft.pf.dta[DayBoundedBarSeq(events.dn.df$timestamp[x], 
        t.start=events.dn.t.TUP[x], t.end=events.dn.t.ERP[x])])) / mean(cur.hft.pf.dta) ), 0)
    events.dn.df$ECP.HFT.rel.pq.rate <- na.fill (events.dn.df$ECP.HFT.pq.rate / 
      (events.dn.df$PREP.HFT.pq.rate * sapply( 1:nrow(events.dn.df), function(x) 
      mean(cur.hft.pq.dta[DayBoundedBarSeq(events.dn.df$timestamp[x], 
        t.start=events.dn.t.TUP[x], t.end=events.dn.t.ERP[x])])) / mean(cur.hft.pq.dta) ), 0)
    # TUP.X
    events.dn.df$TUP.C5.HFT.rel.pf.rate <- na.fill (events.dn.df$TUP.C5.HFT.pf.rate / 
      (events.dn.df$PREP.HFT.pf.rate * sapply( 1:nrow(events.dn.df), function(x) 
      mean(cur.hft.pf.dta[DayBoundedBarSeq(events.dn.df$timestamp[x], 
        t.start=events.dn.t.TUP[x]-5, t.end=events.dn.t.TUP[x]+5)])) / mean(cur.hft.pf.dta) ), 0)
    events.dn.df$TUP.C5.HFT.rel.pq.rate <- na.fill (events.dn.df$TUP.C5.HFT.pq.rate / 
      (events.dn.df$PREP.HFT.pq.rate * sapply( 1:nrow(events.dn.df), function(x) 
      mean(cur.hft.pq.dta[DayBoundedBarSeq(events.dn.df$timestamp[x], 
        t.start=events.dn.t.TUP[x]-5, t.end=events.dn.t.TUP[x]+5)])) / mean(cur.hft.pq.dta) ), 0)
    events.dn.df$TUP.C2.HFT.rel.pf.rate <- na.fill (events.dn.df$TUP.C2.HFT.pf.rate / 
      (events.dn.df$PREP.HFT.pf.rate * sapply( 1:nrow(events.dn.df), function(x) 
      mean(cur.hft.pf.dta[DayBoundedBarSeq(events.dn.df$timestamp[x], 
        t.start=events.dn.t.TUP[x]-2, t.end=events.dn.t.TUP[x]+2)])) / mean(cur.hft.pf.dta) ), 0)
    events.dn.df$TUP.C2.HFT.rel.pq.rate <- na.fill (events.dn.df$TUP.C2.HFT.pq.rate / 
      (events.dn.df$PREP.HFT.pq.rate * sapply( 1:nrow(events.dn.df), function(x) 
      mean(cur.hft.pq.dta[DayBoundedBarSeq(events.dn.df$timestamp[x], 
        t.start=events.dn.t.TUP[x]-2, t.end=events.dn.t.TUP[x]+2)])) / mean(cur.hft.pq.dta) ), 0)
    events.dn.df$TUP.L60.HFT.rel.pf.rate <- na.fill (events.dn.df$TUP.L60.HFT.pf.rate / 
      (events.dn.df$PREP.HFT.pf.rate * sapply( 1:nrow(events.dn.df), function(x) 
      mean(cur.hft.pf.dta[DayBoundedBarSeq(events.dn.df$timestamp[x], 
        t.start=events.dn.t.TUP[x]-60, t.end=events.dn.t.TUP[x]-1)])) / mean(cur.hft.pf.dta) ), 0)
    events.dn.df$TUP.L60.HFT.rel.pq.rate <- na.fill (events.dn.df$TUP.L60.HFT.pq.rate / 
      (events.dn.df$PREP.HFT.pq.rate * sapply( 1:nrow(events.dn.df), function(x) 
      mean(cur.hft.pq.dta[DayBoundedBarSeq(events.dn.df$timestamp[x], 
        t.start=events.dn.t.TUP[x]-60, t.end=events.dn.t.TUP[x]-1)])) / mean(cur.hft.pq.dta) ), 0)
    events.dn.df$TUP.L30.HFT.rel.pf.rate <- na.fill (events.dn.df$TUP.L30.HFT.pf.rate / 
      (events.dn.df$PREP.HFT.pf.rate * sapply( 1:nrow(events.dn.df), function(x) 
      mean(cur.hft.pf.dta[DayBoundedBarSeq(events.dn.df$timestamp[x], 
        t.start=events.dn.t.TUP[x]-30, t.end=events.dn.t.TUP[x]-1)])) / mean(cur.hft.pf.dta) ), 0)
    events.dn.df$TUP.L30.HFT.rel.pq.rate <- na.fill (events.dn.df$TUP.L30.HFT.pq.rate / 
      (events.dn.df$PREP.HFT.pq.rate * sapply( 1:nrow(events.dn.df), function(x) 
      mean(cur.hft.pq.dta[DayBoundedBarSeq(events.dn.df$timestamp[x], 
        t.start=events.dn.t.TUP[x]-30, t.end=events.dn.t.TUP[x]-1)])) / mean(cur.hft.pq.dta) ), 0)
    events.dn.df$TUP.L15.HFT.rel.pf.rate <- na.fill (events.dn.df$TUP.L15.HFT.pf.rate / 
      (events.dn.df$PREP.HFT.pf.rate * sapply( 1:nrow(events.dn.df), function(x) 
      mean(cur.hft.pf.dta[DayBoundedBarSeq(events.dn.df$timestamp[x], 
        t.start=events.dn.t.TUP[x]-15, t.end=events.dn.t.TUP[x]-1)])) / mean(cur.hft.pf.dta) ), 0)
    events.dn.df$TUP.L15.HFT.rel.pq.rate <- na.fill (events.dn.df$TUP.L15.HFT.pq.rate / 
      (events.dn.df$PREP.HFT.pq.rate * sapply( 1:nrow(events.dn.df), function(x) 
      mean(cur.hft.pq.dta[DayBoundedBarSeq(events.dn.df$timestamp[x], 
        t.start=events.dn.t.TUP[x]-15, t.end=events.dn.t.TUP[x]-1)])) / mean(cur.hft.pq.dta) ), 0)
    events.dn.df$TUP.L10.HFT.rel.pf.rate <- na.fill (events.dn.df$TUP.L10.HFT.pf.rate / 
      (events.dn.df$PREP.HFT.pf.rate * sapply( 1:nrow(events.dn.df), function(x) 
      mean(cur.hft.pf.dta[DayBoundedBarSeq(events.dn.df$timestamp[x], 
        t.start=events.dn.t.TUP[x]-10, t.end=events.dn.t.TUP[x]-1)])) / mean(cur.hft.pf.dta) ), 0)
    events.dn.df$TUP.L10.HFT.rel.pq.rate <- na.fill (events.dn.df$TUP.L10.HFT.pq.rate / 
      (events.dn.df$PREP.HFT.pq.rate * sapply( 1:nrow(events.dn.df), function(x) 
      mean(cur.hft.pq.dta[DayBoundedBarSeq(events.dn.df$timestamp[x], 
        t.start=events.dn.t.TUP[x]-10, t.end=events.dn.t.TUP[x]-1)])) / mean(cur.hft.pq.dta) ), 0)
    events.dn.df$TUP.L05.HFT.rel.pf.rate <- na.fill (events.dn.df$TUP.L05.HFT.pf.rate / 
      (events.dn.df$PREP.HFT.pf.rate * sapply( 1:nrow(events.dn.df), function(x) 
      mean(cur.hft.pf.dta[DayBoundedBarSeq(events.dn.df$timestamp[x], 
        t.start=events.dn.t.TUP[x]-5, t.end=events.dn.t.TUP[x]-1)])) / mean(cur.hft.pf.dta) ), 0)
    events.dn.df$TUP.L05.HFT.rel.pq.rate <- na.fill (events.dn.df$TUP.L05.HFT.pq.rate / 
      (events.dn.df$PREP.HFT.pq.rate * sapply( 1:nrow(events.dn.df), function(x) 
      mean(cur.hft.pq.dta[DayBoundedBarSeq(events.dn.df$timestamp[x], 
        t.start=events.dn.t.TUP[x]-5, t.end=events.dn.t.TUP[x]-1)])) / mean(cur.hft.pq.dta) ), 0)
    events.dn.df$TUP.R05.HFT.rel.pf.rate <- na.fill (events.dn.df$TUP.R05.HFT.pf.rate / 
      (events.dn.df$PREP.HFT.pf.rate * sapply( 1:nrow(events.dn.df), function(x) 
      mean(cur.hft.pf.dta[DayBoundedBarSeq(events.dn.df$timestamp[x], 
        t.start=events.dn.t.TUP[x]+1, t.end=events.dn.t.TUP[x]+5)])) / mean(cur.hft.pf.dta) ), 0)
    events.dn.df$TUP.R05.HFT.rel.pq.rate <- na.fill (events.dn.df$TUP.R05.HFT.pq.rate / 
      (events.dn.df$PREP.HFT.pq.rate * sapply( 1:nrow(events.dn.df), function(x) 
      mean(cur.hft.pq.dta[DayBoundedBarSeq(events.dn.df$timestamp[x], 
        t.start=events.dn.t.TUP[x]+1, t.end=events.dn.t.TUP[x]+5)])) / mean(cur.hft.pq.dta) ), 0)
    events.dn.df$TUP.R10.HFT.rel.pf.rate <- na.fill (events.dn.df$TUP.R10.HFT.pf.rate / 
      (events.dn.df$PREP.HFT.pf.rate * sapply( 1:nrow(events.dn.df), function(x) 
      mean(cur.hft.pf.dta[DayBoundedBarSeq(events.dn.df$timestamp[x], 
        t.start=events.dn.t.TUP[x]+1, t.end=events.dn.t.TUP[x]+10)])) / mean(cur.hft.pf.dta) ), 0)
    events.dn.df$TUP.R10.HFT.rel.pq.rate <- na.fill (events.dn.df$TUP.R10.HFT.pq.rate / 
      (events.dn.df$PREP.HFT.pq.rate * sapply( 1:nrow(events.dn.df), function(x) 
      mean(cur.hft.pq.dta[DayBoundedBarSeq(events.dn.df$timestamp[x], 
        t.start=events.dn.t.TUP[x]+1, t.end=events.dn.t.TUP[x]+10)])) / mean(cur.hft.pq.dta) ), 0)
    events.dn.df$TUP.R15.HFT.rel.pf.rate <- na.fill (events.dn.df$TUP.R15.HFT.pf.rate / 
      (events.dn.df$PREP.HFT.pf.rate * sapply( 1:nrow(events.dn.df), function(x) 
      mean(cur.hft.pf.dta[DayBoundedBarSeq(events.dn.df$timestamp[x], 
        t.start=events.dn.t.TUP[x]+1, t.end=events.dn.t.TUP[x]+15)])) / mean(cur.hft.pf.dta) ), 0)
    events.dn.df$TUP.R15.HFT.rel.pq.rate <- na.fill (events.dn.df$TUP.R15.HFT.pq.rate / 
      (events.dn.df$PREP.HFT.pq.rate * sapply( 1:nrow(events.dn.df), function(x) 
      mean(cur.hft.pq.dta[DayBoundedBarSeq(events.dn.df$timestamp[x], 
        t.start=events.dn.t.TUP[x]+1, t.end=events.dn.t.TUP[x]+15)])) / mean(cur.hft.pq.dta) ), 0)
  }
  # end of 3f.2d) HFT rates vs. volume rel vs. dta
  
  # 3g) add filter criteria for data errors (bad-ticks) ------------------------
  if (length(id.events.idx.up) > 0) {
    events.up.df["PREW.spike.bar"] <- 100 * vGetTseriesMaxSpikeBar (events.up.df, 
      c(1:nrow(events.up.df)), "r", t.start=-period.id.window, t.end=0)
    events.up.df["POEW.spike.bar"] <- 100 * vGetTseriesMinSpikeBar (events.up.df, 
      c(1:nrow(events.up.df)), "r", t.start=0, t.end=param.post.event.window)
  }
  if (length(id.events.idx.dn) > 0) {  
    events.dn.df["PREW.spike.bar"] <- 100 * vGetTseriesMinSpikeBar (events.dn.df, 
      c(1:nrow(events.dn.df)), "r", t.start=-period.id.window, t.end=0)
    events.dn.df["POEW.spike.bar"] <- 100 * vGetTseriesMaxSpikeBar (events.dn.df, 
      c(1:nrow(events.dn.df)), "r", t.start=0, t.end=param.post.event.window)
  }
  # end of 3g) add filter criteria for data errors
  
  # 4) Write symbol's data frame into global data frame ------------------------
  # Index format: [symbol, timestamp]
  # - for 1st symbol (i = 1) simply copy the data frame, 
  #   adding the symbol as additional index column
  # - for all other symbols (i > 1) add events according to date/time
  if (i == 1) {
    events.master.up.df <- events.up.df
    events.master.dn.df <- events.dn.df
  } else {
    if (length(id.events.idx.up) > 0)
      events.master.up.df <- merge (events.master.up.df, events.up.df, all=TRUE)
    if (length(id.events.idx.dn) > 0)
      events.master.dn.df <- merge (events.master.dn.df, events.dn.df, all=TRUE)
  }
  # end of 4) Write symbol's data frame into global data frame
} # end for (i in score files)
# create combined timestamp/symbol index for master dataframes
row.names (events.master.up.df) <- paste (events.master.up.df$timestamp, 
                                          events.master.up.df$symbol, sep=" ")
row.names (events.master.dn.df) <- paste (events.master.dn.df$timestamp, 
                                          events.master.dn.df$symbol, sep=" ")

# filter databases for price & liquidity criteria & error cut-offs
events.master.up.df.final <- 
  events.master.up.df[(events.master.up.df$price.c1 >= param.min.price) & 
                      (events.master.up.df$PREP.liq.avg >= param.min.liq) & 
                      (events.master.up.df$PREW.spike.bar <= 66.6) &
                      (events.master.up.df$POEW.spike.bar <= 80),]
events.master.dn.df.final <- 
  events.master.dn.df[(events.master.dn.df$price.c1 >= param.min.price) & 
                        (events.master.dn.df$PREP.liq.avg >= param.min.liq) & 
                        (events.master.dn.df$PREW.spike.bar <= 66.6) &
                        (events.master.dn.df$POEW.spike.bar <= 80),]

save (events.master.up.df, events.master.dn.df, hft.flags.sum.x, hft.mins.sum.x,
      active.stocks.sum.x, stocks.prices.df, stocks.sizes.df, stocks.liqs.df,
      stocks.volas.df, stocks.betas.df, stocks.HFTflags.df, stocks.HFTmins.df,
      stocks.HFTpflags.df, stocks.HFTpquotes.df, 
      file=paste(data.path.out, "DB_", param.filt.abs, "_", param.filt.rel,
                 ".idb", sep=""))
} # end of special loop through abs/rel combinations

# ----- FINALIZE ---------------------------------------------------------------
# filter databases for non overlapping event formation windows
# (ATTENTION: working on sorted array will result in "alphabet bias", 
#  selecting more symbols from start of alphabet --> randomization if needed)
events.master.up.df.filt <- FilterOverlappingEvents (events.master.up.df.final, 
                                                     period.id.window)
events.master.dn.df.filt <- FilterOverlappingEvents (events.master.dn.df.final, 
                                                     period.id.window)

