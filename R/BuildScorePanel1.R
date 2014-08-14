# ******************************************************************************
# Intraday Overreaction - BuildScorePanel.R                                    *
# ******************************************************************************
# This script reads overreaction score files per stock and generates one       *
# combined data-frame with key daily properties for each stock                 *
# ******************************************************************************



# ----- PREP -------------------------------------------------------------------
# path & file info
sf.files <- list.files (path = data.path.sf, pattern = "*.sc2")

symbol.info <- 
  read.csv ("D:/Doktorarbeit/40 Data/Stocks/Taipan/symbol_info.csv", 
            header=TRUE, stringsAsFactors=FALSE, comment.char="", 
            col.names=c("Symbol", "shares_out", "Name", 
                        "Sector", "Industry", "Country"), 
            colClasses=c("character", "numeric", rep("character",4)))
rownames(symbol.info) <- symbol.info$Symbol
suffix <- ".part1"

for (i in 1:736) {
# for (i in 1:length(sf.files)) {
  # 1) Load current score file & HFT data --------------------------------------
  # 1a) score file
  load (paste(data.path.sf, sf.files[i], sep=""))
  cur.symbol <- as.character (strsplit(sf.files[i],".sc2")[1]) # [1] = filename
  print (paste("[BuildScorePanel.R ", format(Sys.time(), "%H:%M:%S"), 
               "] Processing ", cur.symbol, sep=""))
  
  # ***** loaded vars - reference ******************************
  # cur.x.aligned, cur.d.x.aligned, sp.x.aligned, sp.d.x.aligned,
  # cur.rbetas.d.x.shifted, id.dsbmom, id.score,
  # id.volume.dta, id.rv.dta, id.rv.dta.sc, id.rv.1min.dta, sp.id.rv.dta.sc,
  # id.score.hi, id.score.lo

  # convert intraday series' to xts for later period-based aggregation
  # id.score.x <- xts(id.score, order.by=index(cur.x.aligned))
  
  # id.score.daily <- array (id.score, dim=c(daybars, nrow(cur.d.x.aligned)))
  id.score.daily.hi <- array (id.score.hi, dim=c(daybars, nrow(cur.d.x.aligned)))
  id.score.daily.lo <- array (id.score.lo, dim=c(daybars, nrow(cur.d.x.aligned)))

  id.score.daily.max <- apply (id.score.daily.hi, 2, function(x) 
    max(x[11:(daybars-15)]))  
  id.score.daily.max.t <- apply (id.score.daily.hi, 2, function(x) 
    which(x==max(x[11:(daybars-15)]))[1])
  id.score.daily.min <- apply (id.score.daily.lo, 2, function(x) 
    min(x[11:(daybars-15)]))  
  id.score.daily.min.t <- apply (id.score.daily.lo, 2, function(x) 
    which(x==min(x[11:(daybars-15)]))[1])
  
#   # 1b) hft data
#   cur.hft.x <- Load1minHFTDataXTS (paste(data.path.HFT,
#                  paste(cur.symbol,".csv", sep=""), sep="/"))
#   # create aligned HFT series (simplifies all event-bar based lookups)
#   cur.hft.x <- cur.hft.x[index(cur.hft.x) %in% index(cur.x.aligned)]
#   cur.hft.x.aligned <- merge (cur.hft.x, index(cur.x.aligned), fill=0)
#   
#   # create daily HFT series'
#   cur.hft.flags.d.x <- period.apply (cur.hft.x.aligned$Events, 
#                                      endpoints(cur.hft.x.aligned, "days"), sum)
#   cur.hft.mins.d.x <- period.apply (cur.hft.x.aligned$Events, 
#                                     endpoints(cur.hft.x.aligned, "days"),
#                                     FUN=function(x) sum(x>0))
#   cur.hft.quotes.d.x <- period.apply (cur.hft.x.aligned$Quotes, 
#                                       endpoints(cur.hft.x.aligned, "days"), sum)
#   # remove day-times from daily HFT series' timestamps
#   index(cur.hft.flags.d.x) <- as.Date (index(cur.hft.flags.d.x))
#   index(cur.hft.mins.d.x) <- as.Date (index(cur.hft.mins.d.x))
#   index(cur.hft.quotes.d.x) <- as.Date (index(cur.hft.quotes.d.x))
#   
#   # calculate daily volume "participation rates"
#   cur.hft.part.flags.d.x <- cur.hft.flags.d.x / cur.d.x.aligned$Volume
#   cur.hft.part.quotes.d.x <- cur.hft.quotes.d.x / cur.d.x.aligned$Volume
  
#   # 1c) HFT daytime averages
#   # calculate dta overlay curves for total HFT series
#   cur.hft.daily <- array (coredata(cur.hft.x.aligned$Events), 
#                           dim=c(daybars, nrow(cur.d.x.aligned)))
#   cur.hft.pf.daily <- array (coredata(cur.hft.x.aligned$Events/cur.x.aligned$Volume), 
#                              dim=c(daybars, nrow(cur.d.x.aligned)))
#   cur.hft.pq.daily <- array (coredata(cur.hft.x.aligned$Quotes/cur.x.aligned$Volume), 
#                              dim=c(daybars, nrow(cur.d.x.aligned)))
#   # catch cases where both values == 0
#   cur.hft.pf.daily[is.nan(cur.hft.pf.daily)] <- 0
#   cur.hft.pq.daily[is.nan(cur.hft.pq.daily)] <- 0
#   # catch cases where HFT > 0 and volume == 0 --> set to double of max
#   cur.hft.pf.daily[!is.finite(cur.hft.pf.daily)] <-
#     max(cur.hft.pf.daily[is.finite(cur.hft.pf.daily)])
#   cur.hft.pq.daily[!is.finite(cur.hft.pq.daily)] <-
#     max(cur.hft.pq.daily[is.finite(cur.hft.pq.daily)])
#   
#   # flags: unit = % of seconds with flags
#   cur.hft.flags.dta <- array (NA, dim=c(daybars))
#   cur.hft.flags.dta[1:daybars] <- 
#     sapply (1:daybars, function(x) mean(cur.hft.daily[x,])/60)
#   # mins: unit = % of minutes with flags
#   cur.hft.mins.dta <- array (NA, dim=c(daybars))
#   cur.hft.mins.dta[1:daybars] <-
#     sapply (1:daybars, function(x) mean(ifelse(cur.hft.daily[x,]>0,1,0)))
#   # flag & quote rates / volume: unit = % of volume
#   cur.hft.pf.dta <- array (NA, dim=c(daybars))
#   cur.hft.pq.dta <- array (NA, dim=c(daybars))
#   cur.hft.pf.dta[1:daybars] <- sapply (1:daybars, function(x) mean(cur.hft.pf.daily[x,]))
#   cur.hft.pq.dta[1:daybars] <- sapply (1:daybars, function(x) mean(cur.hft.pq.daily[x,]))
#   # smooth dta estimates
#   cur.hft.flags.dta <- array (lowess (cur.hft.flags.dta, f=.05)$y)
#   cur.hft.mins.dta <- array (lowess (cur.hft.mins.dta, f=.05)$y)
#   cur.hft.pf.dta <- array (lowess (cur.hft.pf.dta, f=.05)$y)
#   cur.hft.pq.dta <- array (lowess (cur.hft.pq.dta, f=.05)$y)
  
#   # enable time-based lookup in dta variables
#   # TODO: veeerry dirty code to generate time sequence... (str-cut)
#   rownames(cur.hft.flags.dta) <- 
#     substr (seq(from=as.POSIXct(paste("2012-01-01 ", day.start.time, sep="")), 
#                 to=as.POSIXct(paste("2012-01-01 ", day.end.time, sep="")), 
#                 by="min"), 12, 19)
#   rownames(cur.hft.mins.dta) <- 
#     substr (seq(from=as.POSIXct(paste("2012-01-01 ", day.start.time, sep="")), 
#                 to=as.POSIXct(paste("2012-01-01 ", day.end.time, sep="")), 
#                 by="min"), 12, 19)  
#   rownames(cur.hft.pf.dta) <- 
#     substr (seq(from=as.POSIXct(paste("2012-01-01 ", day.start.time, sep="")), 
#                 to=as.POSIXct(paste("2012-01-01 ", day.end.time, sep="")), 
#                 by="min"), 12, 19)
#   rownames(cur.hft.pq.dta) <- 
#     substr (seq(from=as.POSIXct(paste("2012-01-01 ", day.start.time, sep="")), 
#                 to=as.POSIXct(paste("2012-01-01 ", day.end.time, sep="")), 
#                 by="min"), 12, 19)  
  
#   # 2) build daily sum-of-flags variable for HFT activity (across all stocks)
#   if (i == 1) {
#     hft.flags.sum.x <- cur.hft.flags.d.x
#     hft.mins.sum.x <- cur.hft.mins.d.x
#     active.stocks.sum.x <- xts (order.by=index(cur.d.x.aligned), 
#                                 x=rep(1, nrow(cur.d.x.aligned)))
#   } else {
#     temp.x <- merge(cur.hft.flags.d.x, hft.flags.sum.x, all=TRUE, fill = 0)
#     hft.flags.sum.x <- xts(temp.x$Events + temp.x$Events.1, 
#                            order.by=index(temp.x))
#     temp.x <- merge(cur.hft.mins.d.x, hft.mins.sum.x, all=TRUE, fill = 0)
#     hft.mins.sum.x <- xts(temp.x$Events + temp.x$Events.1, 
#                           order.by=index(temp.x))
#     active.stocks.sum.x <- merge (active.stocks.sum.x, index(cur.d.x.aligned), 
#                                   fill = 0, all = TRUE)
#     active.stocks.sum.x[index(cur.d.x.aligned)] <- 
#       active.stocks.sum.x[index(cur.d.x.aligned)] + 1
#     colnames(active.stocks.sum.x) <- "#stocks"
#   }
  
  # 3) Build dataframe of daily characteristics
  cur.d.x.aligned.ret <- diff (log(cur.d.x.aligned$Close), 1) * 100
#   sp.d.x.aligned.ret <- diff (log(sp.d.x.aligned$Close), 1) * 100
#   cur.d.x.aligned.aret <- cur.d.x.aligned.ret - 
#                             cur.rbetas.d.x.shifted * sp.d.x.aligned.ret
  
  if (i == 1) {
    # build dataframe with date as key
#     stocks.prices.df <- data.frame (cur.d.x.aligned$Close, 
#                                     row.names=index(cur.d.x.aligned))
#     stocks.sizes.df <- data.frame (cur.d.x.aligned$Close * 
#                                      symbol.info[cur.symbol,'shares_out'], 
#                                    row.names=index(cur.d.x.aligned))
#     stocks.liqs.df <-  data.frame (
#       MovingAvg (cur.d.x.aligned$Close * cur.d.x.aligned$Volume, period.days), 
#       row.names=index(cur.d.x.aligned))
#     stocks.volas.df <- data.frame (sapply (seq_along(cur.d.x.aligned.ret), 
#                                            function(i) if (i < period.days) NA else 
#                                              sd(cur.d.x.aligned.ret[i:(i-period.days+1)])),
#                                    row.names=index(cur.d.x.aligned))
#     stocks.betas.df <- data.frame (cur.rbetas.d.x.shifted, 
#                                    row.names=index(cur.d.x.aligned))
#     stocks.HFTflags.df <- data.frame (cur.hft.flags.d.x$Events, 
#                                       row.names=index(cur.hft.flags.d.x))
#     stocks.HFTmins.df <- data.frame (cur.hft.mins.d.x$Events, 
#                                      row.names=index(cur.hft.mins.d.x))
#     stocks.HFTpflags.df <- data.frame (cur.hft.part.flags.d.x, 
#                                        row.names=index(cur.hft.flags.d.x))
#     stocks.HFTpquotes.df <- data.frame (cur.hft.part.quotes.d.x, 
#                                         row.names=index(cur.hft.mins.d.x))
    stocks.score.max <- data.frame (id.score.daily.max, 
                                    row.names=index(cur.d.x.aligned))
    stocks.score.max.t <- data.frame (id.score.daily.max.t, 
                                      row.names=index(cur.d.x.aligned))
    stocks.score.min <- data.frame (id.score.daily.min, 
                                      row.names=index(cur.d.x.aligned))
    stocks.score.min.t <- data.frame (id.score.daily.min.t, 
                                      row.names=index(cur.d.x.aligned))
    
#     names(stocks.prices.df)[1] <- cur.symbol
#     names(stocks.sizes.df)[1] <- cur.symbol
#     names(stocks.liqs.df)[1] <- cur.symbol
#     names(stocks.volas.df)[1] <- cur.symbol
#     names(stocks.betas.df)[1] <- cur.symbol
#     names(stocks.HFTflags.df)[1] <- cur.symbol
#     names(stocks.HFTmins.df)[1] <- cur.symbol
#     names(stocks.HFTpflags.df)[1] <- cur.symbol
#     names(stocks.HFTpquotes.df)[1] <- cur.symbol
    names(stocks.score.max)[1] <- cur.symbol
    names(stocks.score.max.t)[1] <- cur.symbol
    names(stocks.score.min)[1] <- cur.symbol
    names(stocks.score.min.t)[1] <- cur.symbol
  } else {
    # create temporary dataframes for current symbol
#     cur.prices.df <- data.frame (cur.d.x.aligned$Close, 
#                                  row.names=index(cur.d.x.aligned))
#     cur.sizes.df <- data.frame (cur.d.x.aligned$Close * 
#                                   symbol.info[cur.symbol,'shares_out'], 
#                                 row.names=index(cur.d.x.aligned))
#     cur.liqs.df <-  data.frame (
#       MovingAvg (cur.d.x.aligned$Close * cur.d.x.aligned$Volume, period.days), 
#       row.names=index(cur.d.x.aligned))
#     cur.volas.df <- data.frame (sapply (seq_along(cur.d.x.aligned.ret), 
#                                         function(i) if (i < period.days) NA else 
#                                           sd(cur.d.x.aligned.ret[i:(i-period.days+1)])),
#                                 row.names=index(cur.d.x.aligned))
#     cur.betas.df <- data.frame (cur.rbetas.d.x.shifted, 
#                                 row.names=index(cur.d.x.aligned))
#     cur.HFTflags.df <- data.frame (cur.hft.flags.d.x$Events, 
#                                    row.names=index(cur.hft.flags.d.x))
#     cur.HFTmins.df <- data.frame (cur.hft.mins.d.x$Events, 
#                                   row.names=index(cur.hft.mins.d.x))
#     cur.HFTpflags.df <- data.frame (cur.hft.part.flags.d.x, 
#                                     row.names=index(cur.hft.flags.d.x))
#     cur.HFTpquotes.df <- data.frame (cur.hft.part.quotes.d.x, 
#                                      row.names=index(cur.hft.mins.d.x))
    cur.score.max <- data.frame (id.score.daily.max, 
                                    row.names=index(cur.d.x.aligned))
    cur.score.max.t <- data.frame (id.score.daily.max.t, 
                                      row.names=index(cur.d.x.aligned))
    cur.score.min <- data.frame (id.score.daily.min, 
                                    row.names=index(cur.d.x.aligned))
    cur.score.min.t <- data.frame (id.score.daily.min.t, 
                                      row.names=index(cur.d.x.aligned))
    
#     names(cur.prices.df)[1] <- cur.symbol
#     names(cur.sizes.df)[1] <- cur.symbol
#     names(cur.liqs.df)[1] <- cur.symbol
#     names(cur.volas.df)[1] <- cur.symbol
#     names(cur.betas.df)[1] <- cur.symbol
#     names(cur.HFTflags.df)[1] <- cur.symbol
#     names(cur.HFTmins.df)[1] <- cur.symbol
#     names(cur.HFTpflags.df)[1] <- cur.symbol
#     names(cur.HFTpquotes.df)[1] <- cur.symbol
    names(cur.score.max)[1] <- cur.symbol
    names(cur.score.max.t)[1] <- cur.symbol
    names(cur.score.min)[1] <- cur.symbol
    names(cur.score.min.t)[1] <- cur.symbol
    
    # merge temporary dataframes into overall data frames
    # (by=0 is equal to by=row.names)
#     test.df <- merge (stocks.prices.df, cur.prices.df, by=0, all=TRUE, fill=0)
#     stocks.prices.df <- data.frame (test.df[,2:ncol(test.df)], 
#                                     row.names=test.df[,1])
#     test.df <- merge (stocks.sizes.df, cur.sizes.df, by=0, all=TRUE, fill=0)
#     stocks.sizes.df <- data.frame (test.df[,2:ncol(test.df)], 
#                                    row.names=test.df[,1])
#     test.df <- merge (stocks.liqs.df, cur.liqs.df, by=0, all=TRUE, fill=0)
#     stocks.liqs.df <- data.frame (test.df[,2:ncol(test.df)], 
#                                   row.names=test.df[,1])
#     test.df <- merge (stocks.volas.df, cur.volas.df, by=0, all=TRUE, fill=0)
#     stocks.volas.df <- data.frame (test.df[,2:ncol(test.df)], 
#                                    row.names=test.df[,1])
#     test.df <- merge (stocks.betas.df, cur.betas.df, by=0, all=TRUE, fill=0)
#     stocks.betas.df <- data.frame (test.df[,2:ncol(test.df)], 
#                                    row.names=test.df[,1])
#     test.df <- merge (stocks.HFTflags.df, cur.HFTflags.df, by=0, all=TRUE, fill=0)
#     stocks.HFTflags.df <- data.frame (test.df[,2:ncol(test.df)], 
#                                       row.names=test.df[,1])
#     test.df <- merge (stocks.HFTmins.df, cur.HFTmins.df, by=0, all=TRUE, fill=0)
#     stocks.HFTmins.df <- data.frame (test.df[,2:ncol(test.df)], 
#                                      row.names=test.df[,1])
#     test.df <- merge (stocks.HFTpflags.df, cur.HFTpflags.df, by=0, all=TRUE, fill=0)
#     stocks.HFTpflags.df <- data.frame (test.df[,2:ncol(test.df)], 
#                                        row.names=test.df[,1])
#     test.df <- merge (stocks.HFTpquotes.df, cur.HFTpquotes.df, by=0, all=TRUE, fill=0)
#     stocks.HFTpquotes.df <- data.frame (test.df[,2:ncol(test.df)], 
#                                         row.names=test.df[,1])
    test.df <- merge (stocks.score.max, cur.score.max, by=0, all=TRUE, fill=0)
    stocks.score.max <- data.frame (test.df[,2:ncol(test.df)], 
                                      row.names=test.df[,1])
    test.df <- merge (stocks.score.max.t, cur.score.max.t, by=0, all=TRUE, fill=0)
    stocks.score.max.t <- data.frame (test.df[,2:ncol(test.df)], 
                                    row.names=test.df[,1])
    test.df <- merge (stocks.score.min, cur.score.min, by=0, all=TRUE, fill=0)
    stocks.score.min <- data.frame (test.df[,2:ncol(test.df)], 
                                    row.names=test.df[,1])
    test.df <- merge (stocks.score.min.t, cur.score.min.t, by=0, all=TRUE, fill=0)
    stocks.score.min.t <- data.frame (test.df[,2:ncol(test.df)], 
                                      row.names=test.df[,1])
  }
  # end of 3c.1x) special: build daily dataframe/symbol matrix
}

# save panel data objects to file
save (
# save (stocks.prices.df, stocks.sizes.df, stocks.liqs.df, stocks.volas.df,
#       stocks.betas.df, stocks.HFTflags.df, stocks.HFTmins.df, 
#       stocks.HFTpflags.df, stocks.HFTpquotes.df, 
      stocks.score.max, 
      stocks.score.max.t, stocks.score.min, stocks.score.min.t,
      file=paste(data.path.out, "PanelHiLoScores", suffix,".pdb", sep=""))

beep()
# ----- CLEAN-UP ---------------------------------------------------------------
gc()