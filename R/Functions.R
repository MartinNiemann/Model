# ******************************************************************************
# Intraday Overreaction - Functions.R                                          *
# ******************************************************************************
# This file defines supporting functions used in the project                   *
# ******************************************************************************


# ----- CORE: OVERREACTION SCORE -----------------------------------------------

Load1minDataXTS <- function (path) {
  # loads a DTOHLCV csv file into an xts time series
  # Args: path = full path for input file
  # Returns: OHLCV price series with date-time index of class xts
  
  # define column headings and classes
  cols    <- c ("Date", "Time", "Open", "High", "Low", "Close", "Volume")
  cols.cl <- c (rep("character", 2), rep("numeric", 4), "integer")
  
  csv.file <- read.csv (path, header=TRUE, stringsAsFactors=FALSE, 
                        col.names=cols, colClasses=cols.cl, comment.char="")
  xts.series <- xts (csv.file[, 3:7], 
                     as.POSIXct(strptime(as.character(paste(csv.file[, 1], 
                                                      csv.file[, 2], sep =" ")), 
                                         "%m/%d/%Y %H:%M:%S")))
  return (xts.series)
}

Load1minHFTDataXTS  <- function (path) {
  # loads a 1min-aggregated NANEX file into an xts time series
  # Args: path = full path for input file
  # Returns: HFT flag time series with date-time index of class xts
  cols    <- c ("Timestamp", "Events", "Quotes", "Trades")
  cols.cl <- c ("character", rep("integer", 3))

  csv.file <- read.csv (path, header=TRUE, stringsAsFactors=FALSE, 
                        col.names=cols, colClasses=cols.cl, comment.char="")
  xts.series <- xts (csv.file[, 2:4], 
                     as.POSIXct(strptime(csv.file[, 1], "%m/%d/%Y %H:%M:%S")))
  return (xts.series)
}


CalculateRollingBetaXTS <- function (series.d, benchmark.d, rf.d) {
  # calculates rolling robust betas between two time series,  
  # including risk-free rate series. To speed up calculations, the function
  # works on coredata-dataframes and converts the final result back to XTS.
  #
  # Requires: global parameters period.days and period.oneyear
  # Args: xts series of stock(DOHLCV), benchmark(DOHLCV), and Rf (DC)
  # Returns: xts series of rolling betas as per t0 close (for use in t+1)
  
  # calculate daily log return series for stock, benchmark, and Rf
  # (return calculation drops 1st day of series')
  cur.ret.d.df <- as.data.frame (100*diff(log(coredata(series.d$Close))))
  bm.ret.d.df <- as.data.frame (100*diff(log(coredata(benchmark.d$Close))))
  rf.ret.d.df <- as.data.frame (coredata(rf.d$Rf)[-1] / 365) # drop 1st day
  
  # build generic daily index for all loops using lookback of period.days
  roll.index <- seq (period.days+1, nrow(series.d)) # +1 accounts for 1-day
                                                    # lag of return calc.

  # build combined dataset of excess returns
  comb.xret.d.df <- cbind (cur.ret.d.df - rf.ret.d.df, 
                           bm.ret.d.df - rf.ret.d.df)
  colnames (comb.xret.d.df) <- c ("Rax", "Rbx")
  
  # set up NA result vectors for beta estimates
  cur.rbetas.d.x <- xts (zoo(x=c(NA), order.by=index(series.d)))
  cur.rbetas.d <- as.vector (coredata(cur.rbetas.d.x))
  
  # calculate robust betas for all bars with enough data
  for (i in roll.index){
    rlm.res <- rlm (Rax ~ Rbx, data=comb.xret.d.df[max(1,i-period.oneyear):i, ], 
                    psi=psi.bisquare, maxit = 200, scale.est = "Huber")
    cur.rbetas.d[i] <- rlm.res$coefficients["Rbx"]
  }
  # ... and write result back to XTS time series
  coredata (cur.rbetas.d.x) <- cur.rbetas.d
  
  # xts timeseries now contains betas estimated as of day's close
  # (to be applied at next day)
  return (cur.rbetas.d.x)  
}

AlignTimesAndFillXTS <- function (series, ref) {
  # merges OHLC series into timestamp grid of ref series (XTS)
  # NAs are filled with last close and zero volume
  # Args: series = DTOHLCV seriesm, ref=DT timestamp vector to align to
  # Returns: aligned XTS series
  resXTS <- merge (series, index(ref), fill=NA)
  
  # catch first bar NA
  if( is.na(resXTS$Open[1]) ) resXTS$Open[1] <- resXTS$Open[2]
  if( is.na(resXTS$High[1]) ) resXTS$High[1] <- resXTS$Open[2]
  if( is.na(resXTS$Low[1]) ) resXTS$Low[1] <- resXTS$Open[2]
  if( is.na(resXTS$Close[1]) ) resXTS$Close[1] <- resXTS$Open[2]
  if( is.na(resXTS$Volume[1]) ) resXTS$Volume[1] <- 0
  
  # create 1-bar backshifted copy of series
  shiftXTS <- lag.xts (resXTS, 1)
    
  # fill missing bars with previous close and zero volume
  while(any(is.na(resXTS))) {
    # replace NA with lagged series
    resXTS$Open[is.na(resXTS$Open)] <- shiftXTS$Close[is.na(resXTS$Open)]
    resXTS$High[is.na(resXTS$High)] <- shiftXTS$Close[is.na(resXTS$High)]
    resXTS$Low[is.na(resXTS$Low)] <- shiftXTS$Close[is.na(resXTS$Low)]
    resXTS$Close[is.na(resXTS$Close)] <- shiftXTS$Close[is.na(resXTS$Close)]
    resXTS$Volume[is.na(resXTS$Volume)] <- 0
    # update lagged series (solves multiple NAs in a row in loop)
    shiftXTS <- lag.xts (resXTS, 1)
  }
  
  return (resXTS)
}

BackfillOpeningData <- function (series, t.start) {
  daily.open <- array (coredata(series$Open), dim=c(daybars, length(day.ends)))
  daily.high <- array (coredata(series$High), dim=c(daybars, length(day.ends)))
  daily.low <- array (coredata(series$Low), dim=c(daybars, length(day.ends)))
  daily.close <- array (coredata(series$Close), dim=c(daybars, length(day.ends)))
  daily.vol <- array (coredata(series$Volume), dim=c(daybars, length(day.ends)))
  detect.zerovol <- rep(F, length(day.ends))
  for (i in seq(t.start, 1)) {
    detect.zerovol <- ifelse((daily.vol[i, ] == 0 & daily.vol[i+1, ] > 0) | 
                               detect.zerovol, T, F)
    # replace with close of future bar if current bar has zero volume
    daily.close[i, ] <- ifelse (detect.zerovol, 
                                daily.close[i+1, ], daily.close[i, ])
    daily.open[i, ] <- ifelse (detect.zerovol, 
                               daily.close[i+1, ], daily.open[i, ])
    daily.high[i, ] <- ifelse (detect.zerovol, 
                               daily.close[i+1, ], daily.high[i, ])
    daily.low[i, ] <- ifelse (detect.zerovol, 
                              daily.close[i+1, ], daily.low[i, ])
  }
  series[,1] <- array (daily.open, dim=nrow(daily.open) * ncol(daily.open))
  series[,2] <- array (daily.high, dim=nrow(daily.high) * ncol(daily.high))
  series[,3] <- array (daily.low, dim=nrow(daily.low) * ncol(daily.low))
  series[,4] <- array (daily.close, dim=nrow(daily.close) * ncol(daily.close))
  return (series)
}

LocalCenteredSmooth <- function(x, k) {
  res <- (rollapply(x, width=k, FUN=mean, partial=1, align="right") + 
          rollapply(x, width=k, FUN=mean, partial=1, align="left")) / 2  
  return(res)
}

# ----- CORE: BUILD INSTANCE DB ------------------------------------------------

# time filters on event timestamps
FilterEventsByDaytime <- function (events, start.time, end.time) {
  # returns events selected by daytime window (from - to)
  # Args: events = vector of timestamps, start/end.time = %H:%M:%S cut-offs
  # Returns: filtered vector of timestamps
  select <- format(events, format="%H:%M:%S") >= start.time & 
    format(events, format="%H:%M:%S") <= end.time 
  return (events[select])
}

FilterEventsByDate <- function (events, start.date, end.date) {
  # returns events selected by start/end date (from - to)
  # Args: events = vector of timestamps, start/end.date = %H:%M:%S cut-offs
  # Returns: filtered vector of timestamps
  select <- format(events, format="%Y-%m-%d") >= param.start.date &
    format(events, format="%Y-%m-%d") <= param.end.date
  return (events[select])
}

GetEventIdx <- function (score, mom, p.rel, p.abs, up=TRUE) {
  # returns vector of event indices (in cur.x.aligned)
  # Attention: expects several external variables to be defined
  # Args: score, mom are intraday series loaded for each symbol
  #       p.rel, p.abs are parameters for event cut-offs
  if (up) {
    id.events.idx.rel <- which (score > p.rel)
    id.events.idx.abs <- which (mom > p.abs)    
  } else {
    id.events.idx.rel <- which (score < -p.rel)
    id.events.idx.abs <- which (mom < -p.abs)    
  }
  id.events.idx <- id.events.idx.rel[id.events.idx.rel %in% id.events.idx.abs]
  if (length(id.events.idx > 0) ) {
  id.events.ts <- index (cur.x.aligned[id.events.idx])
  id.events.ts.ep <- endpoints (id.events.ts, "days")  # daily endpoints
  id.events.ts.sp <- id.events.ts.ep[-length(id.events.ts.ep)] + 1 # startpoints
  id.events.ts <- id.events.ts[id.events.ts.sp] # those events which are startpoints
  id.events.ts <- FilterEventsByDaytime (id.events.ts, 
                                         param.start.time, param.end.time)
  id.events.ts <- FilterEventsByDate (id.events.ts, 
                                      param.start.date, param.end.date)
  id.events.idx <- which (index(cur.x.aligned) %in% id.events.ts)
  }
  return (id.events.idx)
}

BuildEventDf <- function(events.idx) {
  # creates a base data.frame (for later addition of variables) with
  # timestamp, symbol, and bar colums
  # Args: events.idx = event timestamps
  # Returns: data.frame
  res.df <- data.frame(timestamp=character(0), symbol=character(0), 
                       bar = integer(0))
  if (length(events.idx) > 0) {
    res.df <- data.frame (cbind(as.character.POSIXt(
      index(cur.x.aligned[events.idx])), c(cur.symbol), events.idx),
                          stringsAsFactors = FALSE)
    colnames(res.df) <- c ("timestamp", "symbol", "bar")
    rownames(res.df) <- paste (res.df$timestamp, res.df$symbol, sep=" ")
    res.df$bar <- as.integer (res.df$bar)  # set bar to integer
  }
  return (res.df)
}

# variable time series extractor
GetTseries <- function (df, row, varname) {
  # extracts a time series for one event for given variable (r,ar,v,rv)
  # and returns it as an unclassed vector
  # Args: df = event dataframe with initialized [x]_[t] variables,
  #       row = event index (by row), varname = [x]_
  # Returns: one event's time series for selected variable
  return (as.vector(unlist(unclass(df[row, 
    paste(varname, "_", -period.id.window:param.post.event.window, sep="")]))))
}

# min/max bar index and value
GetTseriesMax_t <- function (df, row, varname) {
  # returns the bar index t relative to the event time t for max(varname)
  # Args: df = event dataframe with initialized [x]_[t] variables,
  #       row = event index (by row), varname = [x]_
  # Returns: relative time index t for max(varname)
  tseries <- GetTseries (df, row, varname)
  tseries.from.event <- tseries[(period.id.window+1):length(tseries)]
  return (which(tseries.from.event == max(tseries.from.event, na.rm=T))[1])
  # attention: return value 1 = event time t0
}
GetTseriesMin_t <- function (df, row, varname) {
  # returns the bar index t relative to the event time [61] for min(varname)
  # Args: df = event dataframe with initialized [x]_[t] variables,
  #       row = event index (by row), varname = [x]_
  # Returns: relative time index t for min(varname)
  tseries <- GetTseries (df, row, varname)
  tseries.from.event <- tseries[(period.id.window+1):length(tseries)]
  return (which(tseries.from.event == min(tseries.from.event, na.rm=T))[1])
  # attention: return value 1 = event time t0
}
GetTseriesMax <- function (df, row, varname) {
  # returns the maximum value for varname
  # Args: df = event dataframe with initialized [x]_[t] variables,
  #       row = event index (by row), varname = [x]_
  # Returns: max(varname)
  tseries <- GetTseries (df, row, varname)
  return (max(tseries, na.rm=TRUE))
}
GetTseriesMin <- function (df, row, varname) {
  # returns the minimum value for varname
  # Args: df = event dataframe with initialized [x]_[t] variables,
  #       row = event index (by row), varname = [x]_
  # Returns: min(varname)
  tseries <- GetTseries (df, row, varname)
  return (min(tseries, na.rm=TRUE))
}

# event characteristics - reversal size
GetTseriesReversalFromLow <- function (df, row, varname, getT) {
  # measures reversal extent and length from event Low
  # Args: df = event dataframe with initialized [x]_[t] variables,
  #       row = event index (by row), varname = [x]_
  #       getT = TRUE returns bar index, = FALSE returns reversal extent
  tseries <- GetTseries (df, row, varname)
  t_min <- GetTseriesMin_t (df, row, varname) + period.id.window
           # attention: minimum t_min is 61 = t0 (event time)
  tseries.from.min <- tseries[t_min:length(tseries)]
  res <- NA
  if( getT ) 
    res <- which(tseries.from.min == max(tseries.from.min, na.rm=TRUE))[1]-1
  else
    res <- max(tseries.from.min, na.rm=TRUE) - min(tseries.from.min, na.rm=TRUE)
  
  return (res)
}
GetTseriesReversalFromHigh <- function (df, row, varname, getT) {
  # measures reversal extent and length from event High
  # Args: df = event dataframe with initialized [x]_[t] variables,
  #       row = event index (by row), varname = [x]_
  #       getT = TRUE returns bar index, = FALSE returns reversal extent
  tseries <- GetTseries (df, row, varname)
  t_max <- GetTseriesMax_t (df, row, varname) + period.id.window
           # attention: minimum t_min is 61 = t0 (event time)
  tseries.from.max <- tseries[t_max:length(tseries)]
  res <- NA
  if( getT ) {
    res <- which(tseries.from.max == min(tseries.from.max, na.rm=TRUE))[1]-1
  }
  else {
    res <- max(tseries.from.max, na.rm=TRUE) - min(tseries.from.max, na.rm=TRUE)
  }
  return (res)
}

# spike detection
GetTseriesMaxSpikeBar <- function (df, row, varname, t.start, t.end) {
  # returns max 1-min close-to-close return, 
  # divided by range in selected period
  # Args: df = event dataframe with initialized [x]_[t] variables,
  #       row = event index (by row), varname = [x]_
  #       t.start/.end = start/end offset for selected period
  #         (indicated as offsets relative to t)
  # Returns: ratio of max bar for selected period
  tseries <- GetTseries (df, row, varname)
  tseries.selected <- 
    tseries[(t.start+period.id.window+1):(t.end+period.id.window+1)]
  tseries.selected.ret <- diff(tseries.selected)
  return (max(tseries.selected.ret, na.rm=T) / 
            (max(tseries.selected, na.rm=T)-min(tseries.selected, na.rm=T)))
}

GetTseriesMinSpikeBar <- function (df, row, varname, t.start, t.end) {
  # returns min 1-min close-to-close return, 
  # divided by range in selected period
  # Args: df = event dataframe with initialized [x]_[t] variables,
  #       row = event index (by row), varname = [x]_
  #       t.start/.end = start/end offset for selected period
  #         (indicated as offsets relative to t)
  # Returns: ratio of (abs) min bar for selected period
  tseries <- GetTseries (df, row, varname)
  tseries.selected <- 
    tseries[(t.start+period.id.window+1):(t.end+period.id.window+1)]
  tseries.selected.ret <- diff(tseries.selected, lag=1)
  return (abs(min(tseries.selected.ret, na.rm=T)) / 
            (max(tseries.selected, na.rm=T)-min(tseries.selected, na.rm=T)))
}

# window averages
GetTseriesAvg <- function (df, row, varname, t.start, t.end) {
  # returns average of chosen variable in selected window,
  # constraining window boundaries to time series
  # Args: df = event dataframe with initialized [x]_[t] variables,
  #       row = event index (by row), varname = [x]_
  #       t.start/.end = start/end offset for selected period
  #         (indicated as offsets relative to t)
  # Returns: average of chosen variable between t.start/.end
  tseries <- GetTseries (df, row, varname)
  sel.start <- max(1, t.start+period.id.window+1)
  sel.end <- min(length(tseries), t.end+period.id.window+1)
  tseries.selected <- tseries[sel.start:sel.end]
  return (mean(tseries.selected, na.rm=TRUE))
}

GetTseriesAvg.01 <- function (df, row, varname, t.start, t.end) {
  # returns average of chosen variable (>0) in selected window,
  # constraining window boundaries to time series
  # Args: df = event dataframe with initialized [x]_[t] variables,
  #       row = event index (by row), varname = [x]_
  #       t.start/.end = start/end offset for selected period
  #         (indicated as offsets relative to t)
  # Returns: average of chosen variable between t.start/.end
  tseries <- GetTseries (df, row, varname)
  sel.start <- max(1, t.start+period.id.window+1)
  sel.end <- min(length(tseries), t.end+period.id.window+1)
  tseries.selected <- tseries[sel.start:sel.end]
  return (mean(tseries.selected>0, na.rm=TRUE))
}

GetTseriesSqRootAvg <- function (df, row, varname, t.start, t.end) {
  # returns average of chosen variable in selected window,
  # by averaging squares, then taking the root of the result
  # (for consistent variance/vola estimators vs. Score Files)
  # constraining window boundaries to time series
  # Args: df = event dataframe with initialized [x]_[t] variables,
  #       row = event index (by row), varname = [x]_
  #       t.start/.end = start/end offset for selected period
  #         (indicated as offsets relative to t)
  # Returns: average of chosen variable between t.start/.end
  tseries <- GetTseries (df, row, varname)
  sel.start <- max(1, t.start+period.id.window+1)
  sel.end <- min(length(tseries), t.end+period.id.window+1)
  tseries.selected <- tseries[sel.start:sel.end]
  if (deactivate.SqRootAvg == TRUE) 
    return (mean(tseries.selected, na.rm=TRUE)) else 
    return (sqrt(mean(tseries.selected^2, na.rm=TRUE)))
}

GetTseriesMaxSel <- function (df, row, varname, t.start, t.end) {
  # returns Max of chosen variable in selected window,
  # constraining window boundaries to time series
  # Args: df = event dataframe with initialized [x]_[t] variables,
  #       row = event index (by row), varname = [x]_
  #       t.start/.end = start/end offset for selected period
  #         (indicated as offsets relative to t)
  # Returns: average of chosen variable between t.start/.end
  tseries <- GetTseries (df, row, varname)
  sel.start <- max(1, t.start+period.id.window+1)
  sel.end <- min(length(tseries), t.end+period.id.window+1)
  tseries.selected <- tseries[sel.start:sel.end]
  return (max(tseries.selected, na.rm=TRUE))
}

GetTseriesMaxSel_t <- function (df, row, varname, t.start, t.end) {
  # returns t(Max) of chosen variable in selected window,
  # constraining window boundaries to time series
  # Args: df = event dataframe with initialized [x]_[t] variables,
  #       row = event index (by row), varname = [x]_
  #       t.start/.end = start/end offset for selected period
  #         (indicated as offsets relative to t)
  # Returns: t(Max) of chosen variable in selected window
  #          t will always be relative to t.start
  tseries <- GetTseries (df, row, varname)
  sel.start <- max(1, t.start+period.id.window+1)
  # determine by how much t will have to be adjusted to reflect
  # constrained window boundary
  start.offset <- sel.start - (t.start+period.id.window+1)
  stopifnot(start.offset >= 0)
  sel.end <- min(length(tseries), t.end+period.id.window+1)
  tseries.selected <- tseries[sel.start:sel.end]
  return (which(tseries.selected == max(tseries.selected, na.rm=TRUE))[1]
          + start.offset - 1)
}

GetTseriesMinSel <- function (df, row, varname, t.start, t.end) {
  # returns Min of chosen variable in selected window,
  # constraining window boundaries to time series
  # Args: df = event dataframe with initialized [x]_[t] variables,
  #       row = event index (by row), varname = [x]_
  #       t.start/.end = start/end offset for selected period
  #         (indicated as offsets relative to t)
  # Returns: average of chosen variable between t.start/.end
  tseries <- GetTseries (df, row, varname)
  sel.start <- max(1, t.start+period.id.window+1)
  sel.end <- min(length(tseries), t.end+period.id.window+1)
  tseries.selected <- tseries[sel.start:sel.end]
  return (min(tseries.selected, na.rm=TRUE))
}

GetTseriesMinSel_t <- function (df, row, varname, t.start, t.end) {
  # returns t(Min) of chosen variable in selected window,
  # constraining window boundaries to time series
  # Args: df = event dataframe with initialized [x]_[t] variables,
  #       row = event index (by row), varname = [x]_
  #       t.start/.end = start/end offset for selected period
  #         (indicated as offsets relative to t)
  # Returns: t(Min) of chosen variable in selected window
  #          t will always be relative to t.start
  tseries <- GetTseries (df, row, varname)
  sel.start <- max(1, t.start+period.id.window+1)
  # determine by how much t will have to be adjusted to reflect
  # constrained window boundary
  start.offset <- sel.start - (t.start+period.id.window+1)
  stopifnot(start.offset >= 0)
  sel.end <- min(length(tseries), t.end+period.id.window+1)
  tseries.selected <- tseries[sel.start:sel.end]
  return (which(tseries.selected == min(tseries.selected, na.rm=TRUE))[1]
          + start.offset - 1)
}

GetTseriesAC <- function (df, row, varname="r", t.start, t.end) {
  # runs a given function on selected variable in selected window,
  # constraining window boundaries to time series
  # Args: df = event dataframe with initialized [x]_[t] variables,
  #       row = event index (by row), varname = [x]_
  #       t.start/.end = start/end offset for selected period
  #         (indicated as offsets relative to t)
  # Returns: average of chosen variable between t.start/.end
  tseries <- GetTseries (df, row, varname)
  sel.start <- max(1, t.start+period.id.window+1)
  sel.end <- min(length(tseries), t.end+period.id.window+1)
  tseries.selected <- tseries[sel.start:sel.end]
  tseries.selected <- na.remove(tseries.selected)
  tseries.selected <- diff(tseries.selected)
  tseries.selected.1 <- tseries[2:length(tseries)]
  tseries.selected.2 <- tseries[1:(length(tseries)-1)]
  return (cor(tseries.selected.1, tseries.selected.2, "p"))
}

GetTseriesFun <- function (df, row, varname, t.start, t.end, FUN) {
  # runs a given function on selected variable in selected window,
  # constraining window boundaries to time series
  # Args: df = event dataframe with initialized [x]_[t] variables,
  #       row = event index (by row), varname = [x]_
  #       t.start/.end = start/end offset for selected period
  #         (indicated as offsets relative to t)
  # Returns: average of chosen variable between t.start/.end
  tseries <- GetTseries (df, row, varname)
  sel.start <- max(1, t.start+period.id.window+1)
  sel.end <- min(length(tseries), t.end+period.id.window+1)
  tseries.selected <- tseries[sel.start:sel.end]
  return (FUN(na.remove(series.selected)))
}

GetTseriesRealigned <- function (df, row, varname, offset=0, normalize=TRUE) {
  # extracts a time series for one event for given variable (r,ar,v,rv,...)
  # and returns it as an unclassed vector
  # if offset <> 0, the series is shifted and NAs inserted at vector 
  # boundaries without available data
  # Args: df = event dataframe with initialized [x]_[t] variables,
  #       row = event index (by row), varname = [x]_
  #       offset = shift series by X minutes
  # Returns: one event's realigned time series for selected variable
  
  c.base <- 16
  offset.per.var <- period.id.window + param.post.event.window + 1
  var.no <- 1
  if (varname == "ar") var.no <- 2
  if (varname == "v") var.no <- 3
  if (varname == "rv") var.no <- 4
  if (varname == "arv") var.no <- 5
  if (varname == "hft") var.no <- 6
  if (varname == "hftpf") var.no <- 7
  if (varname == "hftpq") var.no <- 8
  
  # get full series for variable
  res <- as.vector(unlist(unclass(df[row, (c.base+1:offset.per.var) + 
                                       (var.no-1) * offset.per.var])))
  # if offset is not zero, realign series
  if (offset > 0) {
    # insert NAs at end, then cut at start
    res <- append(res, rep(NA, offset))
    res <- res[-(1:offset)]
  }
  if (offset < 0) {
    # insert NAs at start, then cut at end
    res <- append(res, rep(NA, offset), 0)
    length(res) <- length(res) - offset
    warning ("GetTseriesRealigned: negative offset used")
  }
  # ...and set new t_61 to zero
  if (offset != 0 & normalize==T) {
    res <- res - res[period.id.window+1]
  }
  return (res)
}

vGetTseriesRealigned <- Vectorize (GetTseriesRealigned, 
                                  vectorize.args=c("row", "offset"))

# row-vectorized Get...Functions
vGetTseries <- Vectorize (GetTseries, vectorize.args="row")
vGetTseriesMax <- Vectorize (GetTseriesMax, vectorize.args="row")
vGetTseriesMax_t <- Vectorize (GetTseriesMax_t, vectorize.args="row")
vGetTseriesMin <- Vectorize (GetTseriesMin, vectorize.args="row")
vGetTseriesMin_t <- Vectorize (GetTseriesMin_t,vectorize.args="row")
vGetTseriesReversalFromHigh <- Vectorize (GetTseriesReversalFromHigh, 
                                          vectorize.args="row")
vGetTseriesReversalFromLow <- Vectorize (GetTseriesReversalFromLow, 
                                         vectorize.args="row")
vGetTseriesMaxSpikeBar <- Vectorize (GetTseriesMaxSpikeBar, 
                                       vectorize.args="row")
vGetTseriesMinSpikeBar <- Vectorize (GetTseriesMinSpikeBar, 
                                       vectorize.args="row")
vGetTseriesAvg <- Vectorize (GetTseriesAvg, vectorize.args="row")
vGetTseriesAvg.01 <- Vectorize (GetTseriesAvg.01, vectorize.args="row")
vGetTseriesSqRootAvg <- Vectorize (GetTseriesSqRootAvg, vectorize.args="row")
vGetTseriesMaxSel <- Vectorize (GetTseriesMaxSel, vectorize.args="row")
vGetTseriesMinSel <- Vectorize (GetTseriesMinSel, vectorize.args="row")
vGetTseriesMaxSel_t <- Vectorize (GetTseriesMaxSel_t, vectorize.args="row")
vGetTseriesMinSel_t <- Vectorize (GetTseriesMinSel_t, vectorize.args="row")
vGetTseriesAC <- Vectorize (GetTseriesAC, vectorize.args="row")
vGetTseriesFun <- Vectorize (GetTseriesFun, vectorize.args="row")
# row + t.start/end vectorized generic function
vvGetTseriesFun <- Vectorize (GetTseriesFun, 
                              vectorize.args=c("row", "t.start", "t.end"))

ApplyToMat <- function (x, idx.mat, fun) {
  # applies a function fun to subets of vector x, defined by idx.mat
  # Args: x=a data vector, matrix, or dataframe column
  #       idx.mat=pre-calculated index vector defining data subsets in x
  #       fun=function to apply to subsets of x
  # Returns: a vector of result values given by number of columns in idx.mat
  values.mat <- idx.mat
  values.mat[,1:ncol(idx.mat)] <- x[idx.mat[,1:ncol(idx.mat)]]
  return (apply(values.mat, 2, fun))
}

# returns a trailing moving average of x
MovingAvg <- function (x, n=period.days) {filter (x, rep(1/n,n), sides=1)}
# returns a centered moving avg of x
MovingAvg2 <- function (x, n=period.days) {filter (x, rep(1/n,n), sides=2)}

MovingAvgDf <- function (x, n=period.days) {
  # returns a trailing moving average for each column in a dataframe
  # --> assumes rows as date-dimension
  res <- as.data.frame (sapply(1:ncol(x), function(c) 
    filter (x[, c], rep(1/n,n), sides=1)))
  names(res) <- names(x)
  rownames(res) <- rownames(x)
  return (res)
}

LagDf <- function (df) {
  # returns a lagged version of a data frame, preserving dim-names
  res <- rbind(rep(NA, ncol(df)), df[1:(nrow(df)-1), ])
  names(res) <- names(df)
  rownames(res) <- rownames(df)
  return (res)  
}

DiffDf <- function (df) {
  # returns a lagged version of a data frame, preserving dim-names
  res <- rbind(rep(NA, ncol(df)), df[1:(nrow(df)-1), ])
  names(res) <- names(df)
  rownames(res) <- rownames(df)
  return (df-res)  
}

StdDf <- function (df, off=F) {
  # returns a daily standardized panel data frame, so that only cross-sectional
  # effects will be of relevance
  # --> assumes rows as date-dimension
  if (!off) {
    res <- as.data.frame (t(apply(df, 1, scale)))
    names(res) <- names (df)
    rownames(res) <- rownames (df)
  } else {
    res <- df
  }
  return (res)
}


MinSinceDaystart <- function (timestamps) {
  (as.numeric(times(as.chron(timestamps))) - 
     as.numeric(dates(as.chron(timestamps)))) * 24 * 60 - 570 + 1
}

DayBoundedBarSeq <- function (timestamp, t.start, t.end) {
  min.since.daystart <- MinSinceDaystart (timestamp)
  seq.start <- round(min (daybars, max (1, min.since.daystart + t.start)))
  seq.end <- round(max (1, min (daybars, min.since.daystart + t.end)))
  return (as.integer(seq.start:seq.end))
}

# ----- ANALYSIS: BUILD INSTANCE HEATMAP ---------------------------------------

GetEventYears <- function(idx) {
  # returns a matrix with event counts by year
  # Args: idx = vector of event timestamps
  # build matrix
  tmp.freq.mat <- matrix(0, nrow=length(2006:2013), ncol=1)
  row.names(tmp.freq.mat) <- 2006:2013  
  # count matching years
  for (i in seq_along(idx))
    tmp.freq.mat[as.character(substr(index(cur.x.aligned[idx[i]]),1,4)),1] <-
      tmp.freq.mat[as.character(substr(index(cur.x.aligned[idx[i]]),1,4)),1] + 1
  return(t(tmp.freq.mat))
}

GetEventTimeslices <- function(idx) {
  # returns a matrix with event counts by half-hour daytime slice
  # Args: idx = vector of event timestamps
  # build matrix
  daytimes.freq.mat <- matrix(0, nrow=13, ncol=1)  
  # calculate time.idx as halfhour slices (since 9:30=1)
  time.idx <- 
    ceiling(2*((hours(as.chron(as.character(index(cur.x.aligned[id.events.idx.up]))))-9)+
                 (minutes(as.chron(as.character(index(cur.x.aligned[id.events.idx.up]))))/60)-0.5))
  # count matching years
  for (i in seq_along(idx))
  daytimes.freq.mat[time.idx[i]] <- daytimes.freq.mat[time.idx[i]] + 1
  return(t(daytimes.freq.mat))
}

# ----- ANALYSIS: VARIABLES & TRANSFORMATIONS ----------------------------------
Winsor <- function (x, fraction=.005) {
  # returns a trimmed vector x with values outside of trim quantiles set to
  # the value of the quantile boundaries
  # Args: vector x, fraction = %of values to trim left/right of the distribution
  if(length(fraction) != 1 || fraction < 0 ||
       fraction > 0.5) {
    stop("bad value for 'fraction'")
  }
  lim <- quantile(x, probs=c(fraction, 1-fraction), na.rm=T)
  x[ x < lim[1] ] <- lim[1]
  x[ x > lim[2] ] <- lim[2]
  return (x)
}

RemoveOutliers <- function(x) x[!x %in% boxplot.stats(x)$out]
  # automatic outlier removal using outlier detection of boxplot.stats()

Trim <- function(x, trim=.03) 
  # returns a trimmed vector x, cutting left/r
  x[x>quantile(x, probs=trim, na.rm=T) & x<quantile(x, probs=1-trim, na.rm=T)]

BoxCox <- function (x, lambda, shiftpos = FALSE) {
  # returns a Box-Cox power-transformed vector x
  # Args: vector x, lambda = transformation factor,
  #       shiftpos = switch for automatic addition of constant to shift x to
  #                  positive value range
  c <- 0
  if (shiftpos) c <- max(-min(x, na.rm=T), 0,na.rm=T) + 1
  
  if (min(x+c) <0) stop("x contains negative values - please adjust!")
  
  if( lambda == 0) 
    res <- ln(x+c) else 
      res <- ((x+c)^lambda - 1) / lambda
  return (res)
}

ShiftPos <- function(x) 
  if ( min(x, na.rm=T)>=0 ) x else x + max(-min(x, na.rm=T), 0, na.rm=T) + 1
  # shifts a vector's value range to positive values

na.remove <- function(x) x <- x[!is.na(x)]

Std <- function(x) (na.remove(x) - mean(x, na.rm=T)) / sd(x, na.rm=T)
  # standardize value range of variable (for effective Box-Cox-Transform)

Std <- function(x) (na.remove(x) - mean(x, na.rm=T)) / sd(x, na.rm=T)

Summarize <- function(x) {
  # calculates statistics of interest for a given variable
  univar.summary <- vector (mode="numeric", 0)
  univar.summary <- AppendNamedVector (univar.summary, mean(x, na.rm=T), "mean")
  univar.summary <- AppendNamedVector (univar.summary, min(x, na.rm=T), "min")
  univar.summary <- AppendNamedVector (univar.summary, 
    quantile(x, probs=c(.25, .5, .75), na.rm=TRUE), c("p25", "median", "p75"))
  univar.summary <- AppendNamedVector (univar.summary, max(x, na.rm=T), "max")
  univar.summary <- AppendNamedVector (univar.summary, sd(x, na.rm=T), "sd")
  univar.summary <- AppendNamedVector (univar.summary, 
                                       skewness(x, na.rm=T), "skew")
  univar.summary <- AppendNamedVector (univar.summary, 
                                       kurtosis(x, na.rm=T), "kurt")
  univar.summary <- AppendNamedVector (univar.summary, 
                                       shapiro.test(x)$p.value, "sh.wi.p")
  univar.summary <- AppendNamedVector (univar.summary, 
                                       length(x), "N")
  univar.summary <- AppendNamedVector (univar.summary, 
                                       sum(is.na(x)), "#na")
  univar.summary <- AppendNamedVector (univar.summary, 
                                       sum(is.nan(x)), "#nan")
  return (univar.summary)
}

Summarize2 <- function(x) {
  # calculates statistics of interest for a given variable
  univar.summary <- vector (mode="numeric", 0)
  univar.summary <- AppendNamedVector (univar.summary, mean(x, na.rm=T), "mean")
  univar.summary <- AppendNamedVector (univar.summary, min(x, na.rm=T), "min")
  univar.summary <- AppendNamedVector (univar.summary, 
    quantile(x, probs=c(.01, .05, .25, .5, .75, .95, .99), 
             na.rm=TRUE), c("p01", "p05", "p25", "median", "p75", "p95", "p99"))
  univar.summary <- AppendNamedVector (univar.summary, max(x, na.rm=T), "max")
  univar.summary <- AppendNamedVector (univar.summary, sd(x, na.rm=T), "sd")
  univar.summary <- AppendNamedVector (univar.summary, 
                                       length(x), "N")
  univar.summary <- AppendNamedVector (univar.summary, 
                                       sum(is.na(x)), "#na")
  return (univar.summary)
}

GetRankClasses <- function (rank.by) {
  ranks <- as.matrix (rank.by)
  ranks <- t (sapply (1:nrow(rank.by), function(x) rank (as.matrix(rank.by[x,]), 
              na.last="keep") / length(na.remove(rank.by[x,])) ))
  # recode to HML
  ranks.cl <- ifelse (ranks <= 0.3, "L", ifelse(ranks >= 0.7, "H", "M"))
  
  rownames(ranks.cl) <- rownames(rank.by)
  colnames(ranks.cl) <- colnames(rank.by)
  
  return(ranks.cl)
}


GetRankXtiles <- function (rank.by, cuts, names) {
  ranks <- as.matrix (rank.by)
  ranks <- t (sapply (1:nrow(rank.by), function(x) rank (as.matrix(rank.by[x,]), 
    na.last="keep") / length(na.remove(rank.by[x,])) ))
  # recode
  ranks.cl <- apply (ranks, 2, FUN=cut, breaks=cuts, labels=names, 
                     include.lowest=TRUE)
  
  rownames(ranks.cl) <- rownames(rank.by)
  colnames(ranks.cl) <- colnames(rank.by)
  
  return(ranks.cl)
}

GetRankXtilesVec <- function (rank.by, cuts, names) {
  ranks <- rank.by
  ranks <- rank (ranks, na.last="keep") / length(na.remove(ranks))
  # recode
  ranks.cl <- cut (ranks, breaks=cuts, labels=names, include.lowest=TRUE)
  
  rownames(ranks.cl) <- rownames(rank.by)
  
  return(ranks.cl)
}

# ----- ANALYSIS: HFT ENTRY ----------------------------------------------------
GenerateEntryPlots <- function(entry.df, rank.by, caption, group.by) {
  # creates cumulative HFT entry plots
  # - for all stocks
  # - separate for H/M/L groups by a ranking variable
  # Args:
  #   entry.df = cumulative entry (0/1) by stock
  #   rank.by = ranking criterion value time series for all stocks
  #   caption, group.by = text parts for plot captions
  
  # 1) Prepare ranked H/M/L groups
  # calculate percentile ranks
  ranks <- as.matrix (rank.by)
  ranks <- t (sapply (1:nrow(rank.by), function(x) rank (as.matrix(rank.by[x,]), 
              na.last="keep") / length(na.remove(rank.by[x,])) ))
  # recode to HML
  ranks.cl <- ifelse (ranks <= 0.3, "L", ifelse(ranks >= 0.7, "H", "M"))
  # fix ranks after entry criterion has been reached
  ranks.cl.fixed <- ranks.cl
  for (i in 2:nrow(ranks.cl.fixed)) {
    ranks.cl.fixed[i,] <- ifelse (entry.df[i-1,], 
                                  ranks.cl.fixed[i-1,], ranks.cl[i,])
  }
  
  for (c in 1:ncol(ranks.cl.fixed)) 
    ranks.cl.fixed[,c] <- na.locf(ranks.cl.fixed[,c], , na.rm=F)
  
  for (c in 1:ncol(entry.df)) 
    entry.df[,c] <- na.locf(entry.df[,c], , na.rm=F)
  
  # 2) calculate entry by groups
  entry.df.by <- data.frame (t(sapply(251:nrow(entry.df), function(x)
    tapply(entry.df[x,], ranks.cl.fixed[x,], function(t) 
      100*mean(t, na.rm=T) , simplify=T) )))
  row.names(entry.df.by) <- row.names (entry.df[251:nrow(entry.df),])
  
  # fix effects of subgroup size changes
  for (i in 2:nrow(entry.df.by))
    entry.df.by[i,] <- ifelse (entry.df.by[i,] >= entry.df.by[i-1,],
                               entry.df.by[i,], entry.df.by[i-1,])
  
  # 3) generate plots
  # all stocks
  plot (y=100*apply(entry.df[251:nrow(entry.df),], 1, 
                    FUN=function(x) mean(x, na.rm=T)), type="l", lwd=2,
        x=as.Date(row.names(entry.df[251:nrow(entry.df),])),
        main=caption,
        xlab="Date(t)", ylab="% of stocks")
  
  # H/M/L groups
  plot (y=entry.df.by$H, x=as.Date(row.names(entry.df.by)), 
        type="l", col="red", lwd=2,
        main=paste(caption, "in H/M/L groups by", group.by),
        xlab="Date(t)", ylab="% of stocks in H/M/L groups")
  lines(y=entry.df.by$M, x=as.Date(row.names(entry.df.by)),
        type="l", col="darkgreen", lwd=2)
  lines(y=entry.df.by$L, x=as.Date(row.names(entry.df.by)),
        type="l", col="blue", lwd=2)
  legend("bottomright", legend=c("H ", "M ", "L "), 
         lwd=rep(2,3), lty=rep(1,3), col=c("red", "darkgreen", "blue"))
}

GenerateEntryPlotsExchg <- function(entry.df, caption) {
  # Exchange matrix
  stocks.exchg <- sapply (1:length(colnames(entry.df)), function(x) 
    substr(colnames(entry.df)[x], 
           nchar(colnames(entry.df)[x]), nchar(colnames(entry.df)[x])))
  
  entry.df.by <- data.frame (t(sapply(251:nrow(entry.df), function(x)
    tapply(entry.df[x,], stocks.exchg, function(t) 
      100*mean(t, na.rm=T) , simplify=T) )))
  row.names(entry.df.by) <- row.names (entry.df[251:nrow(entry.df),])
  
  # fix effects of subgroup size changes
  for (i in 2:nrow(entry.df.by))
    entry.df.by[i,] <- ifelse (entry.df.by[i,] >= entry.df.by[i-1,],
                               entry.df.by[i,], entry.df.by[i-1,])
  # N/Q groups
  plot (y=entry.df.by$N, x=as.Date(row.names(entry.df.by)), 
        type="l", col="red", lwd=2,
        main=paste(caption, "for NYSE/NASDAQ primary listing"),
        xlab="Date(t)", ylab="% of stocks per exchange")
  lines(y=entry.df.by$Q, x=as.Date(row.names(entry.df.by)),
        type="l", col="darkgreen", lwd=2)
  legend("bottomright", legend=c("NYSE ", "NASDAQ "), 
         lwd=rep(2,2), lty=rep(1,2), col=c("red", "darkgreen"))
}

GenerateEntryPlotsFlex <- function(entry.df, rank.by, caption, group.by,
                                   cuts, labels) {
  # creates cumulative HFT entry plots
  # - for all stocks
  # - separate for H/M/L groups by a ranking variable
  # Args:
  #   entry.df = cumulative entry (0/1) by stock
  #   rank.by = ranking criterion value time series for all stocks
  #   caption, group.by = text parts for plot captions
  
  # 1) Prepare ranked groups
  # calculate percentile ranks
  ranks.cl <- GetRankXtiles (rank.by, cuts=cuts, names=labels)

  # fix ranks after entry criterion has been reached
  ranks.cl.fixed <- ranks.cl
  for (i in 2:nrow(ranks.cl.fixed)) {
    ranks.cl.fixed[i,] <- ifelse (entry.df[i-1,], 
                                  ranks.cl.fixed[i-1,], ranks.cl[i,])
  }
  
  for (c in 1:ncol(ranks.cl.fixed)) 
    ranks.cl.fixed[,c] <- na.locf(ranks.cl.fixed[,c], , na.rm=F)
  
  for (c in 1:ncol(entry.df)) 
    entry.df[,c] <- na.locf(entry.df[,c], , na.rm=F)
  
  # 2) calculate entry by groups
  entry.df.by <- data.frame (t(sapply(251:nrow(entry.df), function(x)
    tapply(entry.df[x,], ranks.cl.fixed[x,], function(t) 
      100*mean(t, na.rm=T) , simplify=T) )))
  row.names(entry.df.by) <- row.names (entry.df[251:nrow(entry.df),])
  
  # fix effects of subgroup size changes
  for (i in 2:nrow(entry.df.by))
    entry.df.by[i,] <- ifelse (entry.df.by[i,] >= entry.df.by[i-1,],
                               entry.df.by[i,], entry.df.by[i-1,])
  
  # 3) generate plots
  # all stocks
  plot (y=100*apply(entry.df[251:nrow(entry.df),], 1, 
                    FUN=function(x) mean(x, na.rm=T)), type="l", lwd=2,
        x=as.Date(row.names(entry.df[251:nrow(entry.df),])),
        main=caption,
        xlab="Date(t)", ylab="% of stocks")
  
  cols <- rev(rainbow(length(labels), start=rainbow.start, end=rainbow.end))
  
  # plot groups
  plot (y=entry.df.by[,1], x=as.Date(row.names(entry.df.by)), 
        type="l", col=cols[1], lwd=2,
        main=paste(caption, "in quantile classes by", group.by),
        xlab="Date(t)", ylab="% of stocks in H/M/L groups")
  for (i in 2:length(labels)) {
    lines (y=entry.df.by[,i], x=as.Date(row.names(entry.df.by)),
           col=cols[i], lwd=2)
  }
  legend("bottomright", legend=labels, lwd=rep(2, length(labels)), 
         lty=rep(1,length(labels)), col=cols)
}


GenerateEntryTimeSeriess <- function (entry.df, rank.by, cuts, labels) {
  # creates cumulative HFT entry plots
  # - for all stocks
  # - separate for H/M/L groups by a ranking variable
  # Args:
  #   entry.df = cumulative entry (0/1) by stock
  #   rank.by = ranking criterion value time series for all stocks
  #   caption, group.by = text parts for plot captions
  
  # 1) Prepare ranked groups
  # calculate percentile ranks
  ranks.cl <- GetRankXtiles (rank.by, cuts=cuts, names=labels)
  
  # fix ranks after entry criterion has been reached
  ranks.cl.fixed <- ranks.cl
  for (i in 2:nrow(ranks.cl.fixed)) {
    ranks.cl.fixed[i,] <- ifelse (entry.df[i-1,], 
                                  ranks.cl.fixed[i-1,], ranks.cl[i,])
  }
  
  for (c in 1:ncol(ranks.cl.fixed)) 
    ranks.cl.fixed[,c] <- na.locf(ranks.cl.fixed[,c], , na.rm=F)
  
  for (c in 1:ncol(entry.df)) 
    entry.df[,c] <- na.locf(entry.df[,c], , na.rm=F)
  
  # 2) calculate entry by groups
  entry.df.by <- data.frame (t(sapply(252:nrow(entry.df), function(x)
    tapply(entry.df[x,], ranks.cl.fixed[x,], function(t) 
      100*mean(t, na.rm=T) , simplify=T) )))
  row.names(entry.df.by) <- row.names (entry.df[252:nrow(entry.df),])
  
  # fix effects of subgroup size changes
  for (i in 2:nrow(entry.df.by))
    entry.df.by[i,] <- ifelse (entry.df.by[i,] >= entry.df.by[i-1,],
                               entry.df.by[i,], entry.df.by[i-1,])
  return (entry.df.by)
}

# ----- ANALYSIS: INSTANCE DB --------------------------------------------------
FilterOverlappingEvents <- function (dfx, min.diff) {
  # returns a filtered dataframe with events at least min.diff minutes apart
  # Args: df = master eventDB, min.diff = minimum time difference
  # Returns: filtered df
  df.timestamps <- strptime (dfx$timestamp, format="%Y-%m-%d %H:%M:%S")
  
  # select events with at least min.diff minutes time-difference
  ts.last <- df.timestamps[1]
  filt <- rep (FALSE, length(df.timestamps))
  filt[1] <- TRUE
  for (e in 2:length(df.timestamps)) {
    if (difftime(df.timestamps[e], ts.last, units="mins") >= min.diff) {
      # non-overlapping event: set filter = TRUE
      filt[e] <- TRUE
      ts.last <- df.timestamps[e]
    } # else .filt remains FALSE  
  }
  return (dfx[filt, ])
}

FilterOverlappingEvents2 <- function (dfx, slot.length) {
  # returns a filtered dataframe with events filtered by x-minute slots,
  # allowing only the first event per timeslot
  # Args: df = master eventDB, slot.length = slot size in minutes
  # Returns: filtered df
  df.timestamps <- strptime (dfx$timestamp, format="%Y-%m-%d %H:%M:%S")
  
  # get events at starts of timeslots
  slots.ep.idx <- endpoints (df.timestamps, "minutes", k=slot.length)
  slots.sp.idx <- slots.ep.idx[-length(slots.ep.idx)] + 1
  
  # filter events
#   df.timestamps.filt <- df.timestamps[slots.sp.idx]
#   filt <- df.timestamps %in% df.timestamps.filt
  return (dfx[slots.sp.idx, ])
}

BuildFinalFiltDf <- function (filter.method="path") {
  # creates/or overwrites dataframes in outer lexical scope (.GlobalEnv)
  # 1) price/liq/spike filtered InstanceDB
  # 2) with overlapping events removed
  events.master.up.df.final <<- 
    events.master.up.df[(events.master.up.df$price.c1 >= param.min.price) & 
                          (events.master.up.df$PREP.liq.avg >= param.min.liq) & 
                          (events.master.up.df$PREW.spike.bar <= 66.7) &
                          (events.master.up.df$POEW.spike.bar <= 80),]
  events.master.dn.df.final <<- 
    events.master.dn.df[(events.master.dn.df$price.c1 >= param.min.price) & 
                          (events.master.dn.df$PREP.liq.avg >= param.min.liq) & 
                          (events.master.dn.df$PREW.spike.bar <= 66.7) &
                          (events.master.dn.df$POEW.spike.bar <= 80),]
  # fix mysterious observations with NA in all columns
  if (any(is.na(events.master.up.df.final$timestamp))) {
    warning("ATTENTION: NA observations introduced into ..up.df.final")
    print(sum(is.na(events.master.up.df.final$timestamp)))
    events.master.up.df.final <<- 
      events.master.up.df.final[-which(is.na(events.master.up.df.final$timestamp)),]
  }
  if (any(is.na(events.master.dn.df.final$timestamp))) {
    warning("ATTENTION: NA observations introduced into ..dn.df.final")
    print(sum(is.na(events.master.dn.df.final$timestamp)))
    events.master.dn.df.final <<- 
      events.master.dn.df.final[-which(is.na(events.master.dn.df.final$timestamp)),]
  }
  # filter overlapping events
  if (filter.method == "path") {
    events.master.up.df.filt <<- FilterOverlappingEvents (events.master.up.df.final, 
                                                          period.id.window)
    events.master.dn.df.filt <<- FilterOverlappingEvents (events.master.dn.df.final, 
                                                          period.id.window)    
  } else {
    events.master.up.df.filt <<- FilterOverlappingEvents2 (events.master.up.df.final, 
                                                           slot.length=30)
    events.master.dn.df.filt <<- FilterOverlappingEvents2 (events.master.dn.df.final, 
                                                           slot.length=30)        
  }
}

InspectEvent <- function(df, row) t(df[row,c(1:16,(17+241*8):ncol(df))])
  # prints all event variables except "x_t in FEW"

PlotEvent <- function(df, row, v) plot(GetTseries(df, row, v), type="l", lwd=2)
  # wrapper function to plot a selected event's variables in "FEW"

PlotLo <- function(x, lo.filt=.05) {
  plot(x)
  lines(lowess(x, f=lo.filt), col="blue", lwd=2)
}

FixSymbolNames <- function (df) {
  # fixes symbol naming issues in daily stock characteristics data frames
  # (stocks.xxx.df)
  names(df)[names(df) == "BF.B.N"] <- "BF-B.N"
  names(df)[names(df) == "BRK.B.N"] <- "BRK-B.N"
  names(df)[names(df) == "FCE.A.N"] <- "FCE-A.N"
  names(df)[names(df) == "HUB.B.N"] <- "HUB-B.N"
  names(df)[names(df) == "RDS.A.N"] <- "RDS-A.N"
  names(df)[names(df) == "RDS.B.N"] <- "RDS-B.N"
  names(df)[names(df) == "STRA.Q.x"] <- "STRA.Q"
  names(df)[names(df) == "VVUS.Q.x"] <- "VVUS.Q"
  names(df)[names(df) == "UAL.N.x"] <- "UAL.N"
  
  # df <- df[,-which(names(df) %in% c("STRA.Q.y", "VVUS.Q.y", "UAL.N.y"))]
  return (df)
}

# ----- ANALYSIS: STAT PREDICTS ------------------------------------------------
BuildVariableMatrices <- function (test.df, align="EE", min.rev.len=0) {
  # extracts variables r,v, rv, hft, hftpf and hftpq from dataframe
  # Args: align = "EE" for EE-alignment, "TUP" for realigned events to TUP
  if (align == "EE") {
    # 1) EE aligned
    calc.mat.r <- as.matrix(test.df[paste("r_", 
                    -period.id.window:param.post.event.window, sep="")])
    calc.mat.v <- as.matrix(test.df[paste("v_", 
                    -period.id.window:param.post.event.window, sep="")])
    calc.mat.rv <- as.matrix(test.df[paste("rv_", 
                     -period.id.window:param.post.event.window, sep="")])
    calc.mat.hft <- as.matrix(test.df[paste("hft_", 
                      -period.id.window:param.post.event.window, sep="")])
    calc.mat.hftpf <- as.matrix(test.df[paste("hftpf_", 
                        -period.id.window:param.post.event.window, sep="")])
    calc.mat.hftpq <- as.matrix(test.df[paste("hftpq_", 
                        -period.id.window:param.post.event.window, sep="")])
    calc.mat.r.b <- as.matrix(test.df[paste("r_", 
                      -period.id.window:param.post.event.window, sep="")] /
                      test.df$PREP.vola.ids)
    calc.mat.r.v <- as.matrix(test.df[paste("r_", 
                      -period.id.window:param.post.event.window, sep="")] /
                      test.df$PREP.vola)
    ts <- unclass(test.df$timestamp)
    symb <- unclass(test.df$symbol)
      
    # remove NaNs and Infs
    calc.mat.hftpf[is.nan(calc.mat.hftpf)] <- 0
    calc.mat.hftpq[is.nan(calc.mat.hftpq)] <- 0
    for (i in 1:nrow(calc.mat.hftpf)) {
      calc.mat.hftpf[i, which(calc.mat.hftpf[i, ]==Inf)] <- 
        max(calc.mat.hftpf[i, which(is.finite(calc.mat.hftpf[i, ]))])
      calc.mat.hftpq[i, which(calc.mat.hftpq[i, ]==Inf)] <- 
        max(calc.mat.hftpq[i, which(is.finite(calc.mat.hftpq[i, ]))])
    }
    calc.mats <- list (r=calc.mat.r, v=calc.mat.v, rv=calc.mat.rv, hft=calc.mat.hft,
                       hftpf = calc.mat.hftpf, hftpq = calc.mat.hftpq,
                       r.adj.b = calc.mat.r.b, r.adj.v = calc.mat.r.v, 
                       ts = ts, symb = symb)
  } else {
    # 2) TUP aligned
    sel.df <- test.df
    if (min.rev.len > 0) {
      sel.df <- subset (test.df, subset=(test.df$ERP.reversal_t >= 15))
      sel.df <- subset (sel.df, subset=(abs(sel.df$TUP.L15.r) >= 1))
      sel.df <- subset (sel.df, subset=(abs(sel.df$TUP.L15.rel.r) >= 2))
      sel.df <- subset (sel.df, subset=(sel.df$ERP.reversal >= 1))
    }
    
    calc.mat.TUP.r <- t (vGetTseriesRealigned (sel.df, 1:nrow(sel.df), 
                           "r", sel.df[1:nrow(sel.df), ]$ECP.max.cont_t, 
                                               normalize=T))
    calc.mat.TUP.v <- t (vGetTseriesRealigned (sel.df, 1:nrow(sel.df), 
                           "v", sel.df[1:nrow(sel.df), ]$ECP.max.cont_t, 
                                               normalize=F))
    calc.mat.TUP.rv <- t (vGetTseriesRealigned (sel.df, 1:nrow(sel.df), 
                            "rv", sel.df[1:nrow(sel.df), ]$ECP.max.cont_t, 
                                                normalize=F))
    calc.mat.TUP.hft <- t (vGetTseriesRealigned (sel.df, 1:nrow(sel.df), 
                             "hft", sel.df[1:nrow(sel.df), ]$ECP.max.cont_t, 
                                                 normalize=F))
    calc.mat.TUP.hftpf <- t (vGetTseriesRealigned (sel.df, 1:nrow(sel.df), 
                               "hftpf", sel.df[1:nrow(sel.df), ]$ECP.max.cont_t, 
                                                   normalize=F))
    calc.mat.TUP.hftpq <- t (vGetTseriesRealigned (sel.df, 1:nrow(sel.df),
                               "hftpq", sel.df[1:nrow(sel.df), ]$ECP.max.cont_t, 
                                                   normalize=F))
    calc.mat.TUP.r.b <- t (vGetTseriesRealigned (sel.df, 1:nrow(sel.df), 
                           "r", sel.df[1:nrow(sel.df), ]$ECP.max.cont_t, 
                                               normalize=T)) / sel.df$PREP.vola.ids
    calc.mat.TUP.r.v <- t (vGetTseriesRealigned (sel.df, 1:nrow(sel.df), 
                           "r", sel.df[1:nrow(sel.df), ]$ECP.max.cont_t, 
                                               normalize=T)) / sel.df$PREP.vola
    ts <- unclass(sel.df$timestamp)
    symb <- unclass(sel.df$symbol)
    
    # remove NaNs and Infs
    calc.mat.TUP.hftpf[is.nan(calc.mat.TUP.hftpf)] <- 0
    calc.mat.TUP.hftpq[is.nan(calc.mat.TUP.hftpq)] <- 0
    for (i in 1:nrow(calc.mat.TUP.hftpf)) {
      calc.mat.TUP.hftpf[i, which(calc.mat.TUP.hftpf[i, ]==Inf)] <- 
        max(calc.mat.TUP.hftpf[i, which(is.finite(calc.mat.TUP.hftpf[i, ]))])
      calc.mat.TUP.hftpq[i, which(calc.mat.TUP.hftpq[i, ]==Inf)] <- 
        max(calc.mat.TUP.hftpq[i, which(is.finite(calc.mat.TUP.hftpq[i, ]))])
    }
    calc.mats.TUP <- list (r=calc.mat.TUP.r, v=calc.mat.TUP.v, 
                           rv=calc.mat.TUP.rv, hft=calc.mat.TUP.hft, 
                           hftpf = calc.mat.TUP.hftpf, 
                           hftpq = calc.mat.TUP.hftpq, 
                           r.adj.b = calc.mat.TUP.r.b, 
                           r.adj.v = calc.mat.TUP.r.v,
                           ts = ts, symb=symb)    
  }
}

StatPredictCurve <- function (mats, t.start, t.end, align, suffix, f.name, 
                              outdev="dev", ry.lim=c(0,0), std.hft=FALSE, 
                              set=1, wt=NULL) {
  # Builds a 4x1 matrix chart with
  # 1) return stat.predict
  # 2) rel. vola, volume & hft flags
  # 3) HFT flag rate/volume
  # 4) HFT quote rate/volume
  # Args:
  # set short-cuts
  r <- mats$r[,(period.id.window+1+t.start):(t.end+1+period.id.window)]
  v <- mats$v[,(period.id.window+1+t.start):(t.end+1+period.id.window)]
  rv <- mats$rv[,(period.id.window+1+t.start):(t.end+1+period.id.window)]
  hft <- mats$hft[,(period.id.window+1+t.start):(t.end+1+period.id.window)]
  hftpf <- mats$hftpf[,(period.id.window+1+t.start):(t.end+1+period.id.window)]
  hftpq <- mats$hftpq[,(period.id.window+1+t.start):(t.end+1+period.id.window)]
  hftpf <- apply (hftpf, 2, Winsor, fraction=0.02)
  hftpq <- apply (hftpq, 2, Winsor, fraction=0.02)

  r.b <- mats$r.adj.b[,(period.id.window+1+t.start):(t.end+1+period.id.window)]
  r.v <- mats$r.adj.v[,(period.id.window+1+t.start):(t.end+1+period.id.window)]
  if (is.null(wt)) weights <- rep(1, nrow(r)) else weights <- wt
  
  #   hftpf <- log(mats$hftpf[,(period.id.window+1+t.start):(t.end+1+period.id.window)])
#   hftpq <- log(mats$hftpq[,(period.id.window+1+t.start):(t.end+1+period.id.window)])
#   hftpf[hftpf==-Inf] <- -12
#   hftpq[hftpq==-Inf] <- -8
  
  # calculate variables for plot
  sp.mean <- colMeans (r, na.rm=T)
  sp.q1 <- apply (r, 2, quantile, probs=.01, na.rm=T)
  sp.q5 <- apply (r, 2, quantile, probs=.05, na.rm=T)
  sp.q25 <- apply (r, 2, quantile, probs=.25, na.rm=T)
  sp.q50 <- apply (r, 2, quantile, probs=.50, na.rm=T)
  sp.q75 <- apply (r, 2, quantile, probs=.75, na.rm=T)
  sp.q95 <- apply (r, 2, quantile, probs=.95, na.rm=T)
  sp.q99 <- apply (r, 2, quantile, probs=.99, na.rm=T)
  sp.Ns <- apply (r, 2, function(x) sum(!is.na(x)))
  sp.skew <- apply (r, 2, skewness, na.rm=T)
  sp.kurt <- apply (r, 2, kurtosis, na.rm=T) - 3
  prob.rev <- apply (r, 2, function(x) sum(x>0, na.rm=T)/sum(!is.na(x)))
  if (ry.lim[1] > 0) prob.rev <- prob.rev * (ry.lim[1]-ry.lim[2]) + ry.lim[2]
  
  v.mean <- colMeans (v, na.rm=T)
  v.q1 <- apply (v, 2, quantile, probs=.01, na.rm=T)
  v.q5 <- apply (v, 2, quantile, probs=.05, na.rm=T)
  v.q25 <- apply (v, 2, quantile, probs=.25, na.rm=T)
  v.q50 <- apply (v, 2, quantile, probs=.50, na.rm=T)
  v.q75 <- apply (v, 2, quantile, probs=.75, na.rm=T)
  v.q95 <- apply (v, 2, quantile, probs=.95, na.rm=T)
  v.q99 <- apply (v, 2, quantile, probs=.99, na.rm=T)
  
  rv.mean <- colMeans (rv, na.rm=T)
  rv.q1 <- apply (rv, 2, quantile, probs=.01, na.rm=T)
  rv.q5 <- apply (rv, 2, quantile, probs=.05, na.rm=T)
  rv.q25 <- apply (rv, 2, quantile, probs=.25, na.rm=T)
  rv.q50 <- apply (rv, 2, quantile, probs=.50, na.rm=T)
  rv.q75 <- apply (rv, 2, quantile, probs=.75, na.rm=T)
  rv.q95 <- apply (rv, 2, quantile, probs=.95, na.rm=T)
  rv.q99 <- apply (rv, 2, quantile, probs=.99, na.rm=T)
  
  hft.mean <- colMeans (hft, na.rm=T)
  hft.q1 <- apply (hft, 2, quantile, probs=.01, na.rm=T)
  hft.q5 <- apply (hft, 2, quantile, probs=.05, na.rm=T)
  hft.q25 <- apply (hft, 2, quantile, probs=.25, na.rm=T)
  hft.q50 <- apply (hft, 2, quantile, probs=.50, na.rm=T)
  hft.q75 <- apply (hft, 2, quantile, probs=.75, na.rm=T)
  hft.q95 <- apply (hft, 2, quantile, probs=.95, na.rm=T)
  hft.q99 <- apply (hft, 2, quantile, probs=.99, na.rm=T)
  
  hftpf.mean <- colMeans (hftpf, na.rm=T)
  hftpf.q1 <- apply (hftpf, 2, quantile, probs=.01, na.rm=T)
  hftpf.q5 <- apply (hftpf, 2, quantile, probs=.05, na.rm=T)
  hftpf.q25 <- apply (hftpf, 2, quantile, probs=.25, na.rm=T)
  hftpf.q50 <- apply (hftpf, 2, quantile, probs=.50, na.rm=T)
  hftpf.q75 <- apply (hftpf, 2, quantile, probs=.75, na.rm=T)
  hftpf.q95 <- apply (hftpf, 2, quantile, probs=.95, na.rm=T)
  hftpf.q99 <- apply (hftpf, 2, quantile, probs=.99, na.rm=T)
  
  hftpq.mean <- colMeans (hftpq, na.rm=T)
  hftpq.q1 <- apply (hftpq, 2, quantile, probs=.01, na.rm=T)
  hftpq.q5 <- apply (hftpq, 2, quantile, probs=.05, na.rm=T)
  hftpq.q25 <- apply (hftpq, 2, quantile, probs=.25, na.rm=T)
  hftpq.q50 <- apply (hftpq, 2, quantile, probs=.50, na.rm=T)
  hftpq.q75 <- apply (hftpq, 2, quantile, probs=.75, na.rm=T)
  hftpq.q95 <- apply (hftpq, 2, quantile, probs=.95, na.rm=T)
  hftpq.q99 <- apply (hftpq, 2, quantile, probs=.99, na.rm=T) 
  
  if (std.hft) {
    hft.mean <- hft.mean - min(hft.mean, na.rm=T)
    hft.q1 <- hft.q1 - min(hft.q1, na.rm=T)
    hft.q25 <- hft.q25 - min(hft.q25, na.rm=T)
    hft.q50 <- hft.q50 - min(hft.q50, na.rm=T)
    hft.q75 <- hft.q75 - min(hft.q75, na.rm=T)
    hft.q95 <- hft.q95 - min(hft.q95, na.rm=T)
    hft.q99 <- hft.q99 - min(hft.q99, na.rm=T)
    
    hftpf.mean <- hftpf.mean - min(hftpf.mean, na.rm=T)
    hftpf.q1 <- hftpf.q1 - min(hftpf.q1, na.rm=T)
    hftpf.q25 <- hftpf.q25 - min(hftpf.q25, na.rm=T)
    hftpf.q50 <- hftpf.q50 - min(hftpf.q50, na.rm=T)
    hftpf.q75 <- hftpf.q75 - min(hftpf.q75, na.rm=T)
    hftpf.q95 <- hftpf.q95 - min(hftpf.q95, na.rm=T)
    hftpf.q99 <- hftpf.q99 - min(hftpf.q99, na.rm=T)

    hftpq.mean <- hftpq.mean - min(hftpq.mean, na.rm=T)
    hftpq.q1 <- hftpq.q1 - min(hftpq.q1, na.rm=T)
    hftpq.q25 <- hftpq.q25 - min(hftpq.q25, na.rm=T)
    hftpq.q50 <- hftpq.q50 - min(hftpq.q50, na.rm=T)
    hftpq.q75 <- hftpq.q75 - min(hftpq.q75, na.rm=T)
    hftpq.q95 <- hftpq.q95 - min(hftpq.q95, na.rm=T)
    hftpq.q99 <- hftpq.q99 - min(hftpq.q99, na.rm=T)
  }
  
  r.b.mean <- colMeans (r.b, na.rm=T)
  r.b.q1 <- apply (r.b, 2, quantile, probs=.01, na.rm=T)
  r.b.q5 <- apply (r.b, 2, quantile, probs=.05, na.rm=T)
  r.b.q25 <- apply (r.b, 2, quantile, probs=.25, na.rm=T)
  r.b.q50 <- apply (r.b, 2, quantile, probs=.50, na.rm=T)
  r.b.q75 <- apply (r.b, 2, quantile, probs=.75, na.rm=T)
  r.b.q95 <- apply (r.b, 2, quantile, probs=.95, na.rm=T)
  r.b.q99 <- apply (r.b, 2, quantile, probs=.99, na.rm=T)
  r.b.Ns <- apply (r.b, 2, function(x) sum(!is.na(x)))
  r.b.skew <- apply (r.b, 2, skewness, na.rm=T)
  r.b.kurt <- apply (r.b, 2, kurtosis, na.rm=T) - 3

  r.v.mean <- colMeans (r.v, na.rm=T)
  r.v.q1 <- apply (r.v, 2, quantile, probs=.01, na.rm=T)
  r.v.q5 <- apply (r.v, 2, quantile, probs=.05, na.rm=T)
  r.v.q25 <- apply (r.v, 2, quantile, probs=.25, na.rm=T)
  r.v.q50 <- apply (r.v, 2, quantile, probs=.50, na.rm=T)
  r.v.q75 <- apply (r.v, 2, quantile, probs=.75, na.rm=T)
  r.v.q95 <- apply (r.v, 2, quantile, probs=.95, na.rm=T)
  r.v.q99 <- apply (r.v, 2, quantile, probs=.99, na.rm=T)
  r.v.Ns <- apply (r.v, 2, function(x) sum(!is.na(x)))
  r.v.skew <- apply (r.v, 2, skewness, na.rm=T)
  r.v.kurt <- apply (r.v, 2, kurtosis, na.rm=T) - 3
  
  
  # setup 3x1 plot for set 1, and 4x1 plot for set 2
  if (outdev=="dev") dev.new(width=8.3, height=11.7)
  if (outdev=="pdf") pdf(file= paste(f.name, ".pdf", sep=""), 
                         width=8.3, height=11.7)
  if (outdev=="png") png(file= paste(f.name, ".png", sep=""), 
                         width=830*2, height=1170*2, res=200)
  if (set == 1) {
    par (mfrow = c(3, 1), oma = c(1, 0, 2, 0))
    par (mar = c(5, 4, 2, 2)) # bottom, left, top, right
  } else {
    par (mfrow = c(4, 1), oma = c(1, 0, 2, 0))
    par (mar = c(4.5, 4, 1.5, 2)) # bottom, left, top, right
  }

  # returns
  if (set==1) {
  if (ry.lim[1]==0 & ry.lim[2]==0) {
    ub.y <- max(sp.q95)
    lb.y <- min(sp.q1)    
  } else {
    ub.y <- ry.lim[1]
    lb.y <- ry.lim[2]
  }
  
  plot(x=t.start:t.end, y=sp.mean, type="l", 
       ylim=c(lb.y, ub.y), lwd=2,
       xlab=paste("time in minutes from t0=(", align, ")", sep=""), 
       ylab="return from t=0", xaxt='n',
       main="cumulative returns vs. t0")
  axis(1,seq(t.start,t.end, 15),)
  abline(v=0, col="gray")
  lines(x=t.start:t.end, y=sp.q1, col="darkred")  
  lines(x=t.start:t.end, y=sp.q5, col="red")  
  lines(x=t.start:t.end, y=sp.q25, col="orange")  
  lines(x=t.start:t.end, y=sp.q50, col="blue", lwd=2)  
  lines(x=t.start:t.end, y=sp.q75, col="darkgreen")
  lines(x=t.start:t.end, y=sp.q95, col="lightgreen")
  # lines(x=t.start:t.end, y=sp.q99, col="green")
  lines(x=t.start:t.end, y=sp.skew, col="gray", lwd=2, lty=3)
  lines(x=t.start:t.end, y=prob.rev, col="red", lwd=2, lty=3)
  # lines(x=t.start:t.end, y=sp.kurt, col="darkgray", lwd=2, lty=2)
  legend("topright", "(x.y)", legend=c("mean", "q1", "q5", "q25", 
                                       "median", "q75", "q95", "skew", "p.rev"),
         lty=c(rep(1,7), 3, 3), lwd=c(2,1,1,1,2,1,1,2,2), bty="o", bg="white",
         col=c("black", "darkred", "red", "orange", "blue", "darkgreen",
               "lightgreen", "gray", "red"), cex=0.8)
  }
  
  # adjusted returns
  if (ry.lim[1]==0 & ry.lim[2]==0) {
    ub.y <- max(r.v.q95)
    lb.y <- min(r.v.q1)    
  } else {
    ub.y <- ry.lim[1]
    lb.y <- ry.lim[2]
  }
  
  plot(x=t.start:t.end, y=r.v.mean, type="l", 
       ylim=c(lb.y, ub.y), lwd=2,
       xlab=paste("time in minutes from t0=(", align, ")", sep=""), 
       ylab="vola-adj. return from t=0", xaxt='n',
       main="cumulative vola-adj. returns vs. t0")
  axis(1,seq(t.start,t.end, 15))
  abline(v=0, col="gray")
  lines(x=t.start:t.end, y=r.v.q1, col="darkred")  
  lines(x=t.start:t.end, y=r.v.q5, col="red")  
  lines(x=t.start:t.end, y=r.v.q25, col="orange")  
  lines(x=t.start:t.end, y=r.v.q50, col="blue", lwd=2)  
  lines(x=t.start:t.end, y=r.v.q75, col="darkgreen")
  lines(x=t.start:t.end, y=r.v.q95, col="lightgreen")  
  lines(x=t.start:t.end, y=r.v.skew, col="gray", lwd=2, lty=3)
  legend("topright", "(x.y)", legend=c("mean", "q1", "q5", "q25", 
                                       "median", "q75", "q95", "skew"),
         lty=c(rep(1,7), 3), lwd=c(2,1,1,1,2,1,1,2), bty="o", bg="white",
         col=c("black", "darkred", "red", "orange", "blue", "darkgreen",
               "lightgreen", "gray"), cex=0.8)  

#   plot(x=t.start:t.end, y=r.b.mean, type="l", 
#        ylim=c(lb.y, ub.y), lwd=2,
#        xlab=paste("time in minutes from t0=(", align, ")", sep=""), 
#        ylab="vola-adj. return from t=0", xaxt='n',
#        main="cumulative vola-adj. returns vs. t0")
#   axis(1,seq(t.start,t.end, 15))
#   abline(v=0, col="gray")
#   lines(x=t.start:t.end, y=r.b.q1, col="darkred")  
#   lines(x=t.start:t.end, y=r.b.q5, col="red")  
#   lines(x=t.start:t.end, y=r.b.q25, col="orange")  
#   lines(x=t.start:t.end, y=r.b.q50, col="blue", lwd=2)  
#   lines(x=t.start:t.end, y=r.b.q75, col="darkgreen")
#   lines(x=t.start:t.end, y=r.b.q95, col="lightgreen")  
#   lines(x=t.start:t.end, y=r.b.skew, col="gray", lwd=2, lty=3)
#   legend("topright", "(x.y)", legend=c("mean", "q1", "q5", "q25", 
#                                        "median", "q75", "q95", "skew"),
#          lty=c(rep(1,7), 3), lwd=c(2,1,1,1,2,1,1,2), bty="o", bg="white",
#          col=c("black", "darkred", "red", "orange", "blue", "darkgreen",
#                "lightgreen", "gray"), cex=0.8)  
  
  
  # rel volume & vola
  if (set == 1) {
  ub.y <- max(max(r.v.mean), max(v.mean))
  lb.y <- 0
  
  plot(x=t.start:t.end, y=v.mean, type="l", 
       ylim=c(lb.y, ub.y), lwd=2, col = "black", 
       xlab=paste("time in minutes from t0=(", align, ")", sep=""), 
       ylab="factor", xaxt='n',
       main="relative volume & relative vola")
  axis(1,seq(t.start, t.end, 15))
  abline(v=0, col="gray")
  lines(x=t.start:t.end, y=v.q50, col="black", lwd=2, lty=3)
  lines(x=t.start:t.end, y=rv.mean, col="blue", lwd=2)  
  lines(x=t.start:t.end, y=rv.q50, col="blue", lwd=2, lty=3)  
  legend("topright", "(x.y)", legend=c("mean(vol)", "median(vol)",
                                       "mean(vola)", "median(vola)"),
         lty=c(1,3,1,3), lwd=c(2,2,2,2), bty="o", bg="white",
         col=c("black", "black", "blue", "blue"), cex=0.8)
  }
  
  if (set != 1) {
  # HFT flag rate
  ub.y <- max(hft.q95)
  lb.y <- 0

  plot(x=t.start:t.end, y=hft.mean, type="l", 
       ylim=c(lb.y, ub.y), lwd=2, col = "green", 
       xlab=paste("time in minutes from t0=(", align, ")", sep=""), 
       ylab="#HFT flags/minute", xaxt='n',
       main="#HFT flags/minute")
  axis(1,seq(t.start, t.end, 15))
  abline(v=0, col="gray")
  lines(x=t.start:t.end, y=hft.q50, col="green", lwd=2, lty=3)
  lines(x=t.start:t.end, y=hft.q75, col="lightgreen", lwd=1, lty=3)
  lines(x=t.start:t.end, y=hft.q95, col="darkgreen", lwd=1, lty=3)
  legend("topright", "(x.y)", legend=c("mean(HFT)", "median(HFT)",
                                       "q75(HFT)", "q95(HFT)"),
         lty=c(1,3,1,3), lwd=c(2,2,2,2), bty="o", bg="white",
         col=c("green", "green", "lightgreen", "darkgreen"), cex=0.8)
   
  # HFT PF
  ub.y <- max((hftpf.q99))
  lb.y <- min(hftpf.q50)
   
  plot(x=t.start:t.end, y=(hftpf.mean), type="l", 
       ylim=c(lb.y, ub.y), lwd=2, col = "black", 
       xlab=paste("time in minutes from t0=(", align, ")", sep=""), 
       ylab="flags/volume", xaxt='n',
       main="HFT flag rate vs. volume")
  axis(1,seq(t.start,t.end, 15))
  abline(v=0, col="gray")
  lines(x=t.start:t.end, y=(hftpf.q50), col="red", lwd=2, lty=3)
  lines(x=t.start:t.end, y=(hftpf.q75), col="pink", lwd=2, lty=3)
  lines(x=t.start:t.end, y=(hftpf.q95), col="darkred", lwd=2, lty=3)
  lines(x=t.start:t.end, y=(hftpf.q99), col="violet", lwd=2, lty=3)
  legend("topright", "(x.y)", legend=c("mean", "median", "q75", "q95", "q99"),
         lty=c(1,3,3,3,3), lwd=rep(2,5), bty="o", bg="white",
         col=c("black", "red", "pink", "darkred", "violet"), cex=0.8)
  
  # HFT PQ
  ub.y <- max((hftpq.q99))
  lb.y <- min(hftpq.q50)
  
  plot(x=t.start:t.end, y=(hftpq.mean), type="l", 
       ylim=c(lb.y, ub.y), lwd=2, col = "black", 
       xlab=paste("time in minutes from t0=(", align, ")", sep=""), 
       ylab="quotes/volume", xaxt='n',
       main="HFT quote rate vs. volume")
  axis(1,seq(t.start,t.end, 15))
  abline(v=0, col="gray")
  lines(x=t.start:t.end, y=(hftpq.q50), col="red", lwd=2, lty=3)
  lines(x=t.start:t.end, y=(hftpq.q75), col="pink", lwd=2, lty=3)
  lines(x=t.start:t.end, y=(hftpq.q95), col="darkred", lwd=2, lty=3)
  lines(x=t.start:t.end, y=(hftpq.q99), col="violet", lwd=2, lty=3)  
  legend("topright", "(x.y)", legend=c("mean", "median", "q75", "q95", "q99"),
         lty=c(1,3,3,3,3), lwd=rep(2,5), bty="o", bg="white",
         col=c("black", "red", "pink", "darkred", "violet"), cex=0.8)
  }
  
  # create overall title & subtitle
  title (paste("Structure of Extreme Events", suffix, sep=""), outer = TRUE, cex=1.5)
  mtext (paste("N = ", max(sp.Ns)), 1, outer=T, cex=0.75)
  if (outdev != "dev") dev.off() 
}


# ----- TO BE SORTED -----------------------------------------------------------
GetTSTime <- function(ts) times(as.chron(ts))-dates(as.chron(ts))

MergeStockdayPanels <- function(df1, df2) {
  dt1 <- data.table (df1, keep.rownames=T)
  dt1 <- data.table (dt1, key="rn")
  dt2 <- data.table (df2, keep.rownames=T)
  dt2 <- data.table (dt2, key="rn")
  dtm <- merge(dt1, dt2, all=T)
  test.df <- data.frame (dtm)
  row.names(test.df) <- test.df$rn
  return (test.df[, !(colnames(test.df) %in% c("rn"))])
}

## Heteroskedasticity-robust standard error calculation.
SummaryW <- function(model) {
  s <- summary(model)
  X <- model.matrix(model)
  u2 <- residuals(model)^2
  XDX <- 0
  
  ## Here one needs to calculate X'DX. But due to the fact that
  ## D is huge (NxN), it is better to do it with a cycle.
  for(i in 1:nrow(X)) {
    XDX <- XDX + u2[i]*X[i,]%*%t(X[i,])
  }
  
  # inverse(X'X)
  XX1 <- solve(t(X)%*%X)
  
  # Variance calculation (Bread x meat x Bread)
  varcovar <- XX1 %*% XDX %*% XX1
  
  # degrees of freedom adjustment
  dfc <- sqrt(nrow(X))/sqrt(nrow(X)-ncol(X))
  
  # Standard errors of the coefficient estimates are the
  # square roots of the diagonal elements
  stdh <- dfc*sqrt(diag(varcovar))
  
  t <- model$coefficients/stdh
  p <- 2*pnorm(-abs(t))
  results <- cbind(model$coefficients, stdh, t, p)
  dimnames(results) <- dimnames(s$coefficients)
  results
}

Reg1stStage <- function(pdf, regf, classes, full=T, verbose=T) {
  # full sample results
  if (full) {
    print(" --- 1) Overall 1st stage regression ---")
    test.fe2 <- felm (regf, pdf, clustervar=interaction(pdf$date, pdf$symbol))
    if (verbose) print(summary (test.fe2)) else 
      print(rbind(test.fe2$coefficients, test.fe2$rpval))
  }
  # results by level
  for (i in seq_along(levels(classes))) {
    lev <- levels (classes)[i]
    print(paste(paste("2.", i, sep=""),
                "Subset 1st stage regression on level", lev))
    tpdf <- pdf[classes==lev,]
    test.fe2 <- felm (regf, tpdf, clustervar=interaction(tpdf$date, tpdf$symbol))
    if (verbose) print(summary (test.fe2)) else
      print(rbind(test.fe2$coefficients, test.fe2$rpval))
  }
}

Reg1stStageGetHat <- function(pdf, regf, classes, segment=0) {
  # obtains IV estimate "beta-hat" for given segment
  # (0=full sample, 1-N: subset given by X-tile class number)
  if (segment == 0) {
    print(" --- Overall 1st stage regression ---")
    test.fe2 <- felm (regf, pdf, clustervar=interaction(pdf$date, pdf$symbol))
    print(summary(test.fe2))
    beta.hat <- test.fe2$fitted
  } else {
    i <- segment
    lev <- levels(classes)[i]
    print(paste("Subset 1st stage regression on level", lev))
    tpdf <- pdf[classes==lev,]
    test.fe2 <- felm (regf, tpdf, clustervar=interaction(tpdf$date, tpdf$symbol))
    print(summary(test.fe2))
    beta.hat <- test.fe2$fitted
  }
}

Vif <-function (in_frame, thresh=10, trace=T) {
  require(fmsb)
  
  if(class(in_frame) != 'data.frame') in_frame<-data.frame(in_frame)
  
  #get initial vif value for all comparisons of variables
  vif_init<-NULL
  for(val in names(in_frame)){
    form_in<-formula(paste(val,' ~ .'))
    vif_init<-rbind(vif_init,c(val,VIF(lm(form_in,data=in_frame))))
  }
  vif_max<-max(as.numeric(vif_init[,2]))
  
  if(vif_max < thresh){
    if(trace==T){ #print output of each iteration
      prmatrix(vif_init,collab=c('var','vif'),rowlab=rep('',nrow(vif_init)),quote=F)
      cat('\n')
      cat(paste('All variables have VIF < ', thresh,', max VIF ',round(vif_max,2), sep=''),'\n\n')
    }
    return(names(in_frame))
  }
  else{
    
    in_dat<-in_frame
    
    #backwards selection of explanatory variables, stops when all VIF values are below 'thresh'
    while(vif_max >= thresh){
      
      vif_vals<-NULL
      
      for(val in names(in_dat)){
        form_in<-formula(paste(val,' ~ .'))
        vif_add<-VIF(lm(form_in,data=in_dat))
        vif_vals<-rbind(vif_vals,c(val,vif_add))
      }
      max_row<-which(vif_vals[,2] == max(as.numeric(vif_vals[,2])))[1]
      
      vif_max<-as.numeric(vif_vals[max_row,2])
      
      if(vif_max<thresh) break
      
      if(trace==T){ #print output of each iteration
        prmatrix(vif_vals,collab=c('var','vif'),rowlab=rep('',nrow(vif_vals)),quote=F)
        cat('\n')
        cat('removed: ',vif_vals[max_row,1],vif_max,'\n\n')
        flush.console()
      }
      
      in_dat<-in_dat[,!names(in_dat) %in% vif_vals[max_row,1]]
      
    }
    
    return(names(in_dat))
  }
}

PanelResAC <- function (panelres, paneldata, firmvar, timevar) {
  # check cross-sectional autocorrelation in residuals
  hist(tapply(panelres$residuals, paneldata[, timevar], 
              function(x) acf(x, lag.max=1, plot=F)$acf[2,1,1]),
       main="Firm residual correlation", xlab="AC")
  # check time series autocorrelation in residuals
  hist(tapply(panelres$residuals, paneldata[, firmvar], 
              function(x) acf(x, lag.max=1, plot=F)$acf[2,1,1]),
       main="Time residual correlation", xlab="AC")
}

SubstrRight <- function(x, n) {
  substr(x, nchar(x)-n+1, nchar(x))
}

# ----- SUPPORT ----------------------------------------------------------------

beep <- function(n = 3){
  # beeps n times
  for(i in seq(n)){
    system("rundll32 user32.dll,MessageBeep -1")
    Sys.sleep(.5)
  }
}

AppendNamedVector <- function(x, a, name) {
  # appends vector a to existing vector x, naming new elements with name vector
  # Args: x = vector, a = atomic or vector, name has to be of same length
  if (length(a) != length(name)) stop ("name length has to match a")
  x[length(x)+1L:length(a)] <- a
  names(x) <- c(names(x[-(length(x)-seq(from=0, by=1, length.out=length(a)))]), 
                name)
  return (x)
}
