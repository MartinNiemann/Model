# ******************************************************************************
# Intraday Overreaction - CalcScores.R                                         *
# ******************************************************************************
# This script reads index and stock csv files to generate a file with          *
# overreaction scores and components for each stock                            *
# Process:                                                                     *
# 1) load price files for index, risk-free rate, then for each stock in loop   *
# 2) generate date-time aligned series                                         *
# 3) calculate rolling robust betas stock vs. index                            *
# 4) calculate overreaction scores and save to file for each stock in loop     *
# 5) clean-up for obsolete variables                                           *
# ******************************************************************************


# ----- DATA -------------------------------------------------------------------

# get available csv data files
data.files <- list.files (path = data.path, pattern = "*[NQA].csv")
data.files.etf <- list.files (path = data.path.etf, pattern = "*[QN].csv")

# read index and risk-free-rate files into xts series and convert to daily
cur.path <- paste (data.path.etf, data.files.etf[1], sep="/")
sp.x <- Load1minDataXTS (cur.path)
sp.d.x <- to.daily (sp.x, drop.time=TRUE, name=NULL)

rf.d <- read.csv ("D:/Doktorarbeit/40 Data/Rf/FRB_H15.csv", header=TRUE,
                  stringsAsFactors=FALSE, col.names=c("Date", "Rf"), skip=6)
rf.d.x <- xts (as.numeric(rf.d[, 2]), as.Date(strptime(rf.d[, 1],"%m/%d/%Y")))
colnames (rf.d.x) <- "Rf"
rf.d.x <- na.locf (rf.d.x)       # replace missing values with prior value

# ----- CORE LOOP --------------------------------------------------------------
# for (i in 1:100) {
for (i in 1:length(data.files)) {
  # read current stock file into xts series and convert to daily
  cur.path <- paste (data.path, data.files[i], sep="/")
  print (paste("[CalcScores.R] Processing ", data.files[i], sep=""))
  cur.symbol <- as.character(strsplit(data.files[i],".csv")[1])
  cur.x <- Load1minDataXTS (cur.path)
  # skip series with length < one year
  if (nrow(cur.x) < daybars * period.oneyear ) {
    print (paste("[CalcScores.R] Processing skipped for ", data.files[i], 
                 " -- too short", sep=""))
    next
  }
  cur.d.x <- to.daily (cur.x, drop.time=TRUE, name=NULL)
  
  # create aligned daily time series' for stock, index and Rf
  sp.d.x.aligned <- sp.d.x[index(sp.d.x) >= start(cur.d.x) &  # cut excess dates
                             index(sp.d.x) <= end(cur.d.x)]   # at boundaries
  cur.d.x.aligned <- cur.d.x[index(cur.d.x) %in% index(sp.d.x.aligned)]
  sp.d.x.aligned <- sp.d.x.aligned[index(sp.d.x.aligned) %in% index(cur.d.x)]
  stopifnot (all.equal (index(sp.d.x.aligned), index(cur.d.x.aligned)))
  rf.d.x.aligned <- rf.d.x[index(rf.d.x) %in% index(cur.d.x.aligned)]
  stopifnot (all.equal (as.character.Date(index(rf.d.x.aligned)), 
                        as.character.Date(index(cur.d.x.aligned))))

  # cut 1-min series using cut.dates from daily cut procedure
  cut.dates.sp <- index(sp.d.x)[!(index(sp.d.x) %in% index(sp.d.x.aligned))]
  cut.dates.cur <- index(cur.d.x)[!(index(cur.d.x) %in% index(cur.d.x.aligned))]
  
  sp.x.cut <- sp.x[as.character.Date(cut.dates.sp)]        # making use of xts
  cur.x.cut <- cur.x[as.character.Date(cut.dates.cur)]     # by-date selection
  sp.x.aligned <- sp.x[!(index(sp.x) %in% index(sp.x.cut))]        # cut excess
  cur.x.aligned <- cur.x[!(index(cur.x) %in% index(cur.x.cut))]    # days
  # align / fill missing minutes due to different session-cuts
  cur.x.aligned <- AlignTimesAndFillXTS (cur.x.aligned, sp.x.aligned)
  
  stopifnot (all.equal(index(sp.x.aligned), index(cur.x.aligned))) # assert ==
  stopifnot (all(is.numeric(cur.x.aligned)))                       # and no NAs

  # calculate robust one-year betas (t0 --> can be applied t+1)
  cur.rbetas.d.x <- CalculateRollingBetaXTS ( cur.d.x.aligned, sp.d.x.aligned, 
                                              rf.d.x.aligned )
  plot (cur.rbetas.d.x, ylab="Beta (Stock ~ S&P500)", xlab="Date", type="l",
        main="One-year rolling robust CAPM betas of Stock vs. S&P500 ETF")

  # calculate signal components
  # ---------------------------
  # get day-starts and -ends
  day.ends <- endpoints (cur.x.aligned,on='days')[-1] # all elements except 1st
  stopifnot (all(diff(day.ends) == 391))              # assert equal day length
  
  # backfill 1st 10 minutes of opening data in case of zero-bars
  cur.x.aligned <- BackfillOpeningData(cur.x.aligned, 10)
  
  # calculate 1-minute returns and intraday returns (excluding inter-day return)
  ret.x <- 100 * diff (log(cur.x.aligned$Close))    # 1-min returns
  idret <- unclass (ret.x)                          # intraday returns variable
  sp.ret.x <- 100 * diff (log(sp.x.aligned$Close))  # same for benchmark
  sp.idret <- unclass (sp.ret.x)
  idvol <- unclass (cur.x.aligned$Volume)
  idvol[which(is.infinite(idvol))] <- 0
    
  # remove inter-day returns from intraday return series
  # by replacing daily 1st minute returns with intrabar returns from open
  idret[day.ends[1:(length(day.ends))] - daybars+1, 1] <- 100 *
    log (cur.x.aligned$Close[day.ends[1:(length(day.ends))] - daybars+1] / 
           cur.x.aligned$Open[day.ends[1:(length(day.ends))] - daybars+1])
  sp.idret[day.ends[1:(length(day.ends))] - daybars+1, 1] <- 100 *
    log (sp.x.aligned$Close[day.ends[1:(length(day.ends))] - daybars+1] / 
           sp.x.aligned$Open[day.ends[1:(length(day.ends))] - daybars+1])
  
  idret[day.ends[1:(length(day.ends))] - daybars+1, 1] <- ifelse (
    abs(idret[day.ends[1:(length(day.ends))] - daybars+1, 1]) > 1, 
      max(-1, min(1, idret[day.ends[1:(length(day.ends))] - daybars+1, 1])),
      idret[day.ends[1:(length(day.ends))] - daybars+1, 1])
  
  # prepare daily calculations - slicing and left-bounded window index
  idret.daily <- array (idret, dim=c(daybars, length(day.ends)))
  idrv.daily <- idret.daily^2
  sp.idret.daily <- array (sp.idret, dim=c(daybars, length(day.ends)))
  sp.idrv.daily <- sp.idret.daily^2
  idvol.daily <- array (idvol, dim=c(daybars, length(day.ends)))
  index.left <- c (rep(1,period.id.window), 2:(daybars-period.id.window+1))  
  
  # prepare result variables (all except volume, which remains unsmoothed)
  id.daily.dsbmom <- array (NA, dim=c(daybars, length(day.ends)))
  id.daily.rv.dsbavg <- array (NA, dim=c(daybars, length(day.ends)))
  sp.id.daily.dsbmom <- array (NA, dim=c(daybars, length(day.ends)))
  sp.id.daily.rv.dsbavg <- array (NA, dim=c(daybars, length(day.ends)))

  # calculate day-start bounded mom and variance
  rollsum <- rep (NA, length(day.ends))    # array with NA for each day
  rollsum2 <- rollsum
  rollsum.sp <- rollsum
  rollsum <- idret.daily[1, ]              # initialize rolling sums with
  rollsum2 <- idrv.daily[1, ]              # 1st bars of days
  rollsum.sp <- sp.idret.daily[1, ] 
  rollsum2.sp <- sp.idrv.daily[1, ] 
  id.daily.dsbmom[1, ] <- rollsum
  id.daily.rv.dsbavg[1, ] <- rollsum2
  sp.id.daily.dsbmom[1, ] <- rollsum.sp
  sp.id.daily.rv.dsbavg[1, ] <- rollsum2.sp
  for (i in 2:daybars) {
    # for each day, roll sums forward
    if (i > period.id.window) {
      rollsum <- rollsum - idret.daily[index.left[i-1], ]
      rollsum2 <- rollsum2 - idrv.daily[index.left[i-1], ]
      rollsum.sp <- rollsum.sp - sp.idret.daily[index.left[i-1], ]
      rollsum2.sp <- rollsum2.sp - sp.idrv.daily[index.left[i-1], ]
    }
    rollsum <- rollsum + idret.daily[i, ]
    rollsum2 <- rollsum2 + idrv.daily[i, ]
    rollsum.sp <- rollsum.sp + sp.idret.daily[i, ]
    rollsum2.sp <- rollsum2.sp + sp.idrv.daily[i, ]
    id.daily.dsbmom[i, ] <- rollsum
    id.daily.rv.dsbavg[i, ] <- rollsum2 / (i - index.left[i] + 1)
    sp.id.daily.dsbmom[i, ] <- rollsum.sp
    sp.id.daily.rv.dsbavg[i, ] <- rollsum2.sp / (i - index.left[i] + 1)
  }
  id.daily.rv.dsbavg[id.daily.rv.dsbavg < 0 & id.daily.rv.dsbavg > -eps ] <- 0
  id.daily.vol.dsbavg <- sqrt(id.daily.rv.dsbavg)   # vola: sqrt of rv
  stopifnot (all(is.finite(id.daily.vol.dsbavg)))   # assert integrity
  sp.id.daily.rv.dsbavg[sp.id.daily.rv.dsbavg < 0 & 
                          sp.id.daily.rv.dsbavg > -eps ] <- 0
  sp.id.daily.vol.dsbavg <- sqrt(sp.id.daily.rv.dsbavg) # vola: sqrt of rv
  stopifnot (all(is.finite(sp.id.daily.vol.dsbavg)))    # assert integrity
  
  # additional High/Low-based momentum
  # using alternative calculation approach based on prices directly
  id.daily.dsbmom.hi <- array (NA, dim=c(daybars, length(day.ends)))
  id.daily.dsbmom.lo <- array (NA, dim=c(daybars, length(day.ends)))
  cur.cl.daily <- 
    array (coredata(cur.x.aligned$Close), dim=c(daybars, length(day.ends)))
  cur.hi.daily <- 
    array (coredata(cur.x.aligned$High), dim=c(daybars, length(day.ends)))
  cur.lo.daily <- 
    array (coredata(cur.x.aligned$Low), dim=c(daybars, length(day.ends)))
  rollref <- rep (NA, length(day.ends))    # array with NA for each day
  rollref <- cur.cl.daily[1, ]
  rollmin <- rollref
  rollmax <- rollref
  id.daily.dsbmom.hi[1:3, ] <- 0
  id.daily.dsbmom.lo[1:3, ] <- 0
  # drop first bar as reference point to filter out opening crap data
  index.left2 <- index.left
  index.left2[index.left2 == 1] <- 2
  for (i in 4:daybars) {
    # rollref <- cur.cl.daily[index.left[i], ]
    # id.daily.dsbmom.hi[i, ] <- 100 * log (cur.hi.daily[i, ] / rollref)
    # id.daily.dsbmom.lo[i, ] <- 100 * log (cur.lo.daily[i, ] / rollref)
    rollmin <- apply (cur.lo.daily[(index.left[i]):(i-1), ], 2, min)
    rollmax <- apply (cur.hi.daily[(index.left[i]):(i-1), ], 2, max)
    id.daily.dsbmom.hi[i, ] <- 100 * log (cur.hi.daily[i, ] / rollmin)
    id.daily.dsbmom.lo[i, ] <- 100 * log (cur.lo.daily[i, ] / rollmax)
  }
  
  
  # calculate day-time average volatility and volume
  # 90? change of logic: calculate day-time average by looping through 
  # all days, calculating averages for all bars-in-day in parallel
  # ATTENTION: id.daily.vol.alt.dta is a simple dta of 1-min squared returns
  index.left2 <- c(rep(1,period.days), 2:(length(day.ends)-period.days+1))
  rollsum <- id.daily.vol.dsbavg[, 1]      # initialize rollsum with 1st day
  rollsum.v <- idvol.daily[, 1]            # initialize rollsum.v with 1st day
  rollsum.alt <- idrv.daily[, 1]
  rollsum.sp <- sp.id.daily.vol.dsbavg[, 1] 
  id.daily.vol.dsbavg.dta <- array (c(NA),   # result arrays
                                    dim=c(nrow(id.daily.vol.dsbavg), 
                                          ncol(id.daily.vol.dsbavg)))
  id.daily.volume.dta <- id.daily.vol.dsbavg.dta
  sp.id.daily.vol.dsbavg.dta <- id.daily.vol.dsbavg.dta
  id.daily.vol.alt.dta <- id.daily.vol.dsbavg.dta

  for (j in 2:length(day.ends)) {
    rollsum <- rollsum + id.daily.vol.dsbavg[, j]
    rollsum.v <- rollsum.v + idvol.daily[, j]
    rollsum.sp <- rollsum.sp + sp.id.daily.vol.dsbavg[, j]
    rollsum.alt <- rollsum.alt + idrv.daily[, j]    
    if (j > period.days) {
      rollsum <- rollsum - id.daily.vol.dsbavg[, index.left2[j-1]]
      rollsum.v <- rollsum.v - idvol.daily[, index.left2[j-1]]
      rollsum.sp <- rollsum.sp - sp.id.daily.vol.dsbavg[, index.left2[j-1]]
      rollsum.alt <- rollsum.alt - idrv.daily[, index.left2[j-1]]
    }  
    if (j >= period.days) {
      # setting result array here results in NA for 1st period.days
      id.daily.vol.dsbavg.dta[, j] <- rollsum / period.days
      id.daily.volume.dta[, j] <- rollsum.v / period.days
      sp.id.daily.vol.dsbavg.dta[, j] <- rollsum.sp / period.days
      id.daily.vol.alt.dta[, j] <- rollsum.alt / period.days
    }
  }
  id.daily.vol.alt.dta[id.daily.vol.alt.dta < 0 & id.daily.vol.alt.dta > -eps ] <- 0
  id.daily.vol.alt.dta <- sqrt(id.daily.vol.alt.dta)
  stopifnot (all(is.finite(id.daily.vol.alt.dta[,(period.days+1):length(day.ends)])))
  
  # calculate abnormal (beta-adjusted) stock momentum
  cur.rbetas.d.x.shifted <- cur.rbetas.d.x
  coredata(cur.rbetas.d.x.shifted) <- 
    append (cur.rbetas.d.x[-nrow(cur.rbetas.d.x)],1,0)
  cur.rbetas.d.x.shifted <- na.locf (cur.rbetas.d.x.shifted)
  # NAs at start replaced by 1s, and shifted back by 1 day

  id.daily.dsbmom.e <- array(c(NA), dim=c(daybars, length(day.ends)))
  for (j in 1:length(day.ends)) {
    id.daily.dsbmom.e[, j] <- sp.id.daily.dsbmom[, j] * 
                              cur.rbetas.d.x.shifted[j]
  }
  id.daily.dsbmom.a <- id.daily.dsbmom - id.daily.dsbmom.e
  
  # calculate day-start bounded scaled version of rv
  id.daily.vol.dsbavg.dta.scaled <- id.daily.vol.dsbavg.dta
  sp.id.daily.vol.dsbavg.dta.scaled <- sp.id.daily.vol.dsbavg.dta
  for (i in 2:daybars) {
    id.daily.vol.dsbavg.dta.scaled[i,] <- sqrt(min(60, i)) *
      id.daily.vol.dsbavg.dta[i,]
    sp.id.daily.vol.dsbavg.dta.scaled[i,] <- sqrt(min(60, i)) *
      sp.id.daily.vol.dsbavg.dta[i,]
  }
  
  # calculate daily smoothed version of 1-minute dta.rv (.alt calculation)
  id.daily.vol.alt.dta2 <- id.daily.vol.alt.dta
  for (j in (period.days+1):length(day.ends)) {
    id.daily.vol.alt.dta2[,j] <- lowess(id.daily.vol.alt.dta[,j], f=.05)$y
  }
  
  # convert daily-slice arrays back to time series
  id.dsbmom <- array (id.daily.dsbmom, dim=c(nrow(id.daily.dsbmom) *
                                               ncol(id.daily.dsbmom),1))
  id.dsbmom.a <- array (id.daily.dsbmom.a, dim=c(nrow(id.daily.dsbmom.a) *
                                                   ncol(id.daily.dsbmom.a),1))
  id.rv.dta <- array (id.daily.vol.dsbavg.dta, 
                      dim=c(nrow(id.daily.vol.dsbavg.dta) *
                              ncol(id.daily.vol.dsbavg.dta),1))
  id.rv.dta.sc <- array (id.daily.vol.dsbavg.dta.scaled, 
                         dim=c(nrow(id.daily.vol.dsbavg.dta.scaled) *
                                 ncol(id.daily.vol.dsbavg.dta.scaled),1))
  id.rv.1min.dta <- array (id.daily.vol.alt.dta2, 
                           dim=c(nrow(id.daily.vol.alt.dta2) *
                                   ncol(id.daily.vol.alt.dta2),1))
  id.volume.dta  <- array (id.daily.volume.dta, 
                           dim=c(nrow(id.daily.volume.dta) *
                                   ncol(id.daily.volume.dta),1))
  sp.id.dsbmom <- array (sp.id.daily.dsbmom, dim=c(nrow(sp.id.daily.dsbmom) *
                                               ncol(sp.id.daily.dsbmom),1))
  sp.id.rv.dta <- array (sp.id.daily.vol.dsbavg.dta, 
                      dim=c(nrow(sp.id.daily.vol.dsbavg.dta) *
                              ncol(sp.id.daily.vol.dsbavg.dta),1))
  sp.id.rv.dta.sc <- array (sp.id.daily.vol.dsbavg.dta.scaled, 
                         dim=c(nrow(sp.id.daily.vol.dsbavg.dta.scaled) *
                                 ncol(sp.id.daily.vol.dsbavg.dta.scaled),1))
  id.dsbmom.hi <- array (id.daily.dsbmom.hi, dim=c(nrow(id.daily.dsbmom.hi) *
                                               ncol(id.daily.dsbmom.hi),1))
  id.dsbmom.lo <- array (id.daily.dsbmom.lo, dim=c(nrow(id.daily.dsbmom.lo) *
                                               ncol(id.daily.dsbmom.lo),1))
  
  # shift dta vola and volume arrays back 1 day
  # (cut last day and add 1 NA day at front)
  length(id.rv.dta) <- length(id.rv.dta) - daybars     
  id.rv.dta <- append (id.rv.dta, rep(NA, daybars), 0)
  length(id.rv.dta.sc) <- length(id.rv.dta.sc) - daybars     
  id.rv.dta.sc <- append (id.rv.dta.sc, rep(NA, daybars), 0)
  length(id.volume.dta) <- length(id.volume.dta) - daybars
  id.volume.dta <- append (id.volume.dta, rep(NA, daybars), 0)
  length(sp.id.rv.dta) <- length(sp.id.rv.dta) - daybars     
  sp.id.rv.dta <- append (sp.id.rv.dta, rep(NA, daybars), 0)
  length(sp.id.rv.dta.sc) <- length(sp.id.rv.dta.sc) - daybars     
  sp.id.rv.dta.sc <- append (sp.id.rv.dta.sc, rep(NA, daybars), 0)
  
  # calculate score
  id.score <- id.dsbmom / id.rv.dta.sc
  id.score.a <- id.dsbmom.a / id.rv.dta.sc
  id.score.hi <- id.dsbmom.hi / id.rv.dta.sc
  id.score.lo <- id.dsbmom.lo / id.rv.dta.sc
  # ATTENTION: dividing by id.rv.dta.sc (instead of id.rv.dta * sqrt(60)),
  #            allows detecting events at day-start (>=10 mins since start
  #            the dta vola estimate is quite stable)
  
  # save score file for current stock
  # - aligned 1-min and daily prices
  # - betas and id mom, score
  # - volume & vola dtas (stock & spx)
  save (cur.x.aligned, cur.d.x.aligned, sp.x.aligned, sp.d.x.aligned, 
        cur.rbetas.d.x.shifted, id.dsbmom, id.score, # id.dsbmom.a, id.score.a, 
        id.volume.dta, id.rv.dta, id.rv.dta.sc, id.rv.1min.dta, sp.id.rv.dta.sc,
        id.score.hi, id.score.lo,
        file=paste(data.path.out, cur.symbol, ".sc2", sep=""))
}

beep()
# ----- CLEAN-UP ---------------------------------------------------------------
gc()