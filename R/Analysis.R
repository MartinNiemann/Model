# 0. PREP: LOAD INSTANCE-DB ----------------------------------------------------

# 0.a) Load symbol and variable info -------------------------------------------
# get symbol info
symbol.info <- 
  read.csv ("D:/Doktorarbeit/40 Data/Stocks/Taipan/symbol_info2.csv", 
            header=TRUE, stringsAsFactors=FALSE, comment.char="", 
            col.names=c("Symbol", "shares_out", "Name", 
                        "Sector", "Industry", "Country", "shares_fl",
                        "NMS_Date", "SSB_Date_1" ,"SSB_Date_2"), 
            colClasses=c("character", "numeric", rep("character", 4), "numeric",
                         rep("character",3)))
rownames(symbol.info) <- symbol.info$Symbol
class(symbol.info[,8:10]) <- "Date"

# remove excess symbol
symbol.info <- symbol.info[-which(symbol.info$Symbol == "RDS-A.N"),]

# get variable info
# var.info <- read.csv("names.csv", stringsAsFactors=FALSE)
# head(var.info)
# TODO: update variable info to names2.csv

# 0.b) load data bases ---------------------------------------------------------
# create temporary environment for partial DB-load
load.env <- new.env()

# load and merge InstanceDB, Sum series, and Stockday Panel data frames
db.files <- list.files (path = data.path.out, pattern = '*part[1-9].idb')
for (db in seq_along(db.files)) {
  # load part into load-environment
  rm (list=ls(load.env), envir=load.env)
  load (file=paste(data.path.out, db.files[db], sep=""), envir=load.env)

  # copy part 1 into general workspace for merging
  if (db == 1) {
    for (i in seq_along(ls(load.env))) {
      assign(ls(load.env)[i], get(ls(load.env)[i], envir=load.env)) 
    }    
  } else {
    # perform merge procedures for other parts
    # m.1: InstanceDBs
    events.master.up.df <- rbind (events.master.up.df, 
                                  load.env$events.master.up.df)
    events.master.dn.df <- rbind (events.master.dn.df, 
                                  load.env$events.master.dn.df)
    # m.2: sum objects
    active.stocks.sum.x <- active.stocks.sum.x + 
      load.env$active.stocks.sum.x[index(active.stocks.sum.x)]
    hft.flags.sum.x     <- hft.flags.sum.x + 
      load.env$hft.flags.sum.x[index(hft.flags.sum.x)]
    hft.mins.sum.x      <- hft.mins.sum.x + 
      load.env$hft.mins.sum.x[index(hft.mins.sum.x)]
    # m.3: stock-day panels
    for (i in seq_along(ls(load.env, pattern="stocks.*.df"))) {
      cur.name <- ls (load.env, pattern="stocks.*.df")[i]
      assign(cur.name, MergeStockdayPanels(get(cur.name), 
                                           get(cur.name, envir=load.env)))
    }
  }
}
# sort InstanceDB by timestamp and symbol
events.master.up.df <- 
  events.master.up.df[with(events.master.up.df, order(timestamp)), ]
events.master.dn.df <- 
  events.master.dn.df[with(events.master.dn.df, order(timestamp)), ]
# restore InstanceDB timestamp-symbol rownames
row.names (events.master.up.df) <- paste (events.master.up.df$timestamp, 
                                          events.master.up.df$symbol, sep=" ")
row.names (events.master.dn.df) <- paste (events.master.dn.df$timestamp, 
                                          events.master.dn.df$symbol, sep=" ")

# load and merge HiLo-based score PanelDBs
db.files <- list.files (path = data.path.out, pattern = '*part[1-9]bb.pdb')
for (db in seq_along(db.files)) {
  # load part into load-environment
  rm (list=ls(load.env), envir=load.env)
  load (file=paste(data.path.out, db.files[db], sep=""), envir=load.env)
  # copy part 1 into general workspace for merging
  if (db == 1) {
    for (i in seq_along(ls(load.env))) {
      assign(ls(load.env)[i], get(ls(load.env)[i], envir=load.env)) 
    }    
  } else {
    for (i in seq_along(ls(load.env))) {
      cur.name <- ls (load.env)[i]
      assign(cur.name, MergeStockdayPanels(get(cur.name), 
                                           get(cur.name, envir=load.env)))
    }
  }
}

# load and merge distribution PanelDBs
db.files <- list.files (path = data.path.out, pattern = '*part[1-9]c.pdb')
for (db in seq_along(db.files)) {
  # load part into load-environment
  rm (list=ls(load.env), envir=load.env)
  load (file=paste(data.path.out, db.files[db], sep=""), envir=load.env)
  # copy part 1 into general workspace for merging
  if (db == 1) {
    for (i in seq_along(ls(load.env))) {
      assign(ls(load.env)[i], get(ls(load.env)[i], envir=load.env)) 
    }    
  } else {
    for (i in seq_along(ls(load.env))) {
      cur.name <- ls (load.env)[i]
      assign(cur.name, MergeStockdayPanels(get(cur.name), 
                                           get(cur.name, envir=load.env)))
    }
  }
}

# load and merge DSB MOM and RVOL PanelDBs
db.files <- list.files (path = data.path.out, pattern = '*DSBM_RVOL*')
for (db in seq_along(db.files)) {
  # load part into load-environment
  rm (list=ls(load.env), envir=load.env)
  load (file=paste(data.path.out, db.files[db], sep=""), envir=load.env)
  # copy part 1 into general workspace for merging
  if (db == 1) {
    for (i in seq_along(ls(load.env))) {
      assign(ls(load.env)[i], get(ls(load.env)[i], envir=load.env)) 
    }    
  } else {
    for (i in seq_along(ls(load.env))) {
      cur.name <- ls (load.env)[i]
      assign(cur.name, MergeStockdayPanels(get(cur.name), 
                                           get(cur.name, envir=load.env)))
    }
  }
}

# construct MA60-versions of daily stock characteristics for rankings
stocks.liqsMA.df <- MovingAvgDf (LagDf(stocks.liqs.df))
stocks.tovsMA.df <- MovingAvgDf (LagDf(stocks.tovs.df))
stocks.hlrsMA.df <- MovingAvgDf (LagDf(stocks.hlrs.df))
rm(stocks.liqsMA.df)
rm(stocks.tovsMA.df)
rm(stocks.hlrsMA.df)

# fix symbol-name issues in all stocks.xxx Stockday Panels
# Attention: regular expression ^requires "stocks" at start of string
for (i in seq_along(ls(pattern="^stocks.*"))) {
  cur.name <- ls (pattern="^stocks.*")[i]
  test.df <- get (cur.name)
  names(test.df)[names(test.df) == "BF.B.N"] <- "BF-B.N"
  names(test.df)[names(test.df) == "BRK.B.N"] <- "BRK-B.N"
  names(test.df)[names(test.df) == "FCE.A.N"] <- "FCE-A.N"
  names(test.df)[names(test.df) == "HUB.B.N"] <- "HUB-B.N"
  names(test.df)[names(test.df) == "RDS.A.N"] <- "RDS-A.N"
  names(test.df)[names(test.df) == "RDS.B.N"] <- "RDS-B.N"
  # sort columns alphabetically by symbol
  test.df <- test.df[, order(colnames(test.df))]
  assign (cur.name, test.df)
}
rm(test.df)

# check naming consistency
bm.names <- names (stocks.HFTflags.df)
for (i in seq_along(ls(pattern="^stocks.*"))) {
  cur.name <- ls (pattern="^stocks.*")[i]
  test.df <- get (cur.name)
  print(paste(cur.name, names(test.df)[1], names(test.df)[length(names(test.df))]))
  stopifnot(all.equal(names(test.df), bm.names))
}
rm(test.df)
rm(bm.names)

# SPECIAL: EXOGENOUS FACTORS INFLUENCING HFT/EE --------------------------------
# construct time series for each symbol/date in other panels
date.idx <- as.Date(rownames(stocks.HFTflags.df))
stock.idx <- names(stocks.HFTflags.df)
stocks.NMS.df <- as.data.frame (t(sapply(1:length(date.idx), function(x) 
  date.idx[x] >= as.Date(as.chron(symbol.info[stock.idx,]$NMS_Date)))))
stocks.SSB.df <- as.data.frame (t(sapply(1:length(date.idx), function(x) 
  date.idx[x] >= as.Date(as.chron(symbol.info[stock.idx,]$SSB_Date_1)) &
  date.idx[x] <= as.Date(as.chron(symbol.info[stock.idx,]$SSB_Date_2)) )))
stocks.SSB.df[is.na(stocks.SSB.df)] <- FALSE
names(stocks.NMS.df) <- stock.idx
rownames(stocks.NMS.df) <- date.idx
names(stocks.SSB.df) <- stock.idx
rownames(stocks.SSB.df) <- date.idx
rm(stock.idx)
rm(date.idx)

# load & select data frame for univariate analysis
# list.files (path = data.path.out, pattern = "*.idb")
# load (file=paste(data.path.out, "DB_big_2_6.idb", sep=""))

# *** end of 0.b) load data bases ***

param.min.liq = daybars * 00000
BuildFinalFiltDf (filter.method="slot")
 test.events.up.df <- events.master.up.df.filt
test.events.dn.df <- events.master.dn.df.filt


# I. DESCRIPTIVE STATISTICS ----------------------------------------------------

# I.1) Start of HFT activity in stocks -----------------------------------------
# I.1.a) Entry = 1st time ever HFT flag ----------------------------------------
stocks.HFTactive.df <- stocks.HFTflags.df > 0
for (i in 2:nrow(stocks.HFTactive.df)) {
  stocks.HFTactive.df[i, ] <- (stocks.HFTactive.df[i, ] == TRUE | 
                                 stocks.HFTactive.df[i-1, ] == TRUE)
}
rownames(stocks.HFTactive.df) <- rownames(stocks.HFTflags.df)

stocks.HFTactive.df.1stever <- stocks.HFTactive.df

# vars for plot function
entry.df <- stocks.HFTactive.df.1stever
rank.by <- as.matrix (stocks.hlrs.df * stocks.liqs.df * )
caption <- "%Stocks with 1st HFT entry"
group.by <- "liquid daily H/L-vola"
# generate plot
GenerateEntryPlots (entry.df, rank.by, caption, group.by)
GenerateEntryPlotsFlex (entry.df, rank.by, caption, group.by,
                        c(0, 0.2, 0.4, 0.6, 0.8, 1), 
                        c("Q1", "Q2", "Q3", "Q4", "Q5"))
GenerateEntryPlotsFlex (entry.df, rank.by, caption, group.by,
                        c(0, 0.1, 0.2, 0.4, 0.6, 0.8, 0.9, 1), 
                        c("Q1", "Q2", "Q3", "Q4", "Q5", "Q6", "Q7"))
GenerateEntryPlotsExchg (entry.df, caption)
HFT.EN.ts.1stever <- GenerateEntryTimeSeriess (entry.df, rank.by, 
                        c(0, 0.2, 0.4, 0.6, 0.8, 1), 
                        c("Q1", "Q2", "Q3", "Q4", "Q5"))

# I.1.b) Entry = 1 flag per day on average last quarter ------------------------
stocks.HFTactive.df <- apply (na.fill(stocks.HFTflags.df, 0), 2, 
                                      FUN=MovingAvg, period.days)
stocks.HFTactive.df <- na.fill (stocks.HFTactive.df, 0)
stocks.HFTactive.df <- stocks.HFTactive.df > 1
for (i in 2:nrow(stocks.HFTactive.df) ) {
  stocks.HFTactive.df[i,] <- (stocks.HFTactive.df[i, ] == TRUE | 
                                stocks.HFTactive.df[i-1, ] == TRUE)
}
rownames(stocks.HFTactive.df) <- rownames(stocks.HFTflags.df)

# vars for plot function
entry.df <- stocks.HFTactive.df
rank.by <- as.matrix (stocks.liqs.df)
caption <- "%Stocks with average > 1 flag/day for 60 days"
group.by <- "liquidity"
# generate plot
GenerateEntryPlots(entry.df, rank.by, caption, group.by)
GenerateEntryPlotsExchg(entry.df, caption)

# I.1.b2) Entry = 1 flags per hour on average last quarter ---------------------
stocks.HFTactive.df <- apply(na.fill(stocks.HFTflags.df, 0), 2, 
                             FUN=MovingAvg, period.days)
stocks.HFTactive.df <- na.fill(stocks.HFTactive.df, 0)
stocks.HFTactive.df <- stocks.HFTactive.df > 6.5
for (i in 2:nrow(stocks.HFTactive.df) ) {
  stocks.HFTactive.df[i,] <- (stocks.HFTactive.df[i,] == TRUE | 
                                stocks.HFTactive.df[i-1,] == TRUE)
}
rownames(stocks.HFTactive.df) <- rownames(stocks.HFTflags.df)

# vars for plot function
entry.df <- stocks.HFTactive.df
rank.by <- as.matrix (stocks.liqs.df)
caption <- "%Stocks with average > 1 flags/hour for 60 days"
group.by <- "liquidity"
# generate plot
GenerateEntryPlots(entry.df, rank.by, caption, group.by)
GenerateEntryPlotsExchg(entry.df, caption)

# I.1.c) Entry = daily activity last quarter -----------------------------------
stocks.HFTactive.df <- apply(na.fill(stocks.HFTflags.df, 0), 2, 
                             FUN=function(x) 
  rollapply(x, width=period.days, align="right", fill=NA, 
            FUN=function(x) all(x>=1)))
stocks.HFTactive.df <- na.fill(stocks.HFTactive.df, F)

for (i in 2:nrow(stocks.HFTactive.df) ) {
  stocks.HFTactive.df[i,] <- (stocks.HFTactive.df[i,] == TRUE | 
                                stocks.HFTactive.df[i-1,] == TRUE)
}
rownames(stocks.HFTactive.df) <- rownames(stocks.HFTflags.df)
# vars for plot function
entry.df <- stocks.HFTactive.df
rank.by <- as.matrix (log(stocks.liqs.df))
caption <- "%Stocks with at least 1 flag every day for 60 days"
group.by <- "liquidity"
# generate plot
GenerateEntryPlots(entry.df, rank.by, caption, group.by)
GenerateEntryPlotsExchg(entry.df, caption)

# I.1.c2) Entry = daily activity in 90% of days last quarter -------------------
stocks.HFTactive.df <- apply(na.fill(stocks.HFTflags.df, 0), 2, 
  FUN=function(x) rollapply(x, width=period.days, align="right", fill=NA, 
    FUN=function(x) (sum(x>=1)/period.days)>=0.9 ))
stocks.HFTactive.df <- na.fill(stocks.HFTactive.df, F)

for (i in 2:nrow(stocks.HFTactive.df) ) {
  stocks.HFTactive.df[i,] <- (stocks.HFTactive.df[i,] == TRUE | 
                                stocks.HFTactive.df[i-1,] == TRUE)
}
rownames(stocks.HFTactive.df) <- rownames(stocks.HFTflags.df)
stocks.HFTactive.df.90quarter <- stocks.HFTactive.df

rm(stocks.HFTactive.df)
stocks.HFTactive.df.1stever <- as.data.frame(stocks.HFTactive.df.1stever)
stocks.HFTactive.df.90quarter <- as.data.frame(stocks.HFTactive.df.90quarter)

# vars for plot function
entry.df <- stocks.HFTactive.df.90quarter
rank.by <- as.matrix (stocks.hlrs.df * stocks.liqs.df)
caption <- "%Stocks active at least 90% of last 60 days"
group.by <- "liquid daily H/L-vola"
# generate plot
GenerateEntryPlots(entry.df, rank.by, caption, group.by)
GenerateEntryPlotsFlex (entry.df, rank.by, caption, group.by,
                        c(0, 0.2, 0.4, 0.6, 0.8, 1), 
                        c("Q1", "Q2", "Q3", "Q4", "Q5"))
GenerateEntryPlotsExchg(entry.df, caption)
HFT.EN.ts <- GenerateEntryTimeSeriess (entry.df, rank.by, 
                                       c(0, 0.2, 0.4, 0.6, 0.8, 1), 
                                       c("Q1", "Q2", "Q3", "Q4", "Q5"))

# I.2) HFT Frequency by Severity Plot ------------------------------------------
# getting dataframe indices by year

stocks.HFTflagrate.df <- stocks.HFTflags.df / (daybars * 60)
stocks.HFTflagrate.df <- na.fill(stocks.HFTflagrate.df, 0)

stocks.HFTflagrate.2006 <- colMeans(stocks.HFTflagrate.df[
  which(years(as.chron(rownames(stocks.HFTflags.df)))==2006),])
stocks.HFTflagrate.2007 <- colMeans(stocks.HFTflagrate.df[
  which(years(as.chron(rownames(stocks.HFTflags.df)))==2007),])
stocks.HFTflagrate.2008 <- colMeans(stocks.HFTflagrate.df[
  which(years(as.chron(rownames(stocks.HFTflags.df)))==2008),])
stocks.HFTflagrate.2009 <- colMeans(stocks.HFTflagrate.df[
  which(years(as.chron(rownames(stocks.HFTflags.df)))==2009),])
stocks.HFTflagrate.2010 <- colMeans(stocks.HFTflagrate.df[
  which(years(as.chron(rownames(stocks.HFTflags.df)))==2010),])
stocks.HFTflagrate.2011 <- colMeans(stocks.HFTflagrate.df[
  which(years(as.chron(rownames(stocks.HFTflags.df)))==2011),])
stocks.HFTflagrate.2012 <- colMeans(stocks.HFTflagrate.df[
  which(years(as.chron(rownames(stocks.HFTflags.df)))==2012),])
stocks.HFTflagrate.2013 <- colMeans(stocks.HFTflagrate.df[
  which(years(as.chron(rownames(stocks.HFTflags.df)))==2013),])

breaks.freq <- c(0, 0.0001, 0.0002, 0.0005, 0.001, 0.002, 0.005, 
                 0.01, 0.02, 0.05, 0.1, 0.2)

stocks.HFTflagrate.hist <- hist(x=stocks.HFTflagrate.2006, 
                                breaks=breaks.freq, plot=F)
plot(y=100 * stocks.HFTflagrate.hist$counts / 
       mean(active.stocks.sum.x[which(years(
         as.chron(index(active.stocks.sum.x)))==2006)]), 
     x=100 * stocks.HFTflagrate.hist$breaks[-(1)], log="x", type="l", lwd=2,
     xlab="% Flagrate/Second", ylab="% of Stocks", ylim=c(0, 80),
     col=rainbow(8, start=rainbow.start, end=rainbow.end )[1])

stocks.HFTflagrate.hist <- hist(x=stocks.HFTflagrate.2007, 
                                breaks=breaks.freq, plot=FALSE)
lines(y=100 * stocks.HFTflagrate.hist$counts / 
        mean(active.stocks.sum.x[which(years(
          as.chron(index(active.stocks.sum.x)))==2007)]), 
      x=100 * stocks.HFTflagrate.hist$breaks[-(1)],
      lwd=2, col=rainbow(8, start=rainbow.start, end=rainbow.end )[2])

stocks.HFTflagrate.hist <- hist(x=stocks.HFTflagrate.2008, 
                                breaks=breaks.freq, plot=FALSE)
lines(y=100 * stocks.HFTflagrate.hist$counts / 
        mean(active.stocks.sum.x[which(years(
          as.chron(index(active.stocks.sum.x)))==2008)]), 
      x=100 * stocks.HFTflagrate.hist$breaks[-(1)],
      lwd=2, col=rainbow(8, start=rainbow.start, end=rainbow.end )[3])

stocks.HFTflagrate.hist <- hist(x=stocks.HFTflagrate.2009, 
                                breaks=breaks.freq, plot=FALSE)
lines(y=100 * stocks.HFTflagrate.hist$counts / 
        mean(active.stocks.sum.x[which(years(
          as.chron(index(active.stocks.sum.x)))==2009)]), 
      x=100 * stocks.HFTflagrate.hist$breaks[-(1)],
      lwd=2, col=rainbow(8, start=rainbow.start, end=rainbow.end )[4])

stocks.HFTflagrate.hist <- hist(x=stocks.HFTflagrate.2010, 
                                breaks=breaks.freq, plot=FALSE)
lines(y=100 * stocks.HFTflagrate.hist$counts / 
        mean(active.stocks.sum.x[which(years(
          as.chron(index(active.stocks.sum.x)))==2010)]), 
      x=100 * stocks.HFTflagrate.hist$breaks[-(1)],
      lwd=2, col=rainbow(8, start=rainbow.start, end=rainbow.end )[5])

stocks.HFTflagrate.hist <- hist(x=stocks.HFTflagrate.2011, 
                                breaks=breaks.freq, plot=FALSE)
lines(y=100 * stocks.HFTflagrate.hist$counts / 
        mean(active.stocks.sum.x[which(years(
          as.chron(index(active.stocks.sum.x)))==2011)]), 
      x=100 * stocks.HFTflagrate.hist$breaks[-(1)],
      lwd=2, col=rainbow(8, start=rainbow.start, end=rainbow.end )[6])

stocks.HFTflagrate.hist <- hist(x=stocks.HFTflagrate.2012, 
                                breaks=breaks.freq, plot=FALSE)
lines(y=100 * stocks.HFTflagrate.hist$counts / 
        mean(active.stocks.sum.x[which(years(
          as.chron(index(active.stocks.sum.x)))==2012)]), 
      x=100 * stocks.HFTflagrate.hist$breaks[-(1)],
      lwd=2, col=rainbow(8, start=rainbow.start, end=rainbow.end )[7])

stocks.HFTflagrate.hist <- hist(x=stocks.HFTflagrate.2013, 
                                breaks=breaks.freq, plot=FALSE)
lines(y=100 * stocks.HFTflagrate.hist$counts / 
        mean(active.stocks.sum.x[which(years(
          as.chron(index(active.stocks.sum.x)))==2013)]), 
      x=100 * stocks.HFTflagrate.hist$breaks[-(1)],
      lwd=2, col=rainbow(8, start=rainbow.start, end=rainbow.end )[8])

# I.3) HFT activity minute- and flag-rates over time ---------------------------
plot (100 * (hft.mins.sum.x/active.stocks.sum.x)['2006-01-03/2013-08-28'] / 
        daybars, ylab="% of total stock minutes", 
      main="% of HFT affected stocks, by minute")
lines(xts(order.by=index(hft.mins.sum.x), x=MovingAvg (
  100*(hft.mins.sum.x/active.stocks.sum.x)/daybars, period.oneyear)), lwd=2,
      col="blue")
legend("topleft", "(x,y)", c("HFT activity", "1-year rolling avg."), lty=rep(1,2),
       lwd=rep(2,2),col=c("black","blue"))

plot (100 * (hft.flags.sum.x/active.stocks.sum.x)['2006-01-03/2013-08-28'] /
        (daybars * 60), ylab="% of total stock seconds", 
      main="% of HFT affected stocks, by second")
lines(xts(order.by=index(hft.flags.sum.x), x=MovingAvg (
  100*(hft.flags.sum.x/active.stocks.sum.x)/(daybars*60), period.oneyear)), 
      lwd=2, col="blue")
legend("topleft", "(x,y)", c("HFT activity", "1-year rolling avg."), lty=rep(1,2),
       lwd=rep(2,2),col=c("black","blue"))

plot(xts(100*apply(stocks.HFTpquotes.df/stocks.prices.df, 1, mean, na.rm=T, trim=.001), 
         as.Date(rownames(stocks.HFTpquotes.df)))['2011-10-03/2012-01-31'])

plot(xts(100*apply(stocks.HFTpquotes.df, 1, mean, na.rm=T, trim=.001), 
         as.Date(rownames(stocks.HFTpquotes.df)))['2007-01-01/2007-12-31'])

plot(xts(100*apply(stocks.HFTpquotes.df, 1, median, na.rm=T), 
         as.Date(rownames(stocks.HFTpquotes.df)))['2007-01-01/2007-12-31'])

plot(xts(100*apply(stocks.HFTpquotes.df * stocks.liqs.df / stocks.prices.df, 1, quantile, probs=.5, na.rm=T), 
         as.Date(rownames(stocks.HFTpquotes.df)))['2007-01-01/2007-12-31'])

# I.4) Extreme Events: Frequency by Severity ----------------------------------
# generate Yearly Frequency by Severity Plot

# dn-Events
years.f <- years (as.chron(events.master.dn.df$timestamp))
# frequencies
for (i in seq_along(levels(years.f))) {
  sel <- ((events.master.dn.df$EFP.r - 
            events.master.dn.df$ECP.r))[years.f == levels(years.f)[i]]
  test <- hist(pmin(50,sel), breaks=c(0,2,3,4,5,7.5,10,15,20,30,40,50), plot=F)
  if (i == 1) {
    plot(y=test$counts, x=test$mids, log="x", type="l", lwd=2, ylim=c(0, 2000),
         xlab="Severity", ylab="Frequency", col=rainbow(8)[i],
         main="Event Frequency by Severity")
  } else {
    lines(y=test$counts, x=test$mids, type="l", lwd=2,
         col=rainbow(8)[i])
  }
}
legend("topright", "(x.y)", c(2006:2013), lty=rep(1,8),
       lwd=rep(2,8), col=rainbow(8))
# counts
for (i in seq_along(levels(years.f))) {
  sel <- ((events.master.dn.df$EFP.r - 
             events.master.dn.df$ECP.r))[years.f == levels(years.f)[i]]
  test <- hist(pmin(50,sel), breaks=c(0,2,3,4,5,7.5,10,15,20,30,40,50), plot=F)
  if (i == 1) {
    plot(y=test$density, x=test$mids, log="x", type="l", lwd=2,
         xlab="Severity", ylab="Frequency", col=rainbow(8)[i],
         main="Event Frequency by Severity")
  } else {
    lines(y=test$density, x=test$mids, type="l", lwd=2,
          col=rainbow(8)[i])
  }
}
legend("topright", "(x.y)", c(2006:2013), lty=rep(1,8),
       lwd=rep(2,8), col=rainbow(8))

# up-Events
years.f <- years (as.chron(events.master.up.df$timestamp))
# frequencies
for (i in seq_along(levels(years.f))) {
  sel <- (-(events.master.up.df$EFP.r - 
             events.master.up.df$ECP.r))[years.f == levels(years.f)[i]]
  test <- hist(pmin(50,sel), breaks=c(0,2,3,4,5,7.5,10,15,20,30,40,50), plot=F)
  if (i == 1) {
    plot(y=test$counts, x=test$mids, log="x", type="l", lwd=2, ylim=c(0, 2000),
         xlab="Severity", ylab="Frequency", col=rainbow(8)[i],
         main="Event Frequency by Severity")
  } else {
    lines(y=test$counts, x=test$mids, type="l", lwd=2,
          col=rainbow(8)[i])
  }
}
legend("topright", "(x.y)", c(2006:2013), lty=rep(1,8),
       lwd=rep(2,8), col=rainbow(8))
# counts
for (i in seq_along(levels(years.f))) {
  sel <- (-(events.master.up.df$EFP.r - 
             events.master.up.df$ECP.r))[years.f == levels(years.f)[i]]
  test <- hist(pmin(50,sel), breaks=c(0,2,3,4,5,7.5,10,15,20,30,40,50), plot=F)
  if (i == 1) {
    plot(y=test$density, x=test$mids, log="x", type="l", lwd=2,
         xlab="Severity", ylab="Frequency", col=rainbow(8)[i],
         main="Event Frequency by Severity")
  } else {
    lines(y=test$density, x=test$mids, type="l", lwd=2,
          col=rainbow(8)[i])
  }
}
legend("topright", "(x.y)", c(2006:2013), lty=rep(1,8),
       lwd=rep(2,8), col=rainbow(8))

# I.5) Extreme Events: Stat-Predicts over time ---------------------------------
# select database
param.min.liq    <- daybars * 10000
BuildFinalFiltDf()
test.df <- events.master.dn.df.filt
# daytime filter
# ts.filt <- GetTSTime(test.df$timestamp) >= "09:30:00" & 
#   GetTSTime(test.df$timestamp) <= "10:29:00"
# test.df <- subset(test.df, subset=ts.filt)

# parameters
align <- "EE"
t.start <- -60
t.end <- 120
filt <- "filtered"
absrel <- "2_6"
direc <- "dn"
suffix <- paste (" (", direc, "side EEs - ", filt, "10K/min liq.)", sep="")
f.name <- paste ("db", absrel, direc, filt, align, sep="_")
outdev <- "png"
yb <- c(1.5, -1.5)

# extract variable matrices from test dataframe
test.mats <- BuildVariableMatrices (test.df, align, min.rev.len=15)

# I.5.1) All EE Stat-Predicts total and yearly ---------------------------------
# I.5.1a) total stats
StatPredictCurve (test.mats, t.start, t.end, align, suffix, 
                  f.name, outdev, yb, T)
StatPredictCurve (test.mats, t.start, t.end, align, suffix, 
                  paste(f.name,"-2", sep=""), outdev, yb, T, 2)

# I.5.1b) create yearly stats 2006-2013
for (i in 2006:2013) {
  outdev <- "png"
  suffix <- paste (" (", direc, "side EEs - ", filt, ") - ", i, sep="")
  f.name <- paste ("db", absrel, direc, filt, align, i, sep="_")
  
  subset.f <- years (as.chron(test.mats$ts))
  subset.v <- i
  mats <- test.mats
  for (M in 1:8) {
    mats[[M]] <- subset (test.mats[[M]], subset=(subset.f==subset.v))
  }
  StatPredictCurve (mats, t.start, t.end, align, suffix, f.name, outdev, yb, T)
  StatPredictCurve (mats, t.start, t.end, align, suffix, 
                    paste(f.name,"-2", sep=""), outdev, yb, T, 2)  
}
# end of I.5.1) All EE stats total and yearly

# I.5.2) Liquidity-H/M/L EE Stat-Predicts total and yearly ---------------------
# calculate liquidity ranking classes
# - select ranking factor and convert into H/M/L classes
# - lookup-approach allows flexible classification for all subsets!
rank.by <- FixSymbolNames(stocks.liqs.df)
ranks.cl <- GetRankClasses (rank.by)
ranks.f <- as.factor(sapply(1:length(test.mats$ts), FUN=function(x) 
  ranks.cl[as.character(as.Date(test.mats$ts[x])), test.mats$symb[x]]))

# apply subset by H/M/L liquidity
for (L in seq_along(levels(ranks.f))) {
  sel <- levels(ranks.f)[L]
  sub.test.mats <- lapply (test.mats, subset, subset=(ranks.f==sel))
  # .2a) total stats for subset
  suffix <- paste (" (", direc, "side EEs - ", filt, ")", sep="")
  f.name <- paste ("db", absrel, direc, filt, align, sep="_")  
  StatPredictCurve (sub.test.mats, t.start, t.end, align, 
                    paste(suffix, sel, sep="_"), paste(f.name, sel, sep="_"), 
                    outdev, yb, T)
  StatPredictCurve (sub.test.mats, t.start, t.end, align, 
                    paste(suffix, sel, sep="_"), 
                    paste(paste(f.name,"-2", sep=""), sel, sep="_"), 
                    outdev, yb, T, 2)
  # .2b) create yearly stats 2006-2013 for subset
  for (i in 2006:2013) {
    outdev <- "png"
    suffix <- paste (" (", direc, "side EEs - ", filt, ") - ", i, sel, sep="")
    f.name <- paste ("db", absrel, direc, filt, align, sel, i, sep="_")
    
    subset.f <- years (as.chron(sub.test.mats$ts))
    subset.v <- i
    mats <- sub.test.mats
    for (M in 1:8) {
      mats[[M]] <- subset (sub.test.mats[[M]], subset=(subset.f==subset.v))
    }
    StatPredictCurve (mats, t.start, t.end, align, suffix, 
                      f.name, outdev, yb, T)
    StatPredictCurve (mats, t.start, t.end, align, suffix, 
                      paste(f.name,"-2", sep=""), outdev, yb, T, 2)
  }
}
# end of I.5.2) Liquidity-H/M/L EE stats total and yearly

# I.5.3) by-Exchange EE Stat-Predicts total and yearly -------------------------
# construct exchange factor
exchg.f <- as.factor (matrix ( 
  unlist(strsplit(test.mats$symb, split="[.]")), 
  nrow=length(test.mats$symb), ncol=2, byrow=T)[,2])

# apply subset by exchange
for (L in seq_along(levels(exchg.f))) {
  sel <- levels(exchg.f)[L]
  sub.test.mats <- lapply (test.mats, subset, subset=(exchg.f==sel))
  # .2a) total stats for subset
  suffix <- paste (" (", direc, "side EEs - by Exchange - ", filt, ")", sep="")
  f.name <- paste ("db", absrel, direc, filt, align, sep="_")  
  StatPredictCurve (sub.test.mats, t.start, t.end, align, 
                    paste(suffix, sel, sep="_"), paste(f.name, sel, sep="_"), 
                    outdev, yb, T)
  StatPredictCurve (sub.test.mats, t.start, t.end, align, 
                    paste(suffix, sel, sep="_"), 
                    paste(paste(f.name,"-2", sep=""), sel, sep="_"), 
                    outdev, yb, T, 2)
  # .2b) create yearly stats 2006-2013 for subset
  for (i in 2006:2013) {
    outdev <- "png"
    suffix <- paste (" (", direc, "side EEs - by Exchange - ", filt, ") - ", 
                     i, sel, sep="")
    f.name <- paste ("db", absrel, direc, filt, align, sel, i, sep="_")
    
    subset.f <- years (as.chron(sub.test.mats$ts))
    subset.v <- i
    mats <- sub.test.mats
    for (M in 1:8) {
      mats[[M]] <- subset (sub.test.mats[[M]], subset=(subset.f==subset.v))
    }
    StatPredictCurve (mats, t.start, t.end, align, suffix, 
                      f.name, outdev, yb, T)
    StatPredictCurve (mats, t.start, t.end, align, suffix, 
                      paste(f.name,"-2", sep=""), outdev, yb, T, 2)
  }
}

# I.5.4) by-Exchange & liquidity EE Stat-Predicts total and yearly -------------
# construct interaction factor
liq.exchg.f <- interaction(exchg.f, ranks.f)

# apply subset by exchange & liquidity bucket
for (L in seq_along(levels(liq.exchg.f))) {
  sel <- levels(liq.exchg.f)[L]
  sub.test.mats <- lapply (test.mats, subset, subset=(liq.exchg.f==sel))
  # .2a) total stats for subset
  suffix <- paste (" (", direc, "side EEs - by Exchange - ", filt, ")", sep="")
  f.name <- paste ("db", absrel, direc, filt, align, sep="_")  
  StatPredictCurve (sub.test.mats, t.start, t.end, align, 
                    paste(suffix, sel, sep="_"), paste(f.name, sel, sep="_"), 
                    outdev, yb, T)
  StatPredictCurve (sub.test.mats, t.start, t.end, align, 
                    paste(suffix, sel, sep="_"), 
                    paste(paste(f.name,"-2", sep=""), sel, sep="_"), 
                    outdev, yb, T, 2)
  # .2b) create yearly stats 2006-2013 for subset
  for (i in 2006:2013) {
    outdev <- "png"
    suffix <- paste (" (", direc, "side EEs - by Exchange - ", filt, ") - ", 
                     i, sel, sep="")
    f.name <- paste ("db", absrel, direc, filt, align, sel, i, sep="_")
    
    subset.f <- years (as.chron(sub.test.mats$ts))
    subset.v <- i
    mats <- sub.test.mats
    for (M in 1:8) {
      mats[[M]] <- subset (sub.test.mats[[M]], subset=(subset.f==subset.v))
    }
    StatPredictCurve (mats, t.start, t.end, align, suffix, 
                      f.name, outdev, yb, T)
    StatPredictCurve (mats, t.start, t.end, align, suffix, 
                      paste(f.name,"-2", sep=""), outdev, yb, T, 2)
  }
}

# I.6) Extreme Events: Yearly summary plots & tables ---------------------------
# - univariate summary
# - by year: mean, median, iqr

# select database
param.min.liq    <- daybars * 0
BuildFinalFiltDf()
test.df <- events.master.dn.df.final
f.years <- years (as.chron(test.df$timestamp))
f.quarters <- quarters (as.chron(test.df$timestamp))
f.yearqtr <- factor (interaction (f.quarters, f.years), ordered=T)
f.exchg <- as.factor (matrix(unlist(strsplit(test.df$symbol, split="[.]")), 
                             nrow=length(test.df$symbol), ncol=2, byrow=T)[,2])
f.yearmon <- factor(substr(test.df$timestamp, 1, 7), ordered=T)
f.day <- factor(as.Date(test.df$timestamp), ordered=T)

# ranking factor with lookup
rank.by <- FixSymbolNames(stocks.liqs.df)
ranks.cl <- GetRankClasses (rank.by)
f.rank <- factor(sapply(1:nrow(test.df), FUN=function(x) 
  ranks.cl[as.character(as.Date(test.df$timestamp[x])), 
           test.df$symbol[x]]), levels=c("L", "M", "H"), ordered=T)

ranks.cl2 <- GetRankXtiles (rank.by, c(0, .2, .4, .6, .8, 1), 
                                       c("Q1", "Q2", "Q3", "Q4", "Q5"))
f.rank2 <- factor(sapply(1:nrow(test.df), FUN=function(x) 
  ranks.cl2[as.character(as.Date(test.df$timestamp[x])), test.df$symbol[x]]), 
                  levels=c("Q1", "Q2", "Q3", "Q4", "Q5"), ordered=T)

summary(interaction(f.yearmon, f.rank))


# check different aggregates (mean, trimmed-means, median )
plot(tapply(test.df$ECP.max.cont/test.df$PREP.vola, 
            interaction(f.years), 
            function(x) mean(x, na.rm=T, trim=.1)))
plot(tapply(test.df$PREW10.rel.vol, 
            interaction(f.HFT.EN, f.rank), 
            function(x) mean(Winsor(x, .1), na.rm=T)))
plot(tapply(test.df$PREW10.rel.vol, 
            interaction(f.HFT.EN, f.rank), 
            function(x) mean((x), na.rm=T, trim=.05)))
plot(tapply(test.df$PREW10.rel.vol, 
            interaction(f.HFT.EN, f.rank), 
            function(x) median(x, na.rm=T)))
# count
aggregate (test.events.dn.df$ECP.max.cont, 
           by=list(Year=substr(test.events.dn.df$timestamp,1,4)), 
           FUN=length)
# prob.rev
plot(y=tapply(test.df$r_60, f.years, function(x) mean(x>0.1, na.rm=T)), type="l",
     lwd=2, col="blue", x=levels(f.years), xlab="Year", ylab="value")

# r60 / vola
plot(y=tapply(test.df$r_60/test.df$PREP.vola, f.years, 
              function(x) mean(x, na.rm=T)), type="l",
     lwd=2, col="blue", x=levels(f.years), xlab="Year", ylab="value")
# r60
plot(y=tapply(test.df$ar_60, f.years, 
              function(x) mean(x, na.rm=T)), type="l",
     lwd=2, col="blue", x=levels(f.years), xlab="Year", ylab="value")

# I.7) Simple T-Test conditioning upon HFT-Entry -------------------------------

# I.7.1) Calculate HFT Entry Factors *******************************************
# HFT entry factor with lookup
stocks.HFTactive.df <- FixSymbolNames (as.data.frame(stocks.HFTactive.df.1stever))
f.HFT.EN <- as.factor(sapply(1:nrow(test.df), FUN=function(x) 
  stocks.HFTactive.df[as.character(as.Date(test.df$timestamp[x])), 
                      test.df$symbol[x]]))
summary(f.HFT.EN)
f.HFT.EN[which(is.na(f.HFT.EN))] <- F


f.HFT.EN2


# analyze HFT.EN category structure over time
# HFT1
f.struct <- sapply (1:nrow(expand.grid(levels(f.yearqtr), levels(f.HFT.EN))),
       function(x) sum (interaction(f.yearqtr, f.HFT.EN)==
          paste (expand.grid(levels(f.yearqtr), levels(f.HFT.EN))[x,1],
                 expand.grid(levels(f.yearqtr), levels(f.HFT.EN))[x,2], sep=".")))

f.struct.array <- array (f.struct, dim=c(length(f.struct)/2,2))
row.names (f.struct.array) <- levels (f.yearqtr)
colnames (f.struct.array) <- levels (f.HFT.EN)
plot (y=f.struct.array[,1], x=c(2006+(1:nrow(f.struct.array)-1)/4), 
      type="l", lwd=2, col="blue", xlab="Year/Qtr", 
      ylim=c(0, max(f.struct.array)), 
      main="Quarterly event counts by HFT Entry T/F",
      sub="blue: HFT.Entry = False, red: HFT.Entry = True")
lines (y=f.struct.array[,2], x=c(2006+(1:nrow(f.struct.array)-1)/4),
       lwd=2, col="red")

# HFT2
f.struct <- sapply (1:nrow(expand.grid(levels(f.yearqtr), levels(f.HFT.EN2))),
                    function(x) sum (interaction(f.yearqtr, f.HFT.EN2)==
                                       paste (expand.grid(levels(f.yearqtr), levels(f.HFT.EN2))[x,1],
                                              expand.grid(levels(f.yearqtr), levels(f.HFT.EN2))[x,2], sep=".")))

f.struct.array <- array (f.struct, dim=c(length(f.struct)/2,2))
row.names (f.struct.array) <- levels (f.yearqtr)
colnames (f.struct.array) <- levels (f.HFT.EN2)
plot (y=f.struct.array[,1], x=c(2006+(1:nrow(f.struct.array)-1)/4), 
      type="l", lwd=2, col="blue", xlab="Year/Qtr", 
      ylim=c(0, max(f.struct.array)), 
      main="Quarterly event counts by HFT Entry 2 T/F",
      sub="blue: HFT.EN2try = False, red: HFT.EN2try = True")
lines (y=f.struct.array[,2], x=c(2006+(1:nrow(f.struct.array)-1)/4),
       lwd=2, col="red")

# HFT entry 2 factor based on *event-specific* HFT activity
f.HFT.EN2 <- as.factor(test.df$PREW10.HFT.m.rate > 0)
summary (f.HFT.EN2)

# I.7.2) Apply T-Test to selected EE characteristic ****************************
# EE-char: events.size
events.size <- vGetTseriesMaxSel(test.df, 1:nrow(test.df), "r", -60, -1)
events.size <- events.size - test.df$ECP.max.cont
events.size.adj <- events.size / test.df$PREP.vola

plot(events.size)
lines(MovingAvg(events.size, 63), lwd=2, col="orange")

plot(events.size.adj)
lines(MovingAvg(events.size.adj, 63), lwd=2, col="orange")


plot(tapply(events.size, 
            interaction(f.yearqtr), 
            function(x) mean(x, na.rm=T, trim=.1)))

plot(tapply(events.size, 
            interaction(f.yearmon), 
            function(x) length(x)), type="l")

# choose var for t-test
check.var <- events.size.adj
check.var.adj <- check.var
# # perform standardization for each timeslice in f.yearmon
# for (i in 1:length(levels(f.yearmon))) {
#   sel.idx <- which(f.yearmon == levels(f.yearmon)[i])  
#   check.var.adj[sel.idx] <- Std(check.var[sel.idx])
# }

# perform standardization for each timeslice in f.yearqrt
for (i in 1:length(levels(f.yearqtr))) {
  sel.idx <- which(f.yearqtr == levels(f.yearqtr)[i])  
  check.var.adj[sel.idx] <- Std(check.var[sel.idx])
}

plot(check.var.adj)
lines(MovingAvg(check.var.adj, 63), lwd=2, col="orange")

# Plots & T-Test for selected variable, conditioning upon HFT.ENx factor
Boxplot(check.var.adj, interaction(f.HFT.EN, f.rank), id.method="none")
check.f <- interaction(f.HFT.EN2, f.rank2, f.years)
summary(check.f)
tapply (check.var.adj, check.f, function(x) mean(x, na.rm=T))
tapply (check.var.adj, check.f, function(x) sd(x, na.rm=T)/sqrt(sum(!is.na(x))))
for (i in 1:(length(levels(check.f))/2)) {
  i.x <- levels(check.f)[(i-1)*2 + 1]
  i.y <- levels(check.f)[(i-1)*2 + 2]
  print (paste("t.test", i.y, "vs.", i.x, 
               (t.test (y=subset(check.var.adj, subset=(check.f==i.x)), 
    x=subset(check.var.adj, subset=(check.f==i.y)), mu=0))$statistic))  
}

# Subset test series to exclude periods with too small sample size
tapply (check.var.adj, check.f, function(x) length(x))[11:20]
tapply (check.var.adj, check.f, function(x) mean(x, na.rm=T))[1:10]
tapply (check.var.adj, check.f, function(x) sd(x, na.rm=T)/sqrt(sum(!is.na(x))))
for (i in 6:(length(levels(check.f))/2)) {
  i.x <- levels(check.f)[(i-1)*2 + 1]
  i.y <- levels(check.f)[(i-1)*2 + 2]
  print (paste("t.test", i.y, "vs.", i.x, 
               (t.test (y=subset(check.var.adj, subset=(check.f==i.x)), 
                        x=subset(check.var.adj, subset=(check.f==i.y)), mu=0))$statistic))  
}




# QUARTERLY PANEL CHECK --------------------------------------------------------
# create quarterly aggregate panel: EE characteristics by Liquidity Quintile
test.var <- (events.size.adj)
f.HFT <- f.HFT.EN

EE.char <- aggregate (test.var, by=list(f.rank2, f.yearqtr), 
             FUN=function(x) mean(Winsor(x, .1), na.rm=T))
# EE.char <- aggregate (test.var, by=list(f.rank2, f.yearqtr), 
#              FUN=function(x) quantile(x, na.rm=T, probs=.5))
HFT.share <- aggregate (as.numeric(f.HFT)-1, by=list(f.rank2, f.yearqtr), mean)

# get quarterly HFT entry series by liquidity category, rather than from events
HFT.EN.ts.x <- apply.quarterly (HFT.EN.ts, FUN=mean)
HFT.EN.ts.df <- as.data.frame (HFT.EN.ts.x)
HFT.EN.ts.df <- cbind (HFT.EN.ts.x, as.character(
                       interaction(quarters(as.chron(rownames(HFT.EN.ts.x))), 
                                   years(as.chron(rownames(HFT.EN.ts.x))))))
names(HFT.EN.ts.df)[6] <- "Qtr"
# reshape to long format
HFT.EN.ts.df2 <- reshape (HFT.EN.ts.df, 
                         varying = c("Q1", "Q2", "Q3", "Q4", "Q5"), 
                         v.names = "HFT",
                         timevar = "Liq", 
                         times = c("Q1", "Q2", "Q3", "Q4", "Q5"), 
                         direction = "long")
# reorder by time-id & liquidity X-tile
HFT.EN.ts.df2 <- HFT.EN.ts.df2[with(HFT.EN.ts.df2, order(id, Liq)), ]
# construct data frame
# panel.df <- cbind(EE.char, HFT.share[,3])
panel.prep.df <- cbind (EE.char, HFT.EN.ts.df2[,3])
names(panel.prep.df) <- c ("Liq", "Qtr", "EEC", "HFT")

# subset panel years to focus on relevant period
panel.df <- pdata.frame (panel.prep.df[1:100, ], 
                         index=c("Liq", "Qtr"), drop.index=F)
head (panel.df, 30)

# 1) INSPECT DATA
# plot variables by Liq
coplot(EEC ~ Qtr|Liq, type="l", data=panel.df)
coplot(HFT ~ Qtr|Liq, type="l", data=panel.df)
library(gplots)
plotmeans(EEC ~ Liq, main="Heterogeineity across Liq-Groups", data=panel.df)
plotmeans(EEC ~ Qtr, main="Heterogeineity across Time", data=panel.df)
detach("package:gplots")

# 2) SIMPLE OLS CHECKS
# inspect simple OLS model
ols <- lm(EEC~HFT, panel.df)
summary(ols)
yhat <- ols$fitted.values
plot(panel.df$HFT, panel.df$EEC, pch=19, xlab="HFT-share", ylab="EEC")
abline(lm(panel.df$EEC~panel.df$HFT),lwd=3, col="red")

# simulated unit fixed effects using Liq-dummies
fixed.dum.liq <- lm(EEC~HFT+factor(Liq), panel.df)
summary(fixed.dum.liq)
yhat <- fixed.dum.liq$fitted.values
# plot group-wise regression lines and compare with simple OLS
scatterplot(yhat~panel.df$HFT|panel.df$Liq, 
            boxplots=F, xlab="HFT", ylab="yhat", smooth=F)
abline(lm(panel.df$EEC~panel.df$HFT),lwd=3, col="red")
# Wald-test: additional unit fixed effects needed? Null: no
waldtest(ols, fixed.dum.liq, vcov = vcovHAC)
# --> CHECK FIXED UNIT EFFECT

# simulated time fixed effects using Qtr-dummies
fixed.dum.qtr <- lm(EEC~HFT+factor(Qtr), panel.df)
summary(fixed.dum.qtr)
yhat <- fixed.dum.qtr$fitted.values
# plot group-wise regression lines and compare with simple OLS
scatterplot(yhat~panel.df$HFT|panel.df$Qtr, legend.plot=F,
            boxplots=F, xlab="HFT", ylab="yhat", smooth=F)
abline(lm(panel.df$EEC~panel.df$HFT),lwd=3, col="red")
# Wald-test: additional time fixed effects needed? Null: no
waldtest(ols, fixed.dum.qtr, vcov = vcovHAC)
# --> CHECK FIXED TIME EFFECT

# 3) PANEL REGRESSION CHECKS
# pooled model --> equal to simple OLS
test.po <- plm (EEC~HFT, data=panel.df, model="pooling")
summary(test.po)
# Breusch-Pagan-Heteroskedasticity test: Null = no random effects needed
# p < .05: effect is significant, else no need to use it
plmtest(test.po, "individual", type="bp")
plmtest(test.po, "time", type="bp")
plmtest(test.po, "twoways", type="bp")
plmtest(test.po, "twoways", type="kw")
# ---> CHECK RANDOM EFFECTS

# # same thing, different implementation
# # Breusch-Pagan test: null hypothesis is homoskedasticity
# bptest(EEC ~ HFT + factor(Liq), data=panel.df)
# bptest(EEC ~ HFT + factor(Qtr), data=panel.df)

# test poolability: 
# tests the hypothesis that the same coefficients apply to each individual
# p <- .05: significant effects
pooltest (EEC~HFT,data=panel.df,model="within")

# 3a) FIXED EFFECTS
# individual FE
test.fe <- plm (EEC~HFT,data=panel.df, model="within")
summary (test.fe) # p-value indicates whether all coefficients are non-zero
                  # <.05 means model is ok
# coefficient test with hetero-skedasticity-consistent errors
coeftest (test.fe, vcov=vcovHC(test.fe, cluster="group"))

# inspect and test fixed effects
summary(fixef(test.fe))   # display coefficients of fixed effects
pFtest(test.fe, test.po)  # F-test for fixed effects vs. simple OLS
                          # If the p-value is < 0.05 then the 
                          # fixed effects model is a better choice

# time FE
test.fe.t <- plm (EEC~HFT,data=panel.df, model="within", effect="time")
summary (test.fe.t) # p-value indicates whether all coefficients are non-zero
                    # <.05 means model is ok
coeftest (test.fe.t, vcov=vcovHC(test.fe.t, cluster="time"))
summary(fixef(test.fe.t))  # display coefficients of fixed effects
pFtest(test.fe.t, test.po) # F-test for fixed effects vs. simple OLS
                           # <.05: fixed effects model is better choice

# individual+time FE
test.fe2 <- plm (EEC~HFT,data=panel.df, model="within", effect="twoways")
summary (test.fe2)
coeftest (test.fe2, vcov.=vcovHC(test.fe2))
# emulate additional time-specific effect by adding Qtr-dummy
test.fe2b <- plm (EEC~HFT+factor(Qtr),data=panel.df, model="within")
summary (test.fe2b)
# test addition of time effect, given individual effects
pFtest(test.fe2, test.fe)
plmtest(test.fe, "time", type="bp")
# test joint addition of indiv+time effects
pFtest(test.fe2, test.po)

# ADDITIONAL TESTS ***
# serial correlation tests
pwfdtest (EEC~HFT, data=panel.df, h0="fe")
pwartest (test.fe, data=panel.df)
pwartest (test.fe.t, data=panel.df)
pwartest (test.fe2, data=panel.df)

# Testing for cross-sectional dependence/contemporaneous correlation: 
# using Breusch-Pagan LM test of independence and Pasaran CD test
# Null is "no dependence", p<.05: dependence
pcdtest(test.fe, test = "lm")
pcdtest(test.fe.t, test = "lm")
pcdtest(test.fe2, test = "lm")

# testing for serial correlation (Breusch-Godfrey/Wooldridge test)
# in residuals
# Null is "no correlation", p<.05: correlation
pbgtest(test.fe)
pbgtest(test.fe.t)
pbgtest(test.fe2)

# Dickey-Fuller test to check null hypothesis is that the series has a unit root
# (i.e. non-stationary), if unit root is present you can take the first 
# difference of the variable. 
adf.test(panel.df$EEC, k=2)
# end: ADDITIONAL TESTS ***

# 3b) FIXED EFFECTS + LAGS
# individual FE with lagged variable
test.fe.l <- plm (EEC~HFT+lag(EEC, k=1),data=panel.df, model="within")
summary (test.fe.l)
coeftest (test.fe.l, vcov=vcovHC(test.fe.l, cluster="group"))

# individual FE with lagged variables up to 2nd order
test.fe.l2 <- plm (EEC~HFT+lag(EEC, k=1)+lag(EEC, k=2),data=panel.df, model="within")
summary (test.fe.l2)
coeftest (test.fe.l2, vcov.=vcovHC(test.fe.l2, cluster="group"))

# FD first differences model
test.fd <- plm(EEC~HFT,data=panel.df, model="fd")
summary(test.fd)
coeftest(test.fd, vcov=vcovHC(test.fd))

# 3c) RANDOM EFFECTS
# individual RE
test.re <- plm(EEC~HFT,data=panel.df, model="random", random.method="amemiya")
summary(test.re)
coeftest(test.re, vcov.=vcovHC(test.re))
# Hausman test for model consistency fixed/random
phtest(test.fe, test.re) # < p.05 for H0=RE means FE is preferred

# time RE
test.re.t <- plm(EEC~HFT,data=panel.df, model="random", effect="time")
summary(test.re.t)
coeftest(test.re.t, vcov=vcovHC(test.re.t, cluster="time"))
# Hausman test for model consistency fixed/random
phtest(test.fe.t, test.re.t) # < p.05 for H0=RE means FE is preferred

# time RE + individual FE
test.re.t2 <- plm(EEC~HFT+factor(Liq),data=panel.df, model="random", effect="time")
summary(test.re.t2)
coeftest(test.re.t2, vcov=vcovHC(test.re.t2, cluster="time"))

# individual + time RE
test.re2 <- plm(EEC~HFT,data=panel.df, model="random", effect="twoways", random.method="amemiya")
summary(test.re)
coeftest(test.re, vcov=vcovHC(test.re))
# end: QUARTERLY PANEL CHECK ***************************************************

# DAILY PANEL CHECK ------------------------------------------------------------

# 1) Preparation ---------------------------------------------------------------
# 1a) check vars ---------------------------------------------------------------
hist(na.remove(stocks.score.max))
hist(na.remove(stocks.score.min))
hist(na.remove(log1p(stocks.score.max)))
hist(na.remove(log1p(-stocks.score.min)))

hist(na.remove(log(stocks.sizes.df))) # --> log
hist(na.remove(log(stocks.fcaps.df))) # --> log
hist(na.remove((stocks.betas.df)))
hist(na.remove((stocks.hlrs.df)))   # outliers!!!!
summary(na.remove(stocks.hlrs.df))  
hist(na.remove(log(stocks.hlrs.df)))   # outliers!!!!
hist(na.remove((stocks.volas.df)))
summary(na.remove((stocks.volas.df))) # outliers
hist(na.remove(log(stocks.prices.df))) # --> log
hist(na.remove((stocks.tovs.df))) # outliers!!!
hist(na.remove(log(stocks.tovs.df))) # outliers!!!
hist(na.remove(log(stocks.liqs.df))) # outliers
hist(na.remove(log(stocks.vol.max))) # all vol.max/min have outliers
hist(na.remove(log(stocks.vol.max.l1)))
hist(na.remove(log(stocks.vol.max.l5)))
hist(na.remove(log(stocks.vol.max.l15)))
hist(na.remove(log(stocks.vol.min)))
hist(na.remove(log(stocks.vol.min.l1)))
hist(na.remove(log(stocks.vol.min.l5)))
hist(na.remove(log(stocks.vol.min.l15)))

hist(na.remove((stocks.hft.max)))
hist(na.remove((stocks.hft.max.l1)))
hist(na.remove((stocks.hft.max.l5)))
hist(na.remove((stocks.hft.max.l15)))
hist(na.remove((stocks.hft.min)))
hist(na.remove((stocks.hft.min.l1)))
hist(na.remove((stocks.hft.min.l5)))
hist(na.remove((stocks.hft.min.l15)))

hist(na.remove((stocks.idhl.mean.df)))
hist(na.remove((stocks.idhl.median.df)))
hist(na.remove((stocks.idhl.q95.df)))
hist(na.remove((stocks.idhl.q99.df)))
hist(na.remove((stocks.idhl.tail.df)))
hist(na.remove((stocks.idret.kurt.df)))
hist(na.remove((stocks.idret.max.df)))
hist(na.remove((stocks.idret.min.df)))
hist(na.remove((stocks.idret.mean.df)))
hist(na.remove((stocks.idret.median.df)))
hist(na.remove((stocks.idret.meanabs.df)))
hist(na.remove((stocks.idret.q95abs.df)))
hist(na.remove((stocks.idret.q99abs.df)))
hist(na.remove((stocks.idret.sd.df)))
hist(na.remove((stocks.idret.tail.df)))

# 1b) select date range --------------------------------------------------------
# 2006:2010
range.idx <- 252:1498
rownames(stocks.HFTflags.df[range.idx,])

# 2006:2011
range.idx <- 252:1749
rownames(stocks.HFTflags.df[range.idx,])

# 2006:2012
range.idx <- 252:1996
rownames(stocks.HFTflags.df[range.idx,])

# "full"
range.idx <- 252:2161
rownames(stocks.HFTflags.df[range.idx,])

# REG NMS 1: 2007-05-09 to 2007-10-22 (NMS rollout +/- 2 months)
range.idx <- 588:701
rownames(stocks.HFTflags.df[range.idx,])

# REG NMS 2: 2007-06-04 to 2007-09-28 (month-based cut)
range.idx <- 604:685
rownames(stocks.HFTflags.df[range.idx,])

# remove 2008-11-20 if it is included in the sample
range.idx <- range.idx[-which(rownames(stocks.HFTflags.df[range.idx,])=="2008-11-20")]

# SSB: 2008-08-01 to 2008-11-26
range.idx <- 894:976
rownames(stocks.HFTflags.df[range.idx,])

# Naked Access Ban: 2011-11-30
range.idx <- 1688:1769
rownames(stocks.HFTflags.df[range.idx,])

# 1bb) investigate key variables in date range ---------------------------------
# HFT flags
plot(y=apply(((stocks.HFTflags.df)/1)[range.idx, ], 1, 
             function(x) mean(x, na.rm=T, trim=.005)), 
     x=(as.Date(rownames(stocks.HFTflags.df[range.idx, ]))),
     main="Daily avg. #HFT flags", type="l", lwd=2,
     ylab="#flags", xlab="Date")
lines(y=MovingAvg2(apply(((stocks.HFTflags.df))[range.idx, ], 1, 
                         function(x) mean(x, na.rm=T, trim=.005)),252),
      x=(as.Date(rownames(stocks.HFTflags.df[range.idx, ]))), 
      type="l", lwd=2, col="blue")

# HFT flags log
plot(y=(apply((log1p(stocks.HFTflags.df)/1)[range.idx, ], 1, 
             function(x) mean(x, na.rm=T, trim=.005))), 
     x=(as.Date(rownames(stocks.HFTflags.df[range.idx, ]))),
     main="Daily avg. log (#HFT flags)", type="l", lwd=2,
     ylab="#flags", xlab="Date")
lines(y=MovingAvg2(apply((log1p(stocks.HFTflags.df))[range.idx, ], 1, 
                         function(x) mean(x, na.rm=T, trim=.005)),252),
      x=(as.Date(rownames(stocks.HFTflags.df[range.idx, ]))), type="l", lwd=2, col="blue")

# full period, EED+EEU mean
plot(y=apply(((stocks.score.max-stocks.score.min)/1)[range.idx, ], 1, 
             function(x) mean(x, na.rm=T, trim=.03)), 
     x=(as.Date(rownames(stocks.HFTflags.df[range.idx, ]))),
     main="Daily avg. EE score", type="l", lwd=2,
     ylab="EE score", xlab="Date", ylim=c(3,10))
lines(y=MovingAvg2(apply(((stocks.score.max-stocks.score.min))[range.idx, ], 1, 
                         function(x) mean(x, na.rm=T, trim=.03)),252),
      x=(as.Date(rownames(stocks.HFTflags.df[range.idx, ]))), type="l", lwd=2, col="blue")

# full period, EED+EEU median
plot(y=apply(((stocks.score.max-stocks.score.min)/1), 1, 
             function(x) median(x, na.rm=T)), 
     x=(as.Date(rownames(stocks.HFTflags.df))),
     main="Daily median EE score", type="l", lwd=2,
     ylab="EE score", xlab="Date", ylim=c(3,10))
lines(y=MovingAvg2(apply(((stocks.score.max-stocks.score.min)), 1, 
                         function(x) median(x, na.rm=T)),252),
      x=(as.Date(rownames(stocks.HFTflags.df))), type="l", lwd=2, col="blue")
lines(y=4+MovingAvg2(apply((log1p(stocks.HFTflags.df))[range.idx, ], 1, 
                         function(x) mean(x, na.rm=T, trim=.005)),252),
      x=(as.Date(rownames(stocks.HFTflags.df[range.idx, ]))), type="l", lwd=2, col="blue")

# full period, MOMU+MOMD / HLR
plot(y=apply(((stocks.dsbmom.max-stocks.dsbmom.min)/stocks.hlrs.df), 1, 
             function(x) mean(x, na.rm=T, trim=.005)), 
     x=(as.Date(rownames(stocks.HFTflags.df))),
     main="Daily avg. EE size/day-range", type="l", lwd=2,
     ylab="EE size/day-range", xlab="Date", ylim=c(0.8, 1.2))
lines(y=MovingAvg2(apply(((stocks.dsbmom.max-stocks.dsbmom.min)/stocks.hlrs.df), 1, 
              function(x) mean(x, na.rm=T, trim=.005)),252),
      x=(as.Date(rownames(stocks.HFTflags.df))), type="l", lwd=2, col="blue")

# median
plot(y=apply(((stocks.dsbmom.max-stocks.dsbmom.min)/stocks.hlrs.df), 1, 
             function(x) median(x, na.rm=T)), 
     x=(as.Date(rownames(stocks.HFTflags.df))),
     main="Daily median EE size/day-range", type="l", lwd=2,
     ylab="EE size/day-range", xlab="Date", ylim=c(0.8, 1.2))
lines(y=MovingAvg2(apply(((stocks.dsbmom.max-stocks.dsbmom.min)/stocks.hlrs.df), 1, 
                         function(x) median(x, na.rm=T)),252),
      x=(as.Date(rownames(stocks.HFTflags.df))), type="l", lwd=2, col="blue")

# ..unadj. mean
plot(y=apply(((stocks.dsbmom.max-stocks.dsbmom.min)/1), 1, 
             function(x) mean(x, na.rm=T, trim=.03)), 
     x=(as.Date(rownames(stocks.HFTflags.df))),
     main="Daily avg. EE size", type="l", lwd=2,
     ylab="EE size", xlab="Date")
lines(y=MovingAvg2(apply(((stocks.dsbmom.max-stocks.dsbmom.min)), 1, 
                         function(x) mean(x, na.rm=T, trim=.005)),252),
      x=(as.Date(rownames(stocks.HFTflags.df))), type="l", lwd=2, col="blue")

# ..unadj. median
plot(y=apply(((stocks.dsbmom.max-stocks.dsbmom.min)/1), 1, 
             function(x) median(x, na.rm=T)), 
     x=(as.Date(rownames(stocks.HFTflags.df))),
     main="Daily median EE size", type="l", lwd=2,
     ylab="EE size", xlab="Date")
lines(y=MovingAvg2(apply(((stocks.dsbmom.max-stocks.dsbmom.min)/1), 1, 
                         function(x) median(x, na.rm=T)),252),
      x=(as.Date(rownames(stocks.HFTflags.df))), type="l", lwd=2, col="blue")

# 1y MA
plot(y=MovingAvg2(apply(((stocks.dsbmom.max-stocks.dsbmom.min)/stocks.hlrs.df), 1, 
                        function(x) median(x, na.rm=T)),252), 
     x=(as.Date(rownames(stocks.HFTflags.df))),
     main="Daily median EE size, 1Y moving avg.", type="l", lwd=2,
     ylab="EE size", xlab="Date")
lines(y=1.014+MovingAvg2(apply((log1p(stocks.HFTflags.df))[range.idx, ], 1, 
                           function(x) mean(x, na.rm=T, trim=.005)),252)/200,
      x=(as.Date(rownames(stocks.HFTflags.df[range.idx, ]))), type="l", lwd=2, col="blue")

plot(y=MovingAvg2(apply(((stocks.score.max-stocks.score.min)), 1, 
                        function(x) median(x, na.rm=T)),252), 
     x=(as.Date(rownames(stocks.HFTflags.df))),
     main="Daily median EE score, 1Y moving avg.", type="l", lwd=2,
     ylab="EE size", xlab="Date")
lines(y=3.8+MovingAvg2(apply((log1p(stocks.HFTflags.df))[range.idx, ], 1, 
                               function(x) mean(x, na.rm=T, trim=.005)),252)/10,
      x=(as.Date(rownames(stocks.HFTflags.df[range.idx, ]))), type="l", lwd=2, col="blue")


# 1.bbb) investigate correlations ----------------------------------------------
# daily diff correlation EE vs. HFT
rank.sel.fac <- apply (stocks.sizes.df[range.idx, ], 2, mean, na.rm=T)
rank.sel.fac <- rank(rank.sel.fac)/length(rank.sel.fac)
rank.sel.fac <- cut(rank.sel.fac, breaks=c(0, 0.2, 0.4, 0.6, 0.8, 1), 
                    include.lowest=T, labels=c("Q5", "Q4", "Q3", "Q2", "Q1"))
# full sample
cor(Winsor(apply(DiffDf(log1p((stocks.HFTflags.df[range.idx, ]))), 1, median, na.rm=T)),
    Winsor(apply(DiffDf(stocks.score.max-stocks.score.min)[range.idx, ], 1, median, na.rm=T)), use="p")
cor(Winsor(apply(DiffDf(log1p((stocks.HFTflags.df[range.idx, ]))), 1, mean, na.rm=T, trim=.005)),
    Winsor(apply(DiffDf(stocks.score.max-stocks.score.min)[range.idx, ], 1, mean, na.rm=T, trim=.005)), use="p")
# quintiles
for (i in rev(levels(rank.sel.fac))) {
  sel.fac <- i
  print(i)
  print("cross-sectional medians")
  print(cor(Winsor(apply(DiffDf(log1p((stocks.HFTflags.df[range.idx, rank.sel.fac==sel.fac]))), 1, median, na.rm=T)),
      Winsor(apply(DiffDf(stocks.score.max-stocks.score.min)[range.idx, rank.sel.fac==sel.fac], 1, median, na.rm=T)), use="p"))
  print("cross-sectional means")
  print(cor(Winsor(apply(DiffDf(log1p((stocks.HFTflags.df[range.idx, rank.sel.fac==sel.fac]))), 1, mean, na.rm=T, trim=.03)),
      Winsor(apply(DiffDf(stocks.score.max-stocks.score.min)[range.idx, rank.sel.fac==sel.fac], 1, mean, na.rm=T, trim=.03)), use="p"))  
}

# summary stats (cross-sectional) ----------------------------------------------
# stocks.HFTflags.df
# stocks.sizes.df / 1000
# stocks.liqs.df / 1000000
# stocks.tovs.df * 100
# stocks.prices.df
# stocks.score.max - stocks.score.min

test.df <- stocks.dsbmom.max - stocks.dsbmom.min
stocks.remove <- c("AMRN.Q", "EFUT.Q", "FFHL.Q", "ROSG.Q", "UNXL.Q", "VHC.A")
test.df <- test.df[,!(names(test.df) %in% stocks.remove)]
years.fac <- years(as.chron(rownames(test.df[range.idx,])))
mean(Winsor(apply(((test.df)/1)[range.idx, ], 2,
           function(x) mean(Winsor(x), na.rm=T))), na.rm=T)
median(apply(((test.df)/1)[range.idx, ], 2,
             function(x) mean(Winsor(x), na.rm=T)), na.rm=T)
sd(apply(((test.df)/1)[range.idx, ], 2,
             function(x) mean(Winsor(x), na.rm=T)), na.rm=T)
quantile(apply(((test.df)/1)[range.idx, ], 2,
             function(x) mean(Winsor(x), na.rm=T)), na.rm=T, 
         probs=c(0, 0.01, 0.05, 0.1, 0.5, 0.9, 0.95, 0.99, 1))

# yearly subsets
for (i in levels(years.fac)) {
  print(i)
  print(paste("mean", 
    mean(Winsor(apply(test.df[range.idx[years.fac==i], ], 2,
               function(x) mean(Winsor(x), na.rm=T))), na.rm=T)))
  print(paste("median", 
    median(apply(test.df[range.idx[years.fac==i], ], 2,
                 function(x) mean(Winsor(x), na.rm=T)), na.rm=T)))
  print(paste("sd", 
    sd(apply(test.df[range.idx[years.fac==i], ], 2,
                 function(x) mean(Winsor(x), na.rm=T)), na.rm=T)))
  print("quantiles")
  print(
    quantile(apply(((test.df)/1)[years.fac==i, ], 2,
                   function(x) mean(Winsor(x), na.rm=T)), na.rm=T, 
             probs=c(0, 0.01, 0.05, 0.1, 0.5, 0.9, 0.95, 0.99, 1)) )
}

# yearly subsets in size quintiles (only for HFT & EE)
# size ranking
rank.sel.fac <- apply (stocks.sizes.df[range.idx, !(names(stocks.sizes.df) %in% stocks.remove)],
                       2, mean, na.rm=T)
rank.sel.fac <- rank(rank.sel.fac)/length(rank.sel.fac)
rank.sel.fac <- cut(rank.sel.fac, breaks=c(0, 0.2, 0.4, 0.6, 0.8, 1), 
                    include.lowest=T, labels=c("Q5", "Q4", "Q3", "Q2", "Q1"))

# mean
for (s in rev(levels(rank.sel.fac))) {
  print(s)
  int.df <- test.df[, rank.sel.fac==s]
for (i in levels(years.fac)) {
  print(i)
  print(paste("mean", 
              mean(Winsor(apply(int.df[range.idx[years.fac==i], ], 2,
                                function(x) mean(Winsor(x), na.rm=T))), na.rm=T)))
}
}
# median
for (s in rev(levels(rank.sel.fac))) {
  print(s)
  int.df <- test.df[, rank.sel.fac==s]
  for (i in levels(years.fac)) {
    print(i)
    print(paste("median", 
                median(Winsor(apply(int.df[range.idx[years.fac==i], ], 2,
                                  function(x) mean(Winsor(x), na.rm=T))), na.rm=T)))
  }
}


# 2) Check data completeness and filter symbols --------------------------------
for (i in seq_along(ls(pattern="^stocks.*"))) {
  cur.name <- ls (pattern="^stocks.*")[i]
  test <- apply (get(cur.name)[range.idx,], 2, function(x) any(is.na(x)))
  print(cur.name)
  print(summary(test))
}  

# data completeness check (take out stocks with NAs in selected date range)
GetCompleteStocks <- function() {
  stocks.completeV <- apply (LagDf(get("stocks.volas.df"))[range.idx,], 2, 
                             function(x) any(is.na(x)))
  summary(stocks.completeV)
  
  stocks.completeE1 <- apply (LagDf(get("stocks.score.max"))[range.idx,], 2, 
                              function(x) any(is.na(x) | x > 100))
  summary(stocks.completeE1)
  stocks.completeE2 <- apply (LagDf(get("stocks.score.min"))[range.idx,], 2, 
                              function(x) any(is.na(x) | x < -100))
  summary(stocks.completeE2)
  
  # standard price filter
  # stocks.pricefilt <- apply (stocks.prices.df[range.idx,], 2, 
  #                            function(x) any(mean(x)<5 | mean(x)>1000))
  stocks.pricefilt <- apply (LagDf(stocks.prices.df)[range.idx,], 2, 
                             function(x) any((x)<5 | (x)>1000))
  stocks.pricefilt[is.na(stocks.pricefilt)] <- T
  summary(stocks.pricefilt)
  
  # clean intraday return distribution check
  # filters out days with missing/bar-filled data and/or no trading activity
  stocks.distfilt <- apply (get("stocks.idhl.mean.df")[range.idx,], 2, 
                            function(x) any((x==0 | is.na(x))))
  summary(stocks.distfilt)
  
  stocks.sizefilt <- apply (stocks.sizes.df[range.idx,], 2, function(x) 
                            any(mean(x, na.rm=T) < 250 | is.na(x) ) )
  summary(stocks.sizefilt)
  
  # data required for 1st stage regression
  stocks.complete <- (stocks.completeV | stocks.pricefilt)
  summary(stocks.complete)
  # data required for EEU/EED regressions
  stocks.complete <- (stocks.completeV | stocks.pricefilt | stocks.distfilt |
                      stocks.completeE1 | stocks.completeE2 | stocks.sizefilt)
  return (stocks.complete)
}

stocksel.complete <- GetCompleteStocks()
summary(stocksel.complete)

# SPECIAL: select time-matched EventDB -----------------------------------------
BuildFinalFiltDf(filter.method="slot")
events.sel.df <- events.master.dn.df.final[events.master.dn.df.final$symbol %in%
                   names(stocks.HFTflags.df)[!stocksel.complete] &
                   as.Date(events.master.dn.df.final$timestamp) %in% 
                   as.Date(rownames(stocks.HFTflags.df[range.idx,])), ]
events.sel.df <- events.master.up.df.filt[events.master.up.df.filt$symbol %in%
                  names(stocks.HFTflags.df)[!stocksel.complete] &
                  as.Date(events.master.up.df.filt$timestamp) %in% 
                  as.Date(rownames(stocks.HFTflags.df[range.idx,])), ]
events.sel.df <- events.master.dn.df.filt[events.master.dn.df.filt$symbol %in%
  names(stocks.HFTflags.df)[!stocksel.complete] &
  as.Date(events.master.dn.df.filt$timestamp) %in% 
  as.Date(rownames(stocks.HFTflags.df)), ]

events.sel.df$NMS <- sapply(1:nrow(events.sel.df), function(x) 
  stocks.NMS.df[as.character(as.Date(events.sel.df$timestamp[x])),
                events.sel.df$symbol[x]])

events.sel.df$NAB <- events.sel.df$timestamp > "2011-11-30"

events.sel.df$date <- as.ordered(as.Date(events.sel.df$timestamp))
events.sel.df$PR <- 1/events.sel.df$price.c1
events.sel.df$daytime <- as.ordered(round((hours(as.chron(events.sel.df$timestamp)) -9 + 
                                  minutes(as.chron(events.sel.df$timestamp))/60)*2))


hist(log(events.sel.df$PREW60.rel.vol))
hist(log(events.sel.df$PREW60.rel.vola))
hist(sqrt(events.sel.df$PREP.vola.hlr))
hist(log(events.sel.df$PREP.liq.avg))
hist(log(events.sel.df$PREP.tov.avg))
hist((events.sel.df$PR))
hist((events.sel.df$EFP))

hist(sqrt(events.sel.df$PREW60.HFT.rel.pq.rate))
events.sel.df$ECP.max.cont

hist(log(Winsor(events.sel.df$PREW60.HFT.pq.rate+0.00001)))
hist(log(Winsor(events.sel.df$PREW60.HFT.f.rate+1/60)))

events.res <- felm (log(Winsor(PREW10.HFT.f.rate+0.00001)) ~ NAB + 
                      Winsor(log(market.cap)) + 
                      (Winsor(PREP.tov.avg * PREW30.rel.vol)) + 
                      (Winsor(PREP.vola.hlr * PREW30.rel.vola)) + 
                      Winsor(PR) + G(daytime),
                    events.sel.df, clustervar=interaction(events.sel.df$date))
summary (events.res)
qqPlot(events.res$residuals)
PREW30.HFT.pq.rate.hat <- events.res$fitted

hist(PREW60.HFT.pq.rate.hat)
hist(log(events.sel.df$PREW30.rel.r))

# 2nd stage regression
events.res <- felm ((PREW30.rel.r) ~ log(PREW30.HFT.f.rate+0.001) + 
                      Winsor(log(market.cap)) + 
                      (Winsor(PREP.tov.avg * PREW30.rel.vol)) + 
                      (Winsor(PREP.vola.hlr * PREW30.rel.vola)) + 
                      Winsor(PR) + G(date) + G(daytime),
                    events.sel.df, clustervar=interaction(events.sel.df$date))
summary (events.res)
qqPlot(events.res$residuals)

# try Wilcox Test
Boxplot(events.sel.df$EFP.r-events.sel.df$ECP.max.cont, g=events.sel.df$NMS)
wilcox.test((events.sel.df$EFP.r-events.sel.df$ECP.max.cont)[events.sel.df$NMS],
            (events.sel.df$EFP.r-events.sel.df$ECP.max.cont)[!events.sel.df$NMS])
t.test((events.sel.df$EFP.r-events.sel.df$ECP.max.cont)[events.sel.df$NMS],
            (events.sel.df$EFP.r-events.sel.df$ECP.max.cont)[!events.sel.df$NMS])

# control for market factors
(EFP.r-ECP.max.cont)
events.sel.df$TUP.C2.rel.vol

events.res <- felm (((TUP.C2.rel.vol)) ~  
                      Winsor(log(market.cap)) + 
                      (Winsor(PREP.tov.avg * PREW60.rel.vol)) + 
                      (Winsor(PREP.vola.hlr * PREW60.rel.vola)) + 
                      Winsor(PR) + G(date) + G(daytime),
                    events.sel.df, clustervar=interaction(events.sel.df$date))
summary (events.res)
events.res$residuals
wilcox.test((events.res$residuals)[events.sel.df$NMS],
            (events.res$residuals)[!events.sel.df$NMS], alternative="l")

t.test((events.res$residuals)[events.sel.df$NMS],
            (events.res$residuals)[!events.sel.df$NMS], alternative="l")


# 3) Build Panel Dataframe -----------------------------------------------------
# 3a) "Panelize" individual variables ------------------------------------------

# Move process into function to keep workspace clean
ConstructPanel <- function (off=T) {  
  # HFT Entry
  stocks.HEN.pdf <- 
    melt (as.matrix(stocks.HFTactive.df.90quarter[range.idx, !stocksel.complete]))
  stocks.HEN.pdf <- plm.data (stocks.HEN.pdf, index=c("X2", "X1"))
  names(stocks.HEN.pdf) <- c("symbol", "date", "HEN")
  
  # HFL - HFT flags
  # *** ATTENTION: STANDARDIZATION FUNCTION APPLIED ***
  stocks.HFL.pdf <- melt (as.matrix(StdDf(stocks.HFTflags.df[range.idx, !stocksel.complete], off)))
  stocks.HFL.pdf <- plm.data (stocks.HFL.pdf, index=c("X2", "X1"))
  names(stocks.HFL.pdf) <- c("symbol", "date", "HFL")
  
  # HPQ - HFT flagged quote rate
  stocks.HPQ.pdf <- melt (as.matrix(stocks.HFTpquotes.df[range.idx, !stocksel.complete]))
  stocks.HPQ.pdf <- plm.data (stocks.HPQ.pdf, index=c("X2", "X1"))
  names(stocks.HPQ.pdf) <- c("symbol", "date", "HPQ")
  
  # NMS (REG NMS Rollout 2007 July/August)
  stocks.NMS.pdf <- melt (as.matrix(stocks.NMS.df[range.idx, !stocksel.complete]))
  stocks.NMS.pdf <- plm.data (stocks.NMS.pdf, index=c("X2", "X1"))
  names(stocks.NMS.pdf) <- c("symbol", "date", "NMS")
  
  # SSB (SEC Short-Sale Ban 2008 Sep/Oct)
  stocks.SSB.pdf <- melt (as.matrix(stocks.SSB.df[range.idx, !stocksel.complete]))
  stocks.SSB.pdf <- plm.data (stocks.SSB.pdf, index=c("X2", "X1"))
  names(stocks.SSB.pdf) <- c("symbol", "date", "SSB")
  
  # SIZE (ln market cap) -- ATTENTION: LAG
  stocks.SIZE.pdf <- melt (log(as.matrix(LagDf(stocks.sizes.df)[range.idx, !stocksel.complete])))
  stocks.SIZE.pdf <- plm.data (stocks.SIZE.pdf, index=c("X2", "X1"))
  names(stocks.SIZE.pdf) <- c("symbol", "date", "SIZE")
  
  # FLT (ln float cap) -- ATTENTION: LAG
  stocks.FLT.pdf <- melt (log(as.matrix(LagDf(stocks.fcaps.df)[range.idx, !stocksel.complete])))
  stocks.FLT.pdf <- plm.data (stocks.FLT.pdf, index=c("X2", "X1"))
  names(stocks.FLT.pdf) <- c("symbol", "date", "FLT")
  
  # VOLA (classic quarterly vola) -- ATTENTION: LAG
  stocks.VOLA.pdf <- melt (as.matrix(LagDf(stocks.volas.df)[range.idx, !stocksel.complete]))
  stocks.VOLA.pdf <- plm.data (stocks.VOLA.pdf, index=c("X2", "X1"))
  names(stocks.VOLA.pdf) <- c("symbol", "date", "VOLA")
  
  # BETA (classic quarterly vola) -- alreaddy lagged in source
  stocks.BETA.pdf <- melt (as.matrix(stocks.betas.df[range.idx, !stocksel.complete]))
  stocks.BETA.pdf <- plm.data (stocks.BETA.pdf, index=c("X2", "X1"))
  names(stocks.BETA.pdf) <- c("symbol", "date", "BETA")
  
  # VOLE (HLR extremum-based vola)
  stocks.VOLE.pdf <- melt (as.matrix(stocks.hlrs.df[range.idx, !stocksel.complete]))
  stocks.VOLE.pdf <- plm.data (stocks.VOLE.pdf, index=c("X2", "X1"))
  names(stocks.VOLE.pdf) <- c("symbol", "date", "VOLE")
  
  # PR (1/PRICE) -- ATTENTION: LAG
  stocks.PR.pdf <- melt (as.matrix(LagDf(stocks.prices.df)[range.idx, !stocksel.complete]))
  stocks.PR.pdf <- plm.data (stocks.PR.pdf, index=c("X2", "X1"))
  names(stocks.PR.pdf) <- c("symbol", "date", "PR")
  
  # TOV (Turnover)
  stocks.TOV.pdf <- melt (as.matrix(stocks.tovs.df[range.idx, !stocksel.complete]))
  stocks.TOV.pdf <- plm.data (stocks.TOV.pdf, index=c("X2", "X1"))
  names(stocks.TOV.pdf) <- c("symbol", "date", "TOV")
  
  # LIQ (ln $Liquidity)
  stocks.LIQ.pdf <- melt (log(as.matrix(stocks.liqs.df[range.idx, !stocksel.complete])))
  stocks.LIQ.pdf <- plm.data (stocks.LIQ.pdf, index=c("X2", "X1"))
  names(stocks.LIQ.pdf) <- c("symbol", "date", "LIQ")
  
  # VOL (volume)
  stocks.VOL.pdf <- 
    melt (as.matrix((stocks.liqs.df/stocks.prices.df)[range.idx, !stocksel.complete]))
  stocks.VOL.pdf <- plm.data (stocks.VOL.pdf, index=c("X2", "X1"))
  names(stocks.VOL.pdf) <- c("symbol", "date", "VOL")
  
  # EE Score Min/Max -- daily up/down events
  #  **** ATTENTION: STANDARDIZED EEU+EED ****
  # EEU
  stocks.EEU.pdf <- melt (as.matrix(StdDf(stocks.score.max[range.idx, !stocksel.complete], off)))
  stocks.EEU.pdf <- plm.data (stocks.EEU.pdf, index=c("X2", "X1"))
  names(stocks.EEU.pdf) <- c("symbol", "date", "EEU")
  
  # EED
  stocks.EED.pdf <- melt (as.matrix(StdDf(-stocks.score.min[range.idx, !stocksel.complete], off)))
  stocks.EED.pdf <- plm.data (stocks.EED.pdf, index=c("X2", "X1"))
  names(stocks.EED.pdf) <- c("symbol", "date", "EED")
  
  # LAG 1 versions of EEU & EED
  # EEUL
  stocks.EEUL.pdf <- melt (as.matrix(StdDf(LagDf(stocks.score.max)[range.idx, !stocksel.complete], off)))
  stocks.EEUL.pdf <- plm.data (stocks.EEUL.pdf, index=c("X2", "X1"))
  names(stocks.EEUL.pdf) <- c("symbol", "date", "EEUL")
  
  # EEDL
  stocks.EEDL.pdf <- melt (as.matrix(StdDf(LagDf(-stocks.score.min)[range.idx, !stocksel.complete], off)))
  stocks.EEDL.pdf <- plm.data (stocks.EEDL.pdf, index=c("X2", "X1"))
  names(stocks.EEDL.pdf) <- c("symbol", "date", "EEDL")
  #  **** ATTENTION: STANDARDIZED EEU+EEDL ****
  
  # EEUT
  stocks.EEUT.pdf <- melt (as.matrix(stocks.score.max.t[range.idx, !stocksel.complete]))
  stocks.EEUT.pdf <- plm.data (stocks.EEUT.pdf, index=c("X2", "X1"))
  names(stocks.EEUT.pdf) <- c("symbol", "date", "EEUT")
  
  # EEDT
  stocks.EEDT.pdf <- melt (as.matrix(stocks.score.min.t[range.idx, !stocksel.complete]))
  stocks.EEDT.pdf <- plm.data (stocks.EEDT.pdf, index=c("X2", "X1"))
  names(stocks.EEDT.pdf) <- c("symbol", "date", "EEDT")
  
  # HFTU0
  stocks.HFTU0.pdf <- melt (as.matrix(stocks.hft.max[range.idx, !stocksel.complete]))
  stocks.HFTU0.pdf <- plm.data (stocks.HFTU0.pdf, index=c("X2", "X1"))
  names(stocks.HFTU0.pdf) <- c("symbol", "date", "HFTU0")
  
  # HFTD0
  stocks.HFTD0.pdf <- melt (as.matrix(stocks.hft.min[range.idx, !stocksel.complete]))
  stocks.HFTD0.pdf <- plm.data (stocks.HFTD0.pdf, index=c("X2", "X1"))
  names(stocks.HFTD0.pdf) <- c("symbol", "date", "HFTD0")
  
  # HFTU1
  stocks.HFTU1.pdf <- melt (as.matrix(stocks.hft.max.l1[range.idx, !stocksel.complete]))
  stocks.HFTU1.pdf <- plm.data (stocks.HFTU1.pdf, index=c("X2", "X1"))
  names(stocks.HFTU1.pdf) <- c("symbol", "date", "HFTU1")
  
  # HFTD1
  stocks.HFTD1.pdf <- melt (as.matrix(stocks.hft.min.l1[range.idx, !stocksel.complete]))
  stocks.HFTD1.pdf <- plm.data (stocks.HFTD1.pdf, index=c("X2", "X1"))
  names(stocks.HFTD1.pdf) <- c("symbol", "date", "HFTD1")
  
  # HFTU5
  stocks.HFTU5.pdf <- melt (as.matrix(stocks.hft.max.l5[range.idx, !stocksel.complete]))
  stocks.HFTU5.pdf <- plm.data (stocks.HFTU5.pdf, index=c("X2", "X1"))
  names(stocks.HFTU5.pdf) <- c("symbol", "date", "HFTU5")
  
  # HFTD5
  stocks.HFTD5.pdf <- melt (as.matrix(stocks.hft.min.l5[range.idx, !stocksel.complete]))
  stocks.HFTD5.pdf <- plm.data (stocks.HFTD5.pdf, index=c("X2", "X1"))
  names(stocks.HFTD5.pdf) <- c("symbol", "date", "HFTD5")
  
  # HFTU15
  stocks.HFTU15.pdf <- melt (as.matrix(stocks.hft.max.l15[range.idx, !stocksel.complete]))
  stocks.HFTU15.pdf <- plm.data (stocks.HFTU15.pdf, index=c("X2", "X1"))
  names(stocks.HFTU15.pdf) <- c("symbol", "date", "HFTU15")
  
  # HFTD15
  stocks.HFTD15.pdf <- melt (as.matrix(stocks.hft.min.l15[range.idx, !stocksel.complete]))
  stocks.HFTD15.pdf <- plm.data (stocks.HFTD15.pdf, index=c("X2", "X1"))
  names(stocks.HFTD15.pdf) <- c("symbol", "date", "HFTD15")
  
  # VOLU0
  stocks.VOLU0.pdf <- melt (as.matrix(stocks.vol.max[range.idx, !stocksel.complete]))
  stocks.VOLU0.pdf <- plm.data (stocks.VOLU0.pdf, index=c("X2", "X1"))
  names(stocks.VOLU0.pdf) <- c("symbol", "date", "VOLU0")
  
  # VOLD0
  stocks.VOLD0.pdf <- melt (as.matrix(stocks.vol.min[range.idx, !stocksel.complete]))
  stocks.VOLD0.pdf <- plm.data (stocks.VOLD0.pdf, index=c("X2", "X1"))
  names(stocks.VOLD0.pdf) <- c("symbol", "date", "VOLD0")
  
  # VOLU1
  stocks.VOLU1.pdf <- melt (as.matrix(stocks.vol.max.l1[range.idx, !stocksel.complete]))
  stocks.VOLU1.pdf <- plm.data (stocks.VOLU1.pdf, index=c("X2", "X1"))
  names(stocks.VOLU1.pdf) <- c("symbol", "date", "VOLU1")
  
  # VOLD1
  stocks.VOLD1.pdf <- melt (as.matrix(stocks.vol.min.l1[range.idx, !stocksel.complete]))
  stocks.VOLD1.pdf <- plm.data (stocks.VOLD1.pdf, index=c("X2", "X1"))
  names(stocks.VOLD1.pdf) <- c("symbol", "date", "VOLD1")
  
  # VOLU5
  stocks.VOLU5.pdf <- melt (as.matrix(stocks.vol.max.l5[range.idx, !stocksel.complete]))
  stocks.VOLU5.pdf <- plm.data (stocks.VOLU5.pdf, index=c("X2", "X1"))
  names(stocks.VOLU5.pdf) <- c("symbol", "date", "VOLU5")
  
  # VOLD5
  stocks.VOLD5.pdf <- melt (as.matrix(stocks.vol.min.l5[range.idx, !stocksel.complete]))
  stocks.VOLD5.pdf <- plm.data (stocks.VOLD5.pdf, index=c("X2", "X1"))
  names(stocks.VOLD5.pdf) <- c("symbol", "date", "VOLD5")
  
  # VOLU15
  stocks.VOLU15.pdf <- melt (as.matrix(stocks.vol.max.l15[range.idx, !stocksel.complete]))
  stocks.VOLU15.pdf <- plm.data (stocks.VOLU15.pdf, index=c("X2", "X1"))
  names(stocks.VOLU15.pdf) <- c("symbol", "date", "VOLU15")
  
  # VOLD15
  stocks.VOLD15.pdf <- melt (as.matrix(stocks.vol.min.l15[range.idx, !stocksel.complete]))
  stocks.VOLD15.pdf <- plm.data (stocks.VOLD15.pdf, index=c("X2", "X1"))
  names(stocks.VOLD15.pdf) <- c("symbol", "date", "VOLD15")
  
  # IEM
  stocks.IEM.pdf <- melt (as.matrix(stocks.idhl.mean.df[range.idx, !stocksel.complete]))
  stocks.IEM.pdf <- plm.data (stocks.IEM.pdf, index=c("X2", "X1"))
  names(stocks.IEM.pdf) <- c("symbol", "date", "IEM")
  
  # IEMD
  stocks.IEMD.pdf <- melt (as.matrix(stocks.idhl.median.df[range.idx, !stocksel.complete]))
  stocks.IEMD.pdf <- plm.data (stocks.IEMD.pdf, index=c("X2", "X1"))
  names(stocks.IEMD.pdf) <- c("symbol", "date", "IEMD")
  
  # IEQ95
  stocks.IEQ95.pdf <- melt (as.matrix(stocks.idhl.q95.df[range.idx, !stocksel.complete]))
  stocks.IEQ95.pdf <- plm.data (stocks.IEQ95.pdf, index=c("X2", "X1"))
  names(stocks.IEQ95.pdf) <- c("symbol", "date", "IEQ95")
  
  # IEQ99
  stocks.IEQ99.pdf <- melt (as.matrix(stocks.idhl.q99.df[range.idx, !stocksel.complete]))
  stocks.IEQ99.pdf <- plm.data (stocks.IEQ99.pdf, index=c("X2", "X1"))
  names(stocks.IEQ99.pdf) <- c("symbol", "date", "IEQ99")
  
  # IET
  stocks.IET.pdf <- melt (as.matrix(stocks.idhl.tail.df[range.idx, !stocksel.complete]))
  stocks.IET.pdf <- plm.data (stocks.IET.pdf, index=c("X2", "X1"))
  names(stocks.IET.pdf) <- c("symbol", "date", "IET")
  
  # IRM
  stocks.IRM.pdf <- melt (as.matrix(stocks.idret.mean.df[range.idx, !stocksel.complete]))
  stocks.IRM.pdf <- plm.data (stocks.IRM.pdf, index=c("X2", "X1"))
  names(stocks.IRM.pdf) <- c("symbol", "date", "IRM")
  
  # IRQ95
  stocks.IRQ95.pdf <- melt (as.matrix(stocks.idret.q95abs.df[range.idx, !stocksel.complete]))
  stocks.IRQ95.pdf <- plm.data (stocks.IRQ95.pdf, index=c("X2", "X1"))
  names(stocks.IRQ95.pdf) <- c("symbol", "date", "IRQ95")
  
  # IRQ99
  stocks.IRQ99.pdf <- melt (as.matrix(stocks.idret.q99abs.df[range.idx, !stocksel.complete]))
  stocks.IRQ99.pdf <- plm.data (stocks.IRQ99.pdf, index=c("X2", "X1"))
  names(stocks.IRQ99.pdf) <- c("symbol", "date", "IRQ99")
  
  # IRT
  stocks.IRT.pdf <- melt (as.matrix(stocks.idret.tail.df[range.idx, !stocksel.complete]))
  stocks.IRT.pdf <- plm.data (stocks.IRT.pdf, index=c("X2", "X1"))
  names(stocks.IRT.pdf) <- c("symbol", "date", "IRT")
  
  # IRK
  stocks.IRK.pdf <- melt (as.matrix(stocks.idret.kurt.df[range.idx, !stocksel.complete]))
  stocks.IRK.pdf <- plm.data (stocks.IRK.pdf, index=c("X2", "X1"))
  names(stocks.IRK.pdf) <- c("symbol", "date", "IRK")
  
  # IRMA
  stocks.IRMA.pdf <- melt (as.matrix(stocks.idret.meanabs.df[range.idx, !stocksel.complete]))
  stocks.IRMA.pdf <- plm.data (stocks.IRMA.pdf, index=c("X2", "X1"))
  names(stocks.IRMA.pdf) <- c("symbol", "date", "IRMA")
  
  # IRMDA
  stocks.IRMDA.pdf <- melt (as.matrix(stocks.idret.medianabs.df[range.idx, !stocksel.complete]))
  stocks.IRMDA.pdf <- plm.data (stocks.IRMDA.pdf, index=c("X2", "X1"))
  names(stocks.IRMDA.pdf) <- c("symbol", "date", "IRMDA")
  
  # IRS
  stocks.IRS.pdf <- melt (as.matrix(stocks.idret.sd.df[range.idx, !stocksel.complete]))
  stocks.IRS.pdf <- plm.data (stocks.IRS.pdf, index=c("X2", "X1"))
  names(stocks.IRS.pdf) <- c("symbol", "date", "IRS")
  
  # IRMIN
  stocks.IRMIN.pdf <- melt (as.matrix(stocks.idret.min.df[range.idx, !stocksel.complete]))
  stocks.IRMIN.pdf <- plm.data (stocks.IRMIN.pdf, index=c("X2", "X1"))
  names(stocks.IRMIN.pdf) <- c("symbol", "date", "IRMIN")
  
  # IRMAX
  stocks.IRMAX.pdf <- melt (as.matrix(stocks.idret.max.df[range.idx, !stocksel.complete]))
  stocks.IRMAX.pdf <- plm.data (stocks.IRMAX.pdf, index=c("X2", "X1"))
  names(stocks.IRMAX.pdf) <- c("symbol", "date", "IRMAX")
  
  # 3b) build combined regression panel ------------------------------------------
  stocks.panel.pdf <- stocks.HFL.pdf
  stocks.panel.pdf$HPQ <- 1000 * stocks.HPQ.pdf$HPQ
  stocks.panel.pdf$HEN <- stocks.HEN.pdf$HEN
  stocks.panel.pdf$NMS <- stocks.NMS.pdf$NMS
  stocks.panel.pdf$SSB <- stocks.SSB.pdf$SSB
  stocks.panel.pdf$NAB <- sapply(1:nrow(stocks.panel.pdf), function(x)
                          as.Date(stocks.panel.pdf$date[x]) >= ("2011-11-30"))
  stocks.panel.pdf$EEUT30 <- as.ordered(round(stocks.EEUT.pdf$EEUT/30))
  stocks.panel.pdf$EEDT30 <- as.ordered(round(stocks.EEDT.pdf$EEDT/30))
  stocks.panel.pdf$EEUT15 <- as.ordered(round(stocks.EEUT.pdf$EEUT/15))
  stocks.panel.pdf$EEDT15 <- as.ordered(round(stocks.EEDT.pdf$EEDT/15))
  stocks.panel.pdf$DTU <- interaction(stocks.panel.pdf$date, stocks.panel.pdf$EEUT30)
  stocks.panel.pdf$DTD <- interaction(stocks.panel.pdf$date, stocks.panel.pdf$EEDT30)
  stocks.panel.pdf$BETA <- stocks.BETA.pdf$BETA
  stocks.panel.pdf$VOLA <- stocks.VOLA.pdf$VOLA
  stocks.panel.pdf$VOLE <- stocks.VOLE.pdf$VOLE
  stocks.panel.pdf$TOV <- stocks.TOV.pdf$TOV
  stocks.panel.pdf$LIQ <- stocks.LIQ.pdf$LIQ
  stocks.panel.pdf$VOL <- stocks.VOL.pdf$VOL
  stocks.panel.pdf$HPF <- 100000 * stocks.HFL.pdf$HFL / stocks.VOL.pdf$VOL
  stocks.panel.pdf$SIZE <- stocks.SIZE.pdf$SIZE
  stocks.panel.pdf$FLT <- stocks.FLT.pdf$FLT
  stocks.panel.pdf$PR <- 1/stocks.PR.pdf$PR
  stocks.panel.pdf$PRL <- log(stocks.PR.pdf$PR)
  stocks.panel.pdf$EEU <- stocks.EEU.pdf$EEU
  stocks.panel.pdf$EED <- stocks.EED.pdf$EED
  stocks.panel.pdf$EEUL <- stocks.EEUL.pdf$EEUL
  stocks.panel.pdf$EEDL <- stocks.EEDL.pdf$EEDL
  stocks.panel.pdf$HFTU0 <- stocks.HFTU0.pdf$HFTU0
  stocks.panel.pdf$HFTD0 <- stocks.HFTD0.pdf$HFTD0
  stocks.panel.pdf$HFTU1 <- stocks.HFTU1.pdf$HFTU1
  stocks.panel.pdf$HFTD1 <- stocks.HFTD1.pdf$HFTD1
  stocks.panel.pdf$HFTU5 <- stocks.HFTU5.pdf$HFTU5
  stocks.panel.pdf$HFTD5 <- stocks.HFTD5.pdf$HFTD5
  stocks.panel.pdf$HFTU15 <- stocks.HFTU15.pdf$HFTU15
  stocks.panel.pdf$HFTD15 <- stocks.HFTD15.pdf$HFTD15
  stocks.panel.pdf$VOLU0 <- stocks.VOLU0.pdf$VOLU0
  stocks.panel.pdf$VOLD0 <- stocks.VOLD0.pdf$VOLD0
  stocks.panel.pdf$VOLU1 <- stocks.VOLU1.pdf$VOLU1
  stocks.panel.pdf$VOLD1 <- stocks.VOLD1.pdf$VOLD1
  stocks.panel.pdf$VOLU5 <- stocks.VOLU5.pdf$VOLU5
  stocks.panel.pdf$VOLD5 <- stocks.VOLD5.pdf$VOLD5
  stocks.panel.pdf$VOLU15 <- stocks.VOLU15.pdf$VOLU15
  stocks.panel.pdf$VOLD15 <- stocks.VOLD15.pdf$VOLD15
  stocks.panel.pdf$HPFU0 <- ifelse (stocks.VOLU0.pdf$VOLU0 > 0,
    100000 * stocks.HFTU0.pdf$HFTU0 / stocks.VOLU0.pdf$VOLU0, stocks.HFTU0.pdf$HFTU0)
  stocks.panel.pdf$HPFD0 <- ifelse (stocks.VOLD0.pdf$VOLD0 > 0,
    100000 * stocks.HFTD0.pdf$HFTD0 / stocks.VOLD0.pdf$VOLD0, stocks.HFTD0.pdf$HFTD0)
  stocks.panel.pdf$HPFU1 <- ifelse (stocks.VOLU1.pdf$VOLU1 > 0,
    100000 * stocks.HFTU1.pdf$HFTU1 / stocks.VOLU1.pdf$VOLU1, stocks.HFTU1.pdf$HFTU1)
  stocks.panel.pdf$HPFD1 <- ifelse (stocks.VOLD1.pdf$VOLD1 > 0,
    100000 * stocks.HFTD1.pdf$HFTD1 / stocks.VOLD1.pdf$VOLD1, stocks.HFTD1.pdf$HFTD1)
  stocks.panel.pdf$HPFU5 <- ifelse (stocks.VOLU5.pdf$VOLU5 > 0,
    100000 * stocks.HFTU5.pdf$HFTU5 / stocks.VOLU5.pdf$VOLU5, stocks.HFTU5.pdf$HFTU5)
  stocks.panel.pdf$HPFD5 <- ifelse (stocks.VOLD5.pdf$VOLD5 > 0,
    100000 * stocks.HFTD5.pdf$HFTD5 / stocks.VOLD5.pdf$VOLD5, stocks.HFTD5.pdf$HFTD5)
  stocks.panel.pdf$HPFU15 <- ifelse (stocks.VOLU15.pdf$VOLU15 > 0,
    100000 * stocks.HFTU15.pdf$HFTU15 / stocks.VOLU15.pdf$VOLU15, stocks.HFTU15.pdf$HFTU15)
  stocks.panel.pdf$HPFD15 <- ifelse (stocks.VOLD15.pdf$VOLD15 > 0,
    100000 * stocks.HFTD15.pdf$HFTD15 / stocks.VOLD15.pdf$VOLD15, stocks.HFTD15.pdf$HFTD15)
  stocks.panel.pdf$HPQL <- stocks.panel.pdf$HPQ * stocks.panel.pdf$PR
  stocks.panel.pdf$HPFL <- stocks.panel.pdf$HPF * stocks.panel.pdf$PR
  stocks.panel.pdf$HPFLU0 <- stocks.panel.pdf$HPFU0 * stocks.panel.pdf$PR
  stocks.panel.pdf$HPFLD0 <- stocks.panel.pdf$HPFD0 * stocks.panel.pdf$PR
  stocks.panel.pdf$HPFLU1 <- stocks.panel.pdf$HPFU1 * stocks.panel.pdf$PR
  stocks.panel.pdf$HPFLD1 <- stocks.panel.pdf$HPFD1 * stocks.panel.pdf$PR
  stocks.panel.pdf$HPFLU5 <- stocks.panel.pdf$HPFU5 * stocks.panel.pdf$PR
  stocks.panel.pdf$HPFLD5 <- stocks.panel.pdf$HPFD5 * stocks.panel.pdf$PR
  stocks.panel.pdf$HPFLU15 <- stocks.panel.pdf$HPFU15 * stocks.panel.pdf$PR
  stocks.panel.pdf$HPFLD15 <- stocks.panel.pdf$HPFD15 * stocks.panel.pdf$PR
  
  stocks.panel.pdf$IEM <- stocks.IEM.pdf$IEM * 100
  stocks.panel.pdf$IEMD <- stocks.IEMD.pdf$IEMD * 100
  stocks.panel.pdf$IEQ95 <- stocks.IEQ95.pdf$IEQ95 * 100
  stocks.panel.pdf$IEQ99 <- stocks.IEQ99.pdf$IEQ99 * 100
  stocks.panel.pdf$IET <- stocks.IET.pdf$IET
  stocks.panel.pdf$IRM <- stocks.IRM.pdf$IRM * 100
  stocks.panel.pdf$IRQ95 <- stocks.IRQ95.pdf$IRQ95 * 100
  stocks.panel.pdf$IRQ99 <- stocks.IRQ99.pdf$IRQ99 * 100
  stocks.panel.pdf$IRT <- stocks.IRT.pdf$IRT
  stocks.panel.pdf$IRK <- stocks.IRK.pdf$IRK
  stocks.panel.pdf$IRMA <- stocks.IRMA.pdf$IRMA * 100
  stocks.panel.pdf$IRMDA <- stocks.IRMDA.pdf$IRMDA * 100
  stocks.panel.pdf$IRS <- stocks.IRS.pdf$IRS * 100
  stocks.panel.pdf$IRMIN <- stocks.IRMIN.pdf$IRMIN * 100
  stocks.panel.pdf$IRMAX <- stocks.IRMAX.pdf$IRMAX * 100
  
  stocks.panel.pdf$HFLlog <- log(stocks.panel.pdf$HFL+1)
  stocks.panel.pdf$HPFlog <- log(stocks.panel.pdf$HPF+0.01)
  stocks.panel.pdf$HPQlog <- log(stocks.panel.pdf$HPQ+0.01)
  stocks.panel.pdf$HPFLlog <- log(stocks.panel.pdf$HPFL+0.0001)
  stocks.panel.pdf$HPQLlog <- log(stocks.panel.pdf$HPQL+0.0001)
  
  return (stocks.panel.pdf)
}

stocks.panel.pdf <- ConstructPanel (off = T)
names(stocks.panel.pdf)


# 3b) Manual Adjustments of PDF ------------------------------------------------
# remove erroneous variables
stocks.panel.pdf <- stocks.panel.pdf[ , -which(names(stocks.panel.pdf) %in% c("SSB"))]
stocks.panel.pdf <- stocks.panel.pdf[ , -which(names(stocks.panel.pdf) %in% c("NMS"))]
stocks.panel.pdf <- stocks.panel.pdf[ , -which(names(stocks.panel.pdf) %in% c("NAB"))]
names(stocks.panel.pdf)


# adjust erroneous symbols
# TOV
agg <- aggregate(stocks.panel.pdf$TOV, by=list(stocks.panel.pdf$symbol), mean)
plot(agg)
names(agg) <- c("symbol", "tov.avg")
head(agg)
agg$symbol[agg$tov.avg > 0.2]

stocks.panel.pdf$TOV[stocks.panel.pdf$symbol %in% 
                       agg$symbol[agg$tov.avg > 0.2]] <-
  stocks.panel.pdf$TOV[stocks.panel.pdf$symbol %in% 
                         agg$symbol[agg$tov.avg > 0.2]] / 10
# TOV, 2nd round
agg <- aggregate(stocks.panel.pdf$TOV, by=list(stocks.panel.pdf$symbol), mean)
plot(agg)
names(agg) <- c("symbol", "tov.avg")
head(agg)
agg$symbol[agg$tov.avg > 0.1]

stocks.panel.pdf$TOV[stocks.panel.pdf$symbol %in% 
                       agg$symbol[agg$tov.avg > 0.1]] <-
  stocks.panel.pdf$TOV[stocks.panel.pdf$symbol %in% 
                         agg$symbol[agg$tov.avg > 0.1]] / 2
# check TOV adjustments
agg <- aggregate(stocks.panel.pdf$TOV, by=list(stocks.panel.pdf$symbol), mean)
plot(agg)

# SIZE
agg <- aggregate(stocks.panel.pdf$SIZE, by=list(stocks.panel.pdf$symbol), mean)
plot(agg)
names(agg) <- c("symbol", "size.avg")
head(agg)
agg$symbol[agg$size.avg > 12.75]

# adjust for massive capital structure changes during financial crisis
stocks.panel.pdf$SIZE[stocks.panel.pdf$symbol %in% 
                       c("AIG.N", "C.N", "IRE.N")] <-
  stocks.panel.pdf$SIZE[stocks.panel.pdf$symbol %in% 
                          c("AIG.N", "C.N", "IRE.N")] - log(10)
stocks.panel.pdf$SIZE[stocks.panel.pdf$symbol %in% 
                        c("ABV.N", "BAC.N")] <-
  stocks.panel.pdf$SIZE[stocks.panel.pdf$symbol %in% 
                          c("ABV.N", "BAC.N")] - log(5)
# check SIZE adjustments
agg <- aggregate(stocks.panel.pdf$SIZE, by=list(stocks.panel.pdf$symbol), mean)
plot(agg)
# apply adjustments to float
stocks.panel.pdf$FLT[stocks.panel.pdf$symbol %in% 
                        c("AIG.N", "C.N", "IRE.N")] <-
  stocks.panel.pdf$FLT[stocks.panel.pdf$symbol %in% 
                          c("AIG.N", "C.N", "IRE.N")] - log(10*.9)
stocks.panel.pdf$FLT[stocks.panel.pdf$symbol %in% 
                        c("ABV.N", "BAC.N")] <-
  stocks.panel.pdf$FLT[stocks.panel.pdf$symbol %in% 
                          c("ABV.N", "BAC.N")] - log(5*.9)

# 3c) check bivariate distributions to identify outlier/erroneous data ---------
stock.sel <- levels(stocks.panel.pdf$symbol)[round(runif(66)*length(levels(stocks.panel.pdf$symbol)))]
with(stocks.panel.pdf[stocks.panel.pdf$symbol %in% stock.sel,],{
  # scatterplotMatrix(cbind(HFL, HPQ, HPF, HPQL, HPFL))
  scatterplotMatrix(cbind(log(HFL+1), PR, SIZE, log(TOV), log(VOLE)))

#   png(paste(data.path.graph, "ScatMatrix_HFLlog_1.png", sep=""), 
#       height=1024, width=1440, res=144)
#   scatterplotMatrix(cbind(log1p(HFL), HPQ, HPF, HPQL, HPFL))
#   dev.off()
#   png(paste(data.path.graph, "ScatMatrix_HFLlog_2.png", sep=""), 
#       height=1024, width=1440, res=144)
#   scatterplotMatrix(cbind(log1p(HFL), VOLE, TOV, SIZE, PR))
#   dev.off()
#   png(paste(data.path.graph, "ScatMatrix_HFLlog_3.png", sep=""), 
#       height=1024, width=1440, res=144)
#   scatterplotMatrix(cbind(log1p(HFL), VOLA, BETA, LIQ, VOL))
#   dev.off()
#   png(paste(data.path.graph, "ScatMatrix_HFLlog_4.png", sep=""), 
#       height=1024, width=1440, res=144)
#   scatterplotMatrix(cbind(log1p(HFL), FLT, PRL, EEU, EED))
#   dev.off()
#   png(paste(data.path.graph, "ScatMatrix_HFLlog_5.png", sep=""), 
#       height=1024, width=1440, res=144)
#   scatterplotMatrix(cbind(log1p(HFL), log(IEM), log(IEMD+0.01), log(IEQ99), log(IET+0.00001)))
#   dev.off()
#   png(paste(data.path.graph, "ScatMatrix_HFLlog_6.png", sep=""), 
#       height=1024, width=1440, res=144)
#   scatterplotMatrix(cbind(log1p(HFL), IRM, IRMDA, IRQ99, IRT))
#   dev.off()
#   png(paste(data.path.graph, "ScatMatrix_HFLlog_7.png", sep=""), 
#       height=1024, width=1440, res=144)
#   scatterplotMatrix(cbind(log1p(HFL), IRK, IRS, IRMIN, IRMAX))
#   dev.off()
})

# check VIFs
with(stocks.panel.pdf[stocks.panel.pdf$symbol %in% stock.sel,],{
  Vif(cbind(log1p(HFL), VOLE, TOV, SIZE, log(PR), BETA), thresh=10, trace=T)
})
with(stocks.panel.pdf[stocks.panel.pdf$symbol %in% stock.sel,],{
  Vif(cbind(HFL, IEMD, IET, IRQ99, IRK, IRMDA, IRS), thresh=10, trace=T)
})

# 3d) Winsorization & Distribution Check ---------------------------------------
# --> RERUN 3c) bivariate distribution check to validate result

# Winsorization
(names(stocks.panel.pdf))
for (i in c(3:4, 11:ncol(stocks.panel.pdf))) {
  print(paste("Winsorizing var", i, "of", ncol(stocks.panel.pdf)))
  # limit variable cross-section per day
  for(d in seq_along(levels(stocks.panel.pdf$date))) {
    cur.d <- levels(stocks.panel.pdf$date)[d]
    sel <- stocks.panel.pdf$date == cur.d
    stocks.panel.pdf[sel, i] <- Winsor (stocks.panel.pdf[sel, i], 0.001)
  }
  # limit whole variable for all stock-days
  stocks.panel.pdf[, i] <- Winsor (stocks.panel.pdf[, i], 0.001)
}

# check distributions
for (i in c(3:4, 11:ncol(stocks.panel.pdf))) {
  print(names(stocks.panel.pdf)[i])
  print(summary(stocks.panel.pdf[, i]))
  png(file= paste(data.path.graph, "PanelVarDistCheck_", 
                  names(stocks.panel.pdf)[i], ".png", sep=""))
  hist(stocks.panel.pdf[, i], main=names(stocks.panel.pdf)[i])
  dev.off()
}

hist(stocks.panel.pdf$HFL)
hist(stocks.panel.pdf$HFLlog)
hist(stocks.panel.pdf$HPF)
hist(stocks.panel.pdf$HPFlog)
hist(log(stocks.panel.pdf$HPF+.1))
hist(stocks.panel.pdf$HPQ)
hist(stocks.panel.pdf$HPQlog)
hist(log(stocks.panel.pdf$HPQ+.1))
hist(stocks.panel.pdf$HPFL)
hist(stocks.panel.pdf$HPFLlog)
hist(log(stocks.panel.pdf$HPFL+.001))
hist(stocks.panel.pdf$HPQL)
hist(stocks.panel.pdf$HPQLlog)
hist(log(stocks.panel.pdf$HPQL+.001))
hist(sqrt(stocks.panel.pdf$EEU))
hist(sqrt(stocks.panel.pdf$EED))

stocks.panel.pdf$NAB <- as.double(stocks.panel.pdf$NAB) + runif(length(stocks.panel.pdf$NAB))*1e-12
stocks.panel.pdf$TREND <- as.integer(stocks.panel.pdf$date)
  
  
# WORKSPACE INCLUDES PANEL PREPS UNTIL HERE ------------------------------------
# 4) Build Panel Models --------------------------------------------------------
# 4a) Model Specification Check: check residual distributions ------------------
# 1st step: HFL

check.res <- felm(log((HFL+1))~NMS+(VOLE)+SIZE+(PR)+(TOV)+G(date)+G(symbol), 
                  stocks.panel.pdf, clustervar=interaction(stocks.panel.pdf$symbol, 
                                                           stocks.panel.pdf$date))
qqPlot(check.res$residuals, main="HFL")
HFL.hat <- check.res$fitted
summary(check.res)
# 1st step: HPF
check.res <- felm((log(HPF+0.1))~NMS+VOLE+SIZE+PR+(TOV)+G(date)+G(symbol), 
                  stocks.panel.pdf, clustervar=interaction(stocks.panel.pdf$symbol, 
                                                           stocks.panel.pdf$date))
qqPlot(check.res$residuals, main="log(HPF+.1) 1st-stage residuals")
HPF.hat <- check.res$fitted
# 1st step: HPQ
check.res <- felm((log(HPQ+0.01))~NMS+VOLE+SIZE+PR+(TOV)+G(date)+G(symbol), 
                  stocks.panel.pdf, clustervar=interaction(stocks.panel.pdf$symbol, 
                                                           stocks.panel.pdf$date))
qqPlot(check.res$residuals, main="HPQ")
HPQ.hat <- check.res$fitted
# 1st step: HPFL
check.res <- felm((log(HPFL+0.0001))~NMS+VOLE+SIZE+PR+(TOV)+G(date)+G(symbol), 
                  stocks.panel.pdf, clustervar=interaction(stocks.panel.pdf$symbol, 
                                                           stocks.panel.pdf$date))
qqPlot(check.res$residuals, main="HPFL")
HPFL.hat <- check.res$fitted
# 1st step: HPQL
check.res <- felm((log(HPQL+0.0001))~NMS+VOLE+SIZE+PR+(TOV)+G(date)+G(symbol), 
                  stocks.panel.pdf, clustervar=interaction(stocks.panel.pdf$symbol, 
                                                           stocks.panel.pdf$date))
qqPlot(check.res$residuals, main="HPQL")
HPQL.hat <- check.res$fitted

# 2nd step: HFL -- EEU, EED & EEU+EED
check.res <- felm((EEU+EED)~HFL.hat+(VOLE)+SIZE+(PR)+(TOV)+TREND+G(symbol), 
                  stocks.panel.pdf, clustervar=interaction(stocks.panel.pdf$symbol, 
                                                           stocks.panel.pdf$date))
summary(check.res)
qqPlot(check.res$residuals, main="EEU ~ HFL")

check.res <- felm(sqrt(EED)~HFL.hat+(VOLE)+SIZE+PR+(TOV)+G(date)+G(symbol), 
                  stocks.panel.pdf, clustervar=interaction(stocks.panel.pdf$symbol, 
                                                           stocks.panel.pdf$date))
qqPlot(check.res$residuals, main="EED ~ HFL")
summary(check.res)
check.res <- felm(sqrt(EEU+EED)~HFL.hat+(VOLE)+SIZE+PR+(TOV)+G(date)+G(symbol), 
                  stocks.panel.pdf, clustervar=interaction(stocks.panel.pdf$symbol, 
                                                           stocks.panel.pdf$date))
qqPlot(check.res$residuals, main="EEU+EED ~ HFL")
summary(check.res)

# 2nd step: HPQ -- EEU, EED & EEU+EED
check.res <- felm(sqrt(EEU)~HPQ.hat+(VOLE)+SIZE+PR+(TOV)+G(date)+G(symbol), 
                  stocks.panel.pdf, clustervar=interaction(stocks.panel.pdf$symbol, 
                                                           stocks.panel.pdf$date))
qqPlot(check.res$residuals, main="EEU ~ HPQ")
summary(check.res)
check.res <- felm(sqrt(EED)~HPQ.hat+(VOLE)+SIZE+PR+(TOV)+G(date)+G(symbol), 
                  stocks.panel.pdf, clustervar=interaction(stocks.panel.pdf$symbol, 
                                                           stocks.panel.pdf$date))
qqPlot(check.res$residuals, main="EED ~ HPQ")
summary(check.res)
check.res <- felm(sqrt(EEU+EED)~HPQ.hat+(VOLE)+SIZE+PR+(TOV)+G(date)+G(symbol), 
                  stocks.panel.pdf, clustervar=interaction(stocks.panel.pdf$symbol, 
                                                           stocks.panel.pdf$date))
qqPlot(check.res$residuals, main="EEU+EED ~ HPQ")
summary(check.res)

# ...using contemporaneous variables
# 1st step: HPFU
hist(sqrt(stocks.panel.pdf$HFTU15))
check.res <- felm(log(HPFU15+0.0001)~NMS+(VOLE)+SIZE+PR+(TOV)+G(EEUT)+G(date)+G(symbol), 
                  stocks.panel.pdf, clustervar=interaction(stocks.panel.pdf$symbol, 
                                                           stocks.panel.pdf$date))
summary(check.res)
qqPlot(check.res$residuals, main="HPFU15")
HPFU15.hat <- check.res$fitted
head(getfe(check.res),20)
# 1st step: HPFD
check.res <- felm(log(HPFD15+0.0001)~NMS+(VOLE)+SIZE+PR+(TOV)+G(EEDT)+G(date)+G(symbol), 
                  stocks.panel.pdf, clustervar=interaction(stocks.panel.pdf$symbol, 
                                                           stocks.panel.pdf$date))
summary(check.res)
qqPlot(check.res$residuals, main="HPFD15")
HPFD15.hat <- check.res$fitted

# 2nd step: EEU ~ HPFU^
check.res <- felm(sqrt(EEU)~HPFU15.hat+(VOLE)+SIZE+PR+(TOV)+G(EEUT)+G(date)+G(symbol), 
                  stocks.panel.pdf, clustervar=interaction(stocks.panel.pdf$symbol, 
                                                           stocks.panel.pdf$date))
summary(check.res)
# 2nd step: EED ~ HPFD^
check.res <- felm(sqrt(EED)~HPFD15.hat+(VOLE)+SIZE+PR+(TOV)+G(EEDT)+G(date)+G(symbol), 
                  stocks.panel.pdf, clustervar=interaction(stocks.panel.pdf$symbol, 
                                                           stocks.panel.pdf$date))
summary(check.res)

# 4b) Construct Ranking Factors ------------------------------------------------
# construct SIZE/LIQ/VOLA X-tile factors based on stock-averages in sample
# Factor: SIZE
rank.by <- tapply(stocks.panel.pdf$SIZE, stocks.panel.pdf$symbol, mean)
ranks <- rank(rank.by)/length(rank.by)
rank.cl <- cut(ranks, breaks=c(0, 1/3, 2/3, 1), 
               labels=c("L", "M", "H"), include.lowest=TRUE)
rank.cl <- as.matrix(rank.cl)
rownames(rank.cl) <- levels(stocks.panel.pdf$symbol)
classes.size3 <- factor(rank.cl[stocks.panel.pdf$symbol,], 
                   levels=c("H", "M", "L"), ordered=T)

rank.cl <- cut(ranks, breaks=c(0, 1/5, 2/5, 3/5, 4/5, 1), 
               labels=c("Q5", "Q4", "Q3", "Q2", "Q1"), include.lowest=TRUE)
rank.cl <- as.matrix(rank.cl)
rownames(rank.cl) <- levels(stocks.panel.pdf$symbol)
classes.size5 <- factor(rank.cl[stocks.panel.pdf$symbol,], 
                        levels=c("Q1", "Q2", "Q3", "Q4", "Q5"), ordered=T)

rank.cl <- cut(ranks, breaks=c(0, 1/4, 2/4, 3/4, 1), 
               labels=c("Q4", "Q3", "Q2", "Q1"), include.lowest=TRUE)
rank.cl <- as.matrix(rank.cl)
rownames(rank.cl) <- levels(stocks.panel.pdf$symbol)
classes.size4 <- factor(rank.cl[stocks.panel.pdf$symbol,], 
                        levels=c("Q1", "Q2", "Q3", "Q4"), ordered=T)

# Factor: LIQ
rank.by <- tapply(stocks.panel.pdf$LIQ, stocks.panel.pdf$symbol, mean)
ranks <- rank(rank.by)/length(rank.by)
rank.cl <- cut(ranks, breaks=c(0, 1/3, 2/3, 1), 
               labels=c("L", "M", "H"), include.lowest=TRUE)
rank.cl <- as.matrix(rank.cl)
rownames(rank.cl) <- levels(stocks.panel.pdf$symbol)
classes.liq3 <- factor(rank.cl[stocks.panel.pdf$symbol,], 
                        levels=c("H", "M", "L"), ordered=T)

rank.cl <- cut(ranks, breaks=c(0, 1/5, 2/5, 3/5, 4/5, 1), 
               labels=c("Q5", "Q4", "Q3", "Q2", "Q1"), include.lowest=TRUE)
rank.cl <- as.matrix(rank.cl)
rownames(rank.cl) <- levels(stocks.panel.pdf$symbol)
classes.liq5 <- factor(rank.cl[stocks.panel.pdf$symbol,], 
                        levels=c("Q1", "Q2", "Q3", "Q4", "Q5"), ordered=T)

# Factor: TOV
rank.by <- tapply(stocks.panel.pdf$TOV, stocks.panel.pdf$symbol, mean)
ranks <- rank(rank.by)/length(rank.by)
rank.cl <- cut(ranks, breaks=c(0, 1/3, 2/3, 1), 
               labels=c("L", "M", "H"), include.lowest=TRUE)
rank.cl <- as.matrix(rank.cl)
rownames(rank.cl) <- levels(stocks.panel.pdf$symbol)
classes.tov3 <- factor(rank.cl[stocks.panel.pdf$symbol,], 
                       levels=c("H", "M", "L"), ordered=T)

rank.cl <- cut(ranks, breaks=c(0, 1/5, 2/5, 3/5, 4/5, 1), 
               labels=c("Q5", "Q4", "Q3", "Q2", "Q1"), include.lowest=TRUE)
rank.cl <- as.matrix(rank.cl)
rownames(rank.cl) <- levels(stocks.panel.pdf$symbol)
classes.tov5 <- factor(rank.cl[stocks.panel.pdf$symbol,], 
                       levels=c("Q1", "Q2", "Q3", "Q4", "Q5"), ordered=T)

# Factor: VOLA
rank.by <- tapply(stocks.panel.pdf$VOLA, stocks.panel.pdf$symbol, mean)
ranks <- rank(rank.by)/length(rank.by)
rank.cl <- cut(ranks, breaks=c(0, 1/3, 2/3, 1), 
               labels=c("L", "M", "H"), include.lowest=TRUE)
rank.cl <- as.matrix(rank.cl)
rownames(rank.cl) <- levels(stocks.panel.pdf$symbol)
classes.vola3 <- factor(rank.cl[stocks.panel.pdf$symbol,], 
                       levels=c("H", "M", "L"), ordered=T)

rank.cl <- cut(ranks, breaks=c(0, 1/5, 2/5, 3/5, 4/5, 1), 
               labels=c("Q5", "Q4", "Q3", "Q2", "Q1"), include.lowest=TRUE)
rank.cl <- as.matrix(rank.cl)
rownames(rank.cl) <- levels(stocks.panel.pdf$symbol)
classes.vola5 <- factor(rank.cl[stocks.panel.pdf$symbol,], 
                       levels=c("Q1", "Q2", "Q3", "Q4", "Q5"), ordered=T)



# 4c) Check instrument ---------------------------------------------------------
Reg1stStage(stocks.panel.pdf, formula(HFL~NMS+G(date)+G(symbol)), classes.liq5, F, T)
Reg1stStage(stocks.panel.pdf, formula(log1p(HFL)~NMS+G(date)+G(symbol)), classes.size3, F, T)

Reg1stStage(stocks.panel.pdf, formula(HPF~NMS+G(date)+G(symbol)), panel.cl)
Reg1stStage(stocks.panel.pdf, formula(log(HPF+.1)~NMS+G(date)+G(symbol)), panel.cl)

Reg1stStage(stocks.panel.pdf, formula(HPQ~NMS+G(date)+G(symbol)), panel.cl)
Reg1stStage(stocks.panel.pdf, formula(log(HPQ+.1)~NMS+G(date)+G(symbol)), panel.cl)

Reg1stStage(stocks.panel.pdf, formula(HPFL~NMS+G(date)+G(symbol)), panel.cl)
Reg1stStage(stocks.panel.pdf, formula(log(HPFL+.01)~NMS+G(date)+G(symbol)), panel.cl)

Reg1stStage(stocks.panel.pdf, formula(HPQL~NMS+G(date)+G(symbol)), panel.cl)
Reg1stStage(stocks.panel.pdf, formula(log(HPQL+.01)~NMS+G(date)+G(symbol)), panel.cl)

Reg1stStage(stocks.panel.pdf, formula(HEN~NMS+G(date)+G(symbol)), panel.cl)
Reg1stStage(stocks.panel.pdf, formula(BETA~NMS+G(date)+G(symbol)), panel.cl, T)
Reg1stStage(stocks.panel.pdf, formula(VOLA~NMS+G(date)+G(symbol)), panel.cl, T)
Reg1stStage(stocks.panel.pdf, formula(VOLE~NMS+G(date)+G(symbol)), panel.cl)
Reg1stStage(stocks.panel.pdf, formula(TOV~NMS+G(date)+G(symbol)), panel.cl)
Reg1stStage(stocks.panel.pdf, formula(LIQ~NMS+G(date)+G(symbol)), panel.cl)
Reg1stStage(stocks.panel.pdf, formula(log(VOL)~NMS+G(date)+G(symbol)), panel.cl)
Reg1stStage(stocks.panel.pdf, formula(SIZE~NMS+G(date)+G(symbol)), panel.cl)
Reg1stStage(stocks.panel.pdf, formula(FLT~NMS+G(date)+G(symbol)), panel.cl)
Reg1stStage(stocks.panel.pdf, formula(PR~NMS+G(date)+G(symbol)), panel.cl)
Reg1stStage(stocks.panel.pdf, formula(PRL~NMS+G(date)+G(symbol)), panel.cl)
Reg1stStage(stocks.panel.pdf, formula((EEU)~NMS+(VOLE)+PR+SIZE+(TOV)+G(date)+G(symbol)), classes.size3, T, F)
Reg1stStage(stocks.panel.pdf, formula((EED)~NMS+(VOLE)+PR+SIZE+(TOV)+G(date)+G(symbol)), classes.size3, T, F)
Reg1stStage(stocks.panel.pdf, formula(sqrt(EEU+EED)~NMS+VOLE+PR+SIZE+TOV+G(date)+G(symbol)), classes.size5, T, F)


Reg1stStage(stocks.panel.pdf, formula((IEM)~NMS+G(date)+G(symbol)), panel.cl)
Reg1stStage(stocks.panel.pdf, formula((IEQ95)~NMS+G(date)+G(symbol)), panel.cl, T, F)
Reg1stStage(stocks.panel.pdf, formula((IEQ99)~NMS+G(date)+G(symbol)), panel.cl, T, T)
Reg1stStage(stocks.panel.pdf, formula((IET)~NMS+G(date)+G(symbol)), panel.cl, T, F)
Reg1stStage(stocks.panel.pdf, formula((IRM)~NMS+G(date)+G(symbol)), panel.cl, T, F)
Reg1stStage(stocks.panel.pdf, formula((IRQ95)~NMS+G(date)+G(symbol)), panel.cl, T, F)
Reg1stStage(stocks.panel.pdf, formula((IRQ99)~NMS+G(date)+G(symbol)), panel.cl, T, F)
Reg1stStage(stocks.panel.pdf, formula((IRT)~NMS+G(date)+G(symbol)), panel.cl, T, F)
Reg1stStage(stocks.panel.pdf, formula((IRK)~NMS+G(date)+G(symbol)), panel.cl, T, F)
Reg1stStage(stocks.panel.pdf, formula((IRMA)~NMS+G(date)+G(symbol)), panel.cl, T, F)
Reg1stStage(stocks.panel.pdf, formula((IRS)~NMS+G(date)+G(symbol)), panel.cl, T, F)
Reg1stStage(stocks.panel.pdf, formula((IRMIN)~NMS+G(date)+G(symbol)), panel.cl, T, F)
Reg1stStage(stocks.panel.pdf, formula((IRMAX)~NMS+G(date)+G(symbol)), panel.cl, T, F)

str(stocks.panel.pdf)

# 4d) Perform 2SLS IVREG -------------------------------------------------------

# "IEM"     "IEMD"    "IEQ95"  
# "IEQ99"   "IET"     "IRM"     "IRQ95"   "IRQ99"   "IRT"     "IRK"    
# "IRMA"    "IRMDA"   "IRS"     "IRMIN"   "IRMAX" 
# 
# WIF-CHECKED: IEMD, IET, IRQ99, IRK, IRMDA, IRS

# HFL IVREG
hist(log(log(stocks.panel.pdf$IRK)))
HFL.res2 <- felm (log(IET)~HFLlog+VOLE+PR+SIZE+(TOV)+G(date)+G(symbol), 
                  stocks.panel.pdf, iv=HFLlog~NMS, 
                  clustervar=interaction(stocks.panel.pdf$date,
                                         stocks.panel.pdf$symbol))
summary(HFL.res2)
qqPlot(HFL.res2$residuals)

# HPF IVREG
HPF.res2 <- felm (IEQ99~HPF+PR+BETA+SIZE+TOV+G(date)+G(symbol), 
                  stocks.panel.pdf, iv=HPF~NMS, 
                  clustervar=interaction(stocks.panel.pdf$date,
                                         stocks.panel.pdf$symbol))
summary(HPF.res2)
# HPQ IVREG
hist(stocks.panel.pdf$IRMA)
hist(log(stocks.panel.pdf$IRMA+0.001))
HPQ.res2 <- felm (EEU~HPQ+VOLE+PR+SIZE+TOV+G(date)+G(symbol), 
                  stocks.panel.pdf, iv=HPQ~NMS, 
                  clustervar=interaction(stocks.panel.pdf$date,
                                         stocks.panel.pdf$symbol))
summary(HPQ.res2)

# HPQL IVREG
HPQL.res2 <- felm (log(IRT+0.00001)~log(IRMA+0.001)+HPQL+VOLE+PR+SIZE+TOV+G(date)+G(symbol), 
                  stocks.panel.pdf, iv=HPQL~NMS, 
                  clustervar=interaction(stocks.panel.pdf$date,
                                         stocks.panel.pdf$symbol))
summary(HPQL.res2)


# DEFINE SUBSET
subset.vec <- panel.cl =="H"
stocks.panel.sub <- stocks.panel.pdf[subset.vec,]
# HFL IVREG
HFL.res2 <- felm (sqrt(IET)~HPQLlog+VOLE+PR+SIZE+TOV+G(date)+G(symbol), 
                  stocks.panel.sub, iv=HPQLlog~NMS, 
                  clustervar=interaction(stocks.panel.sub$date,
                                         stocks.panel.sub$symbol))
summary(HFL.res2)
qqPlot(HFL.res2$residuals)
# HPF IVREG
HPF.res2 <- felm ((EED+EEU)~HPF+VOLE+PR+SIZE+TOV+G(date)+G(symbol), 
                  stocks.panel.sub, iv=HPF~NMS, 
                  clustervar=interaction(stocks.panel.sub$date,
                                         stocks.panel.sub$symbol))
summary(HPF.res2)
# HPQ IVREG
HPQ.res2 <- felm ((EED+EEU)~HPQ+VOLE+PR+SIZE+TOV+G(date)+G(symbol), 
                  stocks.panel.sub, iv=HPQ~NMS, 
                  clustervar=interaction(stocks.panel.sub$date,
                                         stocks.panel.sub$symbol))
summary(HPQ.res2)
# MANUAL 2SLS
# HFL 1st stage fit
reg.form <- formula (HFL~NMS+VOLE+PR+SIZE+TOV+G(date)+G(symbol))
HFL.res  <- felm (reg.form, stocks.panel.sub, 
                  clustervar=interaction(stocks.panel.sub$symbol, 
                                         stocks.panel.sub$date))
summary(HFL.res)
HFL.hat <- HFL.res$fitted
# HPF 1st stage fit
reg.form <- formula (HPF~NMS+VOLE+PR+SIZE+TOV+G(date)+G(symbol))
HPF.res  <- felm (reg.form, stocks.panel.sub, 
                  clustervar=interaction(stocks.panel.sub$symbol, 
                                         stocks.panel.sub$date))
summary(HPF.res)
HPF.hat <- HPF.res$fitted
# manual 2nd stage regression
HFL.res2a <- felm (EEU+EED~HFL.hat+VOLE+PR+SIZE+TOV+G(date)+G(symbol), 
                   stocks.panel.sub,
                   clustervar=interaction(stocks.panel.sub$symbol, 
                                          stocks.panel.sub$date))
summary(HFL.res2a)
HPF.res2a <- felm (EEU+EED~HPF.hat+VOLE+PR+SIZE+TOV+G(date)+G(symbol), 
                   stocks.panel.sub,
                   clustervar=interaction(stocks.panel.sub$symbol, 
                                          stocks.panel.sub$date))
summary(HPF.res2a)





# ************************************* OLD ************************************
test.ols.p <- biglm (HPQ~NMS, stocks.panel.pdf)
summary(test.ols.p)

test.fe2.p <- biglm (HPQ~NMS+date+symbol, stocks.panel.pdf)
x.p <- summary(test.fe2.p)
head(round(x.p$mat,4))

test.fe <- plm (HFL~NMS, stocks.panel.pdf, model="within", effect="time", )
summary(test.fe)
coeftest (test.fe, vcov=vcovHC(test.fe, cluster="time"))

test.po <- plm (HFL~NMS, data=stocks.panel.pdf, model="pooling")
summary(test.po)
# Breusch-Pagan-Heteroskedasticity test: Null = no random effects needed
# p < .05: effect is significant, else no need to use it
plmtest(test.po, "individual", type="bp")
plmtest(test.po, "time", type="bp")
plmtest(test.po, "twoways", type="bp")
plmtest(test.po, "twoways", type="kw")

test.fe2 <- plm (HFL~NMS, stocks.panel.pdf, model="within", effect="twoways")
summary(test.fe2)



# LIQ
row.names(stocks.liqs.df) <- row.names(stocks.score.min)
colnames(stocks.liqs.df) <- colnames(stocks.score.min)
stocks.LIQ.pdf <- melt (as.matrix(stocks.liqs.df[252:1498,]))
stocks.LIQ.pdf <- pdata.frame (stocks.LIQ.pdf, index=c("X2", "X1"))
names(stocks.LIQ.pdf) <- c("date", "symbol", "LIQ")

# VOL
row.names(stocks.volas.df) <- row.names(stocks.score.min)
colnames(stocks.volas.df) <- colnames(stocks.score.min)
stocks.VOL.pdf <- melt (as.matrix(stocks.volas.df[252:1498,]))
stocks.VOL.pdf <- pdata.frame (stocks.VOL.pdf, index=c("X2", "X1"))
names(stocks.VOL.pdf) <- c("date", "symbol", "VOL")


# get EE stock characteristic
stocks.EEC.pdf <- melt (as.matrix(stocks.score.min2))
stocks.EEC.pdf$X1 <- as.Date(stocks.EEC.pdf$X1)
stocks.EEC.pdf$X2 <- as.character(stocks.EEC.pdf$X2)
stocks.EEC.pdf <- pdata.frame (stocks.EEC.pdf, index=c("X2", "X1"))
names(stocks.EEC.pdf) <- c("date", "symbol", "EEC")

stocks.EECd.pdf <- melt (as.matrix(stocks.score.mindiff))
stocks.EECd.pdf$X1 <- as.Date(stocks.EECd.pdf$X1)
stocks.EECd.pdf$X2 <- as.character(stocks.EECd.pdf$X2)
stocks.EECd.pdf <- pdata.frame (stocks.EECd.pdf, index=c("X2", "X1"))
names(stocks.EECd.pdf) <- c("date", "symbol", "EECd")


# build combined panel data frame
stocks.panel.pdf <- stocks.EEC.pdf
stocks.panel.pdf$HFT <- stocks.HFT.pdf$HFT
stocks.panel.pdf$EECd <- stocks.EECd.pdf$EECd

# obtain non-factorized indices
orig.symb <- as.character(stocks.panel.pdf$symbol)
orig.date <- as.character(stocks.panel.pdf$date)
orig.sd <- paste(orig.symb, orig.date)
new.symb <- as.character(stocks.LIQ.pdf$symbol)
new.date <- as.character(stocks.LIQ.pdf$date)
new.sd <- paste(new.symb, new.date)

# get matched variable values
vec <- vector (mode="numeric", length=nrow(stocks.panel.pdf))
vec[which(orig.sd %in% new.sd)] <- stocks.LIQ.pdf$LIQ[which(orig.sd %in% new.sd)]
vec[vec==0] <- NA
stocks.panel.pdf$LIQ <- vec

vec <- vector (mode="numeric", length=nrow(stocks.panel.pdf))
vec[which(orig.sd %in% new.sd)] <- stocks.VOL.pdf$VOL[which(orig.sd %in% new.sd)]
vec[vec==0] <- NA
stocks.panel.pdf$VOL <- vec

# 2) SIMPLE OLS CHECKS
# inspect simple OLS model
ols <- lm (EEC~HFT, stocks.panel.pdf)
summary(ols)

ols.d <- lm (EECd~HFT, stocks.panel.pdf)
summary(ols.d)
# 3) PANEL REGRESSION CHECKS
# pooled model --> equal to simple OLS
test.po <- plm (EEC~HFT, data=stocks.panel.pdf, model="pooling")
summary(test.po)
# Breusch-Pagan-Heteroskedasticity test: Null = no random effects needed
# p < .05: effect is significant, else no need to use it
plmtest(test.po, "individual", type="bp")
plmtest(test.po, "time", type="bp")
plmtest(test.po, "twoways", type="bp")
plmtest(test.po, "twoways", type="kw")
# ---> CHECK RANDOM EFFECTS

# test poolability: 
# tests the hypothesis that the same coefficients apply to each individual
# p <- .05: significant effects
pooltest (EEC~HFT, data=stocks.panel.pdf, model="within")

# 3a) FIXED EFFECTS
# individual FE
test.fe <- plm (EEC~HFT,data=stocks.panel.pdf, model="within")
summary (test.fe) # p-value indicates whether all coefficients are non-zero
# <.05 means model is ok
# coefficient test with hetero-skedasticity-consistent errors
coeftest (test.fe, vcov=vcovHC(test.fe, cluster="group"))

# inspect and test fixed effects
summary(fixef(test.fe))   # display coefficients of fixed effects
pFtest(test.fe, test.po)  # F-test for fixed effects vs. simple OLS
# If the p-value is < 0.05 then the 
# fixed effects model is a better choice

# time FE
test.fe.t <- plm (EEC~HFT+log(LIQ)+VOL+lag(EEC),data=stocks.panel.pdf, 
                  model="within", effect="time")
summary (test.fe.t) # p-value indicates whether all coefficients are non-zero
# <.05 means model is ok
coeftest (test.fe.t, vcov=vcovHC(test.fe.t, cluster="time"))
summary(fixef(test.fe.t))  # display coefficients of fixed effects
pFtest(test.fe.t, test.po) # F-test for fixed effects vs. simple OLS
# <.05: fixed effects model is better choice

resid.pdf <- stocks.panel.pdf[complete.cases(stocks.panel.pdf),]
resid.pdf$resid <- test.fe.t$residuals

scatterplotMatrix(resid.pdf)



# individual+time FE
test.fe2 <- plm (EEC~HFT,data=stocks.panel.pdf, model="within", effect="twoways")
summary (test.fe2)
coeftest (test.fe2, vcov.=vcovHC(test.fe2))
# emulate additional time-specific effect by adding Qtr-dummy
test.fe2b <- plm (EEC~HFT+factor(Qtr),data=panel.df, model="within")
summary (test.fe2b)
# test addition of time effect, given individual effects
pFtest(test.fe2, test.fe)
plmtest(test.fe, "time", type="bp")
# test joint addition of indiv+time effects
pFtest(test.fe2, test.po)

# ADDITIONAL TESTS ***
# serial correlation tests
pwfdtest (EEC~HFT, data=stocks.panel.pdf, h0="fe")
pwfdtest (EEC~HFT, data=stocks.panel.pdf, h0="fd")
pwartest (test.fe, data=stocks.panel.pdf)
pwartest (test.fe.t, data=stocks.panel.pdf)
pwartest (test.fe2, data=panel.df)

# Testing for cross-sectional dependence/contemporaneous correlation: 
# using Breusch-Pagan LM test of independence and Pasaran CD test
# Null is "no dependence", p<.05: dependence
pcdtest(test.fe, test = "lm")
pcdtest(test.fe.t, test = "lm")
pcdtest(test.fe2, test = "lm")

# testing for serial correlation (Breusch-Godfrey/Wooldridge test)
# in residuals
# Null is "no correlation", p<.05: correlation
pbgtest(test.fe)
pbgtest(test.fe.t)
pbgtest(test.fe2)

# Dickey-Fuller test to check null hypothesis is that the series has a unit root
# (i.e. non-stationary), if unit root is present you can take the first 
# difference of the variable. 
adf.test(panel.df$EEC, k=2)
# end: ADDITIONAL TESTS ***

# 3b) FIXED EFFECTS + LAGS
# individual FE with lagged variable
test.fe.l <- plm (EEC~HFT+lag(EEC, k=1),data=panel.df, model="within")
summary (test.fe.l)
coeftest (test.fe.l, vcov=vcovHC(test.fe.l, cluster="group"))

# individual FE with lagged variables up to 2nd order
test.fe.l2 <- plm (EEC~HFT+lag(EEC, k=1)+lag(EEC, k=2),data=panel.df, model="within")
summary (test.fe.l2)
coeftest (test.fe.l2, vcov.=vcovHC(test.fe.l2, cluster="group"))

# FD first differences model
test.fd <- plm(EEC~HFT,data=stocks.panel.pdf, model="fd")
summary(test.fd)
coeftest(test.fd, vcov=vcovHC(test.fd))

# 3c) RANDOM EFFECTS
# individual RE
test.re <- plm(EEC~HFT,data=panel.df, model="random", random.method="amemiya")
summary(test.re)
coeftest(test.re, vcov.=vcovHC(test.re))
# Hausman test for model consistency fixed/random
phtest(test.fe, test.re) # < p.05 for H0=RE means FE is preferred

# time RE
test.re.t <- plm(EEC~HFT,data=stocks.panel.pdf, model="random", effect="time")
summary(test.re.t)
coeftest(test.re.t, vcov=vcovHC(test.re.t, cluster="time"))
# Hausman test for model consistency fixed/random
phtest(test.fe.t, test.re.t) # < p.05 for H0=RE means FE is preferred

# time RE + individual FE
test.re.t2 <- plm(EEC~HFT+factor(Liq),data=stocks.panel.pdf, model="random", effect="time")
summary(test.re.t2)
coeftest(test.re.t2, vcov=vcovHC(test.re.t2, cluster="time"))

# individual + time RE
test.re2 <- plm(EEC~HFT,data=stocks.panel.pdf, model="random", effect="twoways", 
                random.method="amemiya")
summary(test.re)
coeftest(test.re, vcov=vcovHC(test.re))

# I.8) Overreactions: Rolling variables of interest over time ------------------
# - build dataframes
events.daily.count.df <- as.data.frame (
  as.matrix(by(test.events.dn.df$TUP.C2.rel.vol, 
               as.Date(test.events.dn.df$timestamp), 
               FUN=length)))
events.daily.count.df.up <- as.data.frame (
  as.matrix(by(test.events.up.df$TUP.C2.rel.vol, 
               as.Date(test.events.up.df$timestamp), 
               FUN=length)))

events.daily.intensity.df <- as.data.frame (
  as.matrix(by(test.events.dn.df$r_30, 
               as.Date(test.events.dn.df$timestamp), 
               FUN=function(x) mean(x, trim=.02))))
# - build xts
events.daily.count.x <- merge (xts(events.daily.count.df, 
  as.Date(rownames(events.daily.count.df))), 
    index(active.stocks.sum.x), all=TRUE, fill=0)
events.daily.count.x.up <- merge (xts(events.daily.count.df.up, 
  as.Date(rownames(events.daily.count.df.up))), 
  index(active.stocks.sum.x), all=TRUE, fill=0)
events.daily.intensity.x <- merge (xts(events.daily.intensity.df$V1, 
  as.Date(rownames(events.daily.intensity.df))), 
    index(active.stocks.sum.x), all=TRUE, fill=0)
# - plot OR activity: number of events
# dn
plot(events.daily.count.x, ylim=c(0,13),      
     main="% of stocks with downside-EEs per day - non-overlapping",
     ylab="% of stocks")
lines(xts(order.by=index(events.daily.count.x), x=MovingAvg (
  (events.daily.count.x), period.oneyear)), 
      lwd=2, col="blue")
legend("topleft", "(x,y)", c("%Events/day/#total stocks", 
                             "1-year rolling avg."), lty=rep(1,2),
       lwd=rep(2,2),col=c("black","blue"))
# up
plot(100*events.daily.count.x.up, ylim=c(0,1), 
     main="% of stocks with upside EEs per day - non-overlapping",
     ylab="% of stocks")
lines(xts(order.by=index(events.daily.count.x), x=MovingAvg (
  100*(events.daily.count.x.up), period.oneyear)), 
      lwd=2, col="blue")
legend("topleft", "(x,y)", c("%Events/day/#total stocks", 
                             "1-year rolling avg."), lty=rep(1,2),
       lwd=rep(2,2),col=c("black","blue"))

# - plot OR activity: avg. event intensity
# events.daily.intensity.x[events.daily.intensity.x>0] <- 0
plot(events.daily.intensity.x, 
     main="Avg. relative intraday overreaction intensity per day",
     ylab="Avg. relative overreaction size", ylim=c(0,10))
lines(xts(order.by=index(events.daily.intensity.x), x=MovingAvg (
  (events.daily.intensity.x), period.oneyear)), 
      lwd=2, col="blue")
legend("topleft", "(x,y)", c("#Events/day", "1-year rolling avg."), lty=rep(1,2),
       lwd=rep(2,2),col=c("black","blue"))

# - regression models: Events as Function of HFT
test.df <- cbind((events.daily.count.x), log(hft.flags.sum.x))
test.df[!is.finite(test.df)] <- 0
names(test.df) <- c("Events", "HFT")
summary(rlm(Events ~ HFT, test.df))
scatterplot(as.vector(test.df$HFT), as.vector(test.df$Events),
            main="No. of Overreaction Events vs. HFT activity",
            xlab="log. HFT flags/day", ylab="Number of Overreaction Events",
            ylim=c(0,20), xlim=c(2,13))

# I.9) #EE by SCC --------------------------------------------------------------
# get SCC ranking variable
rank.by <- stocks.liqs.df
ranks.cl <- GetRankClasses (rank.by)
ranks.cl2 <- GetRankXtiles (rank.by, c(0, .2, .4, .6, .8, 1), 
                            c("Q1", "Q2", "Q3", "Q4", "Q5"))


# save SCC vars
# ranks.cl.liq <- ranks.cl
# ranks.cl2.liq <- ranks.cl2
# ranks.cl.beta <- ranks.cl
# ranks.cl2.beta <- ranks.cl2
# ranks.cl.size <- ranks.cl
# ranks.cl2.size <- ranks.cl2

# load SCC vars
ranks.cl <- ranks.cl.liq
ranks.cl2 <- ranks.cl2.liq

caption <- "liquidity"

# calculate subsetting factor for unfiltered cases
# get initial final df with liq/proce filtering
param.min.liq    <- daybars * 0
BuildFinalFiltDf(filter.method="slot")

# up
test.df <- events.master.dn.df.final
ranks.f.uf.up <- factor(sapply(1:nrow(test.df), FUN=function(x) 
  ranks.cl[as.character(as.Date(test.df$timestamp[x])), 
           test.df$symbol[x]]), levels=c("L", "M", "H"), ordered=T)
ranks.f2.uf.up <- factor(sapply(1:nrow(test.df), FUN=function(x) 
  ranks.cl2[as.character(as.Date(test.df$timestamp[x])), test.df$symbol[x]]), 
                         levels=c("Q1", "Q2", "Q3", "Q4", "Q5"), ordered=T)
# ranks.f2.uf.up <- rep(NA, nrow(test.df))
# for (i in 1:nrow(test.df)) 
#   ranks.f2.uf.up[i] <- ranks.cl2[as.character(as.Date(test.df$timestamp[i])), test.df$symbol[i]]

# dn
test.df <- events.master.dn.df.final
ranks.f.uf.dn <- factor(sapply(1:nrow(test.df), FUN=function(x) 
  ranks.cl[as.character(as.Date(test.df$timestamp[x])), 
           test.df$symbol[x]]), levels=c("L", "M", "H"), ordered=T)  
ranks.f2.uf.dn <- factor(sapply(1:nrow(test.df), FUN=function(x) 
  ranks.cl2[as.character(as.Date(test.df$timestamp[x])), test.df$symbol[x]]), 
                         levels=c("Q1", "Q2", "Q3", "Q4", "Q5"), ordered=T)

cols <- c("blue", "darkgreen", "red")
cols <- rev(rainbow(5, start=rainbow.start, end=rainbow.end))
fac <- ranks.f2.uf.dn
sel.df <- events.master.dn.df.final
slotsize <- 30
nrow(sel.df)
length(fac)
levels(fac)
# generate plot across subsets
for (L in 1:length(levels(fac))) {
  # get subset from data.frame.final and apply filter
  test.df <- subset (sel.df, subset=(fac==levels(fac)[L]))
  test.df <- FilterOverlappingEvents2(test.df, slotsize)
  
#   events.daily.count.df <- as.data.frame (
#     as.matrix(by(test.df$symbol, as.Date(test.df$timestamp), FUN=length)))
  events.daily.count.df <- as.data.frame (
    as.matrix(by((test.df$ERP.reversal)/test.df$PREP.vola.hlr, 
                 as.Date(test.df$timestamp), 
                 FUN=function(x) mean(x, trim=.02, na.rm=T))))
  
  events.daily.count.x <- merge (xts(events.daily.count.df, 
                                     as.Date(rownames(events.daily.count.df))), 
                                 index(active.stocks.sum.x), all=TRUE, fill=0)  
  events.daily.count.x[!is.finite(events.daily.count.x)] <- 0
  events.daily.count.x[is.nan(events.daily.count.x)] <- 0
  
  if (L == 1) {
    # define plot
#     plot(events.daily.count.x['2006-01-03/'], ylim=c(0,13), type="l", 
#          minor.ticks=F, col=cols[1],
#          main="# of half-hour windows/day with EE, by segment",
#          ylab="# of EE/day(filtered)")
#     lines (xts(order.by=index(events.daily.count.x['2006-01-03/']), 
#            x=MovingAvg(events.daily.count.x['2006-01-03/'], period.oneyear)),
#            col=cols[1],lwd=3)  
    plot(xts(order.by=index(events.daily.count.x['2006-01-03/']), 
           x=MovingAvg2(events.daily.count.x['2006-01-03/'], period.oneyear)), 
         type="l", minor.ticks=F, col=cols[L], 
         # ylim=c(0,2), 
         main="# of half-hour windows/day with EE, by segment",
         ylab="# of EE/day(filtered)")
    
    } else {
    # add lines for other values
#     lines(events.daily.count.x['2006-01-03/'], lwd=1, col=cols[L])
    lines(xts(order.by=index(events.daily.count.x['2006-01-03/']), 
              x=MovingAvg2(events.daily.count.x['2006-01-03/'], period.oneyear)),
          lwd=1, col=cols[L])
  }
}
legend("topright", legend=levels(fac), lwd=rep(2, length(levels(fac))), 
       lty=rep(1,length(levels(fac))), col=cols)


# get counts for lowest quantile
test.df <- subset (sel.df, subset=(fac==levels(fac)[1]))
test.df <- FilterOverlappingEvents2(test.df, slotsize)
events.daily.count.df <- as.data.frame (
  as.matrix(by(test.df$symbol, as.Date(test.df$timestamp), FUN=length)))
events.daily.count.x1 <- merge (xts(events.daily.count.df, 
                                   as.Date(rownames(events.daily.count.df))), 
                               index(active.stocks.sum.x), all=TRUE, fill=0)  
# get counts for highest quantile
test.df <- subset (sel.df, subset=(fac==levels(fac)[length(levels(fac))]))
test.df <- FilterOverlappingEvents2(test.df, slotsize)
events.daily.count.df <- as.data.frame (
  as.matrix(by(test.df$symbol, as.Date(test.df$timestamp), FUN=length)))
events.daily.count.xn <- merge (xts(events.daily.count.df, 
                                    as.Date(rownames(events.daily.count.df))), 
                                index(active.stocks.sum.x), all=TRUE, fill=0)  
# plot H-L
plot(xts(order.by=index(events.daily.count.xn['2006-01-03/']), 
    x=MovingAvg2(events.daily.count.xn['2006-01-03/'], period.oneyear/2)) -
  xts(order.by=index(events.daily.count.x1['2006-01-03/']), 
    x=MovingAvg2(events.daily.count.x1['2006-01-03/'], period.oneyear/2)),
     type="l", minor.ticks=F, col="darkblue",
     main="Highest vs. Lowest Quantile Event Frequency, 1-year MA")
lines(xts(order.by=index(events.daily.count.xn['2006-01-03/']), 
          x=-0.8+(HFT.EN.ts.1stever[,5] - HFT.EN.ts.1stever[,1])/200))

# OVERALL MARKET VOL CAN SKEW THIS BIG TIME, AS WELL AS RISING LIQ OVER TIME
# --> EITHER ADJUST FOR TIME-SPEC EFFECTS OR FIND A MORE STABLE MEASURE

# vars for plot function
entry.df <- stocks.HFTactive.df.1stever
rank.by <- as.matrix (stocks.liqs.df)
# generate plot
HFT.EN.ts.1stever <- GenerateEntryTimeSeriess (entry.df, rank.by, 
                                               c(0, 0.2, 0.4, 0.6, 0.8, 1), 
                                               c("Q1", "Q2", "Q3", "Q4", "Q5"))
HFT.EN.ts.90qrt <- GenerateEntryTimeSeriess (entry.df, rank.by, 
                                               c(0, 0.2, 0.4, 0.6, 0.8, 1), 
                                               c("Q1", "Q2", "Q3", "Q4", "Q5"))


# DIFFERENCE-IN-DIFFERENCES CHECKS ---------------------------------------------

# 1) RegNMS

# Info: Pilot-Stocks "07/09/2007" - Full roll-out "08/20/2007"

which(!(symbol.info$Symbol %in% names(stocks.betas.df)))



# 2) SSB

# 3) HFT-Entry (Endogeneous)



# II. UNIVARIATE ANALYSIS ------------------------------------------------------

# get variable info
var.info <- read.csv("names.csv", stringsAsFactors=FALSE)
head(var.info)
# II.1) Univariate Descriptive Statistics Summary & Plots-----------------------

# load & select data frame for univariate analysis
univ.test.df <- test.events.dn.df
prefix <- "2.0_6.0_dn_"
vars <- c(4:14, seq(15,15+120,15)[-5], seq(196,196+120,15)[-5], 
          seq(377,377+120,15)[-5], seq(558,558+120,15)[-5], 
          seq(739,739+120,15)[-5], seq(920,920+120,15)[-5],
          1101:1281)

for (v in vars) {
  # get variable
  x <- univ.test.df[,v]
  var.name <- colnames(univ.test.df)[v]
  var.desc <- var.info[var.info$var.name==var.name,]$desc
  print (paste("processing v =", v, ", var.name =", var.name))
  # summary statistics on raw variable
  univar.summary <- Summarize(x)
  
  # apply Box-Cox power transformation if variable distribution normality is
  # is rejected (p.shapiro-wilkens <= 0.1)
  lam <- 1
  if (!is.nan(univar.summary["sh.wi.p"]) & univar.summary["sh.wi.p"] <= 0.1)
    try (lam <- boxcoxnc (ShiftPos(Std(na.remove(x))), method="sw", 
                     plotit=F, lam=c(-50:-11, seq(-10,-3.1, 0.1), 
                                     seq(-3, 3, 0.01),
                                     seq(3.1, 10, 0.1),
                                     11:20))$result[1])
  # add lambda, shiftpos, and shapiro-wilkens test p on transformed variable
  univar.summary <- AppendNamedVector (univar.summary, lam, "bc.lambda")
  univar.summary <- AppendNamedVector (univar.summary, 
    shapiro.test(BoxCox(Std(na.remove(x)), lam, T))$p.value, "bc.sh.wi.p")

  if (v==4) {
    univar.df <- as.data.frame(cbind(ID=v,t(univar.summary)))
  } else {
    univar.df <- rbind(univar.df, (cbind(ID=v,t(univar.summary))))
  }
  
  # Create 2x2 plot for original and transformed variable
  png (paste(data.path.graph, prefix, v, "_", var.name, ".png", sep=""),
       width = 6, height = 6, units = 'in', res = 300) # file
  par (mfrow = c( 2, 2 ), oma = c( 1, 0, 2, 0 ))
  
  # raw variable histogram & qq-plot
  hist.FD (Trim(na.remove(x), .01), col="darkblue", prob=T, 
           main="Histogram - raw", xlab=var.name)
  abline (v=univar.summary["mean"], lwd=2, col="blue")
  abline (v=univar.summary["median"], lwd=2, col="green")
  legend ("topright", legend=c("mean", "median"), col=c("blue", "green"), 
          bty="n", lty=c(1,1), lwd=c(2,2))
  qqPlot (na.remove(x), main="QQ-Plot - raw", ylab=var.name)
  # Box-Cox-transformed variable histogram & qq-plot
  hist.FD (Trim(BoxCox(Std(na.remove(x)), lam, T), .01), col="darkblue", prob=T, 
           main="Histogram - Box-Cox", xlab=paste(var.name, ".bc", sep=""))
  qqPlot (BoxCox(Std(na.remove(x)), lam, T), main="QQ-Plot - Box-Cox", 
          ylab=paste(var.name, ".bc", sep=""))
  
  # create overall title.
  title (main=paste(var.name, var.desc, sep=" - "), outer = TRUE)
  mtext (paste("Box-Cox lambda =", lam), 1, outer=T)
  dev.off() # file close
}
write.csv(x=univar.df, file=paste(prefix, "univariate.csv", sep=""))


# II.1.b) Variable Corrections & Adjustments -----------------------------------
# check outrageously large market cap values
events.master.dn.df$symbol[which(events.master.dn.df$market.cap > 250000)]



# II.2) Variable Transformations -----------------------------------------------
# apply lambda factors and create bc.transformed variables
# >> to be done on the fly using saved lambda parameters for each data.frame <<




# III. BIVARIATE ANALYSIS ------------------------------------------------------

# params
vars <- c("ECP.rel.r", "ECP.HFT.rel.f.rate")
cvars <- c("ECP.rel.vol", "ECP.rel.vola")
vars.tr <-c("neglog", "logfill", "log", "log")
trim <- .03

# ---
Test.df <- data.frame(cbind(test.events.dn.df[vars],
                            test.events.dn.df[cvars]))
colnames(Test.df) <- c("y","x", paste("z", 1:length(cvars), sep=""))



# variable transformations
# x
Test.df$x <- na.fill(Test.df$x, 0)
Test.df$x <- log(Test.df$x)
Test.df$x[!is.finite(Test.df$x)] <- min(Test.df$x[is.finite(Test.df$x)])
vars[2] <- paste(vars[2], ", log-transformed, NAs=0", sep="")
# y
Test.df$y <- log(-Test.df$y)
Test.df$y[!is.finite(Test.df$y)] <- min(Test.df$y[is.finite(Test.df$y)])
vars[1] <- paste(vars[1], ", log-transformed, NAs=0", sep="")
# z1
Test.df$z1 <- log(Test.df$z1)
Test.df$z1[!is.finite(Test.df$z1)] <- min(Test.df$z1[is.finite(Test.df$z1)])
cvars[1] <- paste(cvars[1], ", log-transformed, NAs=0", sep="")
# z2
Test.df$z2 <- log(Test.df$z2)
Test.df$z2[!is.finite(Test.df$z2)] <- min(Test.df$z2[is.finite(Test.df$z2)])
cvars[1] <- paste(cvars[1], ", log-transformed, NAs=0", sep="")

scatterplotMatrix(Test.df)

scatterplot(y=Test.df$y, x=Test.df$x, 
            xlim=c(quantile(Test.df$x, trim, na.rm=T), 
                   quantile(Test.df$x, 1-trim, na.rm=T)), 
            ylim=c(quantile(Test.df$y, trim, na.rm=T),
                   quantile(Test.df$y, 1-trim, na.rm=T)),
            xlab=vars[2], ylab=vars[1])

# GAM Plot
# fit <- gam(data = Test.df, formula = y~s(x))
# summary(fit)
px <- seq(quantile(Test.df$x, trim, na.rm=T), 
          quantile(Test.df$x, 1-trim, na.rm=T), 
          length = 100)
# py <- predict(fit, data.frame(x = px))
# lines(px, py, col = "red", lwd = 2)
# LM fit
lmfit <- lm(data = Test.df, formula = y~x)
summary(lmfit)
py2 <- predict(lmfit, data.frame(x = px))
lines(px, py2, col = "green", lwd = 2)
# RLM fit
rlmfit <- rlm(y~x, data=Test.df)
summary(rlmfit)
py3 <- predict(rlmfit, data.frame(x = px))
lines(px, py3, col = "blue", lwd = 2)
coef(summary(lmfit))
coef(summary(rlmfit))
# rbind(summary(fit)$p.table, summary(fit)$s.table)
# lines(lowess(Test.df$x, Test.df$y), col="orange", lwd = 2)
legend("topleft", "(x.y)", 
       c("LM", "RLM", "GAM"), lty=rep(1,3),
       lwd=rep(2,3),col=c("green","blue", "red"))
# plot residuals to check for heteroscedasticity
plot(resid(lmfit))
plot(fitted(lmfit), abs(resid(lmfit)))
summary(lm(abs(resid(lmfit)) ~ fitted(lmfit)))
# qq plot
qqnorm(resid(lmfit), ylab="Residuals")
qqline(resid(lmfit))
# residual density plot with stripchart for individual points
plot(density(resid(lmfit)))
rug(resid(lmfit))
# histogram with kernel density estimate
hist(lmfit$residuals, prob=T, col="red")
lines(density(lmfit$residuals), col = "blue")
# Tests for non-normality
shapiro.test(resid(lmfit))     # p-value = p(normality) > .1 is still normal
jarque.bera.test(resid(lmfit)) # X^2 > 6: non-normality
# boxplot split by factor variable
# boxplot(split(var, f))



test.df <- cbind(-events.daily.intensity.x, 
                 log(hft.flags.sum.x))
test.df[!is.finite(test.df)] <- 0
names(test.df) <- c("Intensity", "HFT")
summary(rlm(Intensity ~ HFT, test.df))
scatterplot(as.vector(test.df$HFT), as.vector(test.df$Intensity),
            main="Intensity of Overreaction Events vs. HFT activity",
            xlab="log. HFT flags/day", ylab="Avg. Overreaction Intensity",
            ylim=c(0, 15), xlim=c(2,12))

id.df <- data.frame(cbind(abs(id.score), cur.hft.x.aligned$Events,
                          cur.x.aligned$Volume))
colnames(id.df) = c("abs.score", "events", "volume")
str(id.df)

summary(lm(formula = abs.score ~ events, data = id.df))
scatterplot(id.df$events, id.df$abs.score)

# ----- CODE SNIPPETS ----------------------------------------------------------






# data access ----------
# extract column vectors
plot (colMeans(events.up.df[paste("r_", -60:120, sep="")], na.rm=T))
plot (colMeans(events.dn.df[paste("r_", -60:120, sep="")], na.rm=T))
# get 1 var
events.up.df[paste("v_", -60:120, sep="")]
# get 1 var for 1 event (1 value)
events.dn.df['2008-11-13 12:51:00',paste("r_", -60:120, sep="")]

# lookups in original aligned price series
cur.x.aligned[as.character(events.up.df$timestamp[1])] # access via timestamp
cur.x.aligned[events.up.df$bar[1]]                     # access via index

# build zoo object with unique time indices (duplicate times += 1 second)
events.master.up <- zoo(events.master.up.df, 
                        order.by=events.master.up.df$timestamp)
idx <- index(events.master.up)
while (any(duplicated(idx))) {
  idx[duplicated(idx)] <- 
    as.character.POSIXt(as.POSIXct(idx[duplicated(idx)]) + 1)  
}
events.master.up <- zoo(events.master.up.df, 
                        order.by=idx)
# access symbol info by symbol
symbol.info[cur.symbol,'shares_out']

# inspect event ---------------
plot(GetTseries(events.dn.df, 4, "hft"), type="l")

chartSeries(cur.x.aligned['2010-05-06'], theme="white")
plot(id.score[which(as.Date(index(cur.x.aligned)) == "2010-05-06")])
plot(id.dsbmom[which(as.Date(index(cur.x.aligned)) == "2010-05-06")])
InspectEvent(events.master.dn.df, 3)
# inspect event characteristics
PlotEvent(events.master.dn.df, 389, "r")
abline(h=0, col="blue", lwd=2)

head(colnames(test.df),50)

# analyze variable types in data frame
cnames <- colnames(events.up.df)
classes <- sapply(events.up.df, class)
summary(as.factor(classes))
cnames[classes == "data.frame"]
cnames[classes == "matrix"]
str(events.master.dn.df, list.len=2000)
