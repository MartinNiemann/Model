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
  read.csv ("D:/Doktorarbeit/40 Data/Stocks/Taipan/symbol_info2.csv", 
            header=TRUE, stringsAsFactors=FALSE, comment.char="", 
            col.names=c("Symbol", "shares_out", "Name", 
                        "Sector", "Industry", "Country", "shares_fl",
                        "NMS_Date", "SSB_Date_1" ,"SSB_Date_2"), 
            colClasses=c("character", "numeric", rep("character", 4), "numeric",
                         rep("character",3)))
rownames(symbol.info) <- symbol.info$Symbol
class(symbol.info[,8:10]) <- "Date"

suffix <- ".part2"

# ATTENTION: min/max t-code still wrong in this file
#            correct before using these references

for (i in 1:369) {
# for (i in 1:length(sf.files)) {
  # 1) Load current score file & HFT data --------------------------------------
  # 1a) score file
  load (paste(data.path.sf, sf.files[i+368], sep=""))
  cur.symbol <- as.character (strsplit(sf.files[i+368],".sc2")[1]) # [1] = filename
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
  id.price.mid <- coredata(cur.x.aligned$High + cur.x.aligned$Low) / 2
  id.midprice.daily <- array (id.price.mid, dim=c(daybars, nrow(cur.d.x.aligned)))
  id.ret.daily <- log(id.midprice.daily[2:391,] / id.midprice.daily[1:390,])
  id.ret.daily <- rbind(rep(0, ncol(id.ret.daily)), id.ret.daily)
  id.hl.daily <- array (log(coredata(cur.x.aligned$High / cur.x.aligned$Low)), 
                        dim=c(daybars, nrow(cur.d.x.aligned)))
  id.ret5.daily <- log(id.midprice.daily[6:391,] / id.midprice.daily[1:386,])
  id.ret15.daily <- log(id.midprice.daily[16:391,] / id.midprice.daily[1:376,])
  
  
  # calculate daily measures of return distributions
  id.ret.daily.max <- apply (id.ret.daily, 2, function(x) 
                             max(x[11:(daybars-10)]))
  id.ret.daily.min <- apply (id.ret.daily, 2, function(x) 
                             min(x[11:(daybars-10)]))
  id.stdev.daily <- apply (id.ret.daily[11:(daybars-10),], 2, sd, na.rm=T)
  id.stdev5.daily <- apply (id.ret5.daily[11:(nrow(id.ret5.daily)-10),], 2, sd, na.rm=T)
  id.stdev15.daily <- apply (id.ret15.daily[11:(nrow(id.ret15.daily)-10),], 2, sd, na.rm=T)
  id.kurtosis.daily <- apply (id.ret.daily[11:(daybars-10),], 2, kurtosis, na.rm=T)
  id.kurtosis5.daily <- apply (id.ret5.daily[11:(nrow(id.ret5.daily)-10),], 2, kurtosis, na.rm=T)
  id.kurtosis15.daily <- apply (id.ret15.daily[11:(nrow(id.ret15.daily)-10),], 2, kurtosis, na.rm=T)
  id.ret.mean.daily <- apply (id.ret.daily[11:(daybars-10),], 2, mean, na.rm=T)
  id.ret.meanabs.daily <- apply (id.ret.daily[11:(daybars-10),], 2, function(x) 
                                 mean(abs(x), na.rm=T))
  id.ret.medianabs.daily <- apply (id.ret.daily[11:(daybars-10),], 2, function(x) 
                                   median(abs(x), na.rm=T))
  id.ret.q95abs.daily <- apply (id.ret.daily[11:(daybars-10),], 2, function(x) 
    quantile(abs(x), probs=.95, na.rm=T))
  id.ret.q99abs.daily <- apply (id.ret.daily[11:(daybars-10),], 2, function(x) 
    quantile(abs(x), probs=.99, na.rm=T))
  id.hl.mean.daily <- apply (id.hl.daily[11:(daybars-10),], 2, mean, na.rm=T)
  id.hl.median.daily <- apply (id.hl.daily[11:(daybars-10),], 2, median, na.rm=T)
  id.hl.q95abs.daily <- apply (id.hl.daily[11:(daybars-10),], 2, function(x) 
    quantile(abs(x), probs=.95, na.rm=T))
  id.hl.q99abs.daily <- apply (id.hl.daily[11:(daybars-10),], 2, function(x) 
    quantile(abs(x), probs=.99, na.rm=T))
  id.ret.gpdshape.daily <- apply (id.ret.daily[11:(daybars-10),], 2, function(x)   
    gpd.fit(abs(x), "amle")[1])
  id.hl.gpdshape.daily <- apply (id.hl.daily[11:(daybars-10),], 2, function(x) 
    gpd.fit(x, "amle")[1])
  id.ret.gpdshape5.daily <- apply (id.ret5.daily[11:(nrow(id.ret5.daily)-10),], 2, function(x)   
    gpd.fit(abs(x), "amle")[1])
  id.ret.gpdshape15.daily <- apply (id.ret15.daily[11:(nrow(id.ret15.daily)-10),], 2, function(x)   
    gpd.fit(abs(x), "amle")[1])

  
  if (i == 1) {
    # build dataframe with date as key
#     stocks.idret.max.df       id.ret.daily.max
#     stocks.idret.min.df       id.ret.daily.min
#     stocks.idret.sd.df        id.stdev.daily
#     stocks.idret.kurt.df      id.kurtosis.daily
#     stocks.idret.mean.df      id.ret.mean.daily
#     stocks.idret.meanabs.df   id.ret.meanabs.daily
#     stocks.idret.q95abs.df    id.ret.q95abs.daily
#     stocks.idret.q99abs.df    id.ret.q99abs.daily
#     stocks.idhl.mean.df       id.hl.mean.daily
#     stocks.idhl.q95.df        id.hl.q95abs.daily
#     stocks.idhl.q99.df        id.hl.q99abs.daily
#     stocks.idret.tail.df      id.ret.gpdshape.daily
#     stocks.idhl.tail.df       id.hl.gpdshape.daily
        
    stocks.idret.max.df <- data.frame (id.ret.daily.max, 
                                       row.names=index(cur.d.x.aligned))
    stocks.idret.min.df <- data.frame (id.ret.daily.min, 
                                       row.names=index(cur.d.x.aligned))
    stocks.idret.sd.df <- data.frame (id.stdev.daily, 
                                      row.names=index(cur.d.x.aligned))
    stocks.idret.sd5.df <- data.frame (id.stdev5.daily, 
                                      row.names=index(cur.d.x.aligned))
    stocks.idret.sd15.df <- data.frame (id.stdev15.daily, 
                                      row.names=index(cur.d.x.aligned))
    stocks.idret.kurt.df <- data.frame (id.kurtosis.daily, 
                                      row.names=index(cur.d.x.aligned))
    stocks.idret.kurt5.df <- data.frame (id.kurtosis5.daily, 
                                        row.names=index(cur.d.x.aligned))
    stocks.idret.kurt15.df <- data.frame (id.kurtosis15.daily, 
                                         row.names=index(cur.d.x.aligned))
    stocks.idret.mean.df <- data.frame (id.ret.mean.daily, 
                                        row.names=index(cur.d.x.aligned))
    stocks.idret.medianabs.df <- data.frame (id.ret.medianabs.daily, 
                                        row.names=index(cur.d.x.aligned))
    stocks.idret.meanabs.df <- data.frame (id.ret.meanabs.daily, 
                                        row.names=index(cur.d.x.aligned))
    stocks.idret.q95abs.df <- data.frame (id.ret.q95abs.daily, 
                                        row.names=index(cur.d.x.aligned))
    stocks.idret.q99abs.df <- data.frame (id.ret.q99abs.daily, 
                                        row.names=index(cur.d.x.aligned))
    stocks.idhl.mean.df <- data.frame (id.hl.mean.daily, 
                                        row.names=index(cur.d.x.aligned))
    stocks.idhl.median.df <- data.frame (id.hl.median.daily, 
                                       row.names=index(cur.d.x.aligned))
    stocks.idhl.q95.df <- data.frame (id.hl.q95abs.daily, 
                                        row.names=index(cur.d.x.aligned))
    stocks.idhl.q99.df <- data.frame (id.hl.q99abs.daily, 
                                        row.names=index(cur.d.x.aligned))
    stocks.idret.tail.df <- data.frame (id.ret.gpdshape.daily, 
                                        row.names=index(cur.d.x.aligned))
    stocks.idret.tail5.df <- data.frame (id.ret.gpdshape5.daily, 
                                        row.names=index(cur.d.x.aligned))
    stocks.idret.tail15.df <- data.frame (id.ret.gpdshape15.daily, 
                                        row.names=index(cur.d.x.aligned))
    stocks.idhl.tail.df <- data.frame (id.hl.gpdshape.daily, 
                                        row.names=index(cur.d.x.aligned))
      
    names(stocks.idret.max.df)[1] <- cur.symbol
    names(stocks.idret.min.df)[1] <- cur.symbol
    names(stocks.idret.sd.df)[1] <- cur.symbol
    names(stocks.idret.sd5.df)[1] <- cur.symbol
    names(stocks.idret.sd15.df)[1] <- cur.symbol
    names(stocks.idret.kurt.df)[1] <- cur.symbol
    names(stocks.idret.kurt5.df)[1] <- cur.symbol
    names(stocks.idret.kurt15.df)[1] <- cur.symbol
    names(stocks.idret.mean.df)[1] <- cur.symbol
    names(stocks.idret.medianabs.df)[1] <- cur.symbol
    names(stocks.idret.meanabs.df)[1] <- cur.symbol
    names(stocks.idret.q95abs.df)[1] <- cur.symbol
    names(stocks.idret.q99abs.df)[1] <- cur.symbol
    names(stocks.idhl.mean.df)[1] <- cur.symbol
    names(stocks.idhl.median.df)[1] <- cur.symbol
    names(stocks.idhl.q95.df)[1] <- cur.symbol
    names(stocks.idhl.q99.df)[1] <- cur.symbol
    names(stocks.idret.tail.df)[1] <- cur.symbol
    names(stocks.idret.tail5.df)[1] <- cur.symbol
    names(stocks.idret.tail15.df)[1] <- cur.symbol
    names(stocks.idhl.tail.df)[1] <- cur.symbol
    
  } else {
    # create temporary dataframes for current symbol
    cur.idret.max.df <- data.frame (id.ret.daily.max, 
                                       row.names=index(cur.d.x.aligned))
    cur.idret.min.df <- data.frame (id.ret.daily.min, 
                                       row.names=index(cur.d.x.aligned))
    cur.idret.sd.df <- data.frame (id.stdev.daily, 
                                      row.names=index(cur.d.x.aligned))
    cur.idret.sd5.df <- data.frame (id.stdev5.daily, 
                                       row.names=index(cur.d.x.aligned))
    cur.idret.sd15.df <- data.frame (id.stdev15.daily, 
                                        row.names=index(cur.d.x.aligned))
    cur.idret.kurt.df <- data.frame (id.kurtosis.daily, 
                                        row.names=index(cur.d.x.aligned))
    cur.idret.kurt5.df <- data.frame (id.kurtosis5.daily, 
                                         row.names=index(cur.d.x.aligned))
    cur.idret.kurt15.df <- data.frame (id.kurtosis15.daily, 
                                          row.names=index(cur.d.x.aligned))
    cur.idret.mean.df <- data.frame (id.ret.mean.daily, 
                                        row.names=index(cur.d.x.aligned))
    cur.idret.medianabs.df <- data.frame (id.ret.medianabs.daily, 
                                     row.names=index(cur.d.x.aligned))
    cur.idret.meanabs.df <- data.frame (id.ret.meanabs.daily, 
                                           row.names=index(cur.d.x.aligned))
    cur.idret.q95abs.df <- data.frame (id.ret.q95abs.daily, 
                                          row.names=index(cur.d.x.aligned))
    cur.idret.q99abs.df <- data.frame (id.ret.q99abs.daily, 
                                          row.names=index(cur.d.x.aligned))
    cur.idhl.mean.df <- data.frame (id.hl.mean.daily, 
                                       row.names=index(cur.d.x.aligned))
    cur.idhl.median.df <- data.frame (id.hl.median.daily, 
                                    row.names=index(cur.d.x.aligned))
    cur.idhl.q95.df <- data.frame (id.hl.q95abs.daily, 
                                      row.names=index(cur.d.x.aligned))
    cur.idhl.q99.df <- data.frame (id.hl.q99abs.daily, 
                                      row.names=index(cur.d.x.aligned))
    cur.idret.tail.df <- data.frame (id.ret.gpdshape.daily, 
                                        row.names=index(cur.d.x.aligned))
    cur.idret.tail5.df <- data.frame (id.ret.gpdshape5.daily, 
                                     row.names=index(cur.d.x.aligned))
    cur.idret.tail15.df <- data.frame (id.ret.gpdshape15.daily, 
                                     row.names=index(cur.d.x.aligned))
    cur.idhl.tail.df <- data.frame (id.hl.gpdshape.daily, 
                                       row.names=index(cur.d.x.aligned))
    
    names(cur.idret.max.df)[1] <- cur.symbol
    names(cur.idret.min.df)[1] <- cur.symbol
    names(cur.idret.sd.df)[1] <- cur.symbol
    names(cur.idret.sd5.df)[1] <- cur.symbol
    names(cur.idret.sd15.df)[1] <- cur.symbol
    names(cur.idret.kurt.df)[1] <- cur.symbol
    names(cur.idret.kurt5.df)[1] <- cur.symbol
    names(cur.idret.kurt15.df)[1] <- cur.symbol
    names(cur.idret.mean.df)[1] <- cur.symbol
    names(cur.idret.medianabs.df)[1] <- cur.symbol
    names(cur.idret.meanabs.df)[1] <- cur.symbol
    names(cur.idret.q95abs.df)[1] <- cur.symbol
    names(cur.idret.q99abs.df)[1] <- cur.symbol
    names(cur.idhl.mean.df)[1] <- cur.symbol
    names(cur.idhl.median.df)[1] <- cur.symbol
    names(cur.idhl.q95.df)[1] <- cur.symbol
    names(cur.idhl.q99.df)[1] <- cur.symbol
    names(cur.idret.tail.df)[1] <- cur.symbol
    names(cur.idret.tail5.df)[1] <- cur.symbol
    names(cur.idret.tail15.df)[1] <- cur.symbol
    names(cur.idhl.tail.df)[1] <- cur.symbol
    
    # merge temporary dataframes into overall data frames
    stocks.idret.max.df <- MergeStockdayPanels (stocks.idret.max.df, cur.idret.max.df)
    stocks.idret.min.df <- MergeStockdayPanels (stocks.idret.min.df, cur.idret.min.df)
    stocks.idret.sd.df <- MergeStockdayPanels (stocks.idret.sd.df, cur.idret.sd.df)
    stocks.idret.sd5.df <- MergeStockdayPanels (stocks.idret.sd5.df, cur.idret.sd5.df)
    stocks.idret.sd15.df <- MergeStockdayPanels (stocks.idret.sd15.df, cur.idret.sd15.df)
    stocks.idret.kurt.df <- MergeStockdayPanels (stocks.idret.kurt.df, cur.idret.kurt.df)
    stocks.idret.kurt5.df <- MergeStockdayPanels (stocks.idret.kurt5.df, cur.idret.kurt5.df)
    stocks.idret.kurt15.df <- MergeStockdayPanels (stocks.idret.kurt15.df, cur.idret.kurt15.df)
    stocks.idret.mean.df <- MergeStockdayPanels (stocks.idret.mean.df, cur.idret.mean.df)
    stocks.idret.medianabs.df <- MergeStockdayPanels (stocks.idret.medianabs.df, cur.idret.medianabs.df)
    stocks.idret.meanabs.df <- MergeStockdayPanels (stocks.idret.meanabs.df, cur.idret.meanabs.df)
    stocks.idret.q95abs.df <- MergeStockdayPanels (stocks.idret.q95abs.df, cur.idret.q95abs.df)
    stocks.idret.q99abs.df <- MergeStockdayPanels (stocks.idret.q99abs.df, cur.idret.q99abs.df)
    stocks.idhl.mean.df <- MergeStockdayPanels (stocks.idhl.mean.df, cur.idhl.mean.df)
    stocks.idhl.median.df <- MergeStockdayPanels (stocks.idhl.median.df, cur.idhl.median.df)
    stocks.idhl.q95.df <- MergeStockdayPanels (stocks.idhl.q95.df, cur.idhl.q95.df)
    stocks.idhl.q99.df <- MergeStockdayPanels (stocks.idhl.q99.df, cur.idhl.q99.df)
    stocks.idret.tail.df <- MergeStockdayPanels (stocks.idret.tail.df, cur.idret.tail.df)
    stocks.idret.tail5.df <- MergeStockdayPanels (stocks.idret.tail5.df, cur.idret.tail5.df)
    stocks.idret.tail15.df <- MergeStockdayPanels (stocks.idret.tail15.df, cur.idret.tail15.df)
    stocks.idhl.tail.df <- MergeStockdayPanels (stocks.idhl.tail.df, cur.idhl.tail.df)
  }
  # end of 3c.1x) special: build daily dataframe/symbol matrix
}

# save panel data objects to file
save (stocks.idret.max.df, stocks.idret.min.df, stocks.idret.sd.df, 
      stocks.idret.sd5.df, stocks.idret.sd15.df, stocks.idret.kurt.df, 
      stocks.idret.kurt5.df, stocks.idret.kurt15.df, 
      stocks.idret.mean.df, stocks.idret.meanabs.df, 
      stocks.idret.q95abs.df, stocks.idret.q99abs.df, stocks.idhl.mean.df,
      stocks.idhl.q95.df, stocks.idhl.q99.df, stocks.idret.tail.df, 
      stocks.idret.tail5.df, stocks.idret.tail15.df,
      stocks.idhl.tail.df, stocks.idret.medianabs.df, stocks.idhl.median.df,
      file=paste(data.path.out, "PanelDist", suffix,".pdb", sep=""))

beep()
# ----- CLEAN-UP ---------------------------------------------------------------
gc()