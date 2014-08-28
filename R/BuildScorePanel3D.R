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

suffix <- ".part3"

for (i in 1:367) {
# for (i in 1:length(sf.files)) {
  # 1) Load current score file & HFT data --------------------------------------
  # 1a) score file
  load (paste(data.path.sf, sf.files[i+368+369], sep=""))
  cur.symbol <- as.character (strsplit(sf.files[i+368+369],".sc2")[1]) # [1] = filename
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
  # id.dsbmom.daily <- array (id.dsbmom, dim=c(daybars, nrow(cur.d.x.aligned)))
  id.vol.daily <- array (coredata(cur.x.aligned$Volume), 
                         dim=c(daybars, nrow(cur.d.x.aligned)))
  id.voldta.daily <- array (id.volume.dta, 
                            dim=c(daybars, nrow(cur.d.x.aligned)))
  id.voldta.daily <- sapply (1:ncol(id.voldta.daily), function(x) 
    lowess(id.voldta.daily[,x], f=.05)$y)
  
  id.score.daily.max <- apply (id.score.daily.hi, 2, function(x) 
    max(x[11:(daybars-15)]))  
  id.score.daily.max.t <- apply (id.score.daily.hi, 2, function(x) 
    which(x[11:(daybars-15)]==max(x[11:(daybars-15)]))[1]+10)
  id.score.daily.min <- apply (id.score.daily.lo, 2, function(x) 
    min(x[11:(daybars-15)]))  
  id.score.daily.min.t <- apply (id.score.daily.lo, 2, function(x) 
    which(x[11:(daybars-15)]==min(x[11:(daybars-15)]))[1]+10)
  
  # set all values = NA prior to the first occurrence of NA
  # (data before are likely crap or too illiquid)
  id.score.daily.max[1:which(is.na(id.score.daily.max))
                     [sum(is.na(id.score.daily.max))]] <- NA
  id.score.daily.min[1:which(is.na(id.score.daily.min))
                     [sum(is.na(id.score.daily.min))]] <- NA
  
  # remove NaNs and Infs
  id.score.daily.max[is.nan(id.score.daily.max)] <- NA
  id.score.daily.min[is.nan(id.score.daily.min)] <- NA
  id.score.daily.max[!is.finite(id.score.daily.max)] <- NA
  id.score.daily.min[!is.finite(id.score.daily.min)] <- NA
  
  # apply NA to time-based look-up
  id.score.daily.max.t[is.na(id.score.daily.max)] <- NA
  id.score.daily.min.t[is.na(id.score.daily.max)] <- NA
  
  # calculate lookup bars
  id.score.daily.max.bars <- 1 + (1:ncol(id.score.daily.hi)-1) * daybars +
    id.score.daily.max.t    
  id.score.daily.min.bars <- 1 + (1:ncol(id.score.daily.hi)-1) * daybars +
    id.score.daily.min.t
  
  # 1b) hft data
  #   cur.hft.x <- Load1minHFTDataXTS (paste(data.path.HFT,
  #                                          paste(cur.symbol,".csv", sep=""), sep="/"))
  #   # create aligned HFT series (simplifies all event-bar based lookups)
  #   cur.hft.x <- cur.hft.x[index(cur.hft.x) %in% index(cur.x.aligned)]
  #   cur.hft.x.aligned <- merge (cur.hft.x, index(cur.x.aligned), fill=0)
  
  # Contemporaneous DSBMOM
  # remove vola adjustment from dsb hi/lo score
  id.dsbmom.hi <- id.score.hi * id.rv.dta.sc
  id.dsbmom.lo <- id.score.lo * id.rv.dta.sc
  
  id.dsbmom.daily.max.t0 <- id.score.daily.max.bars
  id.dsbmom.daily.max.t0[which(!is.na(id.dsbmom.daily.max.t0))] <- 
    id.dsbmom.hi[id.score.daily.max.bars[which(!is.na(id.score.daily.max.bars))]]
  
  id.dsbmom.daily.min.t0 <- id.score.daily.min.bars
  id.dsbmom.daily.min.t0[which(!is.na(id.dsbmom.daily.min.t0))] <- 
    id.dsbmom.lo[id.score.daily.min.bars[which(!is.na(id.score.daily.min.bars))]]
  
  
  # Relative 15 min Reversal vs. DSBMOM max/min at t0  
  id.daily.max.reversal <- id.score.daily.max.bars
  id.daily.max.reversal[which(!is.na(id.score.daily.max.bars))] <- 100 *
    (100*log (coredata(cur.x.aligned$Close[id.score.daily.max.bars[which(!is.na(id.score.daily.max.bars))]+15]) /
                coredata(cur.x.aligned$Close[id.score.daily.max.bars[which(!is.na(id.score.daily.max.bars))]+0]) ) [,1] ) /  
    id.dsbmom.daily.max.t0[which(!is.na(id.score.daily.max.bars))]
  
  id.daily.min.reversal <- id.score.daily.min.bars
  id.daily.min.reversal[which(!is.na(id.score.daily.min.bars))] <- 100 *
    (100 * log (coredata(cur.x.aligned$Close[id.score.daily.min.bars[which(!is.na(id.score.daily.min.bars))]+15]) /
                  coredata(cur.x.aligned$Close[id.score.daily.min.bars[which(!is.na(id.score.daily.min.bars))]+0]) ) [,1] ) /  
    id.dsbmom.daily.min.t0[which(!is.na(id.score.daily.min.bars))]
  
  # Contemporaneous RELVOL
  id.score.daily.max.t.exna <- 
    id.score.daily.max.t[which(!is.na(id.score.daily.max.t))]
  id.score.daily.min.t.exna <- 
    id.score.daily.min.t[which(!is.na(id.score.daily.min.t))]
  
  id.score.daily.max.t.exna
  
  # Relvol
  id.relvol.daily.max.t0 <- id.score.daily.max.t
  id.relvol.daily.max.t0[which(!is.na(id.score.daily.max.t))] <- 
    sapply(1:length(id.score.daily.max.t.exna), function(x) 
      id.vol.daily[id.score.daily.max.t.exna[x], x]) /
    sapply(1:length(id.score.daily.max.t.exna), function(x)
      id.voldta.daily[id.score.daily.max.t.exna[x], x])
  
  id.relvol.daily.min.t0 <- id.score.daily.min.t
  id.relvol.daily.min.t0[which(!is.na(id.score.daily.min.t))] <- 
    sapply(1:length(id.score.daily.min.t.exna), function(x) 
      id.vol.daily[id.score.daily.min.t.exna[x], x]) /
    sapply(1:length(id.score.daily.min.t.exna), function(x)
      id.voldta.daily[id.score.daily.min.t.exna[x], x])
  
  id.relvol.daily.max.l1 <- id.score.daily.max.t
  id.relvol.daily.max.l1[which(!is.na(id.score.daily.max.t))] <-
    sapply(1:length(id.score.daily.max.t.exna), function(x) 
      id.vol.daily[id.score.daily.max.t.exna[x]-1, x]) /
    sapply(1:length(id.score.daily.max.t.exna), function(x)
      id.voldta.daily[id.score.daily.max.t.exna[x]-1, x])
  
  id.relvol.daily.min.l1 <- id.score.daily.min.t
  id.relvol.daily.min.l1[which(!is.na(id.score.daily.min.t))] <- 
    sapply(1:length(id.score.daily.min.t.exna), function(x) 
      id.vol.daily[id.score.daily.min.t.exna[x]-1, x]) /
    sapply(1:length(id.score.daily.min.t.exna), function(x)
      id.voldta.daily[id.score.daily.min.t.exna[x]-1, x])
  
  id.relvol.daily.max.l5 <- id.score.daily.max.t
  id.relvol.daily.max.l5[which(!is.na(id.score.daily.max.t))] <- 
    sapply(1:length(id.score.daily.max.t.exna), function(x) 
      mean(id.vol.daily[(id.score.daily.max.t.exna[x]-5):(id.score.daily.max.t.exna[x]-1), x])) /
    sapply(1:length(id.score.daily.max.t.exna), function(x)
      mean(id.voldta.daily[(id.score.daily.max.t.exna[x]-5):(id.score.daily.max.t.exna[x]-1), x]))
  
  id.relvol.daily.min.l5 <- id.score.daily.min.t
  id.relvol.daily.min.l5[which(!is.na(id.score.daily.min.t))] <- 
    sapply(1:length(id.score.daily.min.t.exna), function(x) 
      mean(id.vol.daily[(id.score.daily.min.t.exna[x]-5):(id.score.daily.min.t.exna[x]-1), x])) /
    sapply(1:length(id.score.daily.min.t.exna), function(x)
      mean(id.voldta.daily[(id.score.daily.min.t.exna[x]-5):(id.score.daily.min.t.exna[x]-1), x]))
  
  id.relvol.daily.max.l15 <- id.score.daily.max.t
  id.relvol.daily.max.l15[which(!is.na(id.score.daily.max.t))] <- 
    sapply(1:length(id.score.daily.max.t.exna), function(x) 
      mean(id.vol.daily[max(1,(id.score.daily.max.t.exna[x]-15)):(id.score.daily.max.t.exna[x]-1), x])) /
    sapply(1:length(id.score.daily.max.t.exna), function(x)
      mean(id.voldta.daily[max(1,(id.score.daily.max.t.exna[x]-15)):(id.score.daily.max.t.exna[x]-1), x]))
  
  id.relvol.daily.min.l15 <- id.score.daily.min.t
  id.relvol.daily.min.l15[which(!is.na(id.score.daily.min.t))] <- 
    sapply(1:length(id.score.daily.min.t.exna), function(x) 
      mean(id.vol.daily[max(1,(id.score.daily.min.t.exna[x]-15)):(id.score.daily.min.t.exna[x]-1), x])) /
    sapply(1:length(id.score.daily.min.t.exna), function(x)
      mean(id.voldta.daily[max(1,(id.score.daily.min.t.exna[x]-15)):(id.score.daily.min.t.exna[x]-1), x]))
  
  # 3) Build dataframe of daily characteristics
  cur.d.x.aligned.ret <- diff (log(cur.d.x.aligned$Close), 1) * 100
  #   sp.d.x.aligned.ret <- diff (log(sp.d.x.aligned$Close), 1) * 100
  #   cur.d.x.aligned.aret <- cur.d.x.aligned.ret - 
  #                             cur.rbetas.d.x.shifted * sp.d.x.aligned.ret
  
  if (i == 1) {
    # build dataframe with date as key
    stocks.dsbmom.max <- data.frame (id.dsbmom.daily.max.t0,
                                     row.names=index(cur.d.x.aligned))
    stocks.dsbmom.min <- data.frame (id.dsbmom.daily.min.t0,
                                     row.names=index(cur.d.x.aligned))
    stocks.reverse.max <- data.frame (id.daily.max.reversal,
                                      row.names=index(cur.d.x.aligned))
    stocks.reverse.min <- data.frame (id.daily.min.reversal,
                                      row.names=index(cur.d.x.aligned))
    
    stocks.relvol.max <- data.frame (id.relvol.daily.max.t0,
                                     row.names=index(cur.d.x.aligned))
    stocks.relvol.min <- data.frame (id.relvol.daily.min.t0,
                                     row.names=index(cur.d.x.aligned))
    stocks.relvol.max.l1 <- data.frame (id.relvol.daily.max.l1,
                                        row.names=index(cur.d.x.aligned))
    stocks.relvol.min.l1 <- data.frame (id.relvol.daily.min.l1,
                                        row.names=index(cur.d.x.aligned))
    stocks.relvol.max.l5 <- data.frame (id.relvol.daily.max.l5,
                                        row.names=index(cur.d.x.aligned))
    stocks.relvol.min.l5 <- data.frame (id.relvol.daily.min.l5,
                                        row.names=index(cur.d.x.aligned))
    stocks.relvol.max.l15 <- data.frame (id.relvol.daily.max.l15,
                                         row.names=index(cur.d.x.aligned))
    stocks.relvol.min.l15 <- data.frame (id.relvol.daily.min.l15,
                                         row.names=index(cur.d.x.aligned))
    
    names(stocks.dsbmom.max)[1] <- cur.symbol
    names(stocks.dsbmom.min)[1] <- cur.symbol
    names(stocks.reverse.max)[1] <- cur.symbol
    names(stocks.reverse.min)[1] <- cur.symbol
    
    names(stocks.relvol.max)[1] <- cur.symbol
    names(stocks.relvol.min)[1] <- cur.symbol
    names(stocks.relvol.max.l1)[1] <- cur.symbol
    names(stocks.relvol.min.l1)[1] <- cur.symbol
    names(stocks.relvol.max.l5)[1] <- cur.symbol
    names(stocks.relvol.min.l5)[1] <- cur.symbol
    names(stocks.relvol.max.l15)[1] <- cur.symbol
    names(stocks.relvol.min.l15)[1] <- cur.symbol
    
    
  } else {
    # create temporary dataframes for current symbol
    
    cur.dsbmom.max <- data.frame (id.dsbmom.daily.max.t0,
                                  row.names=index(cur.d.x.aligned))
    cur.dsbmom.min <- data.frame (id.dsbmom.daily.min.t0,
                                  row.names=index(cur.d.x.aligned))
    cur.reverse.max <- data.frame (id.daily.max.reversal,
                                   row.names=index(cur.d.x.aligned))
    cur.reverse.min <- data.frame (id.daily.min.reversal,
                                   row.names=index(cur.d.x.aligned))
    
    cur.relvol.max <- data.frame (id.relvol.daily.max.t0,
                                  row.names=index(cur.d.x.aligned))
    cur.relvol.min <- data.frame (id.relvol.daily.min.t0,
                                  row.names=index(cur.d.x.aligned))
    cur.relvol.max.l1 <- data.frame (id.relvol.daily.max.l1,
                                     row.names=index(cur.d.x.aligned))
    cur.relvol.min.l1 <- data.frame (id.relvol.daily.min.l1,
                                     row.names=index(cur.d.x.aligned))
    cur.relvol.max.l5 <- data.frame (id.relvol.daily.max.l5,
                                     row.names=index(cur.d.x.aligned))
    cur.relvol.min.l5 <- data.frame (id.relvol.daily.min.l5,
                                     row.names=index(cur.d.x.aligned))
    cur.relvol.max.l15 <- data.frame (id.relvol.daily.max.l15,
                                      row.names=index(cur.d.x.aligned))
    cur.relvol.min.l15 <- data.frame (id.relvol.daily.min.l15,
                                      row.names=index(cur.d.x.aligned))      
    
    names(cur.dsbmom.max)[1] <- cur.symbol
    names(cur.dsbmom.min)[1] <- cur.symbol
    names(cur.reverse.max)[1] <- cur.symbol
    names(cur.reverse.min)[1] <- cur.symbol
    
    names(cur.relvol.max)[1] <- cur.symbol
    names(cur.relvol.min)[1] <- cur.symbol
    names(cur.relvol.max.l1)[1] <- cur.symbol
    names(cur.relvol.min.l1)[1] <- cur.symbol
    names(cur.relvol.max.l5)[1] <- cur.symbol
    names(cur.relvol.min.l5)[1] <- cur.symbol
    names(cur.relvol.max.l15)[1] <- cur.symbol
    names(cur.relvol.min.l15)[1] <- cur.symbol    
    
    
    stocks.dsbmom.max <- MergeStockdayPanels (stocks.dsbmom.max, cur.dsbmom.max)
    stocks.dsbmom.min <- MergeStockdayPanels (stocks.dsbmom.min, cur.dsbmom.min)
    stocks.reverse.max <- MergeStockdayPanels (stocks.reverse.max, cur.reverse.max)
    stocks.reverse.min <- MergeStockdayPanels (stocks.reverse.min, cur.reverse.min)
    
    stocks.relvol.max <- MergeStockdayPanels (stocks.relvol.max, cur.relvol.max)
    stocks.relvol.min <- MergeStockdayPanels (stocks.relvol.min, cur.relvol.min)
    stocks.relvol.max.l1 <- MergeStockdayPanels (stocks.relvol.max.l1, cur.relvol.max.l1)
    stocks.relvol.min.l1 <- MergeStockdayPanels (stocks.relvol.min.l1, cur.relvol.min.l1)
    stocks.relvol.max.l5 <- MergeStockdayPanels (stocks.relvol.max.l5, cur.relvol.max.l5)
    stocks.relvol.min.l5 <- MergeStockdayPanels (stocks.relvol.min.l5, cur.relvol.min.l5)
    stocks.relvol.max.l15 <- MergeStockdayPanels (stocks.relvol.max.l15, cur.relvol.max.l15)
    stocks.relvol.min.l15 <- MergeStockdayPanels (stocks.relvol.min.l15, cur.relvol.min.l15)
    
  }
  # end of 3c.1x) special: build daily dataframe/symbol matrix
}

# save panel data objects to file
save (
  stocks.dsbmom.max, stocks.dsbmom.min, stocks.reverse.max, stocks.reverse.min,
  stocks.relvol.max, stocks.relvol.min, stocks.relvol.max.l1, stocks.relvol.min.l1,
  stocks.relvol.max.l5, stocks.relvol.min.l5, stocks.relvol.max.l15, stocks.relvol.min.l15,
  
  file=paste(data.path.out, "PanelHiLoDSBM_RVOL", suffix,".pdb", sep=""))

beep()
# ----- CLEAN-UP ---------------------------------------------------------------
gc()