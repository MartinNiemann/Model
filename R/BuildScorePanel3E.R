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
  
  cur.d.x.aligned.hlr <- log (cur.d.x.aligned$High / cur.d.x.aligned$Low) * 100
  
  if (i == 1) {
    # build dataframe with date as key
    stocks.hlrs.df <- data.frame (cur.d.x.aligned.hlr, 
                                  row.names=index(cur.d.x.aligned))
    stocks.pvol.df <- data.frame (volatility(cur.d.x.aligned, 10, 
                                    calc="parkinson", N=period.oneyear), 
                                  row.names=index(cur.d.x.aligned))
    names(stocks.hlrs.df)[1] <- cur.symbol
    names(stocks.pvol.df)[1] <- cur.symbol
  } else {
    # create temporary dataframes for current symbol
    cur.hlrs.df <- data.frame (cur.d.x.aligned.hlr, 
                                  row.names=index(cur.d.x.aligned))
    cur.pvol.df <- data.frame (100 * volatility(cur.d.x.aligned, 10, 
                                             calc="parkinson", N=period.oneyear), 
                                  row.names=index(cur.d.x.aligned))
    names(cur.hlrs.df)[1] <- cur.symbol
    names(cur.pvol.df)[1] <- cur.symbol
    stocks.hlrs.df <- MergeStockdayPanels (stocks.hlrs.df, cur.hlrs.df)
    stocks.pvol.df <- MergeStockdayPanels (stocks.pvol.df, cur.pvol.df)
  }
  # end of 3c.1x) special: build daily dataframe/symbol matrix
}

# save panel data objects to file
save (stocks.hlrs.df, stocks.pvol.df,
      
      file=paste(data.path.out, "PanelParkVol", suffix,".pdb", sep=""))

beep()
# ----- CLEAN-UP ---------------------------------------------------------------
gc()