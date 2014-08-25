# ******************************************************************************
# Intraday Overreaction - BuildInstanceHeatmap.R                               *
# ******************************************************************************
# This script is a reduced version of BuildInstanceDB.r, which solely calcu-   *
# lates number of events for abs/rel combinations.                             *
# ******************************************************************************

# ----- PREP -------------------------------------------------------------------
# path & file info
sf.path = "Output/"
sf.files <- list.files (path = sf.path, pattern = "*.sco")

symbol.info <- 
  read.csv ("D:/Doktorarbeit/40 Data/Stocks/Taipan/symbol_info.csv", 
            header=TRUE, stringsAsFactors=FALSE, comment.char="", 
            col.names=c("Symbol", "shares_out", "Name", 
                        "Sector", "Industry", "Country"), 
            colClasses=c("character", "numeric", rep("character",4)))
rownames(symbol.info) <- symbol.info$Symbol

# params set in Main.R

# abs/rel values for testing
param.filt.rel.vec <- c (seq(from=6.0, to=8.0, by=0.5), 9, 10)
param.filt.abs.vec <- c (seq(from=2.0, to=4.0, by=0.5), 5, 6, 8)

# result matrices
event.freq.mat <- matrix (NA, 100 * length(param.filt.rel.vec) * 
                            length(param.filt.abs.vec), 6)
years.freq.mat <- matrix (0, 100 * length(param.filt.rel.vec) * 
                            length(param.filt.abs.vec), 2+8*4)
times.freq.mat <- matrix (0, 100 * length(param.filt.rel.vec) * 
                            length(param.filt.abs.vec), 2+13*4)
colnames(event.freq.mat) <- c("rel", "abs", "up", "dn", "up.a", "dn.a")

# ----- CORE LOOP --------------------------------------------------------------
# loop through all abs/rel parameter combinations
# counting events in selected stocks sub-sample
m <- 0
for (f in seq(1,100,1)) {
  # load score file
  load (paste(sf.path, sf.files[f], sep=""))
  cur.symbol <- as.character (strsplit(sf.files[f],".sco")[1])
  print (paste("[BuildInstanceHeatmap.R ", format(Sys.time(), "%H:%M:%S"), 
               "] Processing ", cur.symbol, sep=""))
  
  # run through parameter combinations
  for (r in seq_along(param.filt.rel.vec)) {
    for (a in seq_along(param.filt.abs.vec)) {
      
      # check event frequency
      id.events.idx.up <- GetEventIdx (id.score, id.dsbmom,
                                       param.filt.rel.vec[r],
                                       param.filt.abs.vec[a], TRUE)    
      id.events.idx.dn <- GetEventIdx (id.score, id.dsbmom,
                                       param.filt.rel.vec[r],
                                       param.filt.abs.vec[a], FALSE)    
      id.events.idx.up.a <- GetEventIdx (id.score.a, id.dsbmom.a,
                                         param.filt.rel.vec[r],
                                         param.filt.abs.vec[a], TRUE)    
      id.events.idx.dn.a <- GetEventIdx (id.score.a, id.dsbmom.a,
                                         param.filt.rel.vec[r],
                                         param.filt.abs.vec[a], FALSE)    
      
      # write result to result matrix
      m <- m+1
      event.freq.mat[m,] <- c (param.filt.rel.vec[r], param.filt.abs.vec[a], 
                               length(id.events.idx.up), 
                               length(id.events.idx.dn), 
                               length(id.events.idx.up.a), 
                               length(id.events.idx.dn.a))
      
      # sum events by year and by half hour timeframe
      years.freq.mat[m,] <- c(param.filt.rel.vec[r], param.filt.abs.vec[a],
        GetEventYears(id.events.idx.up), GetEventYears(id.events.idx.dn),
        GetEventYears(id.events.idx.up.a), GetEventYears(id.events.idx.dn.a))
      
      times.freq.mat[m,] <- c(param.filt.rel.vec[r], param.filt.abs.vec[a],
        GetEventTimeslices(id.events.idx.up), 
                              GetEventTimeslices(id.events.idx.dn),
        GetEventTimeslices(id.events.idx.up.a), 
                              GetEventTimeslices(id.events.idx.dn.a))
    }
  }
}

# TOTAL EVENT FREQUENCIES --> HEATMAP ------------------------------------------
# generate heatmap for total event frequencies
heatmap.mat <- as.matrix ((by(event.freq.mat[, "up"]+event.freq.mat[, "dn"], 
                               list(as.factor(event.freq.mat[,"rel"]), 
                                    as.factor(event.freq.mat[,"abs"])), 
                               FUN=sum, na.rm=T))[1:length(param.filt.rel.vec), 
                                                   1:length(param.filt.abs.vec)]
                          , dimnames=c("abs","rel"))
round (heatmap.mat)

heatmap (log(heatmap.mat), Rowv=NA, Colv=NA, scale="none",
         col=heat.colors(length(heatmap.mat),alpha=1.0))

# construct Filter Impact Matrices - by level
abs.filter.impact.mat <- round(heatmap.mat[,2:ncol(heatmap.mat)] /
                                 heatmap.mat[,1:(ncol(heatmap.mat)-1)] - 1, 2)
rel.filter.impact.mat <- round(heatmap.mat[2:nrow(heatmap.mat),] /
                                 heatmap.mat[1:(nrow(heatmap.mat)-1),] - 1, 2)
abs.filter.impact.mat
rel.filter.impact.mat

# YEARLY EVENT FREQUENCIES BY SEVERITY -----------------------------------------
by.obj <- (by(years.freq.mat[, 2+1:8] + years.freq.mat[, 2+8+1:8],
            list(as.factor(years.freq.mat[,1]), as.factor(years.freq.mat[,2])),
              FUN=function(x) colSums(x)))
by.obj.a <- (by(years.freq.mat[, 2+16+1:8] + years.freq.mat[, 2+8+16+1:8],
             list(as.factor(years.freq.mat[,1]), as.factor(years.freq.mat[,2])),
                FUN=function(x) colSums(x)))
# data access in by object
by.obj[4,2][[1]][1:8]

param.filt.abs.rel <- list(c(2,6), c(2.5,6.5), c(3,7), c(3.5,7.5), c(4,8), 
                           c(5,10), c(6,10), c(8,10))
# lookup function by value according to list item ID
# then construct Yearly Frequency Charts
GetParamID <- function(abs.rel.list, list.id) {
  # takes one list item(!) from param.filt.abs.rel combinations and returns
  # a pair of IDs for lookup in param.arrays and matrices constructed with them
  abs.rel.pair <- abs.rel.list[[list.id]]
  return (list(which(param.filt.abs.vec == abs.rel.pair[1]),
               which(param.filt.rel.vec == abs.rel.pair[2])))
}
t <- GetParamID(param.filt.abs.rel,1)
heatmap.mat[t[[2]],t[[1]]]

# extract yearly frequencies by severity
freq.severity.2006 <- sapply(1:length(param.filt.abs.rel), 
                             function(x)
  by.obj[GetParamID(param.filt.abs.rel,x)[[2]],
         GetParamID(param.filt.abs.rel,x)[[1]]]
                             [[1]][1]
   )
freq.severity.2007 <- sapply(1:length(param.filt.abs.rel), 
                             function(x)
                               by.obj[GetParamID(param.filt.abs.rel,x)[[2]],
                                      GetParamID(param.filt.abs.rel,x)[[1]]]
                             [[1]][2]
)
freq.severity.2008 <- sapply(1:length(param.filt.abs.rel), 
                             function(x)
                               by.obj[GetParamID(param.filt.abs.rel,x)[[2]],
                                      GetParamID(param.filt.abs.rel,x)[[1]]]
                             [[1]][3]
)
freq.severity.2009 <- sapply(1:length(param.filt.abs.rel), 
                             function(x)
                               by.obj[GetParamID(param.filt.abs.rel,x)[[2]],
                                      GetParamID(param.filt.abs.rel,x)[[1]]]
                             [[1]][4]
)
freq.severity.2010 <- sapply(1:length(param.filt.abs.rel), 
                             function(x)
                               by.obj[GetParamID(param.filt.abs.rel,x)[[2]],
                                      GetParamID(param.filt.abs.rel,x)[[1]]]
                             [[1]][5]
)
freq.severity.2011 <- sapply(1:length(param.filt.abs.rel), 
                             function(x)
                               by.obj[GetParamID(param.filt.abs.rel,x)[[2]],
                                      GetParamID(param.filt.abs.rel,x)[[1]]]
                             [[1]][6]
)
freq.severity.2012 <- sapply(1:length(param.filt.abs.rel), 
                             function(x)
                               by.obj[GetParamID(param.filt.abs.rel,x)[[2]],
                                      GetParamID(param.filt.abs.rel,x)[[1]]]
                             [[1]][7]
)
freq.severity.2013 <- sapply(1:length(param.filt.abs.rel), 
                             function(x)
                               by.obj[GetParamID(param.filt.abs.rel,x)[[2]],
                                      GetParamID(param.filt.abs.rel,x)[[1]]]
                             [[1]][8]
)

# plot in Analysis.R

# ANALYZE EVENT DISTRIBUTION PER DAYTIME ---------------------------------------
by.obj <- (by(times.freq.mat[, 2+1:13] + times.freq.mat[, 2+13+1:13],
             list(as.factor(times.freq.mat[,1]), as.factor(times.freq.mat[,2])),
             FUN=function(x) colSums(x)))
by.obj.a <- (by(times.freq.mat[, 2+26+1:13] + times.freq.mat[, 2+13+26+1:13],
              list(as.factor(times.freq.mat[,1]), as.factor(times.freq.mat[,2])),
              FUN=function(x) colSums(x)))

for (r in 1:length(param.filt.rel.vec) ) {
  for (a in 1:length(param.filt.abs.vec)) {
    png(filename=paste(data.path.graph, "Timeslices.a_", param.filt.abs.vec[a], 
                       "_", param.filt.rel.vec[r], ".png", sep=""))
    barplot((by.obj.a[r,a][[1]])[1:13], col="darkblue", 
            main=paste("Events per time-slice for abs",
                       param.filt.abs.vec[a], "rel", param.filt.rel.vec[r]))
    dev.off()
  }  
}
# nicest distribution during the day (abs/rel) R
2.0/6.0
2.5/6.0
3.0/7.0
3.5/7.5
4.0/8
5.0/10
6.0/10
8.0/10



# nicest distribution during the day (abs/rel) AR
2.0/7.0
2.5/7.5
3.0/8.0
3.5/9.0
4.0/10
5.0/10
6.0/10
8.0/10


   
# ----- CODE SNIPPETS ----------------------------------------------------------
# find matching value/quantile pairs for abs/rel scores (last file loaded only!)
quantile(id.score, probs=ecdf(id.dsbmom)(c(-2,-2.5, -3, -4,-5, -6)), na.rm=T)
quantile(id.dsbmom, probs=ecdf(id.score)(c(-2,-3,-4,-5,-6)), na.rm=T)

