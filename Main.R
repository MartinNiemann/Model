# ******************************************************************************
# Intraday Overreaction - Main.R                                               *
# ******************************************************************************
# This file sets key parameters and calls all source files to perform the      *
# analysis in replicable format.                                               *
#                                                                              *
# Structure:                                                                   *
# - Functions.R contains all helper functions defined in the project           *
# - CalcScores.R builds overreaction score files per stock (raw & excess ret.) *
# - BuildInstanceDB.R reads score files and builds instance data base          *
# - Analysis.R generates key analyses based on instance database               *
# ******************************************************************************


# ----- INIT -------------------------------------------------------------------
# load libraries
library(xts)                   # xts/zoo functions for time-stamped data
library(MASS)                  # rlm (robust linear model) function
library(quantmod)              # OHLC charting & technical analysis
library(chron)                 # chronolocigcal objects for date/time analysis
# library(mgcv)                  # Mixed GAM Computation Vehicle
                               # (glm extension allowing non-linear smoothing)
library(car)                   # scatterplot (enhanced X.Y plot) and
                               # scatterplotMatrix (enhanced pairs() function)
# library(tseries)               # add function jarque.bera.test for normality
# library("plyr")                # split-apply-combine tools
library(grDevices)             # output plots to file
# library(AID)                   # automatic BoxCox Power Transformation
library(moments)               # add skewness and kurtosis functions
library(plm)                   # panel analysis
library(lmtest)                # testing methods for linear models
# library(foreign)               # coplot method for group-wise plots over time
library(reshape)               # melt() function to reshape data frames
library(data.table)            # fast alternative to data.frame
library(lfe)                   # efficient fixed effects estimation
# library(biglm)                 # efficient lm for big datasets
library(gPdtest)               # tail index estimation
# library(gplots)                # group-based heterogeneity plots
library(XLConnect)
# library(TTR)                   # Parkinson (1980) vola estimate

# global vars and settings
eps <- 1e-10
setwd("D:/Doktorarbeit/30 Dissertation/25 Paper Finale/Model")  # project root
data.path <- "E:/Doktorarbeit/Taipan/4_barfill"
data.path.etf <- "E:/Doktorarbeit/Taipan/SPY"
# data.path.HFT <- "D:/Doktorarbeit/40 Data/HFT/3b_by-symbol-1min_TP"
data.path.HFT <- "C:/TMP_DA/3b_by-symbol-1min_TP"
data.path.out <- "Output/"
data.path.graph <- "Graphs/"
data.path.sf = "Output/"
rainbow.start <- 0.16
rainbow.end <- 0.66

# parameters for CalcScores.R
period.days       <- 60          # days
period.oneyear    <- 252         # days
period.id.window  <- 60          # minutes / bars
daybars           <- 391         # 1-min bars per day
day.start.time    <- "09:30:00"  # start of day time
day.end.time      <- "16:00:00"  # end of day time

# parameters for BuildInstanceDB
param.filt.rel   <- 6.0              # mom / daytime avg. vola
param.filt.abs   <- 2.0              # mom in percent
param.start.time <- "09:40:00"       # cut events before time
param.end.time   <- "15:50:00"       # cut events after time
param.start.date <- "2006-01-03"     # cut events before date
param.end.date   <- "2013-08-28"     # cut events after date
param.post.event.window <- 180       # length of post-event window in minutes
param.min.price  <- 5                # cut events < min price
                                     # $10: Zawadowski et al.
                                     # $5: no margin trading --> no day traders
                                     #     and Hendershott et al. 2010
param.max.price <- 1000              # $1000: Hendershott et al. 2010
param.min.liq    <- 20e6             # cut events < min liquidity
deactivate.SqRootAvg <- TRUE         # FALSE=sqrt(mean(x^2)) for vola ratio vars
                                     # TRUE=simple mean
# ----- SOURCES ----------------------------------------------------------------

source("R/Functions.R")              # get helper functions
options(error = expression(beep(2))) # beep on error
options(warn = 1)                    # print warnings as they occur
# source("R/CalcScores.R")             # calculate overreaction score --> files
# source("R/BuildInstanceDB.R")        # read score files and build instance DB
# source("R/BuildInstanceDB_HL.R")     # read score files and build instance DB
# source("R/BuildScorePanel1E.R")      # read score files and build score panel
# source("R/Analysis.R")               # generate key analyses and outputs

# # check memory usage 
for (i in seq_along(ls())) if (object.size(get(ls()[i]))/1000000 > 1)
  print (paste(ls()[i], " ---- ", object.size(get(ls()[i]))/1000000))

# # delete objects from workspace to save memory
# rm(events.master.up.df.filt)
# rm(events.master.up.df.final)
# rm(events.master.dn.df.filt)
# rm(events.master.dn.df.final)
# rm(test.events.up.df)
# rm(test.events.dn.df)
