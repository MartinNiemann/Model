# 1) FULL SAMPLE ---------------------------------------------------------------
suffix="all"

if (step.1) {
# perform stage 1 reg
print("--- 1st stage regression ---")
res1 <- felm (reg1.f, stocks.panel.pdf, exactDOF=T,
              clustervar=interaction(stocks.panel.pdf$date,
                                     stocks.panel.pdf$symbol))
res1.sum <- (summary(res1))
print(res1.sum)

res1b <- felm (reg1b.f, stocks.panel.pdf, exactDOF=T,
               clustervar=interaction(stocks.panel.pdf$date,
                                      stocks.panel.pdf$symbol))
res.w <- waldtest (res1, res1b, test="F")
print(res.w)

# build reg1 result vector
vec <- res.w$F[2]
names(vec) <- "F.INST"
vec <- AppendNamedVector(vec, res1.sum$rse, "rse")
vec <- AppendNamedVector(vec, res1.sum$r2, "r2")
vec <- AppendNamedVector(vec, res1.sum$r2adj, "r2a")
vec <- AppendNamedVector(vec, res1.sum$rdf, "rdf")

vec1 <- rbind("STAGE1", cbind(rownames(summary(res1)$coefficients), 
                              summary(res1)$coefficients), names(vec), vec)
}

# perform stage 2 reg
print("--- 2nd stage regression ---")
res2 <- felm (reg2.f, stocks.panel.pdf, iv=list(iv.f), exactDOF=T,
              clustervar=interaction(stocks.panel.pdf$date,
                                     stocks.panel.pdf$symbol))
res2.sum <- (summary(res2))
print(res2.sum)

# build output vector for model stats reg2
vec <- res2.sum$fstat
names(vec) <- "F"
vec <- AppendNamedVector(vec, res2.sum$rse, "rse")
vec <- AppendNamedVector(vec, res2.sum$r2, "r2")
vec <- AppendNamedVector(vec, res2.sum$r2adj, "r2a")
vec <- AppendNamedVector(vec, res2.sum$rdf, "rdf")

vec2 <- rbind( "STAGE2", cbind(rownames(summary(res2)$coefficients), 
                               summary(res2)$coefficients), names(vec), vec)

if (export.XL)
  writeWorksheetToFile (paste("Results_IVREG", ".xlsx", sep=""), 
                        rbind(vec1, vec2), paste(varname, "_", suffix, sep=""))  



# DEFINE SUBSET
subset.vec <- classes.size3 =="H"
stocks.panel.sub <- stocks.panel.pdf[subset.vec,]
suffix="H"

if (step.1) {
# perform stage 1 reg
print("--- 1st stage regression ---")
res1 <- felm (reg1.f, stocks.panel.sub, exactDOF=T,
              clustervar=interaction(stocks.panel.sub$date,
                                     stocks.panel.sub$symbol))
res1.sum <- (summary(res1))
print(res1.sum)

res1b <- felm (reg1b.f, stocks.panel.sub, exactDOF=T,
               clustervar=interaction(stocks.panel.sub$date,
                                      stocks.panel.sub$symbol))
res.w <- waldtest (res1, res1b, test="F")
print(res.w)

# build reg1 result vector
vec <- res.w$F[2]
names(vec) <- "F.INST"
vec <- AppendNamedVector(vec, res1.sum$rse, "rse")
vec <- AppendNamedVector(vec, res1.sum$r2, "r2")
vec <- AppendNamedVector(vec, res1.sum$r2adj, "r2a")
vec <- AppendNamedVector(vec, res1.sum$rdf, "rdf")

vec1 <- rbind("STAGE1", cbind(rownames(summary(res1)$coefficients), 
                              summary(res1)$coefficients), names(vec), vec)
}

# perform stage 2 reg
print("--- 2nd stage regression ---")
res2 <- felm (reg2.f, stocks.panel.sub, iv=list(iv.f), exactDOF=T,
              clustervar=interaction(stocks.panel.sub$date,
                                     stocks.panel.sub$symbol))
res2.sum <- (summary(res2))
print(res2.sum)

# build output vector for model stats reg2
vec <- res2.sum$fstat
names(vec) <- "F"
vec <- AppendNamedVector(vec, res2.sum$rse, "rse")
vec <- AppendNamedVector(vec, res2.sum$r2, "r2")
vec <- AppendNamedVector(vec, res2.sum$r2adj, "r2a")
vec <- AppendNamedVector(vec, res2.sum$rdf, "rdf")

vec2 <- rbind( "STAGE2", cbind(rownames(summary(res2)$coefficients), 
                               summary(res2)$coefficients), names(vec), vec)

if (export.XL)
  writeWorksheetToFile (paste("Results_IVREG", ".xlsx", sep=""), 
                        rbind(vec1, vec2), paste(varname, "_", suffix, sep="")) 

# DEFINE SUBSET
subset.vec <- classes.size3 =="M"
stocks.panel.sub <- stocks.panel.pdf[subset.vec,]
suffix="M"

if (step.1) {
# perform stage 1 reg
print("--- 1st stage regression ---")
res1 <- felm (reg1.f, stocks.panel.sub, exactDOF=T,
              clustervar=interaction(stocks.panel.sub$date,
                                     stocks.panel.sub$symbol))
res1.sum <- (summary(res1))
print(res1.sum)

res1b <- felm (reg1b.f, stocks.panel.sub, exactDOF=T,
               clustervar=interaction(stocks.panel.sub$date,
                                      stocks.panel.sub$symbol))
res.w <- waldtest (res1, res1b, test="F")
print(res.w)

# build reg1 result vector
vec <- res.w$F[2]
names(vec) <- "F.INST"
vec <- AppendNamedVector(vec, res1.sum$rse, "rse")
vec <- AppendNamedVector(vec, res1.sum$r2, "r2")
vec <- AppendNamedVector(vec, res1.sum$r2adj, "r2a")
vec <- AppendNamedVector(vec, res1.sum$rdf, "rdf")

vec1 <- rbind("STAGE1", cbind(rownames(summary(res1)$coefficients), 
                              summary(res1)$coefficients), names(vec), vec)
}

# perform stage 2 reg
print("--- 2nd stage regression ---")
res2 <- felm (reg2.f, stocks.panel.sub, iv=list(iv.f), exactDOF=T,
              clustervar=interaction(stocks.panel.sub$date,
                                     stocks.panel.sub$symbol))
res2.sum <- (summary(res2))
print(res2.sum)

# build output vector for model stats reg2
vec <- res2.sum$fstat
names(vec) <- "F"
vec <- AppendNamedVector(vec, res2.sum$rse, "rse")
vec <- AppendNamedVector(vec, res2.sum$r2, "r2")
vec <- AppendNamedVector(vec, res2.sum$r2adj, "r2a")
vec <- AppendNamedVector(vec, res2.sum$rdf, "rdf")

vec2 <- rbind( "STAGE2", cbind(rownames(summary(res2)$coefficients), 
                               summary(res2)$coefficients), names(vec), vec)

if (export.XL)
  writeWorksheetToFile (paste("Results_IVREG", ".xlsx", sep=""), 
                        rbind(vec1, vec2), paste(varname, "_", suffix, sep="")) 

# DEFINE SUBSET
subset.vec <- classes.size3 =="L"
stocks.panel.sub <- stocks.panel.pdf[subset.vec,]
suffix="L"

if (step.1) {
# perform stage 1 reg
print("--- 1st stage regression ---")
res1 <- felm (reg1.f, stocks.panel.sub, exactDOF=T,
              clustervar=interaction(stocks.panel.sub$date,
                                     stocks.panel.sub$symbol))
res1.sum <- (summary(res1))
print(res1.sum)

res1b <- felm (reg1b.f, stocks.panel.sub, exactDOF=T,
               clustervar=interaction(stocks.panel.sub$date,
                                      stocks.panel.sub$symbol))
res.w <- waldtest (res1, res1b, test="F")
print(res.w)

# build reg1 result vector
vec <- res.w$F[2]
names(vec) <- "F.INST"
vec <- AppendNamedVector(vec, res1.sum$rse, "rse")
vec <- AppendNamedVector(vec, res1.sum$r2, "r2")
vec <- AppendNamedVector(vec, res1.sum$r2adj, "r2a")
vec <- AppendNamedVector(vec, res1.sum$rdf, "rdf")

vec1 <- rbind("STAGE1", cbind(rownames(summary(res1)$coefficients), 
                              summary(res1)$coefficients), names(vec), vec)
}

# perform stage 2 reg
print("--- 2nd stage regression ---")
res2 <- felm (reg2.f, stocks.panel.sub, iv=list(iv.f), exactDOF=T,
              clustervar=interaction(stocks.panel.sub$date,
                                     stocks.panel.sub$symbol))
res2.sum <- (summary(res2))
print(res2.sum)

# build output vector for model stats reg2
vec <- res2.sum$fstat
names(vec) <- "F"
vec <- AppendNamedVector(vec, res2.sum$rse, "rse")
vec <- AppendNamedVector(vec, res2.sum$r2, "r2")
vec <- AppendNamedVector(vec, res2.sum$r2adj, "r2a")
vec <- AppendNamedVector(vec, res2.sum$rdf, "rdf")

vec2 <- rbind( "STAGE2", cbind(rownames(summary(res2)$coefficients), 
                               summary(res2)$coefficients), names(vec), vec)

if (export.XL)
  writeWorksheetToFile (paste("Results_IVREG", ".xlsx", sep=""), 
                        rbind(vec1, vec2), paste(varname, "_", suffix, sep="")) 