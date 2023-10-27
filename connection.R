library(dplyr)
library(tidyverse)
library(DBI)
library(odbc)


old_files<-list.files("c:/WELLDDATA",full.names = TRUE)
typeof(old_files)

new_files<-unlist(lapply(old_files,function(x) paste0(strsplit(strsplit(x,'_2')[[1]][1],'.csv')[[1]][1],'.csv')))
typeof(new_files)

file.copy(from=old_files,to=new_files)



if(all(old_files != new_files)){
  file.remove(old_files)
}




con <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "ccnyss2", Database = "WELLD", Trusted_Connection = "True")
dataloader <- function(con,file){
  x<-readLines(file,n=1)
  if(length(grep(",", x))>0){
    data<-read.csv(file,skip=0,check.names=FALSE)
  }else{
    data<-read.csv(file,skip=1,check.names=FALSE)
  }
  tablename<- strsplit(strsplit(file,'.csv')[[1]][1],"c:/WELLDDATA/")[[1]][2]
  print(tablename)
  dbWriteTable(
    con,
    name=tablename,
    value=data,
    overwrite = TRUE,
    row.names = FALSE)
  return(0)
}


j<-lapply(new_files,function(x) dataloader(con,x))

DBI::dbDisconnect(con)
