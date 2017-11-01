# README #

### What is this repository for? ###

The goal of this exercise is to compute, for each year in the dataset, which sector had the highest number 
of combined operations. The output line must mention the year, name of the sector, and global aggregate of 
operations. In order to make the exercise more straightforward, we will only compute in the base part for each 
year, the total number of operations per company. Sample output (not based on the real data) would look similar to:

Finance,1996,20090342 
Pharma,1996,12312312
Finance,1997,25612312

### Task ###

Job 1: Takes /data/NASDAQseq, /data/companyList.tsv datasets. Join them to obtain for each record 'sector', 
'daily_stock_volume', and 'year'.

### To build the project run:  ###

### ant clean dist ###
To run the task on the server:
### hadoop jar dist/StockYearJoin.jar StockYearJoin /data/NASDAQseq out ###
