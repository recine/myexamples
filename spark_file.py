#
# This program is designed to be a code generater for pyspark. The problem was the source sql created dataframes that contained 100's of fields and 100 of millions of rows. 
# This python code genrated the pyspark code to create the sql, place it into a data frame and create a table. 
# its a hack :) 
# 
import sqlite3
import pandas 
import os
import sys 
# conn = sqlite3.connect('example.db')
conn = sqlite3.connect(":memory:")
c = conn.cursor()
# line input 
param_1= sys.argv[1] 
param_2= sys.argv[2] 
# output spark file
fname1 = "C:\\Users\\P2731968\\data lake\\etl\\spark_"+param_1+".txt"
sparkf = open(fname1,'w')
#
first_row = True
# this is the place where you change the input file with new DWH drop 
# on unix run gencol_***.py then clean_file.sh creating csv file of all tables and columns 
# load into sqlite by this
df = pandas.read_csv("C:\Users\P2731968\data lake\\gencol_all.csv")
df.to_sql("holdallcol", conn, if_exists='replace', index=False)
## use sqlight db to write a csv file
ddf = pandas.read_sql_query("select distinct table_name from holdallcol", conn)
ddf.to_csv("C:\Users\P2731968\data lake\\tables.csv") 
# index
c.execute("update holdallcol set ind_num = rowid")
conn.commit()
# clean data 
c.execute("update holdallcol set column_name = rtrim(column_name)")
conn.commit()
#
#######################################################################################################################
####  issue with table xbi_dimdate, it is a lookup not a dimention, so I delete it from the table creation table ######
#######################################################################################################################
c.execute("delete from holdallcol where table_name = 'xbi_dimdate'")
conn.commit()
#
# create table delete dupplicate fields
#
c.execute("create table reshold as select min(ind_num), table_name,column_name from (select ind_num, table_name,column_name from holdallcol where table_name in (select table_name from holdallcol where column_name in (select column_name from holdallcol where table_name = 'xbi_fctacctblgprdsumslsmth' and column_name like 'dim%') and table_name like 'xbi_dim%' ) union all select ind_num, table_name,column_name from holdallcol  where table_name = '"+param_1+"') where table_name like 'xbi_dim%' group by column_name")
conn.commit()
#
sql1 = "select min(ind_num), table_name,column_name from (select ind_num, table_name,column_name from holdallcol where table_name in ( \
select table_name from holdallcol where column_name in (select column_name from holdallcol where table_name = '"+param_1+"' and column_name like 'dim%' \
) and table_name like 'xbi_dim%' ) union all select ind_num, table_name,column_name from holdallcol  where table_name = '"+param_1+"'  \
) where table_name like 'xbi_dim%' group by column_name order by column_name"
#
sql11 = "select distinct table_name,column_name from holdallcol where table_name = '"+param_1+"' and column_name not in (select column_name from reshold) order by column_name" 
#
sql2 = "select distinct table_name from holdallcol where column_name in (select column_name from holdallcol where table_name = '"+param_1+"' and column_name like 'dim%') and table_name like 'xbi_dim%'"
#
sql3 = "select distinct '"+param_1+".'||column_name||' = '||table_name||'.'||column_name from holdallcol where column_name in \
(select column_name from holdallcol where table_name = '"+param_1+"' and column_name like 'dim%') and table_name like 'xbi_dim%'"
#
# table report
#
print "-------------------------------\n"
print param_1+"\n"
# this counter for the last field name to omit comma of the dim tables
holdrowsql1 = 0
for row in c.execute(sql1): holdrowsql1 = holdrowsql1+1
print "fields dim tables"
print holdrowsql1    # debug info
# for the last field of the main table do not need to omit comma
holdrowsql11 = 0
for row in c.execute(sql11): holdrowsql11 = holdrowsql11+1
print "fields main table "
print holdrowsql11    # debug info
# for the last table name to omit comma 
holdrowsql2 = 0
for row in c.execute(sql2): holdrowsql2 = holdrowsql2+1
print "dim tables not including main"
print holdrowsql2    # debug info
print "-------------------------------\n"
print " \n" 
first_row = True
lastone = 0
# ---------------------
# Start the spark file
# ---------------------
sparkf.write('spark.catalog.clearCache()\n')
sparkf.write('df = spark.sql("select  \\\n')
# the main table fields
for row in c.execute(sql11): sparkf.write(row[0]+"."+row[1]+", \\\n")
# the dim tables fields
for row in c.execute(sql1): 
	lastone = lastone+1
	if lastone == holdrowsql1: sparkf.write(row[1]+"."+row[2]+" \\\n")
	else: sparkf.write(row[1]+"."+row[2]+", \\\n")
# from clause
lastone = 0
sparkf.write("from \\\n")
sparkf.write(param_2+"."+param_1+", \\\n")
for row in c.execute(sql2):  
	lastone = lastone+1
	if lastone == holdrowsql2: sparkf.write(param_2+"."+row[0]+" \\\n")
	else: sparkf.write(param_2+"."+row[0]+", \\\n")
# where clause 
sparkf.write("where \\\n")
for row in c.execute(sql3):
	 if first_row:
		sparkf.write(row[0]+" \\\n")
		first_row = False
	 else:
		sparkf.write("and \\\n"+row[0]+" \\\n")
sparkf.write('")\n')
sparkf.write('df.cache()\n')
sparkf.write('df.write.format("orc").saveAsTable("tda_xbi_flat.flat_'+param_1+'")\n')
# ---------------------
# End the spark file
# ---------------------
#
# -------- debug help -----
#
print ("-- duplicate column names --\n")
c.execute("create table dim_col as select column_name from (select ind_num, table_name,column_name from holdallcol where table_name in ( \
select table_name from holdallcol where column_name in (select column_name from holdallcol where table_name = '"+param_1+"' and column_name like 'dim%' \
) and table_name like 'xbi_dim%' ) union all select ind_num, table_name,column_name from holdallcol  where table_name = '"+param_1+"'  \
) where table_name like 'xbi_dim%' group by column_name order by column_name")
conn.commit()
c.execute("create table main_col as select column_name from holdallcol where table_name = '"+param_1+"' and column_name not in (select column_name from reshold)")
conn.commit()
solvit = "select dim_col.column_name from main_col,dim_col where main_col.column_name = dim_col.column_name"
for row in c.execute(solvit): print row[0]
# -----------------------
# use sed to delete dups
# -----------------------
# for row in c.execute(solvit): print "sed -i '0,/"+row[0]+"/{/"+row[0]+"/d;}'"+" spark_"+param_1+".txt"
# for row in c.execute(solvit): print  'sed -i "/'+row[0]+'/{x;/'+row[0]+'/!{x;h;b;};x;d}"'+' spark_'+param_1+'.txt'
# code into BAT file
clu = "C:\\Users\\P2731968\\data lake\\etl\\cleanup_dubs_"+param_1+".bat"
cluf = open(clu,'w')
for row in c.execute(solvit): cluf.write('sed -i "/'+row[0]+'/{x;/'+row[0]+'/!{x;h;b;};x;d}"'+' spark_'+param_1+'.txt\n')
cluf.write('del sed*\n')
print (" ")
print ("---- bat file created -----\n")
# ----------------------------
conn.close()
sparkf.close
cluf.close
