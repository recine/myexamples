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
# output hive file
fname0 = "C:\\Users\\P2731968\\data lake\\etl\\hive_"+param_1+".txt"
fname1 = "C:\\Users\\P2731968\\data lake\\etl\\spark_"+param_1+".txt"
fname2 = "C:\\Users\\P2731968\\data lake\\etl\\explain_"+param_1+".txt"
hivef = open(fname0,'w')
sparkf = open(fname1,'w')
explainf = open(fname2,'w')
#
first_row = True
# this is the place where you change the input file with new DWH drop 
# on unix run gencol_***.py then clean_file.sh creating csv file of all tables and columns 
# load into sqlite by this
df = pandas.read_csv("C:\Users\P2731968\data lake\\all_24.csv")
df.to_sql("holdallcol", conn, if_exists='replace', index=False)
## use sqlight db to write a csv file
# ddf = pandas.read_sql_query("select table_name from holdallcol", conn)
# ddf.to_csv("C:\Users\P2731968\data lake\\tables.csv") 
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
sql1 = "select min(ind_num), table_name,column_name from (select ind_num, table_name,column_name from holdallcol where table_name in ( \
select table_name from holdallcol where column_name in (select column_name from holdallcol where table_name = '"+param_1+"' and column_name like 'dim%' \
) and table_name like 'xbi_dim%' ) union all select ind_num, table_name,column_name from holdallcol  where table_name = '"+param_1+"'  \
) where table_name like 'xbi_dim%' group by column_name"
#
sql2 = "select distinct table_name from holdallcol where column_name in (select column_name from holdallcol where table_name = '"+param_1+"' and column_name like 'dim%') and table_name like 'xbi_dim%'"
#
sql3 = "select '"+param_1+".'||column_name||' = '||table_name||'.'||column_name from holdallcol where column_name in \
(select column_name from holdallcol where table_name = '"+param_1+"' and column_name like 'dim%') and table_name like 'xbi_dim%'"
# for the last field name to omit comma 
holdrowsql1 = 0
for row in c.execute(sql1): holdrowsql1 = holdrowsql1+1
print "fields "
print holdrowsql1    # debug info
# for the last table name to omit comma 
holdrowsql2 = 0
for row in c.execute(sql2): holdrowsql2 = holdrowsql2+1
print "tables "
print holdrowsql2    # debug info
# -----------------------
# Start the explain file
# -----------------------
explainf.write("use "+param_2+";\n")
explainf.write(" \n")
explainf.write("EXPLAIN \n")
# fields in the select for explain
lastone = 0
explainf.write("select \n")
for row in c.execute(sql1): 
	lastone = lastone+1
	if lastone == holdrowsql1: explainf.write(row[1]+"."+row[2]+" \n")
	else: explainf.write(row[1]+"."+row[2]+", \n")
# from clause for explain
explainf.write("from \n")
explainf.write(param_2+"."+param_1+",\n")
lastone = 0
for row in c.execute(sql2):  
	lastone = lastone+1
	if lastone == holdrowsql2: explainf.write(param_2+"."+row[0]+"\n")
	else: explainf.write(param_2+"."+row[0]+",\n")
# where clause for explain
explainf.write("where \n")
for row in c.execute(sql3):
	 if first_row:
		explainf.write(row[0]+"\n")
		first_row = False
	 else:
		explainf.write("and\n"+row[0]+"\n")
explainf.write("; \n")
# -----------------------
# end the explain file
# -----------------------
#
first_row = True
lastone = 0
# ---------------------
# Start the hive file
# ---------------------
hivef.write("use "+param_2+";\n")
# table create  for hive
hivef.write("create external table tda_xbi_flat.flat_"+param_1+"\n")
hivef.write("(\n")
lastone = 0
for row in c.execute(sql1): 
	lastone = lastone+1
	if lastone == holdrowsql1: hivef.write(row[2]+" string \n")
	else: hivef.write(row[2]+" string, \n")
hivef.write(")\n")
hivef.write("ROW FORMAT DELIMITED\n")
hivef.write("FIELDS TERMINATED BY ','\n")
hivef.write("STORED AS ORC;\n")
hivef.write(" \n")
hivef.write("INSERT OVERWRITE TABLE tda_xbi_flat.flat_"+param_1+"\n")
# fields in the select for hive
lastone = 0
hivef.write("select \n")
for row in c.execute(sql1): 
	lastone = lastone+1
	if lastone == holdrowsql1: hivef.write(row[1]+"."+row[2]+" \n")
	else: hivef.write(row[1]+"."+row[2]+", \n")
# from clause for hive
hivef.write("from \n")
hivef.write(param_2+"."+param_1+",\n")
lastone = 0
for row in c.execute(sql2):  
	lastone = lastone+1
	if lastone == holdrowsql2: hivef.write(param_2+"."+row[0]+"\n")
	else: hivef.write(param_2+"."+row[0]+",\n")
# where clause for hive
hivef.write("where \n")
for row in c.execute(sql3):
	 if first_row:
		hivef.write(row[0]+"\n")
		first_row = False
	 else:
		hivef.write("and\n"+row[0]+"\n")
hivef.write("; \n")
# ------------------
# end the hive file
# -----------------
#
first_row = True
lastone = 0
# ---------------------
# Start the spark file
# ---------------------
sparkf.write('df = spark.sql("select  \\\n')
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
sparkf.write('df.createOrReplaceTempView("hold_'+param_1+'")\n')
sparkf.write('spark.sql("create table tda_xbi_flat.flat_'+param_1+' STORED AS ORC as select * from hold_'+param_1+'")\n')
# ---------------------
# End the spark file
# ---------------------
#
# -------- debug help -----
#
# testsql1 = "SELECT column_name, COUNT(*) FROM holdallcol where table_name like 'xbi_dim%' GROUP BY column_name HAVING COUNT(*) > 1"
# for row in c.execute(testsql1): print row[0], row[1]
#
# -----------------------
conn.close()
hivef.close
sparkf.close
explainf.close