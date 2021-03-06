import datetime
import time
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool 
import os
from subprocess import Popen, PIPE

def sqlplusFunc2 (sqlstr):
	sqlplus2 = Popen(['sqlplus', '-S', 'place01/pword@99.99.999.99/name.name.name.com'], stdout=PIPE, stdin=PIPE)
#	sqlplus2 = Popen(['sqlplus', '-S', 'place01/pword'], stdout=PIPE, stdin=PIPE)
	sqlplus2.stdin.write(sqlstr)
	out , err = sqlplus2.communicate()
#	sqlplus2.stdin.write("exit")
	holdout = out.replace('"', '')
	holdout = holdout.replace('PLACE01.','')
	print holdout
	return;
	
print "Current time *************  START ****************" + time.strftime("%X")

pool = ThreadPool(3) 
	  
thesql = [
		"set long 9000;\n set head off;\n select substr(dbms_metadata.get_ddl('TABLE','UPD_PRI_TAB','STAGE01'),1,((instr(dbms_metadata.get_ddl('TABLE','UPD_PRI_TAB','STAGE01'), 'SEGMENT')-1)))  from dual;",
		"set long 9000;\n set head off;\n select substr(dbms_metadata.get_ddl('TABLE','PRI_TAB','STAGE01'),1,((instr(dbms_metadata.get_ddl('TABLE','PRI_TAB','STAGE01'), 'SEGMENT')-1)))  from dual;",
		"set long 9000;\n set head off;\n select substr(dbms_metadata.get_ddl('TABLE','RESULT_SET','STAGE01'),1,((instr(dbms_metadata.get_ddl('TABLE','RESULT_SET','STAGE01'), 'SEGMENT')-1)))  from dual;"
	  ]

results = pool.map(sqlplusFunc2, thesql)


print "Current time *************  DONE **************** " + time.strftime("%X")
pool.close() 
pool.join() 




