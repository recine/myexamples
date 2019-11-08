# 
# this is a hack to automate my daily tasks
# 
from 	Tkinter import *
import random
import sqlite3
import subprocess
import os
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool 
from subprocess import Popen, PIPE
import sys
reload(sys)
sys.setdefaultencoding('utf8')


def sqlplusFunc2 (sqlstr):
	sqlplus2 = Popen(['sqlplus', '-S', 'stage/pword@99.99.999.99/prp_sv.corp.com'], stdout=PIPE, stdin=PIPE)
	sqlplus2.stdin.write(sqlstr)
	out , err = sqlplus2.communicate()
	out = out.strip()
	return out;

conn = sqlite3.connect('McTavish.db')
c = conn.cursor()

def sqllitfunc (litstr):
    for row in c.execute(litstr):
      print row	
    return;


# fixed text strings
greetings = ['hola', 'hello', 'hi', 'Hi', 'hey!','hey']
question = ['who are you?','how are you doing?','whats up?']
Tquestion = ['Tables', 'tables','tab','Tab']
responses = ['I can run sql  (select only) in case you are interested ;)','Um I am a machine yaknow... lol', 'Que?','seriuosly?']
toexit = ['Exit','exit','Quit','quit','Q','C','q','c','EXIT','QUIT']
valquestion = ['Val', 'val','v','V']
GGquestion = ['GG', 'gg']
Sesquestion = ['Su','SU','su','s','u','U']
unixcommand = ['ls -al','w','df -k']

# fixed sql 
valsql = """set head off;
			select to_char(time_stamp,'DAY'),
			error_code,substr(error_message,1,50) from load_errors 
			where time_stamp > sysdate-1 and substr(ERROR_MESSAGE,1,2) != 'GG'
			order by time_stamp desc;"""
GGsql =  """set head off;
			select to_char(time_stamp,'DAY'),
			error_code,substr(error_message,1,50) from load_errors 
			where time_stamp > sysdate-1 and substr(ERROR_MESSAGE,1,2) = 'GG'
			order by time_stamp desc;"""
Sessql = """set lines 500;
			SELECT substr(s.sid,1,3) sid,
			substr(s.status,1,10) status,
			substr(s.schemaname,1,10) who,
			substr(a.sql_text,1,35),
			p.program
			FROM   v$session s,
					v$sqlarea a,
					v$process p
			WHERE  s.SQL_HASH_VALUE = a.HASH_VALUE
			AND    s.SQL_ADDRESS = a.ADDRESS
			AND    s.PADDR = p.ADDR;"""
SqlTime = """set head off; 
             set feedback off;
			 select to_char(sysdate, 'DAY mm/dd/yyyy hh:mi:ss AM') from dual;"""

print (" ")
print ("MrMcTavish Ver .000000000001")
print (" ")
while True:
        userInput = raw_input("McTav>> ")
        if userInput in greetings:
                random_greeting = random.choice(greetings)
                print(random_greeting)
        elif userInput in question:
                random_response = random.choice(responses)
                print(random_response)
#                print(userInput)
        elif userInput in Tquestion:
				holdit = sqlplusFunc2("select table_name from user_tables where table_name like 'TEST%';")
				print holdit
        elif userInput.find('select') == 0:
				print " " 
				holdit = sqlplusFunc2(userInput)
				print holdit
				print " " 
        elif userInput.find('time') == 0:
				print " " 
				holdit = sqlplusFunc2(SqlTime)
				print holdit
				print " " 
        elif userInput.find('McTavish') == 0:
				print " " 
				sqllitfunc ("select * from brain where info like '%" +  userInput[9:] + "%'")
				print " " 
        elif userInput.find('?') == 0:
				print ("-- input valid sql select command at prompt")
				print ("-- type Tables - gives list of tables")
				print ("-- Val shows scheduled job verification")
				print ("-- GG GoldenGate verification")
				print ("-- Su shows active DB users")
				print ("-- type Unix + command for server command")
				print ("-- type Dos + command for local command")
				print ("-- type McTavish + keyword to ask McTavish something")
        elif userInput in valquestion:
				holdit = sqlplusFunc2(valsql)
				print holdit
        elif userInput in GGquestion:
				holdit = sqlplusFunc2(GGsql)
				print holdit
        elif userInput in Sesquestion:
				holdit = sqlplusFunc2(Sessql)
				print holdit
        elif userInput.find('unix') == 0:
				p = subprocess.Popen('plink stage -pw Iam50plus5! ' + userInput[5:], shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
				for line in p.stdout.readlines():
				  print line,
				retval = p.wait()
        elif userInput.find('dos') == 0:
				p = subprocess.Popen(userInput[4:], shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
				for line in p.stdout.readlines():
				  print line,
				retval = p.wait()
        elif userInput in toexit:
				sys.exit(1)
        else:
                print("I did not understand that, use ? for help -- McTavish")