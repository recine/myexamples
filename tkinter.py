from 	Tkinter import *
from math import *
import subprocess
import os
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool 
from subprocess import Popen, PIPE
import sys
reload(sys)
sys.setdefaultencoding('utf8')

def sqlplusFunc2 (sqlstr):
	sqlplus2 = Popen(['sqlplus', '-S', 'stage01/Xtransfer1@67.48.243.13/prism03p_svc.corp.chartercom.com'], stdout=PIPE, stdin=PIPE)
	sqlplus2.stdin.write(sqlstr)
	out , err = sqlplus2.communicate()
	out = out.strip()
	return out;
	
def  textbxx():
    T.insert(END, e1.get() + '\n')
#    print e1.get()
    holdit = sqlplusFunc2(e1.get())
#    print holdit
    T.insert(END, holdit + '\n')

root = Tk()
S = Scrollbar(root)
T = Text(root, height=20, width=150)
S.pack(side=RIGHT, fill=Y)
T.pack(side=LEFT, fill=Y)
S.config(command=T.yview)
T.config(yscrollcommand=S.set)

master = Tk()
Label(master, text="Enter: ").grid(row=0)
e1 = Entry(master)
e1.grid(row=0, column=1)
e1.bind("<Return>", textbxx)
Button(master, text='Quit', command=master.destroy).grid(row=3, column=0, sticky=W, pady=4)
Button(master, text='Show', command=textbxx).grid(row=3, column=1, sticky=W, pady=4)

master.eval('tk::PlaceWindow %s center' % master.winfo_pathname(master.winfo_id()))
mainloop()
e1 = ' ' 

# textbxx('test')







