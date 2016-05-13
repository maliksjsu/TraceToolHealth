#!/usr/bin/env python
# -*- coding: utf-8 -*-
#title           :menu.py
#description     :This is a TRACE HEALTH TOOL 
#author          :
#date            :
#version         :0.1
#usage           :python menu.py
#notes           :
#python_version  :2.7.6  
#=======================================================================
 
# Import the modules needed to run the script

import time
import tracename
import connection
import sys, os
import pickle
import flask_sqlalchemy
import sqlalchemy
from sqlalchemy import select
import pymssql
import xlsxwriter
from datetime import datetime



MACHINE_LIST = ['clv1', 'clv2', 'clv3', 'hsw1', 'hsw3', 'hsw4', 'hsw5', 'hswb', 'hswp2',
                'jkt', 'jkt1', 'jkt2', 'jkt3', 'jkt4', 'jkt5', 'jkt6', 'nhm', 'nhm1', 'nhm2', 'nhm3', 'nhm4',
                'nhm9', 'noc1', 'noc2', 'noc3', 'noc5', 'quicktrace', 'snb1', 'vlv1']


conn = pymssql.connect("172.20.2.60:1433", "pttweb", "pttmysql", "TracingPipelinePilot")

global TOOLS

TRACERS= []
STARTDATES =[]
FAILED=[]
ENDDATES =[]
TRACERTOOL={}

# =======================
#     QUERY CLASS
# =======================
 
# Query Class

class RunQuery(object):
    
    def __init__ (self, query=None, start_date=None, finish_date=None, count=0, tracer_name=None):
        
        ''' This is a query tool that can be updated very easily
            all you have to do is all the query and make a new querXXXExecute Method
        '''
        
        self.query = query 
        self.count = count
        self.query_cursor = conn.cursor()
        self.trace_id = []
        self.trace_name = []
        self.tracer_tool = {}
        self.tools = []
        
        
        
        self.tools_failed=[]
        #TOOLS_FAILED.append(self.tools_failed)
        
        self.tracer_name= tracer_name
        TRACERS.append(self.tracer_name)
        
        self.start_date = start_date
        STARTDATES.append(start_date)
        
        self.finish_date = finish_date
        ENDDATES.append(finish_date)
       
        #This query takes start and finish date to generate failed error.
        # Important to keey the JOB.name and JOB_STAGE.job_id at rows[0] and rows[2] 
        # for the parse method
        
        self.query_date =    "SELECT [JOB].[name], [JOB_STAGE].[code], [JOB_STAGE].[job_id], [JOB].[finished_time]"\
                             "FROM [JOB_STAGE] INNER JOIN [JOB]" \
                             "ON [JOB].[id]= [JOB_STAGE].[job_id]"\
                             "AND [JOB_STAGE].[code] = 'FAIL'"\
                             "AND [JOB].[finished_time] >= %s"\
                             "AND [JOB].[finished_time] <= %s "
   
    def queryExecute(self,query):
        
        # Method to run the query and return rows    
        # it can take your query from outside for testing. 
        
        self.query_cursor.execute(query)
        self.rows = self.query_cursor.fetchone()
        return self.rows
    
    def queryDateExecute(self):
        #Method to run the query with two variable input for start and finish dates
        
        self.query_cursor.execute(self.query_date, (self.start_date, self.finish_date))
        self.rows = self.query_cursor.fetchone()
        return self.rows
    
    def printQuery(self):
        #method for just printing the query without any parsing
        #this is more for test purposes for a new query design. 
        
        if self.start_date or self.finish_date:
            self.queryDateExecute()
        else:
            self.queryExecute()
            
        for self.row in self.query_cursor:
            print (self.row)
            self.count = self.count +1
            
        print (self.count)
        
    def parse(self):
        # This is parsing through the tracename.py file. it takes each trace name and 
        # Appends the trace id's that are required from the trace_input
        
        count=0
        self.trace_id = []
        self.trace_name = []
        print ("--------------------------------------------")
        print ("\nRunning the tracer: " + self.tracer_name)
        print ("From Date: " + self.start_date)
        print ( "TO:" + self.finish_date)
        if self.start_date or self.finish_date:
            self.queryDateExecute()
        else:
            self.queryExecute()
        
        while self.rows is not None:    
            trcobj = tracename.TraceNameParser(self.rows[0])
            if trcobj.get_machine_name() == self.tracer_name:
                count = count+1
                self.trace_id.append(self.rows[2])                            # will store the trace [JOB_STAGE].[job_id]
                self.trace_name.append(self.rows[0])                          # will store the [JOB]. Name
            self.rows = self.query_cursor.fetchone()
    
        print ("\n\tNo of failed traces: %d" % count )      
        FAILED.append(count)
        self.genrateReport()
        
    
    def genrateReport(self):
        # Once the parsing is done finiding all the trace id's for the tracer 
        # This method will then run query for each id to find the trace tools that have failed 
        global TOOLS
        global TOOLS_FAILED
        print ("\nGenerating the summary of the traces")
        print ("--------------------------------------------")
        failed_code = []
        
        for id in self.trace_id:
                
            query_tool = "SELECT [JOB_STAGE].code from JOB_STAGE where job_id = {0} and stage_seq_id = " \
                         "(Select stage_seq_id from JOB_STAGE where job_id = {0} and code = 'FAIL') - 1".format(id)
            self.queryExecute(query_tool)
            #For all the id of the traces wit the relevant trace names failed_code will
            # have the required 
            failed_code.append(self.rows)        
                
        # Taking out all the duplicates
        # failed code still has all the duplicates meaning the number of times failed.
        uniq_failed_code = set(failed_code)
        
        # converting failed_code from dictionary to a list of lists
        failed_code = [list(i) for i in failed_code]
        
        # converting failed_code from list of lists to a list. 
        tmp_code = []
        jeera_code = []
        
        for i in failed_code:
            y = (',' .join(i))
            jeera_code.append(y)
    
        failed_code = jeera_code
        
        # TRACE TOOL WIL MAKE A NESTED DICTONARY FOR ALL THE FAILED TOOLS WITH THE TRACER NAME
        TRACERTOOL[self.tracer_name] = {}
        #TOOLS = []
        from collections import Counter
        
        #This code will count the number of duplicates and print out the number of duplicates
        for k,v in Counter(failed_code).most_common():        
            #TOOLS.append(k)
            self.tracer_tool[k] = v 
            TRACERTOOL[self.tracer_name][k]=v
            print ("{} -> {}".format(k,v))
        
         
        #TOOLS = sorted(TOOLS)
        #This code is to sort the dictionary alphabetically and then make two lists. 
        #one for the tool name and the failed numbers to make rows in the excell sheet
        temp = []
        tmp = []
        dictList=[]
        
        for key , value in sorted(TRACERTOOL[self.tracer_name].items()):
            temp = [key, value]
            dictList.append(temp)
        
        #print (dictList) (has two lists alphabetically 
   
        #splitting the list in to      
        for inner_l in dictList:
            for item in inner_l:
                tmp.append(item)
        
        #tool will have the 
        self.tools = (tmp[::2])
        self.tools_failed= (tmp[1::2]) 
        
        TOOLS = self.tools
        TOOLS_FAILED = self.tools_failed     
        
        print (self.tools)
        print (self.tools_failed)
                
        uniq_failed_code = list(set(failed_code))
        
        print ("--------------------------------------------")
        print ("Common failing tracer tools on " + str(self.tracer_name)) 
        
        print (uniq_failed_code)
        print (self.tracer_tool)
        
        #TOOLS = uniq_failed_code
        
        len_failed_code = len(failed_code)
        
        return self.tools
        
    
                
        
def generateQuarterReport():
    
    orig_stdout = sys.stdout
    xl_Sheet_Dir = "C:/Users/Administrator/Documents/tool_Health_Report/TOOL-Health_Report-%Y%m%d-%H%M%S.xlsx"
    file_name = datetime.now().strftime(xl_Sheet_Dir)
    
    file_Dir = "C:/Users/Administrator/Documents/tool_Health_Report/TOOL-Health_Report-%Y%m%d-%H%M%S.txt"
    file_name_1 = datetime.now().strftime(file_Dir)
    f = open(file_name_1, "w")
    sys.stdout = f 
    
    
    
    print (file_name)
    
    if os.path.exists(file_name):
        os.remove(file_path)
            
    workbook = xlsxwriter.Workbook(file_name)
    worksheet = workbook.add_worksheet()
    
    worksheet.set_column('A:A', 20)
    worksheet.write('A1', 'THIS IS AN AUTOMATIC REPORT GENERATED BY THE TRACE HEALTH TOOL at $d')
    data = ()
    data = tuple(TRACERS)
    x = ['1/1/2014', '3/3/2014' ]
    y = [ '3/3/2014', '6/6/2014']
    u = 0
    z= []
    for machines in MACHINE_LIST:
       worksheet.write_column('A3', TRACERS)
       #worksheet.write_column('A4', z)
       worksheet.write_column('B3', STARTDATES)
       worksheet.write_column('C3', ENDDATES)
       worksheet.write_column('D3', FAILED)
       
       
       #if TOOLS:
           #worksheet.write_column('E3', TOOLS)
       
       #worksheet.write_row('E2', TRACERS)
       
       #worksheet.write_column('E4' ,dictList)
       
       for i , j in zip(x,y):
           g = RunQuery(start_date=i, finish_date=j, tracer_name=machines).parse()
           print ("xxxxxxxxxxxxxxx")
           worksheet.write_column('E2', TOOLS)
           print (TOOLS)
           print (TOOLS_FAILED)
    
    
    sys.stdout = orig_stdout
    
    f.close()
    workbook.close() 
def tracerSelect():
   
    global trace_input                   
    
    print ("Available Tracers\n")
    print ("--------------------------------------------------------------------")
    for a, b, c in zip (MACHINE_LIST[::3], MACHINE_LIST[1::3], MACHINE_LIST[2::3]):
        print ('{:<30}{:<30}{:<}'. format(a,b,c))
    
    while True:
        print ("--------------------------------------------------------------------")            
        trace_input = input("\nEnter the tracer initials: ")
        
        if trace_input in MACHINE_LIST:
            
            return trace_input
        else:
            print("Invalid Input")

        break
    
   
# Lists out all the tracers
def queryInterface(tracer_name):
    while True:
        print ("\n--------------------------------------------")
        print ('\tSelect the Query Type')
        #worksheet.write('A2', tracer_name)
        print ('\tYou have selected: ' + tracer_name)
        print ('\n\t\t 1. Search with Date ')
        print ('\t\t 2. Select a different Tracer')
        print ('\t\t 3. Generate Quarterly report')
        print ('\t\t 0. EXIT')
        print ("--------------------------------------------")
        query_choice = int(input('Enter 1 or 0: '))
    
        if query_choice == 1:
            
            start_date = input('\n\tEnter Start Date: ')
            finish_date = input('\tEnter End Date: ')
            
        
            RunQuery(start_date=start_date, finish_date=finish_date, tracer_name=tracer_name).parse()
            #go = RunQuery(query, selected_date).printQuery()
            #yo = RunQuery(query).queryExecute(tracer_name)
        if query_choice == 2:
            
            menu1()
        
        if query_choice == 3:
            
            menu1()
        
        if query_choice == 0:
            
            main_menu()
            
        
    sys.exit(0)


# =======================
#     MENUS FUNCTIONS
# =======================
 
# Main menu

def main_menu():
    
    os.system(['clear', 'cls'][os.name == 'nt'])
    
    print ("Welcome,\n")
    print ("Please choose the menu you want to start:")
    print ("1. Tracer Health")
    print ("2. Trace Tool Health")
    print ("\n0. Quit")
    choice = input(" >>  ")
    exec_menu(choice)
    return

# Execute menu
def exec_menu(choice):
    os.system(['clear', 'cls'][os.name == 'nt'])
    ch = choice.lower()
    if ch == '':
        menu_actions['main_menu']()
    else:
        try:
            menu_actions[ch]()
        except KeyError:
            print ("Invalid selection, please try again.\n")
            menu_actions['main_menu']()
    return
 
# Menu 1
def menu1():
    os.system(['clear', 'cls'][os.name == 'nt'])
    print ("TRACER HEALTH!\n")
    tracerSelect()
    queryInterface(trace_input)
    print ("9. Back")
    print ("0. Quit")
    choice = input(" >>  ")
    exec_menu(choice)
    return
 
 
# Menu 2
def menu2():
    print ("Hello Menu 2 !\n")
    print ("yo")
    print ("9. Back")
    print ("0. Quit")
    choice = input(" >>  ")
    exec_menu(choice)
    return
 
# Back to main menu
def back():
    menu_actions['main_menu']()
 
# Exit program
def exit():
    print (TRACERS, STARTDATES, ENDDATES, TRACERTOOL)
    sys.exit()
    

# =======================
#    MENUS DEFINITIONS
# =======================
 
# Menu definition
menu_actions = {
    'main_menu': main_menu,
    '1': menu1,
    '2': menu2,
    '9': back,
    '0': exit,
}


# =======================
#      MAIN PROGRAM
# =======================

# Main Program
if __name__ == "__main__":
    #print ("TESTING")
    
    # Launch main menu
    generateQuarterReport()
    #main_menu()
    