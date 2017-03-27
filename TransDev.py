# -*- coding: utf-8 -*-
"""
Created on Sun Mar 19 21:24:59 2017

@author: jrbrad
"""

import MySQLdb as mySQL 
#import datetime

mysql_user_name =  'user_name'
mysql_password = 'passwordL'
mysql_ip = '127.0.0.1'
mysql_db = 'assign'

trail_cu_ft = 4000.0
num_days_year = 365.0

def db_get_data(problem_id):
    cnx = db_connect()
                        
    cursor = cnx.cursor()
    cursor.execute("CALL spGetBinpackCap(%s);" % problem_id)
    bin_cap = cursor.fetchall()[0][0]
    cursor.close()
    cursor = cnx.cursor()
    cursor.execute("CALL spGetBinpackData(%s);" % problem_id)
    items = {}
    blank = cursor.fetchall()
    for row in blank:
        items[row[0]] = row[1]
    cursor.close()
    cnx.close()
    return bin_cap, items

def getDBDataList(commandString):
    cnx = db_connect()
    cursor = cnx.cursor()
    cursor.execute(commandString)
    items = []
    for item in list(cursor):
        sub_list = []
        for ele in item:
            sub_list.append(ele)
        items.append(sub_list)
    cursor.close()
    cnx.close()
    return items
    
def getDBDataListEle(commandString):
    cnx = db_connect()
    cursor = cnx.cursor()
    cursor.execute(commandString)
    items = []
    for item in list(cursor):
        items.append(item[0])
    cursor.close()
    cnx.close()
    return items
    
def getDBDataDictEle(commandString):
    cnx = db_connect()
    cursor = cnx.cursor()
    cursor.execute(commandString)
    items = {}
    for item in list(cursor):
        items[item[0]] = item[1]
    cursor.close()
    cnx.close()
    return items
    
def getDBDataDict(commandString):
    cnx = db_connect()
    cursor = cnx.cursor()
    cursor.execute(commandString)
    items = {}
    for item in list(cursor):
        items[item[0]] = item[1:len(item)]
    cursor.close()
    cnx.close()
    return items
    
def getDBDataDictTup(commandString):
    cnx = db_connect()
    cursor = cnx.cursor()
    cursor.execute(commandString)
    items = {}
    for item in list(cursor):
        items[(item[0],item[1])] = item[2]
    cursor.close()
    cnx.close()
    return items

def db_connect():
    cnx = mySQL.connect(user=mysql_user_name, passwd=mysql_password,
                        host=mysql_ip, db=mysql_db)
    return cnx
    
def trans(dist, dcs, stores_vol):

    my_team_or_name = "xxx"
    result = []
    
    """
    Put your algorithm here
    """
            
    return my_team_or_name, result
    
def checkDCCap(dcs,stores_vol,result):
    checkit = {}
    store_v_tmp = {}
    err_dc_constr = False
    #num_constrs = len(dcs[dcs.keys()[0]])
    for item in stores_vol:
        store_v_tmp[item[0]] = item[1]
    for key in dcs.keys():
        checkit[key] = [0.0,0,0]
    for ele in result:
        checkit[ele[1]][0] += store_v_tmp[ele[0]]                # cu feet of volume
        checkit[ele[1]][1] += 1                                  # number of doors
        checkit[ele[1]][2] += store_v_tmp[ele[0]] / trail_cu_ft  # number of drivers per day
    for i in range(len(dcs)):
        err_dc_constr = err_dc_constr or (checkit[i][0] > dcs[i][0]) or (checkit[i][1] > dcs[i][1]) or (checkit[i][2] > dcs[i][2])
        #print
        #print (checkit[i][0],checkit[i][1],checkit[i][2]),dcs[i]
    return err_dc_constr
    
def checkUniqueAssign(store_ids,dc_ids,result):
    """ Create a dictionary frequency histogram of the DC data in result and ensure that the 
    length of the dictionary matches the length of dc_ids and that each key in the dictionary is in dc_ids"""
    err_dc_key = False 
    err_store_key = False 
    err_mult_assign = False
    err_store_not_assign = False
    err_mess = ""
    checkit = {}
    for ele in result:
        checkit[ele[1]] = checkit.get(ele[1],0)+1
    for this_key in checkit.keys():
        if this_key not in dc_ids:
            err_dc_key = True
            err_mess = "Invalid DC key"
    checkit.clear()
    
    """ Create a dictionary frequency histogram of the Store data in result and ensure that the 
    length of the dictionary matches the length of store_ids and that each key in the dictionary is in store_ids
    and each key is mentioned only once """
    for ele in result:
        checkit[ele[0]] = checkit.get(ele[0],0)+1        
    for this_key in checkit.keys():
        if this_key not in store_ids:
            err_store_key = True
            err_mess += "_Invalid Store key"
        if checkit[this_key] > 1:
            err_mult_assign = True
            err_mess += "_Store assigned mult times"
    if len(store_ids) > len(checkit):
        err_store_not_assign = True
        err_mess += " _Stores(s) not assigned"
            
    return err_dc_key or err_store_key or err_mult_assign or err_store_not_assign, err_mess
    
      
            
            
            
silent_mode = False
problems = getDBDataListEle('CALL spGetProblemIds();')
for problem_id in problems:
    dist = getDBDataDictTup('CALL spGetDist(%s);' % str(problem_id))          # Key: (DC id, Store ID),  Value: distance
    dcs = getDBDataDict('CALL spGetDcs(%s);' % str(problem_id))               # SELECT id, cap_cubic_feet, cap_doors, cap_drivers 
    stores_vol = getDBDataList('CALL spGetStores(%s);' % str(problem_id))     # SELECT id, vol_daily
    store_ids = getDBDataListEle('CALL spGetStoreIDs(%s)' % str(problem_id))  # Creates a list of store_id keys
    dc_ids = getDBDataListEle('CALL spGetDCIDs(%s)' % str(problem_id))        # creates a list if dc_id keys
    
    my_team_or_name, result = trans(dist, dcs, stores_vol)
    print result
    
    okStoresAssigned, err_mess = checkUniqueAssign(store_ids,dc_ids,result)
    okCap = checkDCCap(dcs,stores_vol,result)
    if silent_mode:
        if okStoresAssigned or okCap:
            print "P",problem_id," error: " 
            if not okStoresAssigned:
                print '; error with keys or multiple assignment'
            if not okCap:
                print '; exceeded DC capacity'
        else:
            print "P",problem_id,"ok"
    else:
        if okStoresAssigned or okCap:
            print "Problem",problem_id," error: " 
            if not okStoresAssigned:
                print 'either with keys or assignment of stores to multiple DCs'
                print err_mess
            if not okCap:
                print 'DC capacity exceeded'
        else:
            print "Problem",problem_id," OK"
            
            

    
    
    

            


                
        
    
     
