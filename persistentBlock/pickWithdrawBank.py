import MySQLdb
import datetime
from hostInfo_vader import *
from decimal import *
import pickle
from client import *
from bankAcct import *

today = datetime.date.today()
todayStr = str(today)
#todayStr = "2015-11-30"
postStr = todayStr
######################################################## INPUT ##############################################################
minrate = 0.4
maxrate = 0.5
totalAmount = 9000000000
minBalance = 5000000.0 
pctWithdraw = 0.3
cfname = 'C:/Users/sche.STONECASTLEPART/Documents/GitHub/SCCM_Scripts/sccmbatch/datafiles/clients'+'_'+todayStr+'.pkl'
bfname = 'C:/Users/sche.STONECASTLEPART/Documents/GitHub/SCCM_Scripts/sccmbatch/datafiles/bankaccts'+'_'+todayStr+'.pkl'
################################################################################################################################
db = MySQLdb.connect(host = host_,user = user_, passwd = password_, db = datebase_)
cur = db.cursor()

cur.execute("SELECT fdiccert, bankaba, bankaccount, newbalance, minamount, maxamount, newbalance-minamount, max_savings_rate, newcf, wdmax-wdcount wdleft FROM bankbalances WHERE newbalance > 0 AND max_savings_rate >= " + str(minrate) + " AND max_savings_rate < " + str(maxrate) +" ORDER BY max_savings_rate ")
db.commit()
bankData = cur.fetchall()[0:]
Bank = {}
rate = []
maxWithdrawAmount = []


for row in bankData:
    if (bool(row[2])):
        #Bank.update({str(row[0]) + "|" + str(row[2]) + "|" + str(row[1]):abs(float(row[3]))})
        rate.append(float(row[7]))
        maxWithdrawAmount.append(float(row[6]))

rate = set(rate)
rate = list(rate)
rate.sort()

maxWithdrawAmount = set(maxWithdrawAmount)
maxWithdrawAmount = list(maxWithdrawAmount)
maxWithdrawAmount.sort(reverse = True)
tempamt = 0
print("Withdraw by rate: ")
for i in range(len(rate)):
    for row in bankData:
        if ((rate[i] - float(row[7])) < 0.000001) & (float(row[6]) > minBalance) & (tempamt < totalAmount):
            query = "INSERT INTO persistentblocks VALUE ('" + postStr + "','" + str(row[0]) + "','" + str(row[1]) +"','" + str(row[2]) +"','Negative','-" + str(round(pctWithdraw * float(row[6]),2)) + "')"
            print (query)
            print ("The rate of the bank is: " + str(rate[i]) + "; the withdraw count is: " + str(row[9]))
            tempamt = tempamt + float(row[6])

print("*************************************************")            
print("Withdraw by amount: ")
tempamt = 0            
for i in range(len(maxWithdrawAmount)):
    for row in bankData:
        if ((maxWithdrawAmount[i] - float(row[6])) < 0.000001) & (float(row[6]) > minBalance) & (tempamt < totalAmount):
            query = "INSERT INTO persistentblocks VALUE ('" + postStr + "','" + str(row[0]) + "','" + str(row[1]) +"','" + str(row[2]) +"','Negative','-" + str(round(pctWithdraw * float(row[6]),2)) + "')"
            print (query)
            print ("The rate of the bank is: " + str(row[7]) + "; the withdraw count is: " + str(row[9]))
            tempamt = tempamt + float(row[6])
            





db.close()
