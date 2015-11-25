import MySQLdb
import datetime
from hostInfo_yoda import *
from decimal import *

import pickle
from sys import argv
from client import *
from bankAcct import *

today = datetime.date.today()
todayStr = str(today)
todayStr = "2015-10-15"
postStr = "0000-00-09"

######################################################## INPUT ##############################################################
minrate = 0.0
maxrate = 0.50
FDICbalance = 248000.0
maxCount = 1000
pct = 0.0

status_ = "Done"
cfname = 'C:/Users/sche.STONECASTLEPART/Documents/GitHub/SCCM_Scripts/sccmbatch/datafiles/clients'+'_'+todayStr+'.pkl'
bfname = 'C:/Users/sche.STONECASTLEPART/Documents/GitHub/SCCM_Scripts/sccmbatch/datafiles/bankaccts'+'_'+todayStr+'.pkl'
###########################################################################################################################


db = MySQLdb.connect(host = host_,user = user_, passwd = password_, db = datebase_)
cur = db.cursor()

BankDeposit = {}

cur.execute("SELECT clientid, subacctid, amount FROM transactions WHERE postingdate = \'" + todayStr + "\' AND status = \'" + status_ + "\' AND transtype = \'Deposit\'")
db.commit()
DepositClient = cur.fetchall()[0:]

cur.execute("SELECT fdiccert, bankaba, bankaccount, newbalance, minamount, maxamount, maxamount - newbalance, max_savings_rate, newcf, wdmax-wdcount wdleft FROM bankbalances WHERE newbalance > 0 AND max_savings_rate >= " + str(minrate) + " AND max_savings_rate < " + str(maxrate) +" ORDER BY max_savings_rate DESC")
db.commit()
bankData = cur.fetchall()[0:]
Bank = {}
for row in bankData:
    if (bool(row[2])):
        Bank.update({row[1] + "|" + row[2]:abs(float(row[3]))})

cur.execute("SELECT clientid, subid, fdiccert FROM exclusions")
db.commit()
exclusions = cur.fetchall()[0:]       

cur.execute("SELECT fdiccert, bankaba, bankaccount, postingdate FROM persistentblocks WHERE postingdate = \'" + todayStr + "\'")
db.commit()
currentPB = cur.fetchall()[0:]



cf = open(cfname, 'rb')
clients = pickle.load(cf)
cf.close()

bf = open(bfname, 'rb')
banks = pickle.load(bf)
bf.close()

for wC in DepositClient:
    amt = float(wC[2])
    amt_c  = amt
    count = 0
    while (amt > 0.0):
        count = count + 1
        if (count > maxCount):
            print("Client " + str(wC[0]) + "," + str(wC[1]) + "can't be balanced in "+ str(count))
            break
        
        cFDIC={}
        
        for bk in bankData:
            for cpb in currentPB:
                if ((cpb[0] == bk[0]) & (cpb[1] == bk[1]) & (cpb[2] == bk[2])):
                    break
            for ex in exclusions:
                
                if ((ex[0] == wC[0]) & (ex[1] == wC[1]) & (ex[2] == bk[0])):
                    break
            
            if (float(bk[9]) > 2):
                clientBankBalance = 0.0
                
                ck = str(wC[0]) + "," + str(wC[1]) + ","
                client = clients.get(ck)
                wbk = str(bk[0]) + "," + str(bk[2]) + ","
                if bool(client.alloc.get(wbk, 0)):
                    clientBankBalance = float(client.alloc.get(wbk, 0))
                    if clientBankBalance < amt_c * pct:
                        break
                   # print("Client " + str(wC[0]) + "|" + str(wC[1]) + " at Bank " + str(bk[0]) + "|"  + str(bk[1]) + "|"  + str(bk[2]) + " is " + str(clientBankBalance))
                bankBalance = 0.0
                if bool(Bank.get(bk[1] + "|" + bk[2])):
                    bankBalance = float(Bank.get(bk[1] + "|" + bk[2]))
                clientBankBalanceOverMin = min(abs(amt),max(0,round(min(float(bk[6]), float(FDICbalance - clientBankBalance)) - 0.5)))

                if (bk[0] in cFDIC.keys()):
                    if (cFDIC[bk[0]] + clientBankBalanceOverMin > FDICbalance):
                        break
                    else:
                        cFDIC[bk[0]] = cFDIC[bk[0]] + clientBankBalanceOverMin
                else:
                    cFDIC.update({bk[0]:clientBankBalanceOverMin})
                        
                
                if (clientBankBalanceOverMin > 0.0):
                    amt = amt - clientBankBalanceOverMin
                    if bool(wbk in client.alloc.keys()):                        
                        client.alloc[wbk] = client.alloc[wbk]  + Decimal(clientBankBalanceOverMin)
                    else:
                        client.alloc.update({wbk:Decimal(clientBankBalanceOverMin)})
                        
                    Bank[bk[1] + "|" +  bk[2]] = Bank[bk[1] + "|" +  bk[2]] + (clientBankBalanceOverMin)
                    
                    if (bk[1] + "|" +  bk[2]) in BankDeposit.keys():
                        BankDeposit[bk[1] + "|" +  bk[2]] = BankDeposit[bk[1] + "|" +  bk[2]] + (clientBankBalanceOverMin)
                    else:
                        BankDeposit.update({bk[1] + "|" +  bk[2]:clientBankBalanceOverMin})

                    
                        
                        
    print("Client " + str(wC[0]) + "," + str(wC[1]) + "," + "has finished allocation, in "+ str(count) + " cycles")
                
for bk in bankData:
    if ((bk[1] + "|" +  bk[2]) in BankDeposit.keys()):
        if (BankDeposit[bk[1] + "|" +  bk[2]] > 0.0):
            query = "INSERT persistentblocks  VALUE (\'" + postStr + "\', \'" + str(bk[0]) + "\', \'" + str(bk[1]) + "\',\'" + str(bk[2]) + "\',\'Guaranteed\'," + str(BankDeposit[bk[1] + "|" +  bk[2]]) + ")"
            
            print(query)
            #cur.execute(query)
           #db.commit()
        


db.close()


