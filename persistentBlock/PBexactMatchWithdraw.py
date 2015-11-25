import MySQLdb
import datetime
from hostInfo_vader import *
from decimal import *
import pickle
from client import *
from bankAcct import *

today = datetime.date.today()
todayStr = str(today)
#todayStr = today
postStr = "0000-00-01"
######################################################## INPUT ##############################################################
minrate = 0.0
maxrate = 2.0
maxCount = 1000
withDrawMin = 100000.0
pct = 0.1# only the client balance at this bank is greater than pct of the total withdraw, the balance is withdrawed from this bank
status_ = "New"
cfname = 'C:/Users/sche.STONECASTLEPART/Documents/GitHub/SCCM_Scripts/sccmbatch/datafiles/clients'+'_'+todayStr+'.pkl'
bfname = 'C:/Users/sche.STONECASTLEPART/Documents/GitHub/SCCM_Scripts/sccmbatch/datafiles/bankaccts'+'_'+todayStr+'.pkl'
################################################################################################################################
db = MySQLdb.connect(host = host_,user = user_, passwd = password_, db = datebase_)
cur = db.cursor()

cur.execute("SELECT clientid, subacctid, amount FROM transactions WHERE postingdate = \'" + todayStr + "\' AND status = \'" + status_ + "\' AND transtype = \'Withdrawal\'")
db.commit()
withdrawClient = cur.fetchall()[0:]

cur.execute("SELECT fdiccert, bankaba, bankaccount, newbalance, minamount, maxamount, newbalance-minamount, max_savings_rate, newcf, wdmax-wdcount wdleft FROM bankbalances WHERE newbalance > 0 AND max_savings_rate >= " + str(minrate) + " AND max_savings_rate < " + str(maxrate) +" ORDER BY max_savings_rate ")
db.commit()
bankData = cur.fetchall()[0:]
Bank = {}
for row in bankData:
    if (bool(row[2])):
        Bank.update({row[1] + "|" + row[2]:abs(float(row[3]))}) 

cf = open(cfname, 'rb')
clients = pickle.load(cf)
cf.close()

bf = open(bfname, 'rb')
banks = pickle.load(bf)
bf.close()

BankWithdraw = {}
for wC in withdrawClient:
    amt = float(wC[2])
    amt_c  = amt
    count = 0
    while (amt < 0.0):
        count = count + 1
        if (count > maxCount):
            print("Client " + str(wC[0]) + "," + str(wC[1]) + "," + "can't be balanced in "+ str(count) + ", " + str(round(amt)) + " was left.")
            break
        
        for bk in bankData:
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
                    
                clientBankBalanceOverMin = min(abs(amt),max(0,round(min(float(bk[6]), clientBankBalance)-0.5)))
                clientBankBalanceOverMin = round(clientBankBalanceOverMin-0.5)
                
                if (clientBankBalanceOverMin > 0.0):
                    amt = amt + clientBankBalanceOverMin
                    client.alloc[wbk] = client.alloc[wbk]  - Decimal(clientBankBalanceOverMin)
                    Bank[bk[1] + "|" +  bk[2]] = Bank[bk[1] + "|" +  bk[2]] - (clientBankBalanceOverMin)
                    if (bk[1] + "|" +  bk[2]) in BankWithdraw.keys():
                        BankWithdraw[bk[1] + "|" +  bk[2]] = BankWithdraw[bk[1] + "|" +  bk[2]] + (clientBankBalanceOverMin)
                    else:
                        BankWithdraw.update({bk[1] + "|" +  bk[2]:clientBankBalanceOverMin})
                        
    print("Client " + str(wC[0]) + "," + str(wC[1]) + "," + "has finished allocation, in "+ str(count) + " cycles")

totalLeft = 0.0                
for bk in bankData:
    if ((bk[1] + "|" +  bk[2]) in BankWithdraw.keys()):
        if (BankWithdraw[bk[1] + "|" +  bk[2]] > 0.0):
            query = "INSERT persistentblocks  VALUE (\'" + postStr + "\', \'" + str(bk[0]) + "\', \'" + str(bk[1]) + "\',\'" + str(bk[2]) + "\',\'Negative\',-" + str(BankWithdraw[bk[1] + "|" +  bk[2]]) + ")"
            if (BankWithdraw[bk[1] + "|" +  bk[2]] < withDrawMin):
                totalLeft = totalLeft + BankWithdraw[bk[1] + "|" +  bk[2]]
                break
            else:
                print(query)
                print("The rate is: " + str(bk[7]))
                #cur.execute(query)
                #db.commit()     
print("The total fund not withdrawed is: " + str(round(totalLeft)))

db.close()


