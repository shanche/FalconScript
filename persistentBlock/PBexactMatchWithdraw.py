import MySQLdb
import datetime
from hostInfo_yoda import *
from decimal import *
import pickle
from client import *
from bankAcct import *

today = datetime.date.today()
todayStr = str(today)
#todayStr = today
postStr = todayStr
######################################################## INPUT ##############################################################
sortType = "Capacity"
#sortType = "Rate"
minrate = 0.0
maxrate = 4.0
maxCount = 10
withDrawMin = 10.0 # lower withdraw limit for each bank
bankCapicityMin = 1000000
pct = 0.1# only the client balance at this bank is greater than pct of the total withdraw, the balance is withdrawed from this bank
status_ = "New"
cfname = 'C:/Users/sche.STONECASTLEPART/Documents/GitHub/SCCM_Scripts/sccmbatch/datafiles/clients'+'_'+todayStr+'.pkl'
bfname = 'C:/Users/sche.STONECASTLEPART/Documents/GitHub/SCCM_Scripts/sccmbatch/datafiles/bankaccts'+'_'+todayStr+'.pkl'
################################################################################################################################
db = MySQLdb.connect(host = host_,user = user_, passwd = password_, db = datebase_)
cur = db.cursor()

exClientID = []
exSubacctID = []
cur.execute("SELECT clientid, subacctid FROM clients WHERE statement <> \'FICA\'")
db.commit()
ClientToExclude = cur.fetchall()[0:]
for row in ClientToExclude:
    if (bool(row[0])):
        exClientID.append(row[0])
        exSubacctID.append(row[1])

cur.execute("SELECT clientid, subacctid, amount FROM transactions WHERE postingdate = \'" + todayStr + "\' AND status = \'" + status_ + "\' AND transtype = \'Withdrawal\'")
db.commit()
withdrawClient = cur.fetchall()[0:]

cur.execute("SELECT fdiccert, bankaba, bankaccount, newbalance, minamount, maxamount, newbalance-minamount, max_savings_rate, newcf, wdmax-wdcount wdleft FROM bankbalances WHERE newbalance > 0 AND max_savings_rate >= " + str(minrate) + " AND max_savings_rate < " + str(maxrate) +" ORDER BY max_savings_rate ")
db.commit()
bankData = cur.fetchall()[0:]
Bank = {}
rates = []
capacity = []
bankByRate = {}
bankByCapacity = {}
for row in bankData:
    if (bool(row[2]) & (float(row[9]) > 2)):
        Bank.update({str(row[0]) + "," + str(row[2]) + ",":abs(float(row[3]))})
        rates.append(row[7])
        capacity.append(row[6])
        if (row[7] in bankByRate.keys()):
            bankByRate[row[7]].append(str(row[0]) + "," + str(row[2]) + ",")
        else:
            bankByRate.update({row[7]:[str(row[0]) + "," + str(row[2]) + ","]})
        if (row[6] in bankByCapacity.keys()):
            bankByCapacity[row[6]].append(str(row[0]) + "," + str(row[2]) + ",")
        else:
            bankByCapacity.update({row[6]:[str(row[0]) + "," + str(row[2]) + ","]})

rates = set(rates)
rates = list(rates)
rates.sort()
capacity = set(capacity)
capacity = list(capacity)
capacity.sort(reverse = True)

cf = open(cfname, 'rb')
clients = pickle.load(cf)
cf.close()

bf = open(bfname, 'rb')
banks = pickle.load(bf)
bf.close()

icaBanks = []

for bk in Bank.keys():
    for row in ClientToExclude:
        if (float((clients.get(str(row[0]) + "," + str(row[1]) + ",")).alloc.get(bk,0)) > 0.0):
            icaBanks.append(bk)   

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
        
        for bk in Bank.keys():
            if bk in icaBanks:
                break
            else:
                clientBankBalance = 0.0
                
                ck = str(wC[0]) + "," + str(wC[1]) + ","
                client = clients.get(ck)
                wbk = bk
                if bool(client.alloc.get(wbk, 0)):
                    clientBankBalance = float(client.alloc.get(wbk, 0))
                    if clientBankBalance < amt_c * pct:
                        break
                   # print("Client " + str(wC[0]) + "|" + str(wC[1]) + " at Bank " + str(bk[0]) + "|"  + str(bk[1]) + "|"  + str(bk[2]) + " is " + str(clientBankBalance))
                bankBalance = 0.0
                bankBalance = float(Bank.get(bk))
                    
                clientBankBalanceOverMin = min(abs(amt),max(0,round(min(float(bk[6]), clientBankBalance)-0.5)))
                clientBankBalanceOverMin = round(clientBankBalanceOverMin-0.5)
                
                if (clientBankBalanceOverMin > 0.0):
                    amt = amt + clientBankBalanceOverMin
                    client.alloc[wbk] = client.alloc[wbk]  - Decimal(clientBankBalanceOverMin)
                    Bank[bk] = Bank[bk] - (clientBankBalanceOverMin)
                    if (bk) in BankWithdraw.keys():
                        BankWithdraw[bk] = BankWithdraw[bk] + (clientBankBalanceOverMin)
                    else:
                        BankWithdraw.update({bk:clientBankBalanceOverMin})
                        
    print("Client " + str(wC[0]) + "," + str(wC[1]) + "," + "has finished allocation, in "+ str(count) + " cycles")

totalLeft = 0.0
if sortType == "Rate":
    outType = rates
    outContainer = bankByRate
if sortType == "Capacity":
    outType = capacity
    outContainer = bankByCapacity
    
for i in range(len(outType)):
    bkList = outContainer.get(outType[i])
    for j in range(len(bkList)):
        bk = bkList[j]
for bk in bankData:
        if ((str(bk[0]) + "," +  str(bk[2]) + ",") in BankWithdraw.keys()):
            if (BankWithdraw[str(bk[0]) + "," +  str(bk[2]) + ","] > 0.0):
                query = "INSERT persistentblocks  VALUE (\'" + postStr + "\', \'" + str(bk[0]) + "\', \'" + str(bk[1]) + "\',\'" + str(bk[2]) + "\',\'Negative\',-" + str(BankWithdraw[bk[1] + "|" +  bk[2]]) + ")"
                if (BankWithdraw[str(bk[0]) + "," +  str(bk[2]) + ","] < withDrawMin):
                    totalLeft = totalLeft + BankWithdraw[str(bk[0]) + "," +  str(bk[2]) + ","]
                    break
                else:
                    print(query)
                    print("The rate of " + str(bk[0]) + "|" + str(bk[1]) + "|" + str(bk[2]) + " is: " + str(bk[7]))
                    #cur.execute(query)
                    #db.commit()

                
print("The total fund not withdrawed is: " + str(round(totalLeft)))

db.close()


