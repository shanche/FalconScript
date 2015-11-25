import MySQLdb
import datetime
from hostInfo_yoda import *
from sortedcontainers import SortedDict
from collections import OrderedDict

db = MySQLdb.connect(host = host_,user = user_, passwd = password_, db = datebase_)
cur = db.cursor()

import pickle
from sys import argv
from client import *
from bankAcct import *
from sqlalchemy import *


maxValue = 5000000
minValue = 2000000
mRate = .45
status_ = 'Done'

d = datetime.date.today()
datadt = str(d)
datadt = '2015-11-16'

cur.execute("SELECT clientid, subacctid, amount FROM transactions WHERE postingdate = \'" + datadt + "\' AND status = \'" + status_ + "\' AND transtype = \'Withdrawal\'")
db.commit()
withdrawClientToday = cur.fetchall()[0:]


cfname = 'C:/Users/sche.STONECASTLEPART/Documents/GitHub/SCCM_Scripts/sccmbatch/datafiles/clients'+'_'+datadt+'.pkl'
bfname = 'C:/Users/sche.STONECASTLEPART/Documents/GitHub/SCCM_Scripts/sccmbatch/datafiles/bankaccts'+'_'+datadt+'.pkl'

cf = open(cfname, 'rb')
clients = pickle.load(cf)
cf.close()

bf = open(bfname, 'rb')
banks = pickle.load(bf)
bf.close()

withdrawBank = {}

for row in withdrawClientToday:
    for bk in banks:
        bank = banks.get(bk);
        for ck in clients.keys():
            if (ck == (row[0]) + "," + (row[1]) + ","):
                client = clients.get(ck)
                currentBalance = client.alloc.get(bk, 0)
                if (currentBalance > 0 and bank.maxrate > mRate):
                    if (bk in withdrawBank.keys()):
                        withdrawBank[bk] = withdrawBank[bk] + currentBalance
                    else:
                        withdrawBank.update({bk:currentBalance})
                    
                    #print("The balance of client " + (ck) + " at bank " + (bk) + " is: " + str(currentBalance))

withdrawBank = OrderedDict(sorted(withdrawBank.items(), key=lambda t: t[1]))
        



count = 0
for bk in withdrawBank.keys():
     #   print("The withdraw balance of bank " + bk + "is: " + str(withdrawBank[bk]))
        
        if (withdrawBank[bk] > minValue ):
            bank = banks.get(bk);
            query = "Insert INTO persistentblocks VALUES('2015-11-23', '"+bank.fdiccert+"', '"+bank.bankaba+"', '"+bank.bankaccount + "', 'Negative' , -" + str(withdrawBank[bk]) + ")"
            print(query)
            count = count +1
            #try:
          #      cur.execute(query)
           #     db.commit()
           # except exception:
            #    print (exception)
            



db.close()
