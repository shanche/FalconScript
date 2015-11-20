import MySQLdb
from hostInfo_yoda import *
from removeDuplicate import *

db = MySQLdb.connect(host = host_,user = user_, passwd = password_, db = datebase_)
cur = db.cursor()

ICABank = []

cur.execute("SELECT fdiccert, bankaba, bankaccount, minamount, max_savings_rate FROM `bankaccounts` ORDER BY max_savings_rate ")
db.commit()
bankData = cur.fetchall()[0:]

cur.execute("SELECT clientid, subacctid FROM clients WHERE statement <> \'FICA\'")
db.commit()
ICAClient = cur.fetchall()[0:]

cur.execute("SELECT clientid, subacctid, bankaba, bankaccount, SUM(Amount) AS Amount FROM fisagl WHERE account=\'1001\' GROUP BY clientid, subacctid, bankaba, bankaccount ORDER BY clientid DESC")
db.commit()
curAlloc = cur.fetchall()[0:]
ClientBank = {}


for row in curAlloc:
    if (bool(row[0])):
        ClientBank.update({row[0] + row[1] + row[2] + row[3]:(float(row[4]))})

for bk in bankData:  
    for ica in ICAClient:
        if bool(ClientBank.get(ica[0] + ica[1] + bk[1] + bk[2])):
            if (ClientBank.get(ica[0] + ica[1] + bk[1] + bk[2]) > 0.0):
                ICABank.append(bk[1] + "|" + bk[2])

ICABank = removeDuplicate(ICABank)

print("There are " + str(len(ICABank)) + " banks with ICA clients:\n")

for i in range(len(ICABank)):
    print(ICABank[i])
