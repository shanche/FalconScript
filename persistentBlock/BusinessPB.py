import MySQLdb
import datetime
from hostInfo_yoda import *
today = datetime.date.today()
todayStr = str(today)
#todayStr = today
postStr = todayStr
minrate = 0.0
maxrate = 0.90
#####################INPUTS################################################
fdiccert = []
bankaba = []
bankacct = []
option = []
amount = []
# each variable must inserted, even if it is empty
fdiccert.append("57053")
bankaba.append("026013576")
bankacct.append("1501538902")
option.append("Guaranteed")
amount.append("0.0")

fdiccert.append("57053")
bankaba.append("026013576")
bankacct.append("1502105023")
option.append("Guaranteed")
amount.append("0.0")
###########################################################################
db = MySQLdb.connect(host = host_,user = user_, passwd = password_, db = datebase_)
cur = db.cursor()

cur.execute("SELECT fdiccert, bankaba, bankaccount, newbalance, minamount, maxamount, newbalance-minamount, max_savings_rate, newcf, wdmax-wdcount wdleft FROM bankbalances WHERE newbalance > 0 AND max_savings_rate >= " + str(minrate) + " AND max_savings_rate < " + str(maxrate) +" ORDER BY max_savings_rate ")
db.commit()
bankData = cur.fetchall()[0:]
Bank = {}

for i in range(len(fdiccert)):
    query="INSERT persistentblocks  VALUE (\'" + postStr + "\', \'" + fdiccert[i] + "\', \'"+ bankaba[i] + "\', \'"+ bankacct[i] + "\', \'" + option[i] + "\', \'"+ amount[i] + "')"
    print(query)
    cur.execute(query)
    db.commit()

    
db.close()
