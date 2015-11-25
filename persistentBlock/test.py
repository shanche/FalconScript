import MySQLdb
import datetime
from hostInfo_yoda import *

import pickle
from sys import argv
from client import *
from bankAcct import *
from sqlalchemy import *

######################################################################################################################
minrate = 0.30
maxrate = 0.40
######################################################################################################################
d = datetime.date.today()
datadt = "2015-11-16"

cfname = 'datafiles/clients'+'_'+datadt+'.pkl'
bfname = 'datafiles/bankaccts'+'_'+datadt+'.pkl'

cf = open(cfname, 'rb')
clients = pickle.load(cf)
cf.close()

bf = open(bfname, 'rb')
banks = pickle.load(bf)
bf.close()


