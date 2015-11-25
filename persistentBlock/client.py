class Client:
    def __init__(self, clientid, subacctid, insured):
        self.clientid = clientid
        self.subacctid = subacctid
        self.alloc = {}
        self.FDICInsured = insured

    def addAlloc(self, fdiccert, bankaba, bankacct, amt):
        key = str(fdiccert)+","+bankacct+","
        self.alloc[key] = amt

    def getKey(self):
        return self.clientid+','+self.subacctid+','

    def getAmount(self, fdiccert, bankacct):
        key = str(fdiccert)+","+bankacct+","
        return self.alloc.get(key, 0 )  

