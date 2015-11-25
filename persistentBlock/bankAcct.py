class BankAccount:
    def __init__(self, fdiccert, bankaba, bankaccount, minamount, maxamount, maxrate):
                self.fdiccert = fdiccert
                self.bankaba = bankaba
        
                self.bankaccount = bankaccount
                self.minamount = minamount
                self.maxamount = maxamount
                self.maxrate = maxrate
                self.balance = 0

    def getKey(self):
        return self.fdiccert+','+self.bankaccount+','

    def addBalance(self, amount):
        self.balance = self.balance + amount

    def updateMax(self, newMax, db):
        query= "UPDATE bankaccounts SET maxamount="+str(newMax)+" WHERE bankaba="+self.bankaba+" AND bankaccount="+self.bankaccount
        #print query
        db.execute(query)
