import csv
import string

fileName = "BFI 2015-08-26.csv"
topRateBankNumber = 20
depositPerCap = 0.5


with open(fileName, 'r') as csvfile:
    BBreader = csv.reader(csvfile, delimiter=' ', quotechar='|')
    BB = []
    for row in BBreader:
        temp = row[0].split(sep=',')
        temprow = []
        for i in temp:
            #print(i)
            if (i == ''):
                temprow.append(0)
            else:
                temprow.append(float(i))   
        BB.append(temprow)

    blockBankList = []
    for i in range(21):
         blockBankList.append(int(BB[i][0]))

    for row in BB:
        if ():
            blockBankList.append(int(row[0]))
