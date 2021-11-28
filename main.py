from typing import List, Dict
from random import randint
import datetime

OP_READ='R'
OP_WRITE='W'
OP_COMMIT='C'

FAR_IN_THE_FUTURE = datetime.datetime(3000,1,1)


class Item:
    def __init__(self, name, value):
        self.name = name
        self.val = value

    def set(self, val):
        self.val = val
        return

    def log(self):
        print(f"|{self.name}: {self.val}", end="|")

class Operation:
    def __init__(self, optype, cmd:str):
        self.code = optype
        item = cmd.split('=')
        self.item = item[0]
        if self.code == OP_WRITE:
            self.value = int(item[1])
        return

    def log(self):
        if self.code == OP_COMMIT:
            print(f"{self.code}", end=";")
        elif self.code == OP_READ:
            print(f"{self.code}({self.item})", end=";")
        else:
            print(f"{self.code}({self.item}={self.value})", end=";")

class Transaction:
    def __init__(self, schedule, data) :
        self.ops : List[Operation] = []
        self.ptr = 0
        self.name = ""
        self.data = data
        self.write_set = []
        self.read_set = []
        self.startTS = FAR_IN_THE_FUTURE
        self.validationTS = FAR_IN_THE_FUTURE
        self.finishTS = FAR_IN_THE_FUTURE
        if schedule != None:
            self.parse(schedule)
        self.getReadWriteSet()
        return

    def add(self, op):
        self.ops.append(op)

    def parse(self, schedule:str):
        sch = schedule.rstrip('\n').rstrip(';').split(':')
        self.name = sch[0]
        ops = sch[1].split(';')
        for op in ops:
            try:
                op = op.split('_')
                self.add(Operation(op[0], op[1]))
            except IndexError:
                self.add(Operation(op[0], ''))
    
    def getReadWriteSet(self):
        for op in self.ops:
            code = op.code
            dataitem = op.item
            if code == OP_READ:
                if not(dataitem in self.read_set):
                    self.read_set.append(dataitem)
            if code == OP_WRITE:
                if not(dataitem in self.write_set):
                    self.write_set.append(dataitem)

    def next(self):
        # return next operation
        try :
            op = self.ops[self.ptr]
            return op
        except IndexError:
            return ''

    def exec(self):
        # return next operation , 
        try :
            op = self.ops[self.ptr]
            #handle write internally
            if op.code == OP_WRITE:
                self.data[op.item].set(op.value)
            # handle first operation in transaction
            if self.ptr == 0:
                self.startTS = datetime.datetime.now()
            # handle commit
            if op.code == OP_COMMIT:
                self.validationTS = datetime.datetime.now()
            #increment pointer
            self.ptr += 1
            return op
        except IndexError:
            return False

    def log(self):
        print(self.name, end=": ")
        i = self.ptr
        n = len(self.ops)
        if i==n:
            print("No more operation.", end="")
        while i < n:
            self.ops[i].log()
            i += 1
        print()

class Processor:
    def __init__(self, filename):
        self.data = {}
        self.txns = {}
        self.load(filename)
        self.final = ""

    def load(self, filename):
        with open(filename, 'r') as f:
            n = int(f.readline())
            for i in range(n):
                line = f.readline()
                item = line.split('=')
                item = Item(item[0], int(item[1]))
                self.data[item.name] = item
            n = int(f.readline())
            for i in range (n):
                line = f.readline()
                txn = Transaction(line, self.data)
                self.txns[txn.name] = txn

    def log(self):
        for _,v in self.txns.items():
            v.log()

    def run(self):
        while True:
            self.log()
            txn = self.choose_txn()
            if (txn):
                # print execute blabla
                print("execute : ", end=f"{txn.name}-")
                txn.next().log()
                # execute transaction chosen
                self.exec(txn)
                print('\n')
            else:
                print('\n')
                break
        print("Schedule generated:")
        print(self.final)
        return

    def choose_txn(self):
        # override this method 
        # to choose appropriate txn
        # based on chosen algorithm
        # return txn, or 
        # return False (if all txn is done)
        return False

    def exec(self, txn):
        # override this method 
        # to choose appropriate txn
        # based on chosen algorithm
        # return txn, or 
        # return False (if all txn is done)
        return

class SerialOCC(Processor):
    def __init__(self, filename):
        super().__init__(filename)
        self.COMMIT: List[Transaction] = []
        self.TSs = []
        self.rollback = []

    def choose_txn(self):
        # see whether all transactions has finished or not
        if len(self.COMMIT) == len(self.txns):
            return False
        
        # if there is an unfinished transaction:
        txns = list(self.txns.keys())
        
        # prioritize the rolled back transactions:
        if len(self.rollback) > 0:
            return self.rollback[0]
        # if there is no rolled back transaction choose transaction order randomly
        else:
            good_choice = False
            while not good_choice:
                rand = randint(0, len(txns)-1)
                txn:Transaction = self.txns[txns[rand]]
                if not(txn in self.COMMIT):
                    good_choice = True

        return txn

    def exec(self, txn):
        op = txn.exec()
        code = op.code
        item = op.item
        if code == OP_READ:
             self.final += f"{txn.name}-{code}({item}); "
        elif code == OP_WRITE:
            val = op.value
            self.final += f"{txn.name}-{code}({item}={val}); "            

        #if transaction enters the validation phase
        if code == OP_COMMIT:
            self.TSs.append(txn)
            idx = self.TSs.index(txn)
            # validate txn
            success = True
            for i in range (0,idx):
                success = self.compare(txn, self.TSs[i])
                if not success:
                    break
            # if success:
            if success:
                self.final += f"{txn.name}-{code}; "
                self.COMMIT.append(txn)
                # if it has been rolled back before, remove it from rollback record
                if txn in self.rollback:
                    self.rollback.remove(txn)
                # enter the write phase
                self.data = txn.data
                txn.finishTS = datetime.datetime.now()
                # change unstarted transaction's data as well
                self.updateUnstartedTxn()
                # print current data 
                print(f"\ndata after {txn.name} is committed : ", end='')
                for _,v in self.data.items():
                    v.log()

            # if failed, rollback
            else:
                print(f"\nTransaction {txn.name}'s validation is failed. Must rolled back")
                self.final += f"{txn.name}-A; "
                txn.startTS = FAR_IN_THE_FUTURE
                txn.validationTS = FAR_IN_THE_FUTURE
                txn.finishTS = FAR_IN_THE_FUTURE
                txn.ptr = 0
                txn.data = self.data
                self.TSs.remove(txn)
                self.rollback.append(txn)

    def updateUnstartedTxn(self):
        for key in self.txns:
            if self.txns[key].ptr == 0:
                self.txns[key].data = self.data
        return

    def compare(self, tr1, tr2):
        if (tr2.finishTS < tr1.startTS):
            return True
        if (tr1.startTS < tr2.finishTS and tr2.finishTS < tr1.validationTS):
            intersect_item = [value for value in tr1.read_set if value in tr2.write_set]
            if len(intersect_item) == 0:
                return True
        return False

if __name__=="__main__":
    #filename = input("masukkan nama file txt : ")
    filename = "contoh.txt"
    occ = SerialOCC(filename)
    occ.run()