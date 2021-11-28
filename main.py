from typing import List, Dict
from random import randint

OP_READ='R'
OP_WRITE='W'
OP_COMMIT='C'


class Item:
    def __init__(self, name, value):
        self.name = name
        self.val = value

class Operation:
    def __init__(self, optype, cmd:str):
        self.code = optype
        item = cmd.split('=')
        self.item = item[0]
        if self.code == OP_WRITE:
            self.value = int(item[1])
        return

class Transaction:
    def __init__(self, schedule, data) :
        self.ops : List[Operation] = []
        self.ptr = 0
        self.name = ""
        self.data = data
        self.write_set = []
        self.read_set = []
        if schedule != None:
            self.parse(schedule)
        return

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

    def next(self):
        # return next operation
        try :
            op = self.ops[self.ptr]
            return op
        except IndexError:
            return False

    def exec(self):
        # return next operation, increment pointer
        try :
            op = self.ops[self.ptr]
            self.ptr += 1
            return op
        except IndexError:
            return False

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
                print('\n')
                # execute transaction chosen
                self.exec(txn)
            else:
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
        op = txn.exec()
        code = op.code
        item = op.item
        self.final += f"{txn.name}-{code}({item}={val}); "

class SerialOCC(Processor):
    def __init__(self, filename):
        super().__init__(filename)
        self.COMMIT: List[Transaction] = []
        self.START_TS: List[Transaction] = []

    def choose_txn(self):
        # see whether all transactions has finished or not
        if len(self.COMMIT) == len(self.txns):
            return False
        
        # if there is an unfinished transaction:
        txns = list(self.txns.keys())
        
        # choose transaction order randomly or based on input
        rand = randint(0, len(txns)-1)
        txn:Transaction = self.txns[txns[rand]]

        return txn



if __name__=="__main__":
    #filename = input("masukkan nama file txt : ")
    filename = "contoh.txt"
    occ = SerialOCC(filename)
    occ.run()