from datetime import datetime,timedelta
from resp import types
from collections import deque
commands = {
    'ping':0,
    'echo':1,
    'set':2,
    'get':3,
    'exists':4,
    'del':5,
    'incr':6,
    'decr':7,
    'lpush':8,
    'rpush':9,
    'save':10
}
setOptions = {
    'ex':0,
    'px':1,
    'exat':2,
    'pxat':3
}
dataTypesSave = {
    type(""):0,
    type(1):1,
    type([]):2
}
class redis:
    db = {}
    timeDb = {}
    def getRecord(self,key):
        return self.db[key]
    def getResponse(self,data,lock):
        command = data[0].lower()

        if commands[command] == commands['ping']:
            return ['PONG',types[0]]
        elif commands[command] == commands['echo']:
            return [data[1],types[3]]
        elif commands[command] == commands['set']:
            lock.acquire(True)
            isSet = self.executeSet(data[1:])
            if isSet:
                lock.release()
                return ['OK',types[0]]
            else:
                lock.release()
                return ['Error setting record',types[1]]
        elif commands[command] == commands['get']:
            value = self.getRecord(data[1])
            if not value:
                return [None,types[3]]
            elif self.isExpired(data[1]):
                lock.acquire(True)
                self.deleteRecord(data[1])
                lock.release()
                return ['Key is expired',types[1]]
            elif type(value) == type(""):
                return [value,types[3]]
            else:
                return ['Error handling value',types[1]]
        elif commands[command] == commands['exists']:
            ans = self.executeExists(data[1:],lock)
            return [ans,types[2]]
        elif commands[command] == commands['del']:
            lock.acquire(True)
            resp=self.executeDelete(data[1:])
            lock.release()
            return [resp,types[2]]
        elif commands[command] == commands['incr']:
            lock.acquire(True)
            resp=self.executeIncr(data[1])
            lock.release()
            return [resp,types[2]]
        elif commands[command] == commands['decr']:
            lock.acquire(True)
            resp=self.executeDecr(data[1])
            lock.release()
            return [resp,types[2]]
        elif commands[command] == commands['lpush']:
            lock.acquire(True)
            resp=self.executeLpush(data[1],data[2:])
            lock.release()
            return [resp,types[2]]
        elif commands[command] == commands['rpush']:
            lock.acquire(True)
            resp=self.executeRpush(data[1],data[2:])
            lock.release()
            return [resp,types[2]]
        elif commands[command] == commands['save']:
            print('not implemented')
        else:
            return ['Command Not Found',types[1]]
    def setSimpleRecord(self,key,value):
        self.db[key]=value
        return True
    def getExpirationTime(self,value,isMilliseconds):
        if type(value) == type(""):
            value = int(value)
        if isMilliseconds:
            value = value/1000
        expiryTime = datetime.now() + timedelta(seconds=value)
        expiryTime = int(expiryTime.timestamp())
        return expiryTime
    def setRecordWithExpirationTime(self,key,value,time):
        self.timeDb[key]=time
        self.setSimpleRecord(key,value)
        return True
    def executeSet(self,args):
        if len(args)<2:
            return False
        elif len(args)==2:
            return self.setSimpleRecord(args[0],args[1])
        else:
            key = args[0]
            value = args[1]
            option = args[2].lower()
            if len(args)==3:
                return False
            expiryTime = 0
            if setOptions[option] == setOptions['ex']:
                expiryTime=self.getExpirationTime(args[3],False)
            elif setOptions[option] == setOptions['px']:
                expiryTime = self.getExpirationTime(args[3],True)
            elif setOptions[option] == setOptions['exat']:
                expiryTime = args[3]
            elif setOptions[option] == setOptions['pxat']:
                expiryTime = args[3]/1000
            else:
                return False
            return self.setRecordWithExpirationTime(key,value,expiryTime)
    def isExpired(self,key):
        if not (key in self.timeDb):
            return False
        expiry=self.timeDb[key]
        if not expiry:
            return False
        currentTimeStamp = int(datetime.now().timestamp())
        if currentTimeStamp < expiry:
            return False
        return True
    def deleteRecord(self,key):
        try:
            self.db.pop(key)        
            self.timeDb.pop(key)
        except:
            return False
        return True
    def executeExists(self,keys,lock):
        if len(keys) < 1:
            print('Incorrect Argument Error')
        existing =0
        for key in keys:
            if key in self.db:
                if key in self.timeDb and self.isExpired(key):
                    lock.acquire(True)
                    self.deleteRecord(key)
                    lock.release()
                else:
                    existing= existing+1
        return existing
    def executeDelete(self,keys):
        delKeys = 0
        for key in keys:
            if key in self.db:
                if key in self.timeDb:
                    self.timeDb.pop(key)
                self.db.pop(key)
                delKeys=delKeys+1
        return delKeys
    def executeIncr(self,key):
        if key in self.db and not self.isExpired(key):
            val = self.getRecord(key)
            if type(val) == type(1):
                self.setSimpleRecord(key,val+1)
                return val+1
            elif type(val) == type(""):
                self.setSimpleRecord(key,str(int(val)+1))
                return int(val)+1
            else:
                print('type not supported')
        else:
            self.deleteRecord(key)
    def executeDecr(self,key):
        if key in self.db and not self.isExpired(key):
            val = self.getRecord(key)
            if type(val) == type(1):
                self.setSimpleRecord(key,val-1)
                return val-1
            elif type(val) == type(""):
                self.setSimpleRecord(key,str(int(val)-1))
                return int(val)-1
            else:
                print('type not supported')
        else:
            self.deleteRecord(key)
    def executeLpush(self,key,values):
        ans = []
        if key in self.db:
            if not (type(self.db[key]) == type([])):
                print('invalid key')
            ans = self.db[key]
        ans = deque(ans)
        ans.extendleft(values)
        ans = list(ans)
        self.db[key]=ans
        return len(self.db[key])
    def executeRpush(self,key,values):
        ans = []
        if key in self.db:
            if not (type(self.db[key]) == type([])):
                print('invalid key')
            ans = self.db[key]
        ans = deque(ans)
        ans.extend(values)
        ans = list(ans)
        self.db[key]=ans
        return len(self.db[key])
    def arrayToString(self,arr):
        ans=""
        for i in arr :
            ans = ans + i +","
        return ans
    def executeSave(self):
        fileName = 'redis-dbbackup'+'.txt'
        with open(fileName,'w') as file:
            keys = self.db.keys()
            timeKeys = self.timeDb.keys()
            file.write(len(keys))
            file.write(len(timeKeys))
            for key in keys:
                if key in timeKeys:
                    file.write('EX '+str(self.timeDb[key]))
                file.write(dataTypesSave[type(self.db[key])])
                file.write(key)
                val = self.db[key]
                if type(val) == type([]):
                    val = self.arrayToString(val)
                file.write(val)
    def loadDb(self):
        fileName = 'redis-dbbackup'+'.txt'
        self.db.clear()
        self.timeDb.clear()
        with open(fileName,'r') as file:
            lineIndex=0
            lines = file.readlines()
            dbSize = int(lines[lineIndex])
            lineIndex=lineIndex+1
            timeDbSize = int(lines[lineIndex])
            lineIndex=lineIndex+1
            for i in range(dbSize):
                expiryTime= -1
                dataType = -1
                key = ""
                value = ""
                line = lines[lineIndex]
                if line[0:3] == 'EX ':
                    expiryTime = int(line[3:])
                    lineIndex=lineIndex+1
                line = lines[lineIndex]
                dataType = int(line.replace('\n',"").replace('\r',"").strip())
                lineIndex = lineIndex+1
                line=lines[lineIndex]
                key = line.replace('\n',"")
                lineIndex = lineIndex + 1
                value = 


                
