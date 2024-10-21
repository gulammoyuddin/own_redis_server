types = {
    4: "Arrays",
    3: "BulkString",
    2: "Integer",
    0: "String",
    1: "ErrorMessage"
}
class deser:
    def deser(self,text):
        data= text.split('\r\n')[:-1]
        if len(data) == 0:
            return ''
        return self.deserialize(data,0)
    def getDeserObj(self):
        return self.__deserObj
    def deserialize(self,data,index):
        serText = data[index]
        firstByte = serText[0]
        dt = serText[1:]
        if(firstByte == '+'):
            return dt
        elif(firstByte == '-'):
            return dt
        elif(firstByte == ':'):
            return int(dt)
        elif(firstByte == '$'):
            return self.deserBulkString(data,index)
        elif(firstByte == '*'):
            x = self.deserArray(data,index)
            return x
    def deserArray(self,data,index):
        x = data[index][1:]
        x = int(x)
        if (x+index>len(data)):
            print(index)
            return None
        index = index+1
        ans = []
        for i in range(x):
            # print(self.__index)
            
            ans.append(self.deserialize(data,index))
            # print(ans)
            if data[index][0] == '$':
                index=index+1
            index = index+1
        return ans
    def deserBulkString(self,data,index):
        if(data[index] == '$-1'):
            return None
        data = data[index+1]
        return data

class ser:
    __ser = ''
    def getSer(self):
        return self.__ser
    def serialize(self,serObj,type):
        if(type == types[0]):
            return '+'+serObj+'\r\n'
        elif(type == types[1]):
            return '-'+serObj+'\r\n'
        elif(type == types[2]):
            return ':'+str(serObj)+'\r\n'
        elif(type == types[3]):
            return self.serBulkString(serObj)
        elif(type == types[4]):
            return self.serArray(serObj)
        
    def serBulkString(self,obj):
        if(obj == None):
            return '$'+str(-1)+'\r\n\r\n'
        return '$'+str(len(obj))+'\r\n'+obj+'\r\n'
    def serArray(self,obj):
        x = len(obj)
        ans = '*'+str(x)+'\r\n'
        for i in range(x):
            ans = ans + self.serialize(obj[i][0],obj[i][1])
        return ans
        

def serialize(text,type):
    serObj = ser()
    return serObj.serialize(text,type)
# def deserialize(text:str):
#     # print(text.split('\r\n')[:-1])
#     ans = deser(text.split('\r\n')[:-1])
#     return ans.getDeserObj()
# print(deser().deser("*3\r\n$3\r\nset\r\n+ok\r\n*2\r\n$4\r\nnmae\r\n-error\r\n"))

