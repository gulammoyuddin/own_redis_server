import socket
import threading
from resp import serialize,deser,types
from redis import redis

def handleClients(lock,clientSock,address,redis):
    while True:
        data = clientSock.recv(1024) # Receive up to 1024 bytes
        if not data:
            break
        
        data = deser().deser(data.decode())
        res = redis.getResponse(data,lock)
        res = serialize(res[0],res[1])
        
        clientSock.sendall(res.encode())  # Send the same data back to the client
        
        # Close the connection with the client
    clientSock.close()
    print(f"Connection closed with {address}")
def start_redis_server(host='127.0.0.1', port=6379):
    # Create a socket object
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    # Bind the socket to the specified host and port
    server_socket.bind((host, port))
    
    # Listen for incoming connections (backlog of 5)
    server_socket.listen(5)
    print(f"Echo server started on {host}:{port}")
    lock = threading.Lock()
    redisObj = redis()
    while True:
        # Accept a new connection
        client_socket, client_address = server_socket.accept()
        print(f"Connection established with {client_address}")
        threading.Thread(target=handleClients,args=(lock,client_socket,client_address,redisObj,)).start()

        

if __name__ == "__main__":
    start_redis_server()
