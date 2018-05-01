# Distributed Searching

## 1. Getting Started
1. Firstly, download the data from [here]()
2. Extract the data; you should have the following directory structure
    ```
    code/
        server.go 
        slave.go 
        client.go
    data/
        passwords.txt
    ```
3. Run `split.go` like so: 
    ```
    go run split.go
    ```
    Doing so will split the data into chunks and will save in `chunks/` subdirectory inside the `data/`; you should now have the following directory structure 
    ```
    code/
        server.go
        ...
    data/
        passwords.txt
        chunks/ 
            1.txt 
            2.txt
            ...
    ```

## 2. Usage
1. Firstly, run server like so: 
    ```
    go run server.go
    ```
    All options are given below
    ```
    -numChunks int
        total number of chunks (must be numbered from 1-numChunks inclusive) (default 4)
    -portClients string
        port number for clients connection (default "3001")
    -portSlaves string
        port number for slaves connection (default "3000")
    -timeout int
        time threshold (in seconds) for heartbeat from slaves (default 3)
    ```
2. Afterwards, run slave(s) like so: 
    ```
    go run slave.go
    ```
    All options are given below 
    ```
    -chunkIds string
        identifiers of chunk a slave is hosting (default "1 2 3")
    -dataDir string
        data folder containing all chunks (default "../../data/chunks")
    -heartbeatFreq int
        time (in seconds) after which to send periodic heartbeat (default 2)
    -serverAddress string
        IP and port of server (default "127.0.0.1:3000")
    ```
3. Lastly, run client(s) for queries, like so: 
    ```
    go run client.go
    ```
    All options are given below 
    ```
    -bufferSize int
        buffer size of buffer to store incoming messages in (default 10)
    -serverAddress string
        IP and port of server (default "127.0.0.1:3001")
    -toFind string
        string to search from the server (default "helloworld")
    ```

## 3. Technical Details
### 3.1. Internal Architecture
#### 3.1.1. Server 
#### 3.1.2. Slave

### 3.1. Message Codes Semantics
1. **S**(earch) - sent from server to slave to search for a particular number. 
2. **F**(ound) - sent from slave to server. 
3. **N**(ot found) - sent from slave to server.
4. **H**(alt) - sent from server to slave to halt the other processes when the number is found elsewhere. 
5. **T**(erminated) - sent from slave to server as an acknowledgement of halt command from server.
6. **E**(rror) - used internally for server when a slave disconnects during the query and client needs to re-route the query to other slaves.