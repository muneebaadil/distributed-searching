# Distributed Searching

## Usage
1. Firstly, run server like so: 
    ```
    go run server.go
    ```
    All options are given below
    ```
    -portClients string
            port number for clients connection (default "3001")
    -portSlaves string
            port number for slaves connection (default "3000")
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
    -serverAddress string
        IP and port of server (default "127.0.0.1:3000")
    ```
3. Lastly, run client(s) for queries, like so: 
    ```
    go run client.go
    ```
    
## Authors 
* Muneeb Aadil
* Shahnoor Tariq 