# ECE419 Milestone 4
## Team 28: Akino Watanabe (1004133653), Lisa Li (1003924532), Sandra Petkovic (1004018537)

## How to run locally
### Prerequisites

Please have Java 8.x downloaded on your system and zookeeper 3.4.11. The following instructions will be for a MacOS/Unix system.

### Installation instructions

Clone this repository locally with the following command:

```sh
git clone https://github.com/lisa-sa-li/ECE419.git 
```

Populate `servers,cfg` with server name and ports that are available on your local machine. You may also keep them as they are.

```
<serverName> <serverHost> <serverPort>
john0 127.0.0.1 8000
...
```

### Running Zookeeper
Ensure that with your zookeeper installation, there is now a zookeeper-3.4.11/bin folder in this directory.

Start zookeeper on your machine from the root directory with the following commands:

```sh
./zookeeper-3.4.11/bin/zkCli.sh -server 127.0.0.1:2181
```

Open a new terminal and execute:

```sh
./zookeeper-3.4.11/bin/zkServer.sh
```

Zookeeper should now be running.

### Running the code

Open a new terminal. Begin by running the `ant` command to build all the `.jar` files.


Start the ECS client with the following command:

```sh
java -jar m4-ecs.jar
```

To add nodes, run the following command in the ECSClient CLI.

```sh
ECSClient> addnodes <numServers> <cacheStrategy> <cacheSize>

ECSClient> addnodes 2 FIFO 3
```

Run `start` to start all the added servers.

In a new terminal, run the client with the following command:

```sh
java -jar m4-client.jar
```

Your client is now ready to connect to a server.

### Running Soft Delete

To use the soft delete functionality, connect a client to a server (the output from ECSClient will let you know which ports are in use):

```sh
KVClient> connect 127.0.0.1 <portNumber>
```

Once you have connected, try running a PUT command:

```sh
KVClient> PUT soft delete
>> PUT_SUCCESS
```

This will insert the keyvalue pair "soft": "delete" into storage.

You may read this value with a GET command:

```sh
KVClient> GET soft
>> GET_SUCCESS delete
```

To delete this keyvalue pair, run the PUT command:

```sh
KVClient> PUT soft
>> DELETE_SUCCESS
```

A GET command will produce an error now:

```sh
KVClient> GET soft
>> GET_ERROR
```

To recover the deleted value, run the RECOVER command.

```sh
KVClient> RECOVER soft
>> RECOVER_SUCCESS
```

You may now GET or update the keyvalue pair.

```sh
KVClient> GET soft
>> GET_SUCCESS delete
```

For demo purposes, the expiry of a key is 1 minute after it is deleted. After that, the keyvalue pair will no longer exist and the RECOVER command will issue an error:

```sh
KVClient> RECOVER soft
>> RECOVER_ERROR
```