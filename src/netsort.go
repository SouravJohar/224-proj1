package main

import (
    "fmt"
    "gopkg.in/yaml.v2"
    "io/ioutil"
    "io"
    "log"
    "os"
    "strconv"
    "net"
    "time"
    "sort"
    "math/big"
    "math"
)


const SERVER_TYPE = "tcp"


type ServerConfigs struct {
    Servers []struct {
        ServerId int    `yaml:"serverId"`
        Host     string `yaml:"host"`
        Port     string `yaml:"port"`
    } `yaml:"servers"`
}

type Server struct {
    ServerId int  
    Host     string
    Port     string
}


func readServerConfigs(configPath string) map[int]Server {
    f, err := ioutil.ReadFile(configPath)

    if err != nil {
        log.Fatalf("could not read config file %s : %v", configPath, err)
    }

    scs := ServerConfigs{}
    err = yaml.Unmarshal(f, &scs)

    serverMap := make(map[int]Server)

    for _, val := range scs.Servers {
        serverMap[val.ServerId] = Server{ServerId: val.ServerId, Host: val.Host, Port: val.Port}
    }

    return serverMap
}



func handler(conn net.Conn, recvd chan []byte) {

    for {
        tmp := make([]byte, 101)  
        n, err := conn.Read(tmp)
        if err != nil {
            if err != io.EOF {
                fmt.Println("read error:", err)
            }
            break
        }

        recvd <- tmp[:n]

    }
}


func listener(myServerId int, recvd chan []byte, serverMap map[int]Server) {

    l, _ := net.Listen(SERVER_TYPE,  serverMap[myServerId].Host +":"+ serverMap[myServerId].Port)

    for {

        conn, err := l.Accept()
            if err != nil {
                fmt.Println("Error connecting:", err.Error())
                return
            }

            go handler(conn, recvd)
    }
}
        

func sender(array [][]byte, myServerId int, allSent *bool, serverMap map[int]Server, recvd chan []byte) {

    allConnections := make(map[int]net.Conn)

    for id, val := range serverMap {

        if id == myServerId {
            continue
        }
        connection, err := net.Dial(SERVER_TYPE, val.Host +":"+ val.Port)
        if err != nil {
                panic(err)
        }

        allConnections[id] = connection
        defer connection.Close()
    } 

    sigBits := int(math.Log2(float64(len(serverMap))))

    for _, val := range array {

        belongsToServer := int(val[0] >> (8 - sigBits))

        // pre-pend a 0 to signify that we are still streaming
        stream := []byte{0}
        val = append(stream, val...)

        if belongsToServer != myServerId {

            allConnections[belongsToServer].Write(val)

        } else {

            recvd <- val
        }
    }


    // prepare stream complete signal
    signal := []byte{1, byte(myServerId)}
    for i := 0; i <= 98; i++ {
        signal = append(signal, 1)
    }

    // send stream complete signal to all other nodes
    for id, _ := range serverMap {

        if id == myServerId {
            continue
        }

        allConnections[id].Write(signal)
    }

    // kept <- signal 
    *allSent = true

}


func readInputFile(inputFilePath string) [][]byte{
    inputFp, _ := os.Open(inputFilePath)

    // read the file and store byte arrays 
    byteArray := make([][]byte, 0)

    for {
        keyValue := make([]byte, 100)

        _, err := io.ReadFull(inputFp, keyValue)

        if err == io.EOF {
            break
        }
        byteArray = append(byteArray, keyValue)
    }

    inputFp.Close()

    return byteArray
}


func main() {
    log.SetFlags(log.LstdFlags | log.Lshortfile)

    if len(os.Args) != 5 {
        log.Fatal("Usage : ./netsort {serverId} {inputFilePath} {outputFilePath} {configFilePath}")
    }

    // What is my serverId
    serverId, err := strconv.Atoi(os.Args[1])
    if err != nil {
        log.Fatalf("Invalid serverId, must be an int %v", err)
    }

    // Read server configs from file
    scs := readServerConfigs(os.Args[4])

    // read input binary file
    byteArray := readInputFile(os.Args[2])

    recvd := make(chan []byte)
    var counter int

    // start listening to other nodes and accept records and store data into recvd
    go listener(serverId, recvd, scs)


    time.Sleep(2 * time.Second)


    allSent := false
    var toSort [][]byte

    // start sending data that does not belong to my server
    // kept := make(chan []byte, 10000)
    go sender(byteArray, serverId, &allSent, scs, recvd)


    for {

        var val []byte 

        val  = <- recvd

        if val[0] == byte(1) {
            counter += 1
        } else {
            toSort = append(toSort, val[1:])

        }

        // break when all nodes have sent their data
        if counter == len(scs) - 1 && allSent {
            break
        }
    }

    // perform the sorting
    sort.Slice(toSort, func(i, j int) bool {

        x := new(big.Int)
        x.SetBytes(toSort[i][:10])

        y := new(big.Int)
        y.SetBytes(toSort[j][:10])

        return x.Cmp(y) != 1
    }) 

    // write sorted output
    outputFilePath := os.Args[3]
    outputFp, _ := os.Create(outputFilePath)
    

    for _, record := range toSort {
        outputFp.Write(record)
    }
    outputFp.Close()
}
