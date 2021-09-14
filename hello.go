package main // 声明 main 包，表明当前是一个可执行程序

// 导入内置 fmt 包 and my own package
import (
	//	"flag"

	"context"
	"fmt"
	"log"

	"github.com/beltran/gohive"
)

func main() { // main函数，是程序执行的入口
	fmt.Println("Start connnect to Hive!") // 在终端打印 Hello World!

	// usage : ./main -h 127.0.0.1:50000 -d default -q "show tables"

	ctx := context.Background()
	configuration := gohive.NewConnectConfiguration()
	configuration.Service = "hive"
	configuration.Username = "hadoop"

	configuration.FetchSize = 1000

	connection, errConn := gohive.Connect("172.16.16.155", 10000, "NOSASL", configuration)
	if errConn != nil {
		log.Fatal(errConn)
	}
	cursor := connection.Cursor()

	//cursor.Exec(ctx, "INSERT INTO myTable VALUES(1, '1'), (2, '2'), (3, '3'), (4, '4')")
	//if cursor.Err != nil {
	//		log.Fatal(cursor.Err)
	//	}

	cursor.Exec(ctx, "SELECT * FROM etherum.top1000_erc20_token")
	if cursor.Err != nil {
		log.Fatal(cursor.Err)
	}

	var index int32
	var token_contract_address string
	var name string
	var symbol string

	for cursor.HasMore(ctx) {
		cursor.FetchOne(ctx, &index, &token_contract_address, &name, &symbol)
		if cursor.Err != nil {
			log.Fatal(cursor.Err)
		}
		log.Println(index, token_contract_address, name, symbol)
	}

	cursor.Close()
	connection.Close()

}
