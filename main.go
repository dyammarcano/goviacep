package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/dyammarcano/dataprovider"
	bolt "go.etcd.io/bbolt"
	"log"
	"runtime/debug"
	"sync"
)

func main() {
	defer func() {
		if r := recover(); r != nil {
			debug.PrintStack()
			log.Println("Recovered in main() func", r)
		}
	}()

	wg := sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, err := bolt.Open("cache.bolt", 0600, nil)
	if err != nil {
		panic(err)
	}

	// Create a config with driver name to initialize the data provider
	cfg := dataprovider.NewConfigModule().
		WithDriver(dataprovider.SQLiteDataProviderName).
		WithName("database3").
		Build()

	provider, err := dataprovider.NewDataProvider(cfg)
	if err != nil {
		panic(err)
	}

	if err = provider.InitializeDatabase(createTableViaCEP); err != nil {
		panic(err)
	}

	conn := provider.GetConnection()
	defer conn.Close()

	var (
		value   string
		counter int
		end     = "99999999"
	)

	store := NewStore(ctx, conn, db, &wg)

	// query the last stored data to continue the request

	viaCEP := &ViaCEP{}
	if err = conn.Get(viaCEP, getLastViaCEP); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			log.Println("No data found in database")
		} else {
			panic(err)
		}
	}

	if viaCEP.Cep != "" {
		log.Println("Last CEP found: " + formatCep(viaCEP.Cep))
		counter = onlyDigits(viaCEP.Cep)
	}

	for {
		// Convert the integer back to a string with leading zeros
		value = fmt.Sprintf("%08d", counter)
		log.Println("Requesting CEP: " + formatCep(value))

		viaCEP, err = requestViaCEP(value)
		if err != nil {
			panic(err)
		}
		_ = store.Insert(viaCEP)

		if value == end {
			break
		}

		// Increment the counter
		counter++
	}

	wg.Wait()
}

func onlyDigits(s string) int {
	var result int
	for _, v := range s {
		if v >= '0' && v <= '9' {
			result = result*10 + int(v-'0')
		}
	}
	return result
}
