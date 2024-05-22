package main

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/dyammarcano/dataprovider"
	"log"
	"runtime/debug"
)

func main() {
	defer func() {
		if r := recover(); r != nil {
			debug.PrintStack()
			log.Println("Recovered in main() func", r)
		}
	}()

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

	tx, err := conn.Beginx()
	if err != nil {
		panic(err)
	}

	for {
		// Convert the integer back to a string with leading zeros
		value = fmt.Sprintf("%08d", counter)
		log.Println("Requesting CEP: " + formatCep(value))

		viaCEP, err = requestViaCEP(value)
		if err != nil {
			panic(err)
		}

		if _, err = tx.Exec(viaCEP.add()); err != nil {
			panic(err)
		}

		if (counter+1)%1000 == 0 {
			if err = tx.Commit(); err != nil {
				panic(err)
			}

			tx, err = conn.Beginx()
			if err != nil {
				panic(err)
			}
		}

		if value == end {
			break
		}

		// Increment the counter
		counter++
	}

	if err = tx.Commit(); err != nil {
		panic(err)
	}
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
