package main

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/dyammarcano/dataprovider"
	"log"
	"time"
)

func main() {
	// Create a config with driver name to initialize the data provider
	cfg := dataprovider.NewConfigModule().
		WithDriver(dataprovider.SQLiteDataProviderName).
		WithName("database").
		Build()

	provider, err := dataprovider.NewDataProvider(cfg)
	if err != nil {
		panic(err)
	}

	if err = provider.InitializeDatabase(createTableViaCEP); err != nil {
		panic(err)
	}

	conn := provider.GetConnection()

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

	for {
		// Convert the integer back to a string with leading zeros
		value = fmt.Sprintf("%08d", counter)
		log.Println("Requesting CEP: " + formatCep(value))

		viaCEP, err = requestViaCEP(value)
		if err != nil {
			panic(err)
		}

		if _, err = conn.Exec(viaCEP.add()); err != nil {
			panic(err)
		}

		if value == end {
			break
		}

		if counter%1000 == 0 {
			log.Println("Waiting 30 minutes to continue")
			<-time.After(30 * time.Minute)
		}

		// Increment the counter
		counter++
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
