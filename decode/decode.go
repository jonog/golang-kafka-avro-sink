package main

import (
	"fmt"
	"log"
	"os"

	"github.com/hamba/avro/ocf"
)

type Pokemon struct {
	Name string `avro:"name"`
}

func main() {

	f, err := os.Open(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	dec, err := ocf.NewDecoder(f)
	if err != nil {
		log.Fatal(err)
	}

	for dec.HasNext() {
		var record Pokemon
		err = dec.Decode(&record)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println(record.Name)
	}

	if dec.Error() != nil {
		log.Fatal(err)
	}

}
