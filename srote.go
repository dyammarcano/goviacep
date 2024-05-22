package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/jmoiron/sqlx"
	bolt "go.etcd.io/bbolt"
	"sync"
	"time"
)

type Store struct {
	db    *sqlx.DB
	bbolt *bolt.DB
	wg    *sync.WaitGroup
	ctx   context.Context
}

func NewStore(ctx context.Context, db *sqlx.DB, bbolt *bolt.DB, wg *sync.WaitGroup) *Store {
	s := &Store{ctx: ctx, db: db, bbolt: bbolt, wg: wg}
	go s.control()
	return s
}

func (s *Store) Close() {
	s.db.Close()
	s.bbolt.Close()
	s.wg.Done()
}

func (s *Store) control() {
	s.wg.Add(1)
	defer s.wg.Done()

	s.bbolt.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucket([]byte("cep")); err != nil && !errors.Is(err, bolt.ErrBucketExists) {
			return fmt.Errorf("create bucket: %s", err)
		}
		return nil
	})

	dbtx, err := s.db.Beginx()
	if err != nil {
		panic(err)
	}

	counter := 0

	for {
		<-time.After(1 * time.Second)
		select {
		case <-s.ctx.Done():
			if err = dbtx.Commit(); err != nil {
				panic(err)
			}
			break
		default:
			s.bbolt.Update(func(tx *bolt.Tx) error {
				// Assume bucket exists and has keys
				b := tx.Bucket([]byte("cep"))

				c := b.Cursor()
				for k, v := c.First(); k != nil; k, v = c.Next() {
					//log.Printf("key=%s, value=%s\n", k, v)
					if _, err = dbtx.Exec(string(v)); err != nil {
						panic(err)
					}

					// remove the key from the store
					if err = b.Delete(k); err != nil {
						panic(err)
					}

					if (counter+1)%1000 == 0 {
						dbtx, err = s.commitAndReset(dbtx)
						if err != nil {
							panic(err)
						}
					}
				}
				dbtx, err = s.commitAndReset(dbtx)
				if err != nil {
					panic(err)
				}
				return nil
			})
		}
	}
}

func (s *Store) commitAndReset(tx *sqlx.Tx) (*sqlx.Tx, error) {
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return s.db.Beginx()
}

func (s *Store) Insert(v *ViaCEP) error {
	return s.bbolt.Update(func(tx *bolt.Tx) error {
		// Assume bucket exists and has keys
		b := tx.Bucket([]byte("cep"))
		if err := b.Put([]byte(v.Cep), []byte(v.add())); err != nil {
			panic(err)
		}
		return nil
	})
}
