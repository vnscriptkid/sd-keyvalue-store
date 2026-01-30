package main

import (
	"fmt"
	"sync"
	"sync/atomic"
)

type Account struct {
	balance atomic.Int64
	version atomic.Uint64
}

func (a *Account) Read() (bal int64, ver uint64) {
	return a.balance.Load(), a.version.Load()
}

func (a *Account) Deposit(amount int64) {
	for {
		bal, ver := a.Read()
		nextBal := bal + amount

		// try to claim the version
		if a.version.CompareAndSwap(ver, ver+1) {
			// we "won": apply the write
			a.balance.Store(nextBal)
			return
		}
		// lost the race; retry
	}
}

func main() {
	var acct Account
	const workers = 1000
	const amount = int64(1)

	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			acct.Deposit(amount)
			wg.Done()
		}()
	}
	wg.Wait()
	fmt.Printf("Final balance: %d\n", acct.balance.Load())
}
