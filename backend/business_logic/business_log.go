package business_logic

import (
	"bufio"
	"log"
	"net"
	"io"
	"encoding/gob"
	"os"
	"time"
	"sync"
	"fmt"
	"sync/atomic"
)

//used to implement the mutex with the TryLock
type Mutex struct {
	c chan struct{}
}

//id -> Book
var books sync.Map //different indexes of the map are protected by the ring set lock
var booksLen int32

//id counter to create unique ids (since all records are in memory it is assumed an integer is enough)
var idCounter int32

const RINGSIZE = 5
var ring []map[int32]int // collection of sets of bookIds. 

//Need a lock on i to access the set ring[i]
var ringLocks []*Mutex //lock for every set in the ring slices


func NewMutex() *Mutex {
	return &Mutex{make(chan struct{}, 1)}
}

func (m *Mutex) Lock() {
	m.c <- struct{}{}
}

func (m *Mutex) Unlock() {
	<-m.c
}

// try to lock for 10ms, if lock not acquired return false
func (m *Mutex) TryLock() bool {
	timeout := time.Second /100
	timer := time.NewTimer(timeout)
	select {
	case m.c <- struct{}{}:
		timer.Stop()
		return true
	case <- time.After(timeout):
	}
	return false
}

//initialize the book map with some initial records
func InitBookMap() {
	//books = make(map[int32]*Book)
	initRing()
	idCounter = 0
	addBook(&Book{0, "Harry Potter", "J. K. Rolling"})
	addBook(&Book{1, "Sapiens", "Yuval Noah Harari"})
	addBook(&Book{2, "The Art of Happiness", "The Dalhi Lama"})
	addBook(&Book{3, "Predictibly Unpredictable", "Dan Arieli"})
	addBook(&Book{4, "Blink", "Malcolm Gladwell"})
	addBook(&Book{5, "Principia Mathematica", "Bertrand Russel"})
}

func resetRing() {
	for _, lock := range(ringLocks) {
		lock.Lock()
	}
	ring = ring[:0]
	for i := 0; i < RINGSIZE; i++ {
		ring = append(ring,make(map[int32]int))
	}
}
//sets up the ring sets and the locks for it
func initRing() {
	for i := 0; i < RINGSIZE; i++ {
		ringLocks = append(ringLocks, NewMutex())
		ring = append(ring,make(map[int32]int))
	}
}

// returns the ring position of the set containing bookId
// the called is guaranteed to hold the lock for that position
func findInRing(bookId int32) int {
	ringPositions := make([]int, RINGSIZE)
	for i := 0; ; i = (i+1)%RINGSIZE {
		if ringPositions[i] == -1 {
			//looked here before
			continue
		} else {
			ok := ringLocks[i].TryLock()
			if ok {
				if _,found := ring[i][bookId]; found {
					return i
				} else {
					ringPositions[i] = -1 //don't look in this position again
					ringLocks[i].Unlock()
				}
			}
		}
	}
}

//safely add a book to the db
func addBook(book *Book) {
	i := 0
	for {
		if len(ring[i]) > int(booksLen / 2) {
			i = (i+1)%RINGSIZE
			continue
		}
		ok := ringLocks[i].TryLock()
		if ok {
			break
		}
		i = (i+1)%RINGSIZE
	}
	defer ringLocks[i].Unlock()

	book.Id = idCounter
	ring[i][idCounter] = 0
	books.Store(idCounter, book)
	
	atomic.AddInt32(&booksLen, 1)
    atomic.AddInt32(&idCounter, 1)
}

//safely delete a book from the db
func deleteBook(book *Book) {
	ringPos := findInRing(book.Id)
	defer ringLocks[ringPos].Unlock()
	books.Delete(book.Id)
	delete(ring[ringPos], book.Id)
	atomic.AddInt32(&booksLen, -1)
}

//safely sets the contents of a book in the db
func editBook(book *Book) {
	ringPos := findInRing(book.Id)
	defer ringLocks[ringPos].Unlock()
	books.Store(book.Id, book)
}

//safely gets the updated contents of a book from the db
func getBook(book *Book) Book {
	ringPos := findInRing(book.Id)
	defer ringLocks[ringPos].Unlock()
	ret, ok := books.Load(book.Id)
	if !ok {
		panic("somehow trying to get a book that isn't there")
	}
	retBook, ok := ret.(*Book)
	if !ok {
		panic("cant convert book to type Book")
	}
    return *retBook
}

//safely get a typesafe version of the book map
func getAllBooks() map[int32]*Book {
	retMap := make(map[int32]*Book)
	var id int32
	var book *Book
	books.Range(func(k interface{}, v interface{}) bool {
		id = k.(int32)
		book = v.(*Book)
		retMap[id] = book
		return true
		})

	return retMap
}
func Reprocess()
//constantly listen for incoming commands and handle each command appropriately
func HandleCmd(clientMsg *common.ClientMessage) () {
		err = nil
		switch {
			case request.Cmd == "getall":
				allBooks := getAllBooks()
				err = sendMessage(allBooks, rw)
			case request.Cmd == "getone":
				err = sendMessage(getBook(&request.Book), rw)
			case request.Cmd == "new": 
				addBook(&request.Book)
			case request.Cmd == "update":
				editBook(&request.Book)
			case request.Cmd == "delete":
				deleteBook(&request.Book)
			case request.Cmd == "ping":
				err = sendMessage(1, rw)
		}
		if err != nil {
			log.Println("Failed to send response: ", err)
		}
	}
}
