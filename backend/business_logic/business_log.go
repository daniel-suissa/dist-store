package business_logic

import (
	"time"
	"sync"
	"sync/atomic"
	"../../common"
	"log"
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
	initRing()
	idCounter = 0
	addInitialBooks()
}

func addInitialBooks() {
	addBook(&common.Book{0, "Harry Potter", "J. K. Rolling"})
	addBook(&common.Book{1, "Sapiens", "Yuval Noah Harari"})
	addBook(&common.Book{2, "The Art of Happiness", "The Dalhi Lama"})
	addBook(&common.Book{3, "Predictibly Unpredictable", "Dan Arieli"})
	addBook(&common.Book{4, "Blink", "Malcolm Gladwell"})
	addBook(&common.Book{5, "Principia Mathematica", "Bertrand Russel"})
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
func addBook(book *common.Book) {
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
func deleteBook(book *common.Book) {
	ringPos := findInRing(book.Id)
	defer ringLocks[ringPos].Unlock()
	books.Delete(book.Id)
	delete(ring[ringPos], book.Id)
	atomic.AddInt32(&booksLen, -1)
}

//safely sets the contents of a book in the db
func editBook(book *common.Book) {
	ringPos := findInRing(book.Id)
	defer ringLocks[ringPos].Unlock()
	books.Store(book.Id, book)
}

//safely gets the updated contents of a book from the db
func getBook(book *common.Book) common.Book {
	ringPos := findInRing(book.Id)
	defer ringLocks[ringPos].Unlock()
	ret, ok := books.Load(book.Id)
	if !ok {
		panic("somehow trying to get a book that isn't there")
	}
	retBook, ok := ret.(*common.Book)
	if !ok {
		panic("cant convert book to type Book")
	}
    return *retBook
}

//safely get a typesafe version of the book map
func getAllBooks() map[int32]*common.Book {
	retMap := make(map[int32]*common.Book)
	var id int32
	var book *common.Book
	books.Range(func(k interface{}, v interface{}) bool {
		id = k.(int32)
		book = v.(*common.Book)
		retMap[id] = book
		return true
		})

	return retMap
}
func Reprocess(msgLog []*common.AppendMessage) {
	log.Printf("BUSINESS LOGIC: Resetting log and reprocessing\n")
	//get all locks before
	for _, lock := range(ringLocks) {
		lock.Lock()
	}
	//reset the ring
	ring = ring[:0]
	for i := 0; i < RINGSIZE; i++ {
		ring = append(ring,make(map[int32]int))
	}
	idCounter = 0
	addInitialBooks()

	for _, appndMsg := range(msgLog) {
		if !appndMsg.ShouldIgnore {
			ProcessWrite(&appndMsg.Msg)
		}
	}


	for _, lock := range(ringLocks) {
		lock.Unlock()
	}
}

func IsRead(clientMsg *common.ClientMessage) bool {
	if clientMsg.Cmd == "getall" || clientMsg.Cmd == "getone"{ 
		return true
	} else {
		return false
	}
}


func ProcessRead(clientMsg *common.ClientMessage) interface{} {
	log.Printf("BUSINESS LOGIC: Processing %#v\n", *clientMsg)
	if clientMsg.Cmd == "getall" {
		return getAllBooks()
	} else if clientMsg.Cmd == "getone"{
		return getBook(&clientMsg.Book)
	} else {
		log.Printf("Something went terribly wrong with %#v\n", *clientMsg)
		return nil
	}

}
//constantly listen for incoming commands and handle each command appropriately
func ProcessWrite(clientMsg *common.ClientMessage) {
	log.Printf("BUSINESS LOGIC: Processing %#v\n", *clientMsg)
	if clientMsg.Cmd == "new" {
		addBook(&clientMsg.Book)
	} else if clientMsg.Cmd == "update" {
		editBook(&clientMsg.Book)
	} else if clientMsg.Cmd == "delete" {
		deleteBook(&clientMsg.Book)
	} else {
		log.Printf("Something went terribly wrong with %#v\n", *clientMsg)
	}
}

