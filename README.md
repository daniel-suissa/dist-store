# dist-store

Name: Daniel Suissa

Instructions:
	- Get Iris: go get -u github.com/kataras/iris
	- Build the front end (from within the frontend folder): go build frontend_server.go
	- Build the back end (from within the backend folder): go build backend_server.go
	- Run the frontend: ./frontend_server (optionally --listen [PORT])  (mandatory --backend [Addr1, Addr2 ...]). Example: ./frontend_server --backend :61000,:8081,:8082 
	- Run the backend: ./backend_server (optionally --listen [PORT]) (mandatory --backend [Addr1, Addr2 ....] --id [ID]). Example: ./backend_server --listen 8081 --backend :8082,:61000 --id 1
	- make sure there are no spaces between the commas when providing backend addresses

Test: 
	Once the program is running, go to localhost:PORT. The default port is 8080.
	Delete a record by clicking "Delete". Edit a record by clicking "Update", and add a record by clicking "Add a Book"

Assignment State: 
	Done to best of my testing
	At some rare cases there might be candidate competition. It shouldn't happen but if it does, stop one of the backends and restart it

Design Decisions:
	- I used RAFT as a replication strategy. My application is a simple reading list, where the web interface allows for 2 kinds of READ operations and 3 kinds of WRITE operations. Since RAFT is the algorithm I was most comfortable with, and since I could model all WRITES as one type of append entry, I chose RAFT.
	- I decided that all log entries will contain the clients message and that the business logic package will take care of proceesing them into the db

	- Leaders accept messages from client and send append messages based on the state of their logs
	- Followers respond to append messages with "Ok" or "inconsistentLog". The client is reponsible for recovering its log (all of it) by requesting it from the leader. 
	- Entries that were not committed in time before responding with "failure" to the client get a ShouldIgnore = true field on them. They will get committed in the future, but all the nodes will know not to process them.
	- Leaders, Candidates and Followers all submit the nodes that send append entries if the sender have term higher than the reciever and the logs are consistent. Nodes also submit if the term is the same and the log state is more advanced

	- a bit about packages on the backend: 
		(1) The infra packages handles node traking and communications between nodes and threads 
		(2) The raft package handles all message processing, term and log management, and well...all RAFT things
		(3) The business logic package handles the actuall processing of write messages and provide necessary information on read messages. It uses the same locking mechanism that was used in project 3 (appendix A)
	- Pros: 
		- By using the RAFT approach of appending messages to the log and wait for the pinger to send them for commit, I could leverage more goroutines and less hanging around (see con for this one)
		- I tried very hard from the beginning to have an architecture that separated responsibilities to different packages and threads. I didn't want to do a lot of "trial and error" mechanism and so I kept track of nodes, threads, leaders, and candidates. That made understaning my own program easy (hopefully for you as well) (see con for this)
	-Cons:
		- The many goroutines and channel communication made this program extremely hard to debug and required a lot of locking which ofcourse caused disasters at times.
		- By over designing this, I ended up with *a lot* for lines of code
Assignment Feedback:





Appendix A (from the project3 README):

Concurrency method and analysis:
In a few words: I am dividing my main book map into disjoint sets of keys, conrolled by a collection of Mutexes.
	- Definitions:
		- ring: a slice of maps, each map contains the ids associated with this set
		- ringPos: the position of a set of ids inside the ring
		- ringLocks: a collection of Mutexes, each for each disjoint set of ids. 
		- db: the main book map

	- Access Rules:
		- Adding a record
			- acquire a ring lock (ensure exclusive access to a set of records)
			- make the changes
			- release locks
		- Deleting/Reading / Updating
			- search for the correct set and acquire the lock for it (Theta(ringSize))
			- perform the operation on the sync map
			- release locks
	
	- Advantages:
		- up to RINGSIZE threads can access the db if they are referring to different key sets.
	- Distadvantages:
		- Operations can take up to Theta(RINGSIZE) trials of sets before they can access the map. Since this is a very small number and the fact we're making use of a TryLock mechanism - this is negligible