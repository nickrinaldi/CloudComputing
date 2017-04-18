execute the program which simulates the Linda tuple store by running "java P1 <host name>"
where <host name> is any host name of your choosing.

The tuple store is distributed across multiple hosts but represents a centralized tuple store.

add any number of hosts with the add command:
	add (<host>,<ip address>, <port>) (<host 2>, <ip address>, <port>)
	hosts are represented by host name, ip address, and port separated by commas and surrounded
	by parenthesis.

put tuples into the tuple store with the out command:
	out <tuple>
	where <tuple> represents a tuple e.g. ("abc", 1)
	the tuple must be surrounded by parenthesis

get tuples with the rd command:
	rd <tuple>
	where <tuple> represents a tuple including queries e.g. (1, ?var:string) matches the tuple
	(1, "abc"). If no tuple exists, the client requesting the tuple will wait until a tuple that
	matches the request exists in the tuple store.

additionally the in command functions the same as the rd command however removes the retrieved tuple
from the tuple store.

Servers handle requests by spawning thread to deal with the request. These requests primarily spawn from
clients, but can also spawn from other servers. These threads sleep when requesting a tuple from the tuple
store which allows the server to still handle requests during this period.
