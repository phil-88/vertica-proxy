# Vertica-proxy
This is a Crunchy-Proxy fork slightly rewritten for Vertica.
Proxy provides:
 * load balancing
 * query routing between vertica instances (master/replica)
 * query routing between vertica and postgresql instances (master/metadata)
 * query aborting
 * session pooling

Support tested for vsql, JDBC dbeaver, ODBC tableau connections.

Vertica support includes:
 * simple query
 * extended query (2-phase extended query and 3-phase extended query)
 * copy from local
 * pipelined queries
 
Vertica to postgresql protocol fallback suppors only simple query and 2-phase extended query. 
Thats why metadata postgres routing does not work for latest datagrip versions (it uses 3-phase extended query for metadata fetching).
 
Copy from stdin is not supported, so vertica_python copy does not work as well.
 
## How-to
Having golang installed and vertica-proxy pulled into ~/src/go/src/github.com/phil-88/vertica-proxy
``` 
export GOPATH=~/src/go/
export GOROOT=/usr/lib/go-1.9/
export PATH=$GOROOT/bin:$PATH

make build

./build/crunchy-proxy start --config ./build/config.yaml --log-level=debug
```
