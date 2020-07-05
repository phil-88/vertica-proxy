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
 
 
