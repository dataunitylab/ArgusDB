There are two ways users can make use of ArgusDB.
The first is to embed it within an application and use the internal APIs to interact with the database.
To run as a server, ArgusDB uses the Postgres wire protocol with the [pgwire](https://github.com/sunng87/pgwire) crate so it is compatible with existing Postgres clients.
In this scenario, clients will use the ArgusDB query language as defined in @specs/query-language.md.
