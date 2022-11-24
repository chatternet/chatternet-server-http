# Chatter Net Server

Chatter Net is a modern decentralized semantic web built atop self-sovereign identity.

For more about Chatter Net, [consult the client repository](https://github.com/chatternet/chatternet-client-http).

Warning: Chatter Net is currently in the prototype phase. Features are missing, features are broken, and the public interface will change.

This repository hosts a server implementation based on the HTTP protocol.

## TODO

This code base is a work-in-progress.
Here are some important next steps:

- Change server to axum, or nest the warp path filters to get more sensible errors.
  - Compilation times are a bit slow with warp.
  - The filter system can lead to subtle errors (on the part of the programmer).
- Stores messages along side objects.
  - Messages are just another form of objects,
    the distinction currently can lead to duplicate code and unnecessary branching.
- Deletions.

In the medium term, the SQL queries are likely to lead to many bottle necks and improvements can be made:

- Consider query planning.
- The inbox get query is very heavy and will be used often (maybe 10x more than the outbox post),
  some better structures or caching could help.

In the long term, it would nice for this server to expose a Mastodon-compatible API.
