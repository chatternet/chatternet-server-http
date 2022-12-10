# Chatter Net Server

Chatter Net is a modern decentralized semantic web built atop self-sovereign identity.

For more about Chatter Net, [consult the client repository](https://github.com/chatternet/chatternet-client-http).

Warning: Chatter Net is currently in the prototype phase. Features are missing, features are broken, and the public interface will change.

This repository hosts a server implementation based on the HTTP protocol.

## Structure

### ChatterNet

The Chatter Net crate is included as a cargo workspace member.
It will be split out as a separate library once the interfaces are more stable.

This crate contains a partial implementation of ChatterNet data model.

### db

The [`db`] module provides an interface for persisting chatter net objects using sqlite.
The tables and interfaces serve the needs of a ChatterNet server, as opposed to a client.
In particular, the server must be able to build the inbox for any actor.

### handlers

The [`handlers`] module provides interfaces for handling requests and updating the state accordingly.

## TODO

- use foreign indices to synchronize document store with other stores
- use transactions for multi-part DB state changes
