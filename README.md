# vban-server

A VBAN control server. It listens on UDP, answers pings, streams RT state
packets to subscribers, and hands parsed requests to whatever it controls.

Every other VBAN library I found is a client. This is the other side, so
existing clients drive it without changes. `vban-cmd` logs in against it, reads
state back, and writes parameters.

## Use

```rust
let server = Server::start(6980, Identity::default())?;

loop {
    for Request::Set(parameters) in server.poll() {
        // apply them to your own state
    }
    server.publish(state);
}
```

The socket runs on its own thread. `poll` never blocks. Nothing in here knows
what a mixer is.

## Status

TEXT and SERVICE work. AUDIO, SERIAL and MIDI are not implemented. Neither are
query replies, the reply a client expects when a request ends in `?`.

## License

Public domain. See UNLICENSE.
