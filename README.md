# vban-server

UDP server implementation for the VBAN control protocol and real-time state broadcasts.

## Features

- **VBAN-TEXT & Service handling**: Receives and dispatches incoming commands.
- **RT State broadcasting**: Streams real-time meter levels, fader states, and labels to subscribed clients.
- **Submodule support**: Bundles `vban-common` as a git submodule.

## Submodules

To clone with submodules:
```bash
git clone --recurse-submodules https://github.com/pipemeeter/vban-server.git
```

## License

Licensed under either of Apache License, Version 2.0 or MIT license at your option.
