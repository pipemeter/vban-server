//! A VBAN control server: the socket, the dispatch, and nothing else.
//!
//! Split from [`vban_common`] because the two have different jobs and
//! different reasons to change. The common crate is the wire format, and
//! it is pure: it has no socket, no threads and no opinions, which is why
//! its tests need nothing to run against. This crate is the part that
//! actually listens, and it is where retries, timeouts and subscriptions
//! belong.
//!
//! The split matters for the client too. A client needs the same header,
//! the same request grammar and the same pong layout as the server does,
//! and reaching for those should not drag a listening socket in with them.
//!
//! Nothing here knows what a mixer is. The server turns datagrams into
//! [`Request`]s and hands them over; what a `Strip[0].Gain` means is the
//! application's business, and keeping that boundary is what lets this be
//! tested without one.

#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::net::{SocketAddr, UdpSocket};
use std::sync::mpsc::{Receiver, Sender, TryIter};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub use vban_common::rt;

pub use vban_common as protocol;
pub use vban_common::{Parameter, Target};

/// What arrived, already parsed.
#[derive(Debug, Clone)]
pub enum Request {
    /// Apply these assignments, in the order they were sent.
    Set(Vec<Parameter>),
}

/// The listening end, held by whatever is being controlled.
#[derive(Debug)]
pub struct Server {
    requests: Receiver<Request>,
    /// Where it is listening, for logs and for anything that wants to say
    /// so in a window.
    address: String,
    /// The state subscribers are sent. Shared rather than sent down a
    /// channel: a subscriber wants the latest, not every frame since it
    /// last looked, and a channel would queue them all up.
    state: Arc<Mutex<rt::State>>,
}

/// How a server describes itself when a client asks.
#[derive(Debug, Clone)]
pub struct Identity {
    /// The application's name.
    pub application: String,
    /// The machine's name.
    pub host: String,
}

impl Default for Identity {
    fn default() -> Self {
        Self {
            application: "vban-server".to_owned(),
            host: std::env::var("HOSTNAME").unwrap_or_else(|_| "linux".to_owned()),
        }
    }
}

impl Server {
    /// Start listening.
    ///
    /// Binding is allowed to fail without taking the caller with it: the
    /// port may well be taken by an actual Voicemeeter bridge or another
    /// copy of the same program, and refusing to start because a network
    /// feature could not is the wrong trade.
    #[must_use]
    pub fn start(port: u16, identity: Identity) -> Option<Self> {
        // All interfaces: the point of the network protocol is being
        // reachable from the machine that runs the stream deck.
        let socket = match UdpSocket::bind(("0.0.0.0", port)) {
            Ok(socket) => socket,
            Err(err) => {
                log::warn!("no VBAN control server: could not bind port {port}: {err}");
                return None;
            }
        };
        let address = socket
            .local_addr()
            .map_or_else(|_| format!("0.0.0.0:{port}"), |addr| addr.to_string());
        log::info!("VBAN control server listening on {address}");

        // Woken regularly whether or not anything arrives, so subscribers
        // are served on time rather than only when a request happens to
        // come in. Without this the thread blocks in recv_from and a
        // subscribed client hears nothing until someone sends a request.
        if let Err(err) = socket.set_read_timeout(Some(TICK)) {
            log::warn!("could not set the VBAN read timeout: {err}");
        }

        let state = Arc::new(Mutex::new(rt::State::default()));
        let shared = Arc::clone(&state);
        let (sender, requests) = std::sync::mpsc::channel();
        std::thread::Builder::new()
            .name("vban-control".to_owned())
            .spawn(move || serve(&socket, &sender, &identity, &shared))
            .ok()?;

        Some(Self {
            requests,
            address,
            state,
        })
    }

    /// Hand over the state subscribers should see.
    ///
    /// Called by the owner whenever it likes; the server sends whatever
    /// the latest is at its own pace. A poisoned lock is ignored rather
    /// than panicked on: the socket thread only ever reads this, so the
    /// worst a lost update costs is one stale packet.
    pub fn publish(&self, state: rt::State) {
        if let Ok(mut held) = self.state.lock() {
            *held = state;
        }
    }

    /// Everything that arrived since this was last asked.
    #[must_use]
    pub fn poll(&self) -> TryIter<'_, Request> {
        self.requests.try_iter()
    }

    #[must_use]
    pub fn address(&self) -> &str {
        &self.address
    }
}

/// The socket thread.
///
/// It ends when the channel does, which is when the owner has dropped the
/// server - there is no other way out, and none is wanted: a control
/// server that stops listening after a bad packet is worse than useless.
fn serve(
    socket: &UdpSocket,
    sender: &Sender<Request>,
    identity: &Identity,
    state: &Arc<Mutex<rt::State>>,
) {
    let mut buffer = [0u8; vban_common::MAX_PACKET_SIZE];
    // Who is subscribed, and until when. A subscription is renewed by
    // asking again, which is how the protocol has a client say it is still
    // there - so one that goes away stops being sent to on its own.
    let mut subscribers: HashMap<SocketAddr, Instant> = HashMap::new();
    let mut frame: u32 = 0;
    let mut next_send = Instant::now();

    loop {
        // Due first, so a burst of requests cannot starve the subscribers.
        if Instant::now() >= next_send {
            frame = frame.wrapping_add(1);
            send_state(socket, &mut subscribers, state, frame);
            next_send += TICK;
            // If we fell behind - a slow frame, a suspended machine - the
            // next send is now rather than a burst catching up.
            if next_send < Instant::now() {
                next_send = Instant::now() + TICK;
            }
        }

        let (read, from) = match socket.recv_from(&mut buffer) {
            Ok(got) => got,
            // A timeout is the ordinary case, not a failure: it is what
            // wakes this loop to send state.
            Err(err)
                if matches!(
                    err.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                continue;
            }
            Err(err) => {
                log::warn!("VBAN receive failed: {err}");
                continue;
            }
        };

        // Anything that is not VBAN is ignored in silence at trace level.
        // This is an open UDP port; it will receive scans and strays, and
        // logging each one at warn would bury the real traffic.
        let Some(packet) = vban_common::parse(&buffer[..read]) else {
            log::trace!("ignoring {read} bytes from {from} that are not VBAN");
            continue;
        };

        match packet {
            vban_common::Packet::Text { body, header } => {
                // A request ending in `?` is a question, and the client is
                // waiting on the answer rather than sending a change.
                if body.trim_end().ends_with('?') {
                    answer_query(socket, from, &body, state, header.frame);
                    continue;
                }
                let parameters = vban_common::parse_request(&body);
                if parameters.is_empty() {
                    log::debug!("empty VBAN request from {from}: {body:?}");
                    continue;
                }
                log::info!(
                    "VBAN request ({} parameter{}) from {from} on stream {:?}: {}",
                    parameters.len(),
                    if parameters.len() == 1 { "" } else { "s" },
                    header.stream_name,
                    body.trim()
                );
                if sender.send(Request::Set(parameters)).is_err() {
                    // The owner is gone. Nothing left to control.
                    return;
                }
            }
            vban_common::Packet::Service {
                service,
                header,
                timeout,
            } => {
                if let vban_common::Service::RegisterRt = service {
                    // The fourth format byte is how many seconds it wants.
                    // Zero would mean a subscription that expires the
                    // instant it is made, so it is read as the protocol's
                    // usual default instead.
                    let seconds = if timeout == 0 { 15 } else { u64::from(timeout) };
                    let until = Instant::now() + Duration::from_secs(seconds);
                    if subscribers.insert(from, until).is_none() {
                        log::info!("{from} subscribed to state packets for {seconds}s");
                    }
                    continue;
                }
                service_packet(socket, from, service, header.frame, identity);
            }
            vban_common::Packet::Other { header } => {
                log::trace!("ignoring a VBAN {:?} packet from {from}", header.protocol);
            }
        }
    }
}

/// How often subscribers are sent the state, and how long the socket
/// waits for a packet before doing so.
///
/// Fifty milliseconds, which is the rate the mixer's own meters move at.
/// Faster would be sending the same numbers twice; slower and a level
/// meter driven from this would visibly step.
const TICK: Duration = Duration::from_millis(50);

/// Send the current state to everyone subscribed, and forget the ones
/// whose subscription has run out.
fn send_state(
    socket: &UdpSocket,
    subscribers: &mut HashMap<SocketAddr, Instant>,
    state: &Arc<Mutex<rt::State>>,
    frame: u32,
) {
    let now = Instant::now();
    subscribers.retain(|who, until| {
        let alive = *until > now;
        if !alive {
            log::info!("{who} stopped subscribing");
        }
        alive
    });
    if subscribers.is_empty() {
        return;
    }

    // Built once for everyone: the packet does not depend on who is
    // asking, and a dozen subscribers should not mean a dozen copies.
    let Ok(held) = state.lock() else {
        return;
    };
    let mut packet = rt::payload(&held);
    drop(held);
    packet[..vban_common::HEADER_SIZE].copy_from_slice(&vban_common::rt_header(frame).to_bytes());

    for who in subscribers.keys() {
        if let Err(err) = socket.send_to(&packet, who) {
            log::debug!("could not send state to {who}: {err}");
        }
    }
}

/// Answer a query, reading from the state the owner last published.
///
/// The TEXT channel is otherwise one-way, so this is the only way a client
/// gets a specific value back without subscribing to the whole state
/// packet. It answers from what has already been published rather than
/// asking the owner, which keeps the socket thread from blocking on it.
fn answer_query(
    socket: &UdpSocket,
    from: SocketAddr,
    body: &str,
    state: &Arc<Mutex<rt::State>>,
    frame: u32,
) {
    let Ok(held) = state.lock() else {
        return;
    };
    let mut answers = Vec::new();
    for name in body.trim_end().trim_end_matches('?').split([';', '\n']) {
        let name = name.trim();
        if name.is_empty() {
            continue;
        }
        match answer_one(&held, name) {
            Some(value) => answers.push(format!("{name}={value};")),
            // Named back rather than dropped, so a client can tell a
            // parameter we do not answer from one that is simply zero.
            None => answers.push(format!("{name}=?;")),
        }
    }
    drop(held);

    let payload = answers.join("");
    log::info!("VBAN query from {from}: {} -> {payload}", body.trim());
    let reply = vban_common::encode(&vban_common::reply_header(frame), payload.as_bytes());
    if let Err(err) = socket.send_to(&reply, from) {
        log::debug!("could not answer {from}: {err}");
    }
}

/// One parameter's current value, as text.
fn answer_one(state: &rt::State, name: &str) -> Option<String> {
    let lower = name.to_ascii_lowercase();
    let (head, field) = lower.split_once('.')?;
    let index: usize = head
        .split_once('[')
        .and_then(|(_, rest)| rest.strip_suffix(']'))
        .and_then(|digits| digits.parse().ok())?;

    let strip = head.starts_with("strip");
    let word = if strip {
        *state.strip_state.get(index)?
    } else {
        *state.bus_state.get(index)?
    };
    let gain = if strip {
        *state.strip_gain.first()?.get(index)?
    } else {
        *state.bus_gain.get(index)?
    };
    let labels = if strip {
        &state.strip_labels
    } else {
        &state.bus_labels
    };

    let bit = |mask: u32| Some(u8::from(word & mask != 0).to_string());
    match field {
        "gain" => Some(format!("{gain:.1}")),
        "mute" => bit(rt::state::MUTE),
        "solo" => bit(rt::state::SOLO),
        "mono" => bit(rt::state::MONO),
        "eq.on" | "eqon" => bit(rt::state::EQ_ON),
        "sel" => bit(rt::state::SEL),
        "label" => labels.get(index).cloned(),
        _ => rt::state::BUS_A
            .iter()
            .chain(rt::state::BUS_B.iter())
            .zip(["a1", "a2", "a3", "a4", "a5", "b1", "b2", "b3"])
            .find(|(_, route)| *route == field)
            .and_then(|(mask, _)| bit(*mask)),
    }
}

/// Answer a service packet.
fn service_packet(
    socket: &UdpSocket,
    from: SocketAddr,
    service: vban_common::Service,
    frame: u32,
    identity: &Identity,
) {
    match service {
        vban_common::Service::Ping => {
            // A pong describes what answered, and it has to be the full
            // size: a client checks the packet's length before it will
            // believe it, so a bare header is not a short answer but no
            // answer at all.
            let body = vban_common::pong::payload(&identity.application, &identity.host);
            let reply = vban_common::encode(&vban_common::pong_header(frame), &body);
            if let Err(err) = socket.send_to(&reply, from) {
                log::debug!("could not pong {from}: {err}");
            }
        }
        // Subscriptions are accepted and not yet served: the state packet
        // is the next piece of work. Saying so in the log beats a client
        // waiting on packets that never come, which is exactly how a real
        // client behaves against this today - it logs in, subscribes, and
        // then waits.
        other => log::debug!("VBAN service {other:?} from {from} is not answered yet"),
    }
}
