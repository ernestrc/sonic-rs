#[macro_use]
extern crate sonic;
#[macro_use]
extern crate rux;
#[macro_use]
extern crate log;
extern crate env_logger;

use rux::buf::*;
use rux::handler::*;
use rux::mux::*;
use rux::poll::*;
use rux::prop::server::{Server, ServerConfig};
use rux::sys::socket::*;
use rux::system::System;
use sonic::SonicMessage;
use sonic::server::handler::SonicHandler;
use std::os::unix::io::RawFd;

const BUF_SIZE: usize = 512;
const EPOLL_BUF_CAP: usize = 2048;
const EPOLL_LOOP_MS: isize = -1;
const MAX_CONN: usize = 512;

/// Simple proxy to showcase SonicHandler usage
#[derive(Clone, Debug)]
struct SonicProxy {
  incoming_input_buffers: Vec<ByteBuffer>,
  incoming_output_buffers: Vec<ByteBuffer>,
  outgoing_input_buffers: Vec<ByteBuffer>,
  outgoing_output_buffers: Vec<ByteBuffer>,
}

impl<'b> HandlerFactory<'b, EpollEvent, MuxCmd> for SonicProxy {
  type H = ProxyHandler<'b>;

  fn done(&mut self, _: ProxyHandler<'b>, index: usize) {
    self.incoming_input_buffers[index].clear();
    self.incoming_output_buffers[index].clear();
    self.outgoing_input_buffers[index].clear();
    self.outgoing_output_buffers[index].clear();
  }

  fn new(&'b mut self, epfd: EpollFd, index: usize, incoming: RawFd) -> ProxyHandler<'b> {
    let inet_addr_std: ::std::net::SocketAddr = "0.0.0.0:10001".parse().unwrap();
    let inet_addr = InetAddr::from_std(&inet_addr_std);
    let sockaddr = SockAddr::Inet(inet_addr);
    let outgoing = socket(AddressFamily::Inet, SockType::Stream, SockFlag::empty(), 6).unwrap();
    connect(outgoing, &sockaddr).unwrap();

    epfd.register(outgoing,
                &EpollEvent {
                  events: EPOLLET | EPOLLIN | EPOLLOUT,
                  data: Action::encode(Action::Notify(index, outgoing)),
                })
      .unwrap();


    let outgoing_handler = SonicHandler::new(&mut self.outgoing_input_buffers[index],
                                             &mut self.outgoing_output_buffers[index],
                                             epfd,
                                             outgoing);

    let incoming_handler = SonicHandler::new(&mut self.incoming_input_buffers[index],
                                             &mut self.incoming_output_buffers[index],
                                             epfd,
                                             incoming);
    ProxyHandler::new(incoming, incoming_handler, outgoing, outgoing_handler)
  }
}

struct ProxyHandler<'b> {
  incoming: RawFd,
  incoming_handler: SonicHandler<'b>,
  outgoing: RawFd,
  outgoing_handler: SonicHandler<'b>,
}

impl<'b> ProxyHandler<'b> {
  fn new(incoming: RawFd, incoming_handler: SonicHandler<'b>, outgoing: RawFd,
         outgoing_handler: SonicHandler<'b>)
         -> ProxyHandler<'b> {
    ProxyHandler {
      incoming: incoming,
      outgoing: outgoing,
      incoming_handler: incoming_handler,
      outgoing_handler: outgoing_handler,
    }
  }
}

impl<'b> Drop for ProxyHandler<'b> {
  fn drop(&mut self) {
    error!("unistd::close: ${:?}", ::rux::close(self.outgoing));
  }
}

impl<'b> Handler<EpollEvent, MuxCmd> for ProxyHandler<'b> {
  fn ready(&mut self, event: EpollEvent) -> MuxCmd {
    if event.data as i32 == self.incoming {
      trace!("ready(): incoming");
      match self.incoming_handler.ready(event) {
        e @ MuxCmd::Close => return e,
        _ => {}
      };
    } else {
      trace!("ready(): outgoing");
      match self.outgoing_handler.ready(event) {
        e @ MuxCmd::Close => return e,
        _ => {}
      };
    }

    frame!(self.incoming_handler.input_buffer, |msg| {
        trace!("Client: {:?}", msg);
        self.outgoing_handler.ready(msg)
      })
      .unwrap();

    frame!(self.outgoing_handler.input_buffer, |msg| {
        trace!("Server: {:?}", msg);
        self.incoming_handler.ready(msg)
      })
      .unwrap();

    MuxCmd::Keep
  }
}

impl<'b> Reset for ProxyHandler<'b> {
  fn reset(&mut self, _: EpollFd) {}
}

pub fn main() {
  ::env_logger::init().unwrap();

  info!("BUF_SIZE: {}; EPOLL_BUF_CAP: {}; EPOLL_LOOP_MS: {}; MAX_CONN: {}",
        BUF_SIZE,
        EPOLL_BUF_CAP,
        EPOLL_LOOP_MS,
        MAX_CONN);

  let config = ServerConfig::tcp(("127.0.0.1", 10002))
    .unwrap()
    .max_conn(MAX_CONN)
    .io_threads(1)
    .epoll_config(EpollConfig {
      loop_ms: EPOLL_LOOP_MS,
      buffer_capacity: EPOLL_BUF_CAP,
    });

  let protocol = SonicProxy {
    incoming_input_buffers: vec!(ByteBuffer::with_capacity(BUF_SIZE); MAX_CONN),
    incoming_output_buffers: vec!(ByteBuffer::with_capacity(BUF_SIZE); MAX_CONN),
    outgoing_input_buffers: vec!(ByteBuffer::with_capacity(BUF_SIZE); MAX_CONN),
    outgoing_output_buffers: vec!(ByteBuffer::with_capacity(BUF_SIZE); MAX_CONN),
  };

  let server = Server::new_with(config, |epfd| {
      SyncMux::new(MAX_CONN, EPOLLIN | EPOLLOUT | EPOLLET | EPOLLRDHUP, epfd, protocol)
    })
    .unwrap();

  System::build(server).start().unwrap();
}
