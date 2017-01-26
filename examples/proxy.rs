#[macro_use]
extern crate sonic;
#[macro_use]
extern crate rux;
#[macro_use]
extern crate log;
extern crate env_logger;

use rux::buf::*;
use rux::error::*;
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
struct ProxyFactory {
}

struct Buffers {
  incoming_input_buffer: ByteBuffer,
  incoming_output_buffer: ByteBuffer,
  outgoing_input_buffer: ByteBuffer,
  outgoing_output_buffer: ByteBuffer,
}

impl<'p> HandlerFactory<'p, ProxyHandler<'p>, Buffers> for ProxyFactory {

  fn new_resource(&self) -> Buffers {
    Buffers {
  incoming_input_buffer: ByteBuffer::with_capacity(BUF_SIZE),
  incoming_output_buffer: ByteBuffer::with_capacity(BUF_SIZE),
  outgoing_input_buffer: ByteBuffer::with_capacity(BUF_SIZE),
  outgoing_output_buffer: ByteBuffer::with_capacity(BUF_SIZE),
    }
  }
  fn new_handler(&mut self, epfd: EpollFd, incoming: RawFd) -> ProxyHandler<'p> {
    let inet_addr_std: ::std::net::SocketAddr = "0.0.0.0:10001".parse().unwrap();
    let inet_addr = InetAddr::from_std(&inet_addr_std);
    let sockaddr = SockAddr::Inet(inet_addr);
    let outgoing = socket(AddressFamily::Inet, SockType::Stream, SockFlag::empty(), 6).unwrap();
    connect(outgoing, &sockaddr).unwrap();
    debug!("connected to {:?}", &inet_addr_std);

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
    report_err!(::rux::close(self.outgoing));
  }
}

impl<'b> Handler<EpollEvent, MuxCmd> for ProxyHandler<'b> {
  fn on_next(&mut self, event: EpollEvent) -> MuxCmd {
    if event.data as i32 == self.incoming {
      trace!("on_next(): incoming");
      keep!(self.incoming_handler.on_next(event));
    } else {
      trace!("on_next(): outgoing");
      keep!(self.outgoing_handler.on_next(event));
    }

    keep!(frame!(self.incoming_handler.input_buffer, |msg| {
        trace!("Client: {:?}", msg);
        self.outgoing_handler.on_next(msg)
      })
      .unwrap());

    keep!(frame!(self.outgoing_handler.input_buffer, |msg| {
        trace!("Server: {:?}", msg);
        self.incoming_handler.on_next(msg)
      })
      .unwrap());

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

  let server = Server::new_with(config, |epfd| {
      SyncMux::new(MAX_CONN, epfd, ProxyFactory)
    })
    .unwrap();

  System::build(server).start().unwrap();
}
