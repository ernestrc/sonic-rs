use error::*;
use model::SonicMessage;
use rux::{send as rsend, recv as rrecv};
use rux::buf::ByteBuffer;
use rux::handler::{Handler, Reset};
use rux::handler::mux::MuxCmd;
use rux::poll::*;
use rux::sys::socket::*;
use std::os::unix::io::RawFd;

static MIN_BUFFER_SIZE: &'static usize = &128;
static DEFAULT_MAX_MESSAGE_SIZE: &'static usize = &(1024 * 1024);

#[macro_export]
macro_rules! or_complete {
    ($e:expr, $sel:expr, $fd: expr) => {{
        match $e {
            Ok(cmd) => cmd,
            Err(e) => {
                error!("or_complete!: {}", e);
                return $sel.complete_err(e, $fd);
            },
        }
    }}
}

// TODO should not take a Handler, rather it should be designed
// to interact with new design where it is owned by the
// downstream handler
pub struct SonicHandler<'b, H>
  where H: Handler<SonicMessage, Option<SonicMessage>> + Reset,
{
  next: H,
  epfd: EpollFd,
  pub input_buffer: &'b mut ByteBuffer,
  pub output_buffer: &'b mut ByteBuffer,
  is_readable: bool,
  is_writable: bool,
  max_msg_size: usize,
  closed_write: bool,
  completing: bool,
}

impl<'b, H> SonicHandler<'b, H>
  where H: Handler<SonicMessage, Option<SonicMessage>> + Reset,
{
  pub fn new(input_buffer: &'b mut ByteBuffer, output_buffer: &'b mut ByteBuffer, epfd: EpollFd,
             next: H)
             -> SonicHandler<'b, H> {
    assert!(input_buffer.capacity() >= *MIN_BUFFER_SIZE);
    assert!(output_buffer.capacity() >= *MIN_BUFFER_SIZE);
    SonicHandler {
      next: next,
      input_buffer: input_buffer,
      output_buffer: output_buffer,
      epfd: epfd,
      is_readable: false,
      is_writable: false,
      closed_write: false,
      max_msg_size: *DEFAULT_MAX_MESSAGE_SIZE,
      completing: false,
    }
  }

  #[inline(always)]
  fn try_write(&mut self, fd: RawFd) -> Result<MuxCmd> {
    if !self.output_buffer.is_readable() {
      if self.closed_write {
        return Ok(MuxCmd::Close);
      }
      if self.completing {
        let epfd = self.epfd;
        self.reset(epfd);
      }
    }

    if !self.is_writable {
      return Ok(MuxCmd::Keep);
    }

    let mut len = self.output_buffer.readable();

    while len > 0 {

      match rsend(fd, From::from(&*self.output_buffer), MSG_DONTWAIT)? {
        None => {
          trace!("SonicHandler::try_write(): not ready");
          self.is_writable == false;
          break;
        }
        Some(cnt) => {
          trace!("SonicHandler::try_write(): written {} bytes", cnt);
          self.output_buffer.consume(cnt);
          if cnt == len {
            break;
          }
          len = self.output_buffer.readable();
        }
      }
    }

    if self.closed_write && !self.output_buffer.is_readable() {
      return Ok(MuxCmd::Close);
    }

    Ok(MuxCmd::Keep)
  }

  #[inline(always)]
  fn try_read(&mut self, fd: RawFd) -> Result<()> {
    if self.closed_write || !self.is_readable {
      return Ok(());
    }

    while self.input_buffer.is_writable() {

      match rrecv(fd, From::from(&mut *self.input_buffer), MSG_DONTWAIT)? {
        Some(0) => {
          trace!("SonicHandler::try_read(): EOF");
          self.is_readable == false;
          self.closed_write = true;
          break;
        }
        Some(cnt) => {
          trace!("SonicHandler::try_read(): read {} bytes", cnt);
          self.input_buffer.extend(cnt)
        }
        None => {
          trace!("SonicHandler::try_read(): not ready");
          self.is_readable == false;
          break;
        }
      }

    }

    if self.is_readable && !self.input_buffer.is_writable() {
      // reserve more capacity and retry
      let additional = self.input_buffer.capacity();
      self.input_buffer.reserve(additional);
      self.assert_max_size(&*self.input_buffer)?;

      trace!("SonicHandler::try_read(): increased input_buffer size by {}", additional);
      return self.try_read(fd);
    }

    Ok(())
  }

  fn complete_err(&mut self, err: Error, fd: RawFd) -> MuxCmd {
    self.completing = true;

    // FIXME if error occurs when some bytes have been flushed
    self.output_buffer.clear();

    let msg = SonicMessage::complete::<()>(Err(err), "".to_owned());
    self.buffer(msg).unwrap();
    self.try_write(fd).unwrap()
  }

  #[inline(always)]
  fn assert_max_size(&self, buffer: &ByteBuffer) -> Result<()> {
    if buffer.capacity() > self.max_msg_size {
      error!("message surpasses max msg size of {}", self.max_msg_size);
      bail!(ErrorKind::MessageTooBig(self.max_msg_size));
    }

    Ok(())
  }

  fn buffer(&mut self, msg: SonicMessage) -> Result<()> {
    match msg.to_buffer(self.output_buffer) {
      Err(Error(ErrorKind::BufferTooSmall(msg), _)) => {
        let additional = self.output_buffer.capacity();
        self.output_buffer.reserve(additional);

        self.assert_max_size(&*self.output_buffer)?;

        trace!("SonicHandler::buffer(): increased output_buffer size by {}", additional);
        self.buffer(msg)
      }
      Ok(_) => Ok(()),
      Err(e) => Err(e),
    }
  }
}

impl<'b, H> Reset for SonicHandler<'b, H>
  where H: Handler<SonicMessage, Option<SonicMessage>> + Reset,
{
  fn reset(&mut self, epfd: EpollFd) {
    self.epfd = epfd;
    self.completing = false;
    self.input_buffer.clear();
    self.output_buffer.clear();
    self.next.reset(epfd);
  }
}

impl<'b, H> Handler<EpollEvent, MuxCmd> for SonicHandler<'b, H>
  where H: Handler<SonicMessage, Option<SonicMessage>> + Reset,
{
  fn ready(&mut self, event: EpollEvent) -> MuxCmd {
    let fd = event.data as i32;
    let kind = event.events;
    trace!("ready({:?}: {:?})", &fd, &kind);

    if kind.contains(EPOLLHUP) {
      trace!("fd={}: EPOLLHUP", fd);
      return MuxCmd::Close;
    }

    if kind.contains(EPOLLRDHUP) {
      trace!("fd={}: EPOLLRDHUP", fd);
      self.closed_write = true;
    }

    if kind.contains(EPOLLIN) {
      trace!("fd={}: EPOLLIN", fd);
      self.is_readable = true;
    }

    if kind.contains(EPOLLOUT) {
      trace!("fd={}: EPOLLOUT", fd);
      self.is_writable = true;
    }

    if kind.contains(EPOLLERR) {
      let err = format!("fd={}: EPOLLERR", fd);
      error!("{}", err);
      or_complete!(Err(err.into()), self, fd);
    }

    or_complete!(self.try_read(fd), self, fd);

    loop {
      match or_complete!(SonicMessage::from_buffer(&mut *self.input_buffer), self, fd) {
        Some(msg) => {
          if let Some(res) = self.next.ready(msg) {
            trace!("next.ready(): {:?}", res);
            or_complete!(self.buffer(res), self, fd);
          }
        }
        None => {
          trace!("no message to frame from input buffer");
          break;
        }
      }
    }

    or_complete!(self.try_write(fd), self, fd)
  }
}

#[cfg(test)]
mod tests {
  extern crate env_logger;
  use model::*;
  use rux::sys::socket::*;
  use serde_json::Value;
  use super::*;

  const BUF_SIZE: usize = 256;

  struct TestHandler;

  impl Reset for TestHandler {
    fn reset(&mut self, _: EpollFd) {}
  }

  impl Handler<SonicMessage, Option<SonicMessage>> for TestHandler {
    fn ready(&mut self, i: SonicMessage) -> Option<SonicMessage> {
      println!(">>>>>>>>>> {:?}", i);
      Some(i)
    }
  }

  fn setup_logger() {
    let mut builder = env_logger::LogBuilder::new();
    builder.parse("trace");
    builder.init(); // ignore
  }

  fn get_stream_msgs() -> Vec<SonicMessage> {
    let mut q = String::new();

    for _ in 1..100 {
      q.push_str("select * from what?");
    }
    vec![SonicMessage::AuthenticateMsg(Authenticate::new("marcelino".to_owned(),
                                                         "panivino".to_owned(),
                                                         Some("1234".to_owned()))),
         SonicMessage::QueryMsg(Query::new(q, None, None, Value::String("".to_owned()))),
         SonicMessage::Acknowledge,
         SonicMessage::StreamStarted("jrklew".to_owned()),
         SonicMessage::TypeMetadata(vec![(".jre".to_owned(), Value::Bool(true))]),
         SonicMessage::OutputChunk(vec![Value::Null]),
         SonicMessage::QueryProgress {
           status: QueryStatus::Queued,
           progress: 0.4_f64,
           total: None,
           units: Some("sdkljelkdq".to_owned()),
         },
         SonicMessage::StreamCompleted(Some("".to_owned()), ".to".to_owned())]
  }

  fn new_handler<'a>(input_buffer: &'a mut ByteBuffer, output_buffer: &'a mut ByteBuffer)
                     -> SonicHandler<'a, TestHandler> {
    let epfd: EpollFd = EpollFd::new(0);
    SonicHandler::new(input_buffer, output_buffer, epfd, TestHandler)
  }

  fn setup_socketpair() -> (RawFd, RawFd, EpollEvent, EpollEvent, EpollEvent, EpollEvent) {
    let (s1, s2) = socketpair(AddressFamily::Unix, SockType::Stream, 0, SockFlag::empty()).unwrap();
    let readable = EpollEvent {
      data: s1,
      events: EPOLLIN,
    };
    let writable = EpollEvent {
      data: s1,
      events: EPOLLOUT,
    };
    let half_close = EpollEvent {
      data: s1,
      events: EPOLLRDHUP,
    };
    let close = EpollEvent {
      data: s1,
      events: EPOLLHUP,
    };
    (s1, s2, readable, writable, half_close, close)
  }

  fn assert_error_contains(msg: SonicMessage, chunk: &'static str) {
    let e: Option<String> = match msg {
      SonicMessage::StreamCompleted(e, _) => e,
      m => panic!("unexpeted message: {:?}", m),
    };

    let error = e.unwrap_or_else(|| panic!("CompleteStream message was not an error"));

    assert!(error.contains(chunk));
  }

  #[test]
  fn ser_de_correctly() {
    setup_logger();
    let (_, s2, readable, writable, _, _) = setup_socketpair();
    let mut input_buffer = ByteBuffer::with_capacity(BUF_SIZE);
    let mut output_buffer = ByteBuffer::with_capacity(BUF_SIZE);
    let mut handler = new_handler(&mut input_buffer, &mut output_buffer);
    let mut cmd;

    let msgs = get_stream_msgs();

    for msg in msgs {
      cmd = handler.ready(readable.clone());
      assert_eq!(cmd, MuxCmd::Keep);

      ::rux::write(s2, &msg.clone().into_bytes().unwrap()).unwrap();
      cmd = handler.ready(readable.clone());
      assert_eq!(cmd, MuxCmd::Keep);

      cmd = handler.ready(writable.clone());
      assert_eq!(cmd, MuxCmd::Keep);

      let mut buf = ByteBuffer::with_capacity(2048);
      let cnt = ::rux::read(s2, From::from(&mut buf)).unwrap().unwrap();
      buf.extend(cnt);
      trace!("{:?}", String::from_utf8_lossy(From::from(&buf)));
      let reply = SonicMessage::from_buffer(&mut buf).unwrap();
      trace!("{:?}", String::from_utf8_lossy(From::from(&buf)));
      assert_eq!(reply, Some(msg));
    }
  }


  #[test]
  fn shutdown_on_close() {
    setup_logger();
    let (_, _, _, _, _, close) = setup_socketpair();
    let mut input_buffer = ByteBuffer::with_capacity(BUF_SIZE);
    let mut output_buffer = ByteBuffer::with_capacity(BUF_SIZE);
    let mut handler = new_handler(&mut input_buffer, &mut output_buffer);
    let cmd = handler.ready(close);
    assert_eq!(cmd, MuxCmd::Close);
  }


  #[test]
  fn flush_shutdown_on_half_close() {
    {
      setup_logger();
      let (_, _, _, _, half_close, _) = setup_socketpair();
      let mut input_buffer = ByteBuffer::with_capacity(BUF_SIZE);
      let mut output_buffer = ByteBuffer::with_capacity(BUF_SIZE);
      let mut handler = new_handler(&mut input_buffer, &mut output_buffer);
      let cmd = handler.ready(half_close);
      assert_eq!(cmd, MuxCmd::Close);
    }


    {
      setup_logger();
      let (_, s2, readable, writable, half_close, _) = setup_socketpair();
      let mut input_buffer = ByteBuffer::with_capacity(BUF_SIZE);
      let mut output_buffer = ByteBuffer::with_capacity(BUF_SIZE);
      let msg = get_stream_msgs().pop().unwrap();
      let mut handler = new_handler(&mut input_buffer, &mut output_buffer);
      let mut cmd;

      ::rux::write(s2, &msg.clone().into_bytes().unwrap()).unwrap();
      cmd = handler.ready(readable.clone());
      assert_eq!(cmd, MuxCmd::Keep);

      cmd = handler.ready(half_close);
      assert_eq!(cmd, MuxCmd::Keep);

      cmd = handler.ready(writable.clone());
      assert_eq!(cmd, MuxCmd::Close);

      let mut buf = ByteBuffer::with_capacity(BUF_SIZE);
      let cnt = ::rux::read(s2, From::from(&mut buf)).unwrap().unwrap();
      buf.extend(cnt);
      let reply = SonicMessage::from_buffer(&mut buf).unwrap();
      assert_eq!(reply, Some(msg));
    }
  }


  #[test]
  fn buffered_ser_de() {
    setup_logger();
    let (_, s2, readable, writable, _, _) = setup_socketpair();
    let mut input_buffer = ByteBuffer::with_capacity(BUF_SIZE);
    let mut output_buffer = ByteBuffer::with_capacity(128);
    let mut handler = new_handler(&mut input_buffer, &mut output_buffer);
    let mut cmd;

    let msgs = get_stream_msgs();

    for msg in msgs {
      let bytes = msg.clone().into_bytes().unwrap();
      let (slice1, slice2) = bytes.split_at(5);
      ::rux::write(s2, slice1).unwrap();
      cmd = handler.ready(readable.clone());
      assert_eq!(cmd, MuxCmd::Keep);

      ::rux::write(s2, slice2).unwrap();
      cmd = handler.ready(readable.clone());
      assert_eq!(cmd, MuxCmd::Keep);

      cmd = handler.ready(writable.clone());
      assert_eq!(cmd, MuxCmd::Keep);

      let mut buf = ByteBuffer::with_capacity(2048);
      let cnt = ::rux::read(s2, From::from(&mut buf)).unwrap().unwrap();
      buf.extend(cnt);
      trace!("{:?}", String::from_utf8_lossy(From::from(&buf)));
      let reply = SonicMessage::from_buffer(&mut buf).unwrap();
      trace!("{:?}", String::from_utf8_lossy(From::from(&buf)));
      assert_eq!(reply, Some(msg));
    }
  }

  #[test]
  fn enforce_msg_size() {
    setup_logger();
    let (_, s2, readable, writable, _, _) = setup_socketpair();
    let mut input_buffer = ByteBuffer::with_capacity(BUF_SIZE);
    let mut output_buffer = ByteBuffer::with_capacity(BUF_SIZE);
    let mut handler = SonicHandler {
      is_readable: false,
      is_writable: false,
      max_msg_size: 256,
      closed_write: false,
      completing: false,
      input_buffer: &mut input_buffer,
      output_buffer: &mut output_buffer,
      epfd: EpollFd::new(0),
      handler: TestHandler,
    };
    let mut cmd;

    let msg = SonicMessage::OutputChunk(vec!(Value::Bool(true); 256));
    let bytes = msg.into_bytes().unwrap();

    assert!(bytes.len() > 256);

    ::rux::write(s2, &bytes).unwrap();
    cmd = handler.ready(readable.clone());
    assert_eq!(cmd, MuxCmd::Keep);

    cmd = handler.ready(writable.clone());
    assert_eq!(cmd, MuxCmd::Keep);

    let mut buf = ByteBuffer::with_capacity(2048);
    let cnt = ::rux::read(s2, From::from(&mut buf)).unwrap().unwrap();
    buf.extend(cnt);
    let reply = SonicMessage::from_buffer(&mut buf).unwrap();
    assert_error_contains(reply.unwrap(), "256");
  }
}
