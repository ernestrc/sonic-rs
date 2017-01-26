use error::*;
use io::len_prefix_frame;
use serde_json::Value;

pub mod protocol;

// marker trait for messages that client can initiate connection with
pub trait Command {}

#[derive(Debug, Clone, PartialEq)]
pub struct Query {
  pub id: Option<String>,
  pub query: String,
  pub trace_id: Option<String>,
  pub auth: Option<String>,
  pub config: Value,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Authenticate {
  user: String,
  key: String,
  trace_id: Option<String>,
}

impl Query {
  pub fn new(query: String, trace_id: Option<String>, auth: Option<String>, config: Value)
             -> Query {
    Query {
      id: None,
      query: query,
      trace_id: trace_id,
      auth: auth,
      config: config,
    }
  }

  pub fn get_raw<'a>(&'a self) -> &'a str {
    &(self.query)
  }

  pub fn get_config<'a>(&'a self, key: &str) -> Result<&'a Value> {
    let v = self.config.get(key).ok_or(format!("missing key '{}' in query config", key))?;
    Ok(v)
  }

  pub fn get_opt<'a>(&'a self, key: &str) -> Option<&'a Value> {
    self.config.get(key)
  }
}

impl Authenticate {
  pub fn new(user: String, key: String, trace_id: Option<String>) -> Authenticate {
    Authenticate {
      user: user,
      key: key,
      trace_id: trace_id,
    }
  }
}

impl Command for Authenticate {}
impl Command for Query {}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum QueryStatus {
  Queued,
  Started,
  Running,
  Waiting,
  Finished,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SonicMessage {
  // client ~> server
  Acknowledge,

  StreamStarted(String),

  QueryMsg(Query),

  AuthenticateMsg(Authenticate),

  // client <~ server
  TypeMetadata(Vec<(String, Value)>),

  QueryProgress {
    status: QueryStatus,
    progress: f64,
    total: Option<f64>,
    units: Option<String>,
  },

  OutputChunk(Vec<Value>),

  StreamCompleted(Option<String>, String),
}

impl SonicMessage {
  // StreamComplete error
  pub fn complete<T>(e: Result<T>, trace_id: String) -> SonicMessage {
    let variation = match e {
      Ok(_) => None,
      Err(e) => Some(format!("{}", e).to_owned()),
    };
    SonicMessage::StreamCompleted(variation, trace_id).into()
  }

  pub fn into_json(self) -> Result<Value> {
    let msg: protocol::ProtoSonicMessage = From::from(self);

    let v = ::serde_json::to_value(&msg)?;

    Ok(v)
  }

  pub fn from_slice(slice: &[u8]) -> Result<SonicMessage> {
    let proto = ::serde_json::from_slice::<protocol::ProtoSonicMessage>(slice)?;
    let msg = proto.into_msg()?;

    Ok(msg)
  }

  pub fn from_bytes(buf: Vec<u8>) -> Result<SonicMessage> {
    Self::from_slice(buf.as_slice())
  }

  pub fn into_bytes(self) -> Result<Vec<u8>> {
    let bytes = ::serde_json::to_vec(&self.into_json()?)?;
    let s = len_prefix_frame(bytes)?;
    Ok(s)
  }
}

#[cfg(feature="server")]
impl ::rux::buf::Buffered for SonicMessage {
  type Error = Error;

  fn max_size() -> usize {
    usize::max_value()
  }

  fn from_buffer(buf: &mut ::rux::buf::ByteBuffer) -> Result<Option<SonicMessage>> {
    use byteorder::{BigEndian, ReadBytesExt};
    use std::io;

    let readable = buf.readable();
    if readable < 4 {
      return Ok(None);
    }

    let len_buf = &mut [0u8; 4];
    buf.read(len_buf)?;
    let mut rdr = io::Cursor::new(len_buf);
    let len = rdr.read_i32::<BigEndian>()? as usize;
    let total_len = len + 4;

    if readable < total_len {
      return Ok(None);
    }

    let proto: protocol::ProtoSonicMessage =
      ::serde_json::from_slice(buf.slice(4).split_at(len).0)?;
    let msg = proto.into_msg()?;

    buf.consume(total_len);

    Ok(Some(msg))
  }

  fn to_buffer(self, buf: &mut ::rux::buf::ByteBuffer) -> Result<Option<SonicMessage>> {
    use byteorder::{BigEndian, WriteBytesExt};
    use std::io;

    let mark = buf.mark();
    let before = buf.writable();
    let proto_msg: protocol::ProtoSonicMessage = self.into();

    match ::serde_json::to_writer(buf, &proto_msg) {
      // TODO catch too small error
      Err(e) => {
        buf.reset_from(mark);
        return Err(e).chain_err(|| ErrorKind::BufferTooSmall(proto_msg.into_msg().unwrap()));
      }
      Ok(_) if buf.writable() < 4 => {
        buf.reset_from(mark);
        // let err: Error = "not enough space in buf for length prefix (4b)".into();
        return Ok(Some(proto_msg.into_msg().unwrap()));
      }
      Ok(_) => {}
    };

    let len = before - buf.writable();
    let len_buf: &mut [u8] = &mut [0_u8; 4];

    {
      let mut len_rdr = io::Cursor::new(&mut *len_buf);
      len_rdr.write_i32::<BigEndian>(len as i32)?;
    }

    buf.write_at(mark.next_write, len_buf)?;

    Ok(None)
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  // TODO test partial read
  #[cfg(feature="server")]
  #[test]
  fn msg_buffer() {
    use rux::buf::ByteBuffer;

    let msg = SonicMessage::StreamCompleted(Some("1234".to_owned()), "ERR".to_owned());
    let len = msg.clone().into_bytes().unwrap().len();

    let mut buf = ByteBuffer::with_capacity(1024);

    {
      msg.clone().to_buffer(&mut buf).unwrap();
    }

    assert_eq!(buf.readable(), len);

    {

      let _msg = SonicMessage::from_slice(&buf.slice(0).split_at(4).1).unwrap();
      assert_eq!(msg, _msg);
    }

    {
      let _msg = SonicMessage::from_buffer(&mut buf)
        .unwrap()
        .unwrap();

      assert_eq!(msg, _msg);
    }

    {
      msg.clone().to_buffer(&mut buf).unwrap();
    }

    {
      msg.clone().to_buffer(&mut buf).unwrap();
    }

    assert_eq!(buf.readable(), len * 2);

    {
      let _msg = SonicMessage::from_buffer(&mut buf)
        .unwrap()
        .unwrap();

      assert_eq!(msg, _msg);
    }

    {
      let _msg = SonicMessage::from_buffer(&mut buf)
        .unwrap()
        .unwrap();

      assert_eq!(msg, _msg);
    }

    assert_eq!(buf.readable(), 0);
  }
}
