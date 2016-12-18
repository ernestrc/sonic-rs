use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use error::Result;
use io::len_prefix_frame;
use serde_json::Value;
use std::io;

// marker trait for messages that client can initiate connection with
pub trait Command {}

#[derive(Debug, Clone, PartialEq)]
pub struct Query {
    id: Option<String>,
    query: String,
    trace_id: Option<String>,
    auth: Option<String>,
    config: Value,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Authenticate {
    user: String,
    key: String,
    trace_id: Option<String>,
}

impl Query {
    pub fn new(
        query: String,
        trace_id: Option<String>,
        auth: Option<String>,
        config: Value
    ) -> Query {
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
        let v = try!(self.config.search(key).ok_or(format!("missing key {} in query config", key)));
        Ok(v)
    }

    pub fn get_opt<'a>(&'a self, key: &str) -> Option<&'a Value> {
        self.config.search(key)
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

    pub fn into_json(self) -> Value {
        let msg: protocol::ProtoSonicMessage = From::from(self);

        ::serde_json::to_value(&msg)
    }

    pub fn from_slice(slice: &[u8]) -> Result<SonicMessage> {
        let proto = try!(::serde_json::from_slice::<protocol::ProtoSonicMessage>(slice));
        let msg = try!(proto.into_msg());

        Ok(msg)
    }

    pub fn from_bytes(buf: Vec<u8>) -> Result<SonicMessage> {
        Self::from_slice(buf.as_slice())
    }

    pub fn into_bytes(self) -> Result<Vec<u8>> {
        let bytes = try!(::serde_json::to_vec(&self.into_json()));
        let s = try!(len_prefix_frame(bytes));
        Ok(s)
    }

    #[cfg(feature="server")]
    pub fn from_buffer(buffer: &mut ::rux::buf::ByteBuffer) -> Result<Option<SonicMessage>> {
        let readable = buffer.readable();
        if readable < 4 {
            return Ok(None);
        }

        let len_buf = &mut [0u8; 4];
        try!(buffer.read(len_buf));
        let mut rdr = io::Cursor::new(len_buf);
        let len = try!(rdr.read_i32::<BigEndian>()) as usize;
        let total_len = len + 4;

        if readable < total_len {
            return Ok(None);
        }

        let proto: protocol::ProtoSonicMessage = 
            try!(::serde_json::from_slice(buffer.slice(4).split_at(len).0));
        let msg = try!(proto.into_msg());

        buffer.consume(total_len);

        Ok(Some(msg))
    }

    #[cfg(feature="server")]
    pub fn to_buffer(self, buffer: &mut ::rux::buf::ByteBuffer) -> Result<()> {
        let pos = buffer.next_write();
        let before = buffer.writable();
        let proto_msg: protocol::ProtoSonicMessage = From::from(self);

        try!(::serde_json::to_writer(buffer, &proto_msg));

        let len = before - buffer.writable();
        let len_buf: &mut [u8] = &mut [0_u8; 4];

        {
            let mut len_rdr = io::Cursor::new(&mut *len_buf);
            try!(len_rdr.write_i32::<BigEndian>(len as i32));
        }

        try!(buffer.write_at(pos, len_buf));

        Ok(())
    }
}

pub mod protocol {
    #[cfg(feature = "serde_macros")]
    include!("protocol.rs.in");

    #[cfg(not(feature = "serde_macros"))]
    include!(concat!(env!("OUT_DIR"), "/protocol.rs"));
}

#[cfg(test)]
mod tests {
    use serde_json::*;
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
