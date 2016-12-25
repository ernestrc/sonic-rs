

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

use error::Result;
use model::SonicMessage;
use std::io::Cursor;
use std::os::unix::io::RawFd;
use std::ptr;

// TODO refactor to use stdlib instead of nix

#[macro_export]
macro_rules! eagain {
    ($syscall:expr, $name:expr, $($arg:expr),*) => {{
        let res;
        loop {
            match $syscall($($arg),*) {
                Ok(m) => {
                    res = Ok(m);
                    break;
                },
                Err(::nix::Error::Sys(a@::nix::errno::EINTR)) |
                    Err(::nix::Error::Sys(a@::nix::errno::EAGAIN))=> {
                        debug!("{}: {}: re-submitting syscall", $name, a);
                        continue;
                    },
                    Err(err) => {
                        res = Err(err);
                        break;
                    }
            }
        }
        res
    }}
}

pub fn read_next(len: usize, fd: RawFd, buf: &mut [u8]) -> Result<usize> {
  let b = try!(eagain!(::nix::unistd::read, "unistd::read", fd, buf));

  if b == len {
    // result as intended
    Ok(b)
  } else if b > 0 {
    // signal interruped the read or error occurred
    // or less than 'len' bytes were available for read.
    // reissuing the read will either indicate
    // the cause of the error or read the remaining bytes
    let rem = len - b;
    let mut rembuf = vec!(0; rem);

    debug!("unistd::read {} bytes: intended {}", b, len);

    let r = read_next(rem, fd, rembuf.as_mut_slice());
    buf.split_at_mut(b).1.copy_from_slice(rembuf.as_slice());
    r
  } else {
    // EOF reached, no data to read at this point
    debug!("unistd::read 0 bytes: EOF");
    Ok(b)
  }
}

pub fn read_message(fd: RawFd) -> Result<SonicMessage> {

  let len_buf = &mut [0; 4];

  // read length header bytes
  try!(read_next(4, fd, len_buf));

  let mut rdr = Cursor::new(len_buf);

  // decode length header
  let len = try!(rdr.read_i32::<BigEndian>()) as usize;

  let mut buf = vec!(0; len);

  // read message bytes
  try!(read_next(len, fd, buf.as_mut_slice()));

  SonicMessage::from_slice(buf.as_slice())
}

pub fn len_prefix_frame(mut bytes: Vec<u8>) -> Result<Vec<u8>> {

  let len = bytes.len();

  bytes.reserve(4);

  unsafe {
    {
      let p = bytes.as_mut_ptr();
      // Shift everything over to make space for the length prefix
      ptr::copy(p, p.offset(4), len);
    }
    bytes.set_len(len + 4);
  }

  try!(bytes.split_at_mut(4).0.write_i32::<BigEndian>(len as i32));

  Ok(bytes)
}
