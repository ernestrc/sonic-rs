use error::Result;
use io::*;

use model::SonicMessage;
use std::io::Write;
use std::net::TcpStream;
use std::net::ToSocketAddrs;
use std::os::unix::io::AsRawFd;
use std::sync::mpsc::Sender;
use std::thread;

// TODO rewrite using mio and futures
fn send_cmd(stream: &mut TcpStream, cmd: SonicMessage) -> Result<()> {

    debug!("framing command {:?}", &cmd);

    // frame command
    let fbytes = try!(cmd.into_bytes());

    debug!("framed command into {} bytes", fbytes.len());

    // send query
    try!(stream.write(&fbytes.as_slice()));

    Ok(())
}

pub fn stream<A>(addr: A, cmd: SonicMessage, tx: Sender<Result<SonicMessage>>) -> Result<()>
    where A: ToSocketAddrs,
{

    let mut stream = try!(TcpStream::connect(addr));

    // set timeout 10s
    try!(stream.set_read_timeout(Some(::std::time::Duration::new(10, 0))));

    // FIXME when librux I/O layer is ready
    thread::spawn(move || {

        let tx = tx.clone();

        match send_cmd(&mut stream, cmd) {
            Ok(_) => {

                let fd = stream.as_raw_fd();

                loop {
                    match read_message(fd) {
                        o @ Ok(_) => tx.send(o).unwrap(),
                        e @ Err(_) => {
                            tx.send(e).unwrap();
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                tx.send(Err(e)).unwrap();
            }
        }
    });

    Ok(())
}
