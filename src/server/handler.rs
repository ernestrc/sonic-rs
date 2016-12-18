use error::Result;
use model::SonicMessage;
use rux::buf::ByteBuffer;
use rux::handler::Handler;
use rux::handler::mux::{MuxCmd, MuxEvent};
use rux::poll::*;
use std::io;

pub struct SonicHandler<'b, H>
    where H: Handler<In = Result<SonicMessage>, Out = SonicMessage>,
{
    handler: H,
    rbuffer: &'b mut ByteBuffer,
    wbuffer: &'b mut ByteBuffer,
    is_readable: bool,
    is_writable: bool,
    complete: bool,
}

impl<'b, H> SonicHandler<'b, H>
    where H: Handler<In = Result<SonicMessage>, Out = SonicMessage>,
{
    // pub fn new(
    //    handler: H,
    //    rbuffer: &'b mut ByteBuffer,
    //    wbuffer: &'b mut ByteBuffer
    // -> SonicHandler<'b, H> {
    //    SonicHandler {
    //        handler: handler,
    //        rbuffer: rbuffer,
    //        wbuffer: wbuffer,
    //    }
    //
}

#[macro_export]
macro_rules! mtry {
    ($e:expr, $sel:expr) => {{
        match $e {
            Ok(_) => {},
            Err(e) => {
                let err = format!("unexpected SonicHandler err: {}", e); 
                error!("{}", err);
                $sel.wbuffer.clear();
                $sel.complete = true;
                SonicMessage::complete(Err(err.into()), 
                                       "".to_owned()).to_buffer($sel.wbuffer).unwrap();

                // TODO
                $sel.try_write().unwrap();
            }
        }
    }}
}

// TODO should read/write until EAGAIN and keep state of readable/writable
impl<'b, H> Handler for SonicHandler<'b, H>
    where H: Handler<In = Result<SonicMessage>, Out = SonicMessage>,
{
    type In = MuxEvent;
    type Out = MuxCmd;

    fn ready(&mut self, event: MuxEvent) -> MuxCmd {
        let fd = event.fd;
        let kind = event.kind;

        if kind.contains(EPOLLHUP) {
            return MuxCmd::Close;
        }

        if kind.contains(EPOLLIN) {
            self.is_readable = true;
        }

        if kind.contains(EPOLLOUT) {
            self.is_writable = true;
        }

        if kind.contains(EPOLLERR) {
            let err = format!("fd={}: EPOLERR", fd);
            error!("{}", err);
            let res = self.handler.ready(Err(err.into()));

            if let &SonicMessage::StreamCompleted(_, _) = &res {
                self.complete = true;
            }

            mtry!(res.to_buffer(self.wbuffer), self);
        }

        MuxCmd::Keep
    }
}
