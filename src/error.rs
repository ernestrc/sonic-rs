error_chain! {

    links {
        RuxError(::rux::error::Error, ::rux::error::ErrorKind) #[cfg(feature="server")];
    }

    foreign_links {
        Json(::serde_json::Error);
        IoError(::std::io::Error);
        ParseAddr(::std::net::AddrParseError);
        Utf8Error(::std::string::FromUtf8Error);
        NixError(::nix::Error);
    }

    errors {

        BufferOverflowError(max: usize) {
            description("Buffer overflow error")
                display("Buffer exceeded max of {} bytes", max)
        }

        QueryError(msg: String) {
            description("error in query")
                display("{}", msg)
        }

        Proto(msg: String) {
            description("Sonicd protocol error")
                display("Sonicd protocol error: {}: lib is v.{}", msg, super::VERSION)
        }
    }
}
