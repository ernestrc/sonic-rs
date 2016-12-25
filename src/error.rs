use model::SonicMessage;

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
    BufferTooSmall(msg: SonicMessage) {
      description("buffer too small for SonicMessage")
      display("buffer too small for SonicMessage: {:?}", msg)
    }

    MessageTooBig(max_size: usize) {
      description("message bigger than max message size")
      display("message bigger than max message size of {:?}", max_size)
    }

    Proto(msg: String) {
      description("Sonic protocol error")
      display("Sonic protocol error: {}: lib is v.{}", msg, super::VERSION)
    }
  }
}
