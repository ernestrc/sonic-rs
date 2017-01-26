extern crate nix;
extern crate byteorder;
extern crate serde;
#[macro_use]
extern crate serde_json;
#[macro_use]
extern crate error_chain;
#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;

#[cfg(feature = "server")]
#[macro_use]
extern crate rux;

pub mod error;
mod api;
mod model;
#[macro_use]
pub mod io;

#[cfg(feature = "server")]
pub mod server;

pub use api::stream;
pub use model::{SonicMessage, QueryStatus, Query, Authenticate};

static VERSION: &'static str = env!("CARGO_PKG_VERSION");
