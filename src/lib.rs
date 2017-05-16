#![cfg_attr(feature = "serde_macros", feature(custom_derive, plugin))]
#![cfg_attr(feature = "serde_macros", plugin(serde_macros))]
extern crate serde;
extern crate serde_json;
extern crate nix;
extern crate byteorder;
#[macro_use] extern crate error_chain;
#[macro_use] extern crate log;

mod error;
mod api;
mod model;
#[macro_use] pub mod io;

pub use api::stream;
pub use model::{SonicMessage, QueryStatus, Query, Authenticate};
pub use error::{Result, Error, ErrorKind};

static VERSION: &'static str = env!("CARGO_PKG_VERSION");
