#![cfg_attr(feature = "serde_macros", feature(custom_derive, plugin))]
#![cfg_attr(feature = "serde_macros", plugin(serde_macros))]
extern crate serde;
extern crate serde_json;
extern crate nix;
extern crate byteorder;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate error_chain;
#[macro_use]
extern crate log;

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
