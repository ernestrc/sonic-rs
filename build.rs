#[cfg(not(feature = "serde_macros"))]
mod inner {
  extern crate syntex;
  extern crate serde_codegen;

  use std::env;
  use std::path::Path;

  pub fn main() {
    let out_dir = env::var_os("OUT_DIR").unwrap();

    let psrc = Path::new("src/model/protocol.rs.in");
    let pdst = Path::new(&out_dir).join("protocol.rs");

    serde_codegen::expand(&psrc, &pdst).unwrap();

    let usrc = Path::new("cli/src/util.rs.in");
    let udst = Path::new(&out_dir).join("util.rs");

    serde_codegen::expand(&usrc, &udst).unwrap();
  }
}

#[cfg(feature = "serde_macros")]
mod inner {
  pub fn main() {}
}

fn main() {
  inner::main();
}
