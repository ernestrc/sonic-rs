extern crate serde_json;
extern crate serde;
extern crate regex;
extern crate docopt;
extern crate rustc_serialize;
extern crate env_logger;
extern crate ansi_term;
#[macro_use]
extern crate error_chain;
#[macro_use]
extern crate log;
#[macro_use]
extern crate sonic;
extern crate rpassword;
extern crate pbr;

mod util {
  #[cfg(feature = "serde_macros")]
  include!("util.rs.in");

  #[cfg(not(feature = "serde_macros"))]
  include!(concat!(env!("OUT_DIR"), "/util.rs"));
}


use docopt::Docopt;
use pbr::ProgressBar;
use rpassword::read_password;
use sonic::{Authenticate, SonicMessage};
use std::cell::RefCell;
use std::io::{Write, stderr, stdout};
use std::path::PathBuf;
use std::process;

#[cfg_attr(rustfmt, rustfmt_skip)]
const USAGE: &'static str = "
.
                           d8b
                           Y8P

.d8888b   .d88b.  88888b.  888  .d8888b
88K      d88\"\"88b 888 \"88b 888 d88P\"
\"Y8888b. 888  888 888  888 888 888
     X88 Y88..88P 888  888 888 Y88b.
 88888P'  \"Y88P\"  888  888 888  \"Y8888P

Usage:
  sonic <source> [options] -e <query>
  sonic <source> [options] -f <file>
  sonic login [options]
  sonic -h | --help
  sonic --version

Options:
  -e, --execute         Run query literal
  -f, --file            Run file contents as query
  -c <config>           Override default configuration file ($HOME/.sonicrc)
  -d <foo=var> ...      Replace variable in query in the form of `${foo}` with value `var`
  -r, --rows-only       Skip printing column names
  -S, --silent          Disable progress bar
  -v, --verbose         Enable debug level logging
  -h, --help            Print this message
  --version             Print version
";

static VERSION: &'static str = env!("CARGO_PKG_VERSION");
static COMMIT: Option<&'static str> = option_env!("SONIC_COMMIT");

#[derive(Debug, RustcDecodable)]
struct Args {
  arg_file: String,
  arg_query: String,
  flag_silent: bool,
  flag_rows_only: bool,
  flag_d: Vec<String>,
  flag_file: bool,
  flag_c: String,
  flag_verbose: bool,
  arg_source: String,
  cmd_login: bool,
  flag_execute: bool,
  flag_version: bool,
  flag_help: bool,
}

error_chain! {
    links {
        SonicError(::sonic::error::Error, ::sonic::error::ErrorKind);
    }

    foreign_links {
        IoError(::std::io::Error);
        Json(::serde_json::Error);
        RecvError(::std::sync::mpsc::RecvError);
    }

    errors {
        InvalidFormat(co: String, msg: String) {
            description("invalid format error")
                display("invalid format {}: {}", co, msg)
        }
    }
}

fn hide<T: Write>(pb: &mut ProgressBar<T>) {
  pb.show_bar = false;
  pb.show_speed = false;
  pb.show_percent = false;
  pb.show_counter = false;
  pb.show_time_left = false;
  pb.show_tick = false;
  pb.show_message = false;
}

fn show<T: Write>(pb: &mut ProgressBar<T>) {
  pb.show_bar = true;
  pb.show_speed = false;
  pb.show_percent = true;
  pb.show_counter = true;
  pb.show_time_left = true;
  pb.show_tick = true;
  pb.show_message = true;
}

fn exec(host: &str, port: &u16, query: SonicMessage, rows_only: bool, silent: bool) -> Result<()> {

  let out = RefCell::new(stdout());
  let mut buf: Vec<SonicMessage> = Vec::new();
  let mut pb = ProgressBar::on(stderr(), 0);
  pb.format("╢░░_╟");
  pb.tick_format("▀▐▄▌");
  show(&mut pb);

  let (tx, rx) = ::std::sync::mpsc::channel();

  try!(sonic::stream((host, *port), query, tx));

  let draw = |b: &mut Vec<SonicMessage>| {
    let len = b.len();
    for msg in b.drain(..len) {
      let cols = match msg {
        SonicMessage::OutputChunk(data) => {
          data.iter().fold(String::new(), |acc, val| format!("{}{:?}\t", acc, val))
        }
        SonicMessage::TypeMetadata(data) => {
          data.iter().fold(String::new(), |acc, col| format!("{}{:?}\t", acc, col.0))
        }
        _ => panic!("not possible!"),
      };
      let row = format!("{}\n", cols.trim_right());
      let mut bout = out.borrow_mut();
      bout.write_all(row.as_bytes()).unwrap();
      bout.flush().unwrap();
    }
  };

  let res: Result<()>;

  loop {
    match try!(rx.recv()) {
      Ok(SonicMessage::StreamStarted(trace_id)) => {
        debug!("started query with trace_id: {}", trace_id);
      }
      Ok(msg @ SonicMessage::TypeMetadata(_)) => {
        if !rows_only {
          buf.push(msg);
        }
      }
      Ok(msg @ SonicMessage::OutputChunk(_)) => {
        buf.push(msg);
        if !silent && !pb.is_finish {
          // tick format breaks suffix length
          // so we need to disable it before finishing bar
          hide(&mut pb);
          pb.tick();
          pb.finish();
        }
        draw(&mut buf);
      }
      Ok(SonicMessage::QueryProgress { status, progress, total, .. }) => {
        if !silent && !pb.is_finish {

          pb.message(&format!("{:?} ", status));
          debug!("{:?}: {:?}/{:?}", status, progress, total);

          if let Some(total) = total {
            pb.total = total as u64;
          }

          if progress >= 1.0 {
            pb.add(progress as u64);
          } else {
            pb.tick();
          };
        }
      }
      Ok(SonicMessage::StreamCompleted(None, trace_id)) => {
        debug!("stream '{}' completed successfully", trace_id);
        res = Ok(());
        break;
      }
      Ok(SonicMessage::StreamCompleted(Some(cause), trace_id)) => {
        debug!("stream '{}' failed with error {:?}", &cause, trace_id);
        let error: Error = cause.into();
        res = Err(error).chain_err(|| format!("error when running query; trace_id: {}", trace_id));
        break;
      }
      Err(e) => {
        let error: Error = e.into();
        res = Err(error).chain_err(|| "unexpected channel recv error");
        break;
      }
      Ok(a) => debug!("ignoring msg {:?}", a),
    }
  }

  draw(&mut buf);

  res
}

pub fn login(host: &str, tcp_port: &u16) -> Result<()> {

  let user = std::env::var("USER").unwrap_or("unknown".to_owned());

  try!(stdout().write(b"Enter key: "));
  try!(stdout().flush());

  let key = try!(read_password());

  let (tx, rx) = ::std::sync::mpsc::channel();

  let cmd = SonicMessage::AuthenticateMsg(Authenticate::new(user.to_owned(), key, None));

  try!(sonic::stream((host, *tcp_port), cmd, tx));

  let token: String;

  loop {
    match try!(rx.recv()) {
      Ok(SonicMessage::OutputChunk(data)) => {
        token = try!(util::parse_token(data));
        break;
      }
      Ok(SonicMessage::StreamCompleted(None, trace_id)) => {
        return Err(format!("protocol error: server returned no data for '{}'", trace_id).into());
      }
      Ok(SonicMessage::StreamCompleted(Some(cause), trace_id)) => {
        let error: Error = cause.into();
        return Err(error)
          .chain_err(|| format!("error when running login command; trace_id: {}", trace_id));
      }
      Ok(_) => {}
      Err(e) => return Err(e.into()),
    };
  }

  debug!("generated token for user {:?}: {:?}", &user, &token);

  let path = util::get_config_path();
  let config = try!(util::read_config(&path));

  let new_config = util::ClientConfig { auth: Some(token), ..config };

  try!(util::write_config(&new_config, &path));

  println!("OK");
  Ok(())
}

fn _main(args: Args) -> Result<()> {

  let Args { arg_file,
             arg_query,
             flag_silent,
             flag_rows_only,
             flag_d,
             flag_file,
             flag_c,
             arg_source,
             cmd_login,
             flag_execute,
             flag_version,
             .. } = args;

  let util::ClientConfig { host, tcp_port, sources, auth } = if flag_c != "" {
    debug!("sourcing passed config in path '{:?}'", &flag_c);
    try!(util::read_config(&PathBuf::from(flag_c)))
  } else {
    debug!("sourcing default config in path '$HOME/.sonicrc'");
    try!(util::get_default_config())
  };

  if flag_file {

    let query_str = try!(util::read_file_contents(&PathBuf::from(&arg_file)));
    let split = try!(util::split_key_value(&flag_d));
    let injected = try!(util::inject_vars(&query_str, &split));
    let query = try!(util::build(arg_source, sources, auth, injected));

    exec(&host, &tcp_port, query, flag_rows_only, flag_silent)

  } else if flag_execute {

    let query = try!(util::build(arg_source, sources, auth, arg_query));

    exec(&host, &tcp_port, query, flag_rows_only, flag_silent)

  } else if cmd_login {

    login(&host, &tcp_port)

  } else if flag_version {

    println!("Sonic CLI version {} ({})", VERSION, COMMIT.unwrap_or_else(|| "dev"));

    Ok(())

  } else {

    panic!("unexpected args");

  }
}

fn main() {

  let args: Args = Docopt::new(USAGE)
    .and_then(|d| d.decode())
    .unwrap_or_else(|e| e.exit());

  let verbose: bool = args.flag_verbose;

  if verbose {
    let mut builder = env_logger::LogBuilder::new();
    builder.parse("debug");
    builder.init().unwrap();
  } else {
    env_logger::init().unwrap();
  }

  debug!("parsed args {:?}", args);

  match _main(args) {
    Ok(_) => {}
    Err(e) => {
      util::report_error(&e, verbose);
      process::exit(1)
    }
  }
}
