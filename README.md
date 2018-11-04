# Sonic-rs

Rust client library for the Sonic query protocol.

# Install CLI
Using Cargo:
```
$ cargo install sonic

...

$ sonic -h

                           d8b
                           Y8P

.d8888b   .d88b.  88888b.  888  .d8888b
88K      d88""88b 888 "88b 888 d88P"
"Y8888b. 888  888 888  888 888 888
     X88 Y88..88P 888  888 888 Y88b.
 88888P'  "Y88P"  888  888 888  "Y8888P

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

```

# Library usage
See [cli/src/main.rs](cli/src/main.rs).

# Contribute
If you would like to contribute to the project, please fork the project, include your changes and submit a pull request back to the main repository.

# License
MIT License
