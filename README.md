# fxt-python

[Fuschia Trace Format](https://fuchsia.googlesource.com/fuchsia/+/refs/heads/main/docs/reference/tracing/trace-format.md) (fxt) is a file format for storing trace / counter events in a compact binary format. These trace files can then be viewed with an interactive web-based UI: https://ui.perfetto.dev/

FXT was created by Google for use in their experimental operating system [Fuschia](https://fuchsia.dev/fuchsia-src). It's very well documented, simple to write, and can express lots of different types of events and data.

This repo is a library for reading and parsing FXT files
