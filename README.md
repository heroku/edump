edump
=====

An Erlang crashdump analysis library

Usage
-----

```erlang
application:start(edump).
{ok, Index} = edump_index:new([{stream, {file, "../erl_crash.dump"}}]).
```
Once an index is built you can start parsing bits of detail out of the
crash dump.

```erlang
edump_viewer:general_info(Index).
{ok, Procs} = edump_viewer:processes(Index).
PList = edump_viewer:process_list(Index).
```

By default the processes are ordered by stack heap size so to view
the details on the largest consumer of memory.

```erlang
edump_viewer:process_info(Procs, hd(lists:reverse(PList))).
```
