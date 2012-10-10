%%% @author Alex Arnell <alex.arnell@gmail.com>
%%% @copyright 2012 Heroku
%%% @doc
%%% Mirrors the functionality of crashdump_viewer.
%%% @end
-module(edump_viewer).

%% Public API
-export([general_info/1,
         general_info/2,
         processes/1,
         processes/2,
         process_list/1,
         process_info/2]).

%%%-------------------------------------------------------------------
%%% Types & Macros
%%%-------------------------------------------------------------------

-include("edump_index.hrl").
-include_lib("observer/src/crashdump_viewer.hrl").

%%%-------------------------------------------------------------------
%%% Public API
%%%-------------------------------------------------------------------

general_info(Index) ->
    general_info(Index, []).

general_info(Index, Options) ->
    FullSeg = full_tag(next_segment(?erl_crash_dump, Index)),
    GInfo = parse_segment(FullSeg, Index, Options),
    NumProcs = edump_index:num_procs(Index),
    NumFuns = edump_index:num_funs(Index),
    NumEts = edump_index:num_ets(Index),
    NumTimers = edump_index:num_timers(Index),
    GInfo#general_info{
        num_procs = NumProcs,
        num_fun = NumFuns,
        num_ets = NumEts,
        num_timers = NumTimers
        }.

processes(Index) ->
    processes(Index, []).

processes(Index, Options) ->
    {ok, build_segment_table(?proc, Index, Options)}.

process_list({edump_segment, ?proc, Table}) ->
    ets:select(Table, [{{'_', {'_', '$1'}}, [], ['$1']}]).

process_info({edump_segment, ?proc, Table}, Pid) ->
    [Record] = ets:select(Table, [{{'$1', {'_', Pid}}, [], ['$1']}]),
    Record.

%%%-------------------------------------------------------------------
%%% Internal Functions
%%%-------------------------------------------------------------------

build_segment_table(Segment, Index, Options) ->
    SortPos = proplists:get_value(sortpos, Options, default_sortpos(Segment)),
    Table = ets:new(edump_segment_table, [ordered_set, {keypos, 2}]),
    build_segment_table(Table, next_segment(Segment, Index), Index, SortPos, Options),
    {edump_segment, Segment, Table}.

build_segment_table(_Tab, end_of_segment, _Idx, _SortPos, _Opt) ->
    ok;
build_segment_table(Tab, Segment, Idx, SortPos, Opt) ->
    Record = parse_segment(full_tag(Segment), Idx, Opt),
    SortKey = {element(SortPos, Record), element(2, Record)},
    ets:insert(Tab, {Record, SortKey}),
    build_segment_table(Tab, next_segment(Segment, Idx), Idx, SortPos, Opt).

default_sortpos(?proc) ->
    #proc.stack_heap;
default_sortpos(_) ->
    2.

full_tag({{_Pos, Tag}, Id}) ->
    {Tag, Id}.

next_segment({{_, Tag}=Key, _}, {edump_index, TabId}) ->
    [NextSegment] = ets:lookup(TabId, ets:next(TabId, Key)),
    case NextSegment of
        {{_Pos, Tag}, _Id} -> NextSegment;
        _ -> end_of_segment
    end;
next_segment(Segment, {edump_index, TabId}) when is_atom(Segment) ->
    Ms = [{{{'$1', '$2'}, '$3'},[{'=:=', '$2', Segment}],['$_']}],
    {[NextSegment], _} = ets:select(TabId, Ms, 1),
    case NextSegment of
        {{_Pos, Segment}, _Id} -> NextSegment;
        _ -> end_of_segment
    end.

parse_segment(Segment, Index, Options) ->
    Opts = {stream,
              {behaviour, edump_indexed_parser,
               [{index, Index}, {tag_name, Segment}]}},
    Stream = open_stream([Opts] ++ Options),
    SegmentParser = segment_parser(Segment),
    case get_chunk(Stream) of
        eof ->
            throw({error, data_not_available});
        Chunk ->
            State0 = SegmentParser(Segment, undefined),
            parse_segment(Stream, SegmentParser, Chunk, State0)
    end.

parse_segment(_Stream, _Parser, eof, State0) ->
    State0;
parse_segment(_Stream, _Parser, <<$=:8, _NextSegment/binary>>, State0) ->
    State0;
parse_segment(Stream, Parser, Chunk, State0) ->
    case binary:split(Chunk, <<"\n">>, [trim]) of
        [Incomplete] ->
            case get_chunk(Stream) of
                eof ->
                    State1 = Parser(Stream, Parser, Incomplete, State0),
                    parse_segment(Stream, Parser, eof, State1);
                More ->
                    parse_segment(Stream, Parser,
                                  <<More/binary, Chunk/binary>>, State0)
            end;
        [Line, Rest] ->
            State1 = Parser(split_line(Line), State0),
            parse_segment(Stream, Parser, Rest, State1)
    end.

segment_parser({?erl_crash_dump, _}) ->
    fun erl_crash_dump_parser/2;
segment_parser({?proc, _}) ->
    fun proc_parser/2.

erl_crash_dump_parser({?erl_crash_dump, _Vsn}, undefined) ->
    #general_info{};
erl_crash_dump_parser({undefined, CreatedAt}, GInfo) ->
    GInfo#general_info{ created = CreatedAt };
erl_crash_dump_parser({<<"Slogan">>, Value}, GInfo) ->
    GInfo#general_info{ slogan=Value };
erl_crash_dump_parser({<<"System version">>, Value}, GInfo) ->
    GInfo#general_info{ system_vsn=Value };
erl_crash_dump_parser({<<"Compiled">>, Value}, GInfo) ->
    GInfo#general_info{ compile_time=Value };
erl_crash_dump_parser({<<"Taints">>, <<>>}, GInfo) ->
    erl_crash_dump_parser({<<"Taints">>, <<"NA">>}, GInfo);
erl_crash_dump_parser({<<"Taints">>, Value}, GInfo) ->
    GInfo#general_info{ taints=Value };
erl_crash_dump_parser({<<"Atoms">>, Value}, GInfo) ->
    % for some reason OTP viewer ignores this field and calculates it using
    % other means
    GInfo#general_info{ num_atoms=Value }.

%Number of heap fragments: 0
%Heap fragment data: 0
%Link list: [<0.3.0>, <0.6.0>, <0.5.0>]
%Reductions: 2814
%OldHeap: 377
%Heap unused: 279
%OldHeap unused: 267
%Program counter: 0x00000000205749a0 (init:loop/1 + 40)
%CP: 0x0000000000000000 (invalid)
%arity = 0
proc_parser({_, Pid}, undefined) ->
    #proc{ pid = Pid };
proc_parser({<<"Name">>, Val}, P) ->
    P#proc{ name=Val };
proc_parser({<<"State">>, Val}, P) ->
    P#proc{ state=Val };
proc_parser({<<"Spawned as">>, Val}, P) ->
    P#proc{ init_func=Val };
proc_parser({<<"Spawned by">>, Val}, P) ->
    P#proc{ parent=Val };
proc_parser({<<"Started">>, Val}, P) ->
    P#proc{ start_time=Val };
proc_parser({<<"Message queue length">>, Val}, P) ->
    P#proc{ msg_q_len=to_int(Val) };
proc_parser({Stack, Val}, P) when
        Stack =:= <<"Stack+heap">>;
        Stack =:= <<"Stack">> ->
    P#proc{ stack_heap=to_int(Val) };
proc_parser(_Missing, P) ->
    P.

to_int(Bin) when is_binary(Bin) ->
    list_to_integer(binary_to_list(Bin)).

split_line(Line) ->
    case binary:split(Line, <<": ">>) of
        [Head, Val] -> {Head, Val};
        [Val] -> {undefined, Val}
    end.

%% raw file processing oriented helpers
%%--------------------------------------------------------------------

open_stream(StreamOpts) ->
    {ok, Stream} = gen_stream:start_link(StreamOpts),
    Stream.

read(Stream) ->
    gen_stream:next_chunk(Stream).

get_chunk(S) ->
    case read(S) of
        {next_chunk, end_of_stream} -> 
            eof;
        {next_chunk, Bin} ->
            Bin
    end.

