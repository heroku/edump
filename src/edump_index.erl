%%% @author Alex Arnell <alex.arnell@gmail.com>
%%% @copyright 2012 Heroku
%%% @doc
%%% Generates an index for a crashdump.
%%%
%%% Index generation is completely isolated, it is therefore possible and
%%% completely safe to generate an index for more than 1 crash_dump file in
%%% parallel. The resulting index() term can then be fed into edump_viewer for
%%% further processing of individual segments.
%%% @end
-module(edump_index).

%% Public API
-export([new/1,
         save/2,
         save/3,
         load/1,
         load/2,
         segments/1,
         sub_segments/1,
         num_procs/1,
         num_ets/1,
         num_funs/1,
         num_timers/1]).

%%%-------------------------------------------------------------------
%%% Types & Macros
%%%-------------------------------------------------------------------

-include("edump_index.hrl").
-include_lib("stdlib/include/qlc.hrl").

-type index() :: {?MODULE, term()}. % ets table id
-export_type([index/0]).

-define(index(Tab), ({?MODULE, Tab})).
-define(val(K,L), (proplists:get_value(K, L))).

%%%-------------------------------------------------------------------
%%% Public API
%%%-------------------------------------------------------------------

%% @doc
%% Constructs a crashdump index.
%% @end
-spec new(StreamInfo) -> {ok, index()} | {error, Reason} when
    StreamInfo :: [{term(), term()}],
    Reason :: not_a_crashdump | version_not_supported.

new(StreamInfo) ->
    Stream = open(StreamInfo),
    case read(Stream) of
        {next_chunk, end_of_stream} -> {error, not_a_crashdump};
        {next_chunk, <<$=:8, TagAndRest/binary>>} ->
            {Tag, Id, Rest, N1} = tag(Stream, TagAndRest, 1),
            case Tag of
                ?erl_crash_dump ->
                    Tab = create_index(),
                    insert_index(Tab, stream, ?val(stream, StreamInfo), 0),
                    insert_index(Tab, Tag, Id, N1+1),
                    indexify(Tab, Stream, Rest, N1),
                    check_if_truncated(Tab),
                    close_stream(Stream),
                    {ok, ?index(Tab)};
                _Other ->
                    % not an erlang crashdump
                    close_stream(Stream),
                    {error, not_a_crashdump}
            end;
        {next_chunk, <<"<Erlang crash dump>", _Rest/binary>>} -> 
            %% old version - no longer supported
            close_stream(Stream),
            {error, version_not_supported}
    end.

%% @doc
%% Saves the index to disk.
%% @end
-spec save(Index, Filename) -> ok when
    Index :: index(),
    Filename :: string() | iolist().

save({?MODULE, Tab}, Filename) ->
    ets:tab2file(Tab, Filename).

%% @doc
%% Saves the index to disk passing additional options to ets:tab2file.
%% @end
-spec save(Index, Filename, Options) -> ok when
    Index :: index(),
    Filename :: string() | iolist(),
    Options :: [Option],
    Option :: atom(). % see ets:tab2file for option list

save({?MODULE, Tab}, Filename, Options) ->
    ets:tab2file(Tab, Filename, Options).

%% @doc
%% Loads an index from disk.
%% @end
-spec load(Filename) -> {ok, Index} when
    Filename :: string() | iolist(),
    Index :: index().

load(Filename) ->
    {ok, Tab} = ets:file2tab(Filename),
    {ok, ?index(Tab)}.

%% @doc
%% Loads an index from disk passing additional options to ets:file2tab.
%% @end
-spec load(Filename, Options) -> {ok, Index} when
    Filename :: string() | iolist(),
    Options :: [Option],
    Option :: term(), % see ets:file2tab for option list
    Index :: index().

load(Filename, Options) ->
    {ok, Tab} = ets:file2tab(Filename, Options),
    {ok, ?index(Tab)}.

segments({?MODULE, Table}) ->
    QH = qlc:q([ Tab || {{Pos, Tab}, _Id} <- ets:table(Table), Pos > 0 ]),
    qlc:e(QH, [{unique_all, true}]).

sub_segments({?MODULE, Table}) ->
    QH = qlc:q([ {Tab, Id} || {{Pos, Tab}, Id} <- ets:table(Table), Pos > 0 ]),
    qlc:e(QH, [{unique_all, true}]).

num_procs(Index) ->
    count(?proc, table(Index)).

num_ets(Index) ->
    count(?ets, table(Index)).

num_funs(Index) ->
    count(?fu, table(Index)).

num_timers(Index) ->
    count(?timer, table(Index)).

%%%-------------------------------------------------------------------
%%% Internal Functions
%%%-------------------------------------------------------------------

%% raw gen_stream processing helpers
%%--------------------------------------------------------------------

%% @private
open(StreamOpts) ->
    {ok, Stream} = gen_stream:start_link(StreamOpts),
    Stream.

%% @private
close_stream(Stream) ->
    gen_stream:stop(Stream),
    ok.

%% @private
read(Stream) ->
    gen_stream:next_chunk(Stream).

%% ets helpers
%%--------------------------------------------------------------------

%% @private
table({?MODULE, Tab}) ->
    Tab.

%% @private
create_index() ->
    ets:new(?MODULE, [ordered_set, protected]).

%% @private
insert_index(Tab, Tag, Id, Pos) ->
    ets:insert(Tab, {{Pos, Tag}, Id}).

count(Tag, Tab) ->
    ets:select_count(Tab, [{{{'_', Tag}, '_'}, [], [true]}]).

%% parsing helpers
%%--------------------------------------------------------------------

%% @private
tag(S, Bin, N) ->
    tag(S, Bin, N, [], [], tag).

%% @private
tag(_S, <<$\n:8, _/binary>>=Rest, N, Gat, Di, _Now) ->
    {tag_to_atom(lists:reverse(Gat)), lists:reverse(Di), Rest, N};
tag(S, <<$\r:8, Rest/binary>>, N, Gat, Di, Now) ->
    tag(S, Rest, N+1, Gat, Di, Now);
tag(S, <<$::8, IdAndRest/binary>>, N, Gat, Di, tag) ->
    tag(S, IdAndRest, N+1, Gat, Di, id);
tag(S, <<Char:8, Rest/binary>>, N, Gat, Di, tag) ->
    tag(S, Rest, N+1, [Char|Gat], Di, tag);
tag(S, <<Char:8, Rest/binary>>, N, Gat, Di, id) ->
    tag(S, Rest, N+1, Gat, [Char|Di], id);
tag(S, <<>>, N, Gat, Di, Now) ->
    case read(S) of
        {next_chunk, Chunk} when is_binary(Chunk) ->
            tag(S, Chunk, N, Gat, Di, Now);
        {next_chunk, end_of_stream} ->
            {tag_to_atom(lists:reverse(Gat)), lists:reverse(Di), <<>>, N}
    end.

%% @private
indexify(Tab, S, Bin, N) ->
    case binary:match(Bin, <<"\n=">>) of
        {Start, Len} ->
            Pos = Start+Len,
            <<_:Pos/binary, TagAndRest/binary>> = Bin,
            {Tag, Id, Rest, N1} = tag(S, TagAndRest, N+Pos),
            insert_index(Tab, Tag, Id, N1+1), % +1 to get past newline
            put(last_tag, {Tag, Id}),
            indexify(Tab, S, Rest, N1);
        nomatch ->
            case read(S) of
                {next_chunk, Chunk0} when is_binary(Chunk0) ->
                    {Chunk, N1} = case binary:last(Bin) of
                        $\n ->
                            {<<$\n, Chunk0/binary>>, N+byte_size(Bin)-1};
                        _ ->
                            {Chunk0, N+byte_size(Bin)}
                    end,
                    indexify(Tab, S, Chunk, N1);
                {next_chunk, end_of_stream} ->
                    end_of_stream
            end
    end.

%% @private
check_if_truncated(Tab) ->
    case ets:last(Tab) of
        {_Pos, ?ende} ->
            ok;
        TruncatedTag ->
            find_truncated_proc(Tab, TruncatedTag)
    end.

%% @private
find_truncated_proc(Tab, {?atoms, _Id}) ->
    insert_index(Tab, truncated_proc, ?atoms, 0); 

find_truncated_proc(Tab, {Tag, Pid}) ->
    case is_proc_tag(Tag) of
        true -> 
            insert_index(Tab, truncated_proc, Pid, 0);
        false -> 
            %% This means that the dump is truncated between ?proc and
            %% ?proc_heap => memory info is missing for all procs.
            insert_index(Tab, truncated_proc, "<0.0.0>", 0)
    end.

%% @private
is_proc_tag(Tag)  when Tag==?proc;
        Tag == ?proc_dictionary;
        Tag == ?proc_messages;
        Tag == ?proc_dictionary;
        Tag == ?debug_proc_dictionary;
        Tag == ?proc_stack;
        Tag == ?proc_heap ->
    true;
is_proc_tag(_) ->
    false.

%% @private
tag_to_atom("allocated_areas") -> ?allocated_areas;
tag_to_atom("allocator") -> ?allocator;
tag_to_atom("atoms") -> ?atoms;
tag_to_atom("binary") -> ?binary;
tag_to_atom("debug_proc_dictionary") -> ?debug_proc_dictionary;
tag_to_atom("end") -> ?ende;
tag_to_atom("erl_crash_dump") -> ?erl_crash_dump;
tag_to_atom("ets") -> ?ets;
tag_to_atom("fun") -> ?fu;
tag_to_atom("hash_table") -> ?hash_table;
tag_to_atom("hidden_node") -> ?hidden_node;
tag_to_atom("index_table") -> ?index_table;
tag_to_atom("instr_data") -> ?instr_data;
tag_to_atom("internal_ets") -> ?internal_ets;
tag_to_atom("loaded_modules") -> ?loaded_modules;
tag_to_atom("memory") -> ?memory;
tag_to_atom("mod") -> ?mod;
tag_to_atom("no_distribution") -> ?no_distribution;
tag_to_atom("node") -> ?node;
tag_to_atom("not_connected") -> ?not_connected;
tag_to_atom("num_atoms") -> ?num_atoms;
tag_to_atom("old_instr_data") -> ?old_instr_data;
tag_to_atom("port") -> ?port;
tag_to_atom("proc") -> ?proc;
tag_to_atom("proc_dictionary") -> ?proc_dictionary;
tag_to_atom("proc_heap") -> ?proc_heap;
tag_to_atom("proc_messages") -> ?proc_messages;
tag_to_atom("proc_stack") -> ?proc_stack;
tag_to_atom("timer") -> ?timer;
tag_to_atom("visible_node") -> ?visible_node;
tag_to_atom(UnknownTag) ->
    io:format("WARNING: Found unexpected tag:~s~n", [UnknownTag]),
    list_to_atom(UnknownTag).
