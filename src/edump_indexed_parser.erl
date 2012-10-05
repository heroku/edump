%%% @author Alex Arnell <alex.arnell@gmail.com>
%%% @copyright 2012 Heroku
%%% @doc
%%% A gen_stream implemention that streams chunks of known edump offsets for
%%% parsing.
%%% @end
-module(edump_indexed_parser).
-behaviour(gen_stream).

-include("gen_stream.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

%% Public gen_stream Callbacks
-export([init/3,
         terminate/1,
         stream_length/0,
         stream_length/1,
         extract_block/3,
         extract_split_block/4,
         extract_final_block/3,
         inc_progress/2]).

%%%-------------------------------------------------------------------
%%% Types and Macros
%%%-------------------------------------------------------------------

-define(stream, {0, stream}).

%%%-------------------------------------------------------------------
%%% Public gen_stream Callbacks
%%%-------------------------------------------------------------------

%% @doc 
%% Called when gen_stream initiated without extra options.
%% @end

stream_length() ->
    throw({error, {missing_option, tag_name}}).

%% @doc 
%% Called when gen_stream is initiated with options list.
%% @end

stream_length(Options) ->
    TagName = get_tagname(Options),
    Tab = get_index_table(Options),
    Tags = select_matching_tag(Tab, TagName),
    Pos = hd(Tags),
    NextPos = case Tags of
        [Pos] ->
            {NextP, _NextTag} = ets:next(Tab, {Pos, TagName}),
            NextP;
        _ ->
            hd(lists:reverse(Tags))
    end,
    Length = NextPos - Pos - 1, % we don't care about the trailing \n
    %io:format("stream_length=~p~n", [Length]),
    Length.
        
%% This function is called to track pct_complete
%% It increments based on the number of non-empty objects found
inc_progress(Seen, Chunk) ->
    Seen + byte_size(Chunk).

%% This function is called when gen_stream gets {behaviour, sorted_dets, ExtraArgs}
%% ExtraArgs should be [{table_name, Name}, {ram_file, TrueOrFalse}]
%% ram_file is optional and defaults to false
%% The behaviour state will be the open dets Name
init(#gstr_args{}, _Pos, Options) ->
    case get_tagname(Options) of
        {} -> throw(missing_tagname_error());
        Segment ->
            IndexTab = get_index_table(Options),
            % TODO support other types of gen_stream streams
            case get_stream(IndexTab) of
                {file, Filename} ->
                    case file:open(Filename, [read, binary]) of
                        {ok, Fd} ->
                            Offset = hd(select_matching_tag(IndexTab, Segment)),
                            {Fd, Offset};
                        Other ->
                            throw({invalid_file, Other})
                    end
            end
    end.

%% The table is closed on termination.
terminate({Fd, _}) ->
    file:close(Fd).


%% This function is called by gen_stream on normal block retrieval
extract_block(State, Pos, ChunkSize) ->
    %io:format("Pos=~p, ChunkSize=~p~n", [Pos, ChunkSize]),
    {get_binary_chunk(State, Pos, ChunkSize), State}.

%% This function is called only if structure is circular.
%% It occurs when the next chunk is split between end and beginning.
extract_split_block(_State, _HeadSize, _TailSize, _Pos) ->
    throw({error, not_circular}).

%% This function is only called when the last chunk is to be returned.
%% The ChunkSize should be the remaining length, not the original ChunkSize.
extract_final_block(State, Pos, ChunkSize) ->
    %% This should never happen with an infinite stream,
    %% but is no different than normal extract_block.
    %io:format("final: Pos=~p, ChunkSize=~p~n", [Pos, ChunkSize]),
    extract_block(State, Pos, ChunkSize).

%%%-------------------------------------------------------------------
%%% Internal Functions
%%%-------------------------------------------------------------------

get_binary_chunk({Fd, Offset}, StartPos, ChunkSize) ->
    case file:pread(Fd, Offset + StartPos, ChunkSize) of
        {ok, Bin} -> Bin;
        eof -> <<>>
    end.

missing_tagname_error() ->
    {error, {missing_option, tag_name}}.

get_tagname(Options) ->
    proplists:get_value(tag_name, Options, {}).

get_index_table(Options) ->
    {edump_index, Tab} = proplists:get_value(index, Options),
    Tab.

get_stream(Tab) ->
    [{?stream, Stream}] = ets:lookup(Tab, ?stream),
    %io:format("stream=~p~n", [Stream]),
    Stream.

select_matching_tag(Tab, {SegTag, SegId}) ->
    Ms = ets:fun2ms(fun({{Pos,Tag}, Id}) when Tag =:= SegTag, Id =:= SegId -> Pos end),
    ets:select(Tab, Ms).
