%%%-------------------------------------------------------------------
%%% File    : gen_stream.erl
%%% Author  : Jay Nelson <jay@duomark.com>
%%% Description : 
%%%   gen_stream is a gen_server that feeds a binary to callers one
%%%   chunk at a time.  When the gen_stream is started, the source of
%%%   the binary is specified along with the number of processes,
%%%   buffers per process and the size of each chunk to return.  The
%%%   caller can request the percent complete to report progress
%%%   when dealing with large streams (provided the length can be
%%%   determined ahead or time or functionally as the binary is
%%%   generated).
%%%
%%%   When the source is a binary or file, there is an option to
%%%   make the stream circular (i.e., continuously repeating and
%%%   infinite in length).
%%%
%%% Created : 13 Nov 2007
%%%-------------------------------------------------------------------
-module(gen_stream).
-vsn('1.0').

-behaviour(gen_server).

-include_lib("kernel/include/file.hrl").


%% API
-export([start_link/1, stop/1, next_chunk/1,
	 stream_size/1, stream_pos/1, pct_complete/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

%% spawned stream funs
-export([bin_stream/3, bin_stream/4]).

%% gen_stream Behaviour definition
-export([behaviour_info/1]).
behaviour_info(callbacks) ->
    [
     {init, 3},
     {terminate, 1},
     {stream_length, 0},
     {stream_length, 1},
     {extract_block, 3},
     {extract_split_block, 4},
     {extract_final_block, 3},
     {inc_progress, 2}
    ];
behaviour_info(_Other) ->
    undefined.

-define(DEFAULT_CHUNK_COUNT, 1).   %% 1 chunk buffers per process.
-define(DEFAULT_CHUNK_SIZE, 8*1024 - 64).  %% Subtract out header.
-define(DEFAULT_NUM_PROCS, 1).     %% 1 process.
-define(DEFAULT_CIRCULAR, false).  %% One pass data stream.

-record(gstr_state, {
	  chunk_count = ?DEFAULT_CHUNK_COUNT,
	  chunk_size = ?DEFAULT_CHUNK_SIZE,
	  circular = ?DEFAULT_CIRCULAR,
	  proc_count = ?DEFAULT_NUM_PROCS,
	  procs,
	  source,
	  source_size,
	  consumed = 0
%%	  mod_state
	 }).

-include("gen_stream.hrl").


%%====================================================================
%% API
%%====================================================================

%%--------------------------------------------------------------------
%% @spec start_link(proplist()) -> {ok, pid()}
%% @doc
%%   Starts a gen_server to deliver the stream a chunk at a time.
%%
%%   The following initialization options are allowed:
%%   <ul>
%%   <li>{stream, Type} where Type is one of:</li>
%%       <ul>
%%       <li>{binary, binary()} => a raw binary</li>
%%       <li>{file, filename()} => a file to read as binary()</li>
%%       <li>{behaviour, atom()} => a behaviour module name</li>
%%       </ul>
%%   <li>{chunks_per_proc, integer()} => number of proc chunk buffers</li>
%%   <li>{chunk_size, integer()} => size of each chunk buffer in bytes</li>
%%   <li>{circular, true | false} => whether stream repeats
%%   <li>{num_processes, integer()} => num processes to generate chunks</li>
%%   </ul>
%%--------------------------------------------------------------------
start_link(Options) ->
    gen_server:start_link(?MODULE, [Options], []).

%%--------------------------------------------------------------------
%% @spec stop(pid()) -> void
%% @doc
%%   Stop the gen_stream process.
%% @end
%%--------------------------------------------------------------------
stop(Server) ->
    gen_server:call(Server, stop),
    ok.

%%--------------------------------------------------------------------
%% @spec next_chunk(pid()) -> {next_chunk, binary() | end_of_stream}
%% @doc
%%   Get the next chunk of the binary or an atom signalling there is
%%   no more data available.
%% @end
%%--------------------------------------------------------------------
next_chunk(Server) ->
    gen_server:call(Server, next_chunk).

%%--------------------------------------------------------------------
%% @spec stream_size(pid()) -> {stream_size, integer() | atom()}
%% @doc
%%   Report the size of the stream.  It may be an atom when the
%%   stream 'is_circular' or if a behaviour module chooses to set
%%   the stream size to 'infinite', 'unknown' or some other value.
%% @end
%%--------------------------------------------------------------------
stream_size(Server) ->
    gen_server:call(Server, stream_size).

%%--------------------------------------------------------------------
%% @spec stream_pos(pid()) -> {stream_pos, integer() | atom()}
%% @doc
%%   Report the position of the stream.  It is the number of bytes
%%   already processed from the beginning of the stream.
%% @end
%%--------------------------------------------------------------------
stream_pos(Server) ->
    gen_server:call(Server, stream_pos).

%%--------------------------------------------------------------------
%% @spec pct_complete(pid()) -> {pct_complete, integer() | is_circular}
%% @doc
%%   Report the percent complete from 0 to 100.  If the stream is
%%   infinite, report {pct_complete, is_circular}.
%% @end
%%--------------------------------------------------------------------
pct_complete(Server) ->
    gen_server:call(Server, pct_complete).

%%--------------------------------------------------------------------
%%% External functions
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @spec init(proplist()) -> {pct_complete, integer() | is_circular}
%% @doc
%%   Report the percent complete from 0 to 100.  If the stream is
%%   infinite, report {pct_complete, is_circular}.
%% @end
%%--------------------------------------------------------------------
init([Options]) ->
    {InitState, ExtraArgs} = init_options(Options, #gstr_state{}),
    StartedState = start_buffer_procs(InitState, ExtraArgs),
    {ok, StartedState}.


%%--------------------------------------------------------------------
%% @spec handle_call(atom(), pid(), gstr_state()) ->
%%           {reply, {next_chunk, binary() | end_of_stream}, gstr_state()}
%%            | {reply, {pct_complete, integer() | is_circular}, gstr_state()}
%%            | {reply, {unknown_request, term()}, gstr_state()}
%%            | {stop, normal, gstr_state()}
%% @doc
%%   Handle calls for next_chunk of binary, percent complete or to
%%   stop the gen_server.
%% @end
%%--------------------------------------------------------------------
handle_call(next_chunk, _From,
	    #gstr_state{procs=end_of_stream} = State) ->
    %% io:format("Proc count now 0~n"),
    {reply, {next_chunk, end_of_stream}, State};

%% next_chunk returns a binary or end_of_stream when source is binary or file.
%% behaviours are free to return any term, but must compute progress.
handle_call(next_chunk, _From,
	    #gstr_state{procs = Buffers, source_size = Size,
			consumed = Seen, source = Src} = State) ->
    {{value, FirstBufferProc}, RestBuffers} = queue:out(Buffers),
    {next_chunk, Chunk} = SubbinReply = get_next_chunk(FirstBufferProc),
    {ProcList, NewSeen} = update_progress(Src, Size, Chunk, Seen,
					  FirstBufferProc, RestBuffers),
    %% io:format("Proc count was: ~w~n", [queue:len(Buffers)]),
    {reply, SubbinReply,
     State#gstr_state{procs = ProcList, consumed = NewSeen}};

%% stream_size can be any term, but pct_complete only works if it is integer
handle_call(stream_size, _From, #gstr_state{source_size = Size} = State) ->
    {reply, {stream_size, Size}, State};

handle_call(stream_pos, _From, #gstr_state{consumed = Seen} = State) ->
    {reply, {stream_pos, Seen}, State};

handle_call(pct_complete, _From,
	    #gstr_state{circular = IsCircular} = State)
  when IsCircular =:= true ->
    {reply, {pct_complete, is_circular}, State};

handle_call(pct_complete, _From,
	    #gstr_state{procs=end_of_stream} = State) ->
    {reply, {pct_complete, 100}, State};

handle_call(pct_complete, _From,
	    #gstr_state{source_size = Size, consumed = Seen} = State)
  when is_integer(Size) ->
    {reply, {pct_complete, (Seen * 100) div Size}, State};

%% If Size is not an integer, return its value every time...
%% For example: {pct_complete, infinite}
handle_call(pct_complete, _From, #gstr_state{source_size = Size} = State) ->
    {reply, {pct_complete, Size}, State};

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

handle_call(Request, _From, State) ->
    {reply, {unknown_request, Request}, State}.


%%--------------------------------------------------------------------
%% @spec handle_cast(term(), gstr_state()) -> {noreply, gstr_state()}
%% @doc
%%   No cast calls are expected.
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @spec handle_info(term(), gstr_state()) -> {noreply, gst_state()}
%% @doc
%%   No info calls are expected.
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @spec terminate(Reason, State) -> void()
%% @doc
%%   No cleanup is necessary, the linked processes will die.
%% @end
%%--------------------------------------------------------------------
%% terminate(_Reason, #gstr_state{source = {behaviour, Module}}) ->
%%   when is_atom(ModState) andalso ModState =/= none ->
%%     Module:terminate(ModState),
%%     ok;

terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @spec code_change(atom(), gstr_state(), term()) ->
%%         {ok, gstr_state()}
%% @doc
%%   Code change has not been necessary yet.
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------

init_options(PropList, #gstr_state{chunk_count = ChunkCount,
				   chunk_size = ChunkSize,
				   circular = Circular,
				   proc_count = ProcCount}
	     = InitState) ->

    %% Source of stream is a required option...
    Source = proplists:get_value(stream, PropList),
    {ValidSource, SourceSize} = validate_source(Source),

    %% All others are optional values, with a default...
    ChunksPerProc = proplists:get_value(chunks_per_proc, PropList, ChunkCount),
    ChunkBytes = proplists:get_value(chunk_size, PropList, ChunkSize),
    Continuous = proplists:get_value(circular, PropList, Circular),
    NumProcs = proplists:get_value(num_processes, PropList, ProcCount),

    %% io:format("~n~s: ~w~n~s: ~w~n~s: ~w~n~s: ~w~n~s: ~w~n",
    %%           ["ChunksPerProc", ChunksPerProc,
    %%            "ChunkBytes", ChunkBytes,
    %%            "Continuous", Continuous,
    %%            "NumProcs", NumProcs,
    %%            "SourceSize", SourceSize]),

    %% Return the new set of options.
    if
	Continuous =:= true andalso is_integer(SourceSize) =:= false ->
	    throw({stream_cannot_be_circular, {size_specified, SourceSize}});
	true -> ok
    end,
    NewState = InitState#gstr_state{chunk_count = ChunksPerProc,
				    chunk_size = ChunkBytes,
				    circular = Continuous,
				    proc_count = NumProcs,
				    source_size = SourceSize},

    %% Strip off extra behaviour args -- they should be translated to ModState
    {ModSource, ExtraArgs} = case ValidSource of
				 {behaviour, Module, Args} ->
				     {{behaviour, Module}, Args};
				 _Other ->
				     {ValidSource, []}
			     end,
    {NewState#gstr_state{source = ModSource}, ExtraArgs}.


%% validate_source will crash the initialization on invalid stream type.
validate_source({binary, Bin} = Source) when is_binary(Bin) ->
    {Source, size(Bin)};
validate_source({behaviour, Module} = Source) when is_atom(Module) ->
    Size = Module:stream_length(),
    {Source, Size};
validate_source({behaviour, Module, ExtraArgs} = Source) when is_atom(Module) ->
    Size = Module:stream_length(ExtraArgs),
    {Source, Size};
validate_source({file, FileName} = Source) when is_list(FileName) ->
    {ok, FileInfo} = file:read_file_info(FileName),
    {Source, FileInfo#file_info.size}.


%%--------------------------------------------------------------------
%% Progress update is automatic or requires Module:inc_progress/2.
%% The queue of processes is also rotated by this function.
%%--------------------------------------------------------------------

%% Any type at end of stream reports completion...
update_progress(_Bin, _Size, end_of_stream, Seen,
		FirstProc, RestProcs) ->
    [P ! {stop} || P <- [ FirstProc | queue:to_list(RestProcs)]],
    {end_of_stream, Seen};

%% Behaviours update their own progress...
update_progress({behaviour, Module}, Size, Chunk, Seen,
		FirstProc, RestProcs) ->
    {queue:in(FirstProc, RestProcs),
     case is_integer(Size) of
	 true ->  Module:inc_progress(Seen, Chunk);
	 false -> Seen
     end};

%% Binaries and files are updated based on the size(Chunk) value...
update_progress(_Bin, Size, Chunk, Seen, FirstProc, RestProcs)
  when is_binary(Chunk) ->
    {queue:in(FirstProc, RestProcs),
     case is_integer(Size) of
	 true -> Seen + size(Chunk);
	 false -> Seen
     end};

%% Anything else can't report progress.
update_progress(_Bin, _Size, _Chunk, Seen, FirstProc, RestProcs) ->
    {queue:in(FirstProc, RestProcs), Seen}.


%%--------------------------------------------------------------------
%% There can be more than one process to deliver binary chunks.
%% Each process contains ChunksPerProc buffers to prevent delays.
%% The requests are distributed round-robin to the processes so that
%% two consecutive next_chunk/2 requests will be with two separate
%% processes.
%%--------------------------------------------------------------------
start_buffer_procs(
  #gstr_state{chunk_count = ChunksPerProc, chunk_size = ChunkSize,
	      proc_count = NumProcs, circular = IsCircular,
	      source = Source, source_size = Size} = State,
  ExtraArgs) ->
    Procs = launch_procs(Source, Size, NumProcs, ChunksPerProc,
			 ChunkSize, IsCircular, ExtraArgs),
    State#gstr_state{procs = Procs}.

%%--------------------------------------------------------------------
%% The SrcType determines the function to run in launched processes.
%%--------------------------------------------------------------------
launch_procs(SrcType, SrcSize, NumProcs, NumChunks,
	     ChunkSize, IsCircular, ExtraArgs) ->
    ProcNums = lists:seq(0, NumProcs-1),
    SkipSize = NumProcs * ChunkSize,
    Procs =
	case SrcType of
	    {binary, Bin} ->
		[bin_proc(Bin, SrcSize, Num * ChunkSize, NumChunks,
			  ChunkSize, SkipSize, IsCircular)
		 || Num <- ProcNums];
	    {behaviour, Module} ->
		[fun_proc(Module, SrcSize, Num * ChunkSize, NumChunks,
			  ChunkSize, SkipSize, IsCircular, ExtraArgs)
		 || Num <- ProcNums];
	    {file, FileName} ->
		[file_proc(FileName, SrcSize, Num * ChunkSize,
			   NumChunks, ChunkSize, SkipSize, IsCircular)
		 || Num <- ProcNums]
	end,
    queue:from_list(Procs).

%%====================================================================
%% Fixed binary stream...
bin_proc(Bin, BinSize, Pos, 1, ChunkSize, SkipSize, IsCircular) ->
    ChunkParams =
	#gstr_args{bin=Bin, bin_size = BinSize,
		   skip_size=SkipSize, num_chunks=1,
		   chunk_size=ChunkSize, is_circular=IsCircular},
    Args = [ChunkParams, Pos, none],
    proc_lib:spawn_link(?MODULE, bin_stream, Args);

bin_proc(Bin, BinSize, Pos, NumChunks, ChunkSize, SkipSize, IsCircular) ->
    ChunkParams =
	#gstr_args{bin=Bin, bin_size=BinSize,
		   skip_size=SkipSize, num_chunks=NumChunks,
		   chunk_size=ChunkSize, is_circular=IsCircular},
    Args = [ChunkParams, Pos, queue:new(), none],
    proc_lib:spawn_link(?MODULE, bin_stream, Args).

%%--------------------------------------------------------------------
%% With 1 buffer, fetch the subbinary, then wait for a request...
bin_stream(#gstr_args{} = ChunkParams, Pos, ModState) ->
    {Reply, NewPos, NewModState} =
	fetch_subbin(ChunkParams, Pos, ModState),
    bin_reply(Reply),
    bin_stream(ChunkParams, NewPos, NewModState).
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% Multiple buffers works the same but rotates the queue and always
%% maintains a full set of buffers.
bin_stream(#gstr_args{num_chunks=NumBuffers} = ChunkParams,
	   Pos, Buffers, ModState) ->

    %% Bins = [io_lib:format("~s,", [B])
    %%         || B <- queue:to_list(Buffers)],
    %% io:format("~w: [~s]~n", [self(), Bins]),

    case queue:len(Buffers) of
	0 ->
	    %% Buffer is empty, fetch a new one first...
	    {Subbin, NextPos, NewModState} =
		fetch_subbin(ChunkParams, Pos, ModState),
	    NewBuffers = queue:in(Subbin, Buffers),
	    %% Schedule a new fetch soon...
	    self() ! {fetch_chunk},
	    %% Fetch or reply to a client request.
	    {LastPos, LastBuffers, LastModState} =
		bin_reply_or_fetch(ChunkParams, NextPos, NewBuffers,
				   NewModState),
	    bin_stream(ChunkParams, LastPos, LastBuffers, LastModState);
	NumBuffers ->
	    %% Buffer is full, so just get the chunk and reply only.
	    {{value, Reply}, PoppedBuffers} = queue:out(Buffers),
	    bin_reply(Reply),
	    bin_stream(ChunkParams, Pos, PoppedBuffers, ModState);
	Count when Count > 0 andalso Count < NumBuffers ->
	    %% Buffer is not full, schedule a new fetch soon...
	    self() ! {fetch_chunk},
	    %% Reply to client or fetch next subbin.
	    {NextPos, NewBuffers, NewModState} =
		bin_reply_or_fetch(ChunkParams, Pos, Buffers, ModState),
	    bin_stream(ChunkParams, NextPos, NewBuffers, NewModState)
    end.
%%--------------------------------------------------------------------

    
%%--------------------------------------------------------------------
%% A buffer process loads its buffer and then waits for a request
%% before replying.  This is intended to pre-fetch the next sub-
%% binary for a quicker response when the caller needs a result.

bin_reply(Subbin) ->
    receive
	{next_chunk, From} -> From ! {next_chunk, Subbin};
	{stop} -> die
    end.

%%--------------------------------------------------------------------
bin_reply_or_fetch(#gstr_args{} = ChunkParams, Pos,
		   Buffers, ModState) ->
    receive
	{next_chunk, From} ->
	    {{value, Subbin}, NewBuffers} = queue:out(Buffers),
	    From ! {next_chunk, Subbin},
	    {Pos, NewBuffers, ModState};
	{fetch_chunk} ->
	    {Subbin, NextPos, NewModState} =
		fetch_subbin(ChunkParams, Pos, ModState),
	    NewBuffers = queue:in(Subbin, Buffers),
	    {NextPos, NewBuffers, NewModState};
	{stop} -> {stop, Buffers, ModState}
    end.
%%====================================================================


%%====================================================================
%% Functionally defined stream (using a behaviour)...
fun_proc(Module, SrcSize, Pos, NumBuffers, ChunkSize,
	 SkipSize, IsCircular, ExtraArgs) ->
    ChunkParams =
	#gstr_args{bin=Module, bin_size=SrcSize,
		   skip_size=SkipSize, num_chunks=NumBuffers,
		   chunk_size=ChunkSize, is_circular=IsCircular},
    ModState = Module:init(ChunkParams, Pos, ExtraArgs),
    Args = [ChunkParams, Pos, ModState],
    proc_lib:spawn_link(?MODULE, bin_stream, Args).
%%====================================================================


%%====================================================================
%% File-based stream...
file_proc(FileName, FileSize, Pos, 1, ChunkSize,
	  SkipSize, IsCircular) ->
    {ok, IoDevice} = file:open(FileName, [read, binary]),
    ChunkParams =
	#gstr_args{bin=IoDevice, bin_size=FileSize,
		   skip_size=SkipSize, num_chunks=1,
		   chunk_size=ChunkSize, is_circular=IsCircular},
    Args = [ChunkParams, Pos, none],
    proc_lib:spawn_link(?MODULE, bin_stream, Args);

file_proc(FileName, FileSize, Pos, NumChunks, ChunkSize,
	  SkipSize, IsCircular) ->
    {ok, IoDevice} = file:open(FileName, [read, binary]),
    ChunkParams =
	#gstr_args{bin=IoDevice, bin_size=FileSize,
		   skip_size=SkipSize, num_chunks=NumChunks,
		   chunk_size=ChunkSize, is_circular=IsCircular},
    Args = [ChunkParams, Pos, queue:new(), none],
    proc_lib:spawn_link(?MODULE, bin_stream, Args).

%%====================================================================


%% Retrieve the next subbin from a buffer process.
get_next_chunk(Pid) ->
    Pid ! {next_chunk, self()},
    receive
	{next_chunk, _Subbin} = Reply -> Reply
    end.


%%--------------------------------------------------------------------
%% fetch_subbin excises a piece of a larger binary and updates Pos.

%% Position off end of binary...
fetch_subbin(#gstr_args{bin_size=BinSize, is_circular=false},
	     Pos, ModState)
  when BinSize =< Pos ->
    {end_of_stream, Pos, ModState};

%% Last chunk is only partially full...
fetch_subbin(#gstr_args{bin=BinSrc, bin_size=BinSize,
			skip_size=SkipSize, chunk_size=ChunkSize,
			is_circular=IsCircular},
	     Pos, ModState)
  when BinSize < Pos + ChunkSize ->
    {Next, NextPos, NewModState} =
	case IsCircular of
	    true ->
		HeadSize = BinSize - Pos,
		TailSize = ChunkSize - HeadSize,
		SkipPos = BinSize - HeadSize - TailSize,
		{Subbin, ModState1} =
		    extract_split_block(BinSrc, HeadSize,
					TailSize, SkipPos,
					ModState),
		NewPos = Pos + SkipSize - BinSize,
		{Subbin, NewPos, ModState1};
	    false ->
		{Subbin, ModState1} =
		    extract_final_block(BinSrc, Pos,
					BinSize - Pos,
					ModState),
		NewPos = Pos + SkipSize,
		{Subbin, NewPos, ModState1}
	end,
    {Next, NextPos, NewModState};

fetch_subbin(#gstr_args{bin=BinSrc, bin_size=BinSize,
			skip_size=SkipSize, chunk_size=ChunkSize,
			is_circular=IsCircular}, Pos, ModState) ->
    {Subbin, NewModState} =
	extract_block(BinSrc, Pos, ChunkSize, ModState),
    NextPos = Pos + SkipSize,
    NewPos = case IsCircular =:= true andalso NextPos >= BinSize of
		 true ->  NextPos - BinSize;
		 false -> NextPos
	     end,
    {Subbin, NewPos, NewModState}.
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% Extracting sub-binaries for each binary source type.
%%--------------------------------------------------------------------
extract_block(Bin, Pos, ChunkSize, ModState)
  when is_binary(Bin) ->
    <<_Skip:Pos/binary, Subbin:ChunkSize/binary, _Rest/binary>> = Bin,
    {Subbin, ModState};

extract_block(Module, Pos, ChunkSize, ModState)
  when is_atom(Module) ->
    Module:extract_block(ModState, Pos, ChunkSize);

extract_block(IoDevice, Pos, ChunkSize, ModState) ->
    file:position(IoDevice, Pos),
    {ok, Subbin} = file:read(IoDevice, ChunkSize),
    {Subbin, ModState}.

extract_split_block(Bin, HeadSize, TailSize, SkipPos, ModState)
  when is_binary(Bin) ->
    <<T:TailSize/binary, _Skip:SkipPos/binary,
      H:HeadSize/binary>> = Bin,
    {list_to_binary([H,T]), ModState};

extract_split_block(Module, HeadSize, TailSize, SkipPos, ModState)
  when is_atom(Module) ->
    Module:extract_split_block(ModState, HeadSize, TailSize, SkipPos);

extract_split_block(IoDevice, HeadSize, TailSize, SkipPos, ModState) ->
    Positions = [{0, TailSize}, {TailSize + SkipPos, HeadSize}],
    {ok, [T,H]} = file:pread(IoDevice, Positions),
    {list_to_binary([H,T]), ModState}.

extract_final_block(Bin, Pos, _ChunkSize, ModState)
  when is_binary(Bin) ->
    <<_Skip:Pos/binary, Rest/binary>> = Bin,
    {Rest, ModState};

extract_final_block(Module, Pos, ChunkSize, ModState)
  when is_atom(Module) ->
    Module:extract_final_block(ModState, Pos, ChunkSize);

extract_final_block(IoDevice, Pos, ChunkSize, ModState) ->
    file:position(IoDevice, Pos),
    {ok, Rest} = file:read(IoDevice, ChunkSize),
    file:close(IoDevice),
    {Rest, ModState}.
