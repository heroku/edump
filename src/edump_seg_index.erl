%% @copyright Geoff Cant
%% @author Geoff Cant <nem@erlang.geek.nz>
%% @version {@vsn}, {@date} {@time}
%% @doc Dump -> tag/segment index stream processor.
%% @end
-module(edump_seg_index).

-compile(export_all).

-record(tag, {body_start, body_len, name, id}).

-record(state, {tags = gb_sets:new()}).

new() ->
    #state{}.

to_list(#state{tags = Tags}) ->
    gb_sets:to_list(Tags).

to_file(S = #state{}, File) ->
    file:write_file(File,
                    erlang:term_to_binary(S, [{compressed, 6}])).

from_file(File) ->
    {ok, Bin} = file:read_file(File),
    erlang:binary_to_term(Bin).

from_dump_file(FileName) ->
    from_dump_file(FileName, new()).

from_dump_file(FileName, State) ->
    from_dump_stream([{stream, {file, FileName}}], State).

from_dump_stream(StreamOpts, State) ->
    {ok, Stream} = gen_stream:start_link(StreamOpts),
    read_block({incomplete_tag_start, 0}, Stream, 0, <<>>, State).

find_tags(MS, #state{tags = Tags}) ->
    ets:match_spec_run(gb_sets:to_list(Tags),
                       ets:match_spec_compile(MS)).

tag_names(#state{tags = Tags}) ->
    Cnt = gb_sets:fold(fun (#tag{name=Name}, D) ->
                               dict:update_counter(Name, 1, D)
                       end,
                       dict:new(),
                       Tags),
    dict:to_list(Cnt).

extract_tag_body(#tag{body_start = Start, body_len = Len},
                 FileName) ->
    {ok, File} = file:open(FileName, [binary, raw, read]),
    {ok, Body} = file:pread(File, Start, Len),
    file:close(File),
    Body.

read_block({tag_start, BlockOffset, Tag},
           Stream, StreamOffset, Block, State) ->
    read_block(read_tag_body(StreamOffset, BlockOffset, Block, Tag),
               Stream, StreamOffset, Block, State);
read_block({tag, BlockOffset, Tag},
           Stream, StreamOffset, Block, State) ->
    read_block(read_tag_start(StreamOffset, BlockOffset, Block),
               Stream, StreamOffset, Block, add_tag(Tag, State));
read_block({incomplete_tag_start, BlockOffset} = _IC,
           Stream, StreamOffset, Block, State) ->
    case gen_stream:next_chunk(Stream) of
        {next_chunk, end_of_stream} ->
            {error, {truncated, tag_start,
                     StreamOffset + BlockOffset,
                     extract(Block, BlockOffset, byte_size(Block)),
                     State}};
        {next_chunk, <<"<Erlang crash dump>", _Rest/binary>>} ->
            %% old version - no longer supported
            gen_stream:stop(Stream),
            {error, version_not_supported};
        {next_chunk, Data} ->
            OldData = binary:part(Block, BlockOffset,
                                  byte_size(Block) - BlockOffset),
            NewBlock = <<OldData/binary, Data/binary>>,
            read_block(read_tag_start(StreamOffset, 0, NewBlock),
                       Stream, StreamOffset + BlockOffset, NewBlock, State)
    end;
read_block({incomplete_tag_body, #tag{name = <<"end">>}, _},
           Stream, _Offset, _Block, State) ->
    gen_stream:stop(Stream),
    {ok, State};
read_block({incomplete_tag_body, Tag, BlockOffset} = _IC,
           Stream, StreamOffset, Block, State) ->
    case gen_stream:next_chunk(Stream) of
        {next_chunk, end_of_stream} ->
            {error, {truncated, tag_body,
                     StreamOffset + BlockOffset,
                     extract(Block, BlockOffset, byte_size(Block)),
                     State}};
        {next_chunk, <<"<Erlang crash dump>", _Rest/binary>>} ->
            %% old version - no longer supported
            gen_stream:stop(Stream),
            {error, version_not_supported};
        {next_chunk, NewBlock} ->
            NewStreamOffset = StreamOffset + byte_size(Block),
            read_block(read_tag_body(NewStreamOffset, 0, NewBlock, Tag),
                       Stream, NewStreamOffset, NewBlock, State)
    end.

read_tag_start(StreamOffset, BlockOffset, Block) ->
    MaxLen = byte_size(Block) - BlockOffset,
    case binary:match(Block, [<<"\n">>, <<"\r\n">>],
                      [{scope, {BlockOffset, MaxLen}}]) of
        {TagEnd, Sz} when Sz >= 1, Sz =< 2 ->
            TagLen = TagEnd - BlockOffset,
            Tag = case binary:match(Block, <<":">>,
                                    [{scope, {BlockOffset, TagLen}}]) of
                      {SplitPos, 1} ->
                          #tag{name = extract(Block, BlockOffset,
                                              SplitPos),
                               id = extract(Block, SplitPos + 1,
                                            TagEnd)};
                      nomatch ->
                          #tag{name = extract(Block, BlockOffset, TagEnd)}
                  end,
            TagBodyStart = TagEnd + Sz,
            {tag_start, TagBodyStart,
             Tag#tag{body_start = StreamOffset + TagBodyStart}};
        nomatch ->
            {incomplete_tag_start, BlockOffset}
    end.

extract(Block, Start, End) ->
    binary:copy(binary:part(Block, Start, End - Start)).

read_tag_body(StreamOffset, BlockTagStart, Block,
              Tag = #tag{body_start=StreamStart})
  when is_integer(BlockTagStart), BlockTagStart >= 0,
       is_binary(Block) ->
    MaxLen = byte_size(Block) - BlockTagStart,
    case binary:match(Block, [<<"\n=">>, <<"\r\n=">>],
                      [{scope, {BlockTagStart, MaxLen}}]) of
        nomatch ->
            {incomplete_tag_body, Tag, BlockTagStart};
        {End, Sz} when Sz >= 2, Sz =< 3 ->
            StreamEnd = StreamOffset + End,
            BodyLen = StreamEnd - StreamStart,
            {tag, _NewBlockOffset = End + Sz, Tag#tag{body_len = BodyLen}}
    end.

add_tag(Tag, State = #state{tags = Tags}) ->
    State#state{tags = gb_sets:add_element(Tag, Tags)}.

report(StreamOffset, Block, IC) ->
    io:format("Offset ~p blksz ~p state ~p~n",
              [StreamOffset, byte_size(Block), IC]).
