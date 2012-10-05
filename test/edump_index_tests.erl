-module(edump_index_tests).

-include("../src/edump_index.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(SEGMENTS, ([?allocated_areas,
                    ?allocator,
                    ?atoms,
                    ?ende,
                    ?erl_crash_dump,
                    ?ets,
                    ?fu,
                    ?hash_table,
                    ?index_table,
                    ?loaded_modules,
                    ?memory,
                    ?mod,
                    ?no_distribution,
                    ?node,
                    ?port,
                    ?proc,
                    ?proc_dictionary,
                    ?proc_heap,
                    ?proc_stack,
                    %% the following tags could be present,
                    %% but are not in the test dump
                    %?binary,
                    %?debug_proc_dictionary,
                    %?hidden_node,
                    %?instr_data,
                    %?internal_ets,
                    %?not_connected,
                    %?num_atoms,
                    %?old_instr_data,
                    %?proc_messages,
                    %?visible_node,
                    ?timer])).

%%%-------------------------------------------------------------------
%%% EUnit Test Suites
%%%-------------------------------------------------------------------

construction_test_() ->
    [?_assertMatch({ok, {edump_index, _}}, new_from_file())].

parsed_tags_test_() ->
    {foreach,
     fun new_from_file/0,
     fun cleanup_index/1,
     [fun basic_parse_tests/1,
      fun all_tags_present_tests/1,
      fun generates_segments_list/1,
      fun provides_counts/1]}.

save_test_() ->
    {setup,
     fun setup_save_to_disk/0,
     fun cleanup_index_and_disk/1,
     fun saves_to_disk/1}.

load_test_() ->
    {setup,
     fun setup_load_from_disk/0,
     fun cleanup_index_and_disk/1,
     fun loads_from_disk/1}.

%%%-------------------------------------------------------------------
%%% Fixture Setup and Teardown
%%%-------------------------------------------------------------------

new_from_file() ->
    Opts = [{stream, {file, "../test/data/erl_crash.dump"}}],
    edump_index:new(Opts).

setup_save_to_disk() ->
    {ok, Index} = new_from_file(),
    {_, TabId} = Index,
    Filename = ["test_table_", TabId, ".dump"],
    {Index, Filename}.

setup_load_from_disk() ->
    State = {Index, Filename} = setup_save_to_disk(),
    ok =edump_index:save(Index, Filename),
    State.

cleanup_index({ok, {_, Tab}}) ->
    ets:delete(Tab).

cleanup_index_and_disk({Index, Filename}) ->
    file:delete(Filename),
    cleanup_index({ok, Index}).

%%%-------------------------------------------------------------------
%%% Test Assertions
%%%-------------------------------------------------------------------

basic_parse_tests({ok, {_, Tab}}) ->
    [?_assert(ets:info(Tab, memory) > 0),
     ?_assertEqual({0, stream}, ets:first(Tab)),
     ?_assertMatch({_, 'end'}, ets:last(Tab))].

all_tags_present_tests({ok, {_, Tab}}) ->
    [ ?_assert(is_present(Tab, Tag)) || Tag <- ?SEGMENTS ].

generates_segments_list({ok, Index}) ->
    Expected = ordsets:from_list(?SEGMENTS),
    Result = ordsets:from_list(edump_index:segments(Index)),
    Intersection = ordsets:intersection(Expected, Result),
    [?_assertEqual(Expected, Intersection),
     ?_assertEqual(Result, Intersection),
     ?_assertEqual(Expected, Result),
     ?_assertNot(ordsets:is_element(stream, Result)),
     ?_assertNot(ordsets:is_element(truncated, Result))].

provides_counts({ok, Index}) ->
    [?_assertEqual(32, edump_index:num_procs(Index)),
     ?_assertEqual(21, edump_index:num_ets(Index)),
     ?_assertEqual(501, edump_index:num_funs(Index)),
     ?_assertEqual(1, edump_index:num_timers(Index))].

saves_to_disk({Index, Filename}) ->
    {inorder,
     [?_assertNot(filelib:is_regular(Filename)),
      ?_assertEqual(ok, edump_index:save(Index, Filename)),
      ?_assert(filelib:is_regular(Filename))]}.

loads_from_disk({{_, OrigTab}, Filename}) ->
    Reply = {ok, {_, FromDisk}} = edump_index:load(Filename),
    [basic_parse_tests(Reply),
     ?_assertEqual(info(OrigTab), info(FromDisk)),
     ?_assertEqual(all_objects(OrigTab), all_objects(FromDisk))].

%%%-------------------------------------------------------------------
%%% Test Helpers
%%%-------------------------------------------------------------------

is_present(Tab, Tag) ->
    ets:select_count(Tab, [{{{'_', Tag}, '_'}, [], [true]}]) > 0.

info(Tab) ->
    ets:info(Tab).

all_objects(Tab) ->
    ets:match(Tab, '$1').

