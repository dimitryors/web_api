-module(espool).
-behaviour(application).
-behaviour(supervisor).

-export([start/0, stop/0, es_post/3]).
-export([start/2, stop/1]).
-export([init/1]).

start() ->
    application:start(?MODULE).

stop() ->
    application:stop(?MODULE).

start(_Type, _Args) ->
    supervisor:start_link({local, espool_sup}, ?MODULE, []).

stop(_State) ->
    ok.

init([]) ->
    {ok, Pools} = application:get_env(espool, pools),
    PoolSpecs = lists:map(fun({Name, SizeArgs, WorkerArgs}) ->
        PoolArgs = [{name, {local, Name}},
            		{worker_module, espool_worker}] ++ SizeArgs,
        poolboy:child_spec(Name, PoolArgs, WorkerArgs)
    end, Pools),
    {ok, {{one_for_one, 10, 10}, PoolSpecs}}.

es_post(PoolName, Url, Query) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {post, Url, Query})
    end).
