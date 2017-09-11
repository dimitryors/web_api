-module(espool_worker).
-behaviour(gen_server).
-behaviour(poolboy_worker).

-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-record(state, {conn}).

start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

init(Args) ->
    process_flag(trap_exit, true),
    Hostname = proplists:get_value(hostname, Args),
    Port = proplists:get_value(port, Args),
    Conn = Hostname ++ ":" ++ Port,
    {ok, #state{conn=Conn}}.

handle_call({post, Url, Query}, _From, #state{conn=Conn}=State) ->
	Method = post,
	URL = Conn ++ Url, 
	Header = [],
	Type = "",
	Body = jiffy:encode(Query),
	HTTPOptions = [],
    Options = [],
	{ok, {{_Version, 200, _ReasonPhrase}, _Headers, BodyRequest}} = httpc:request(Method, {URL, Header, Type, Body}, HTTPOptions, Options),
    {reply, BodyRequest, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{conn=Conn}) ->
    ok = epgsql:close(Conn),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
