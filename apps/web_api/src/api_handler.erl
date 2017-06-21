-module(api_handler).

-export([init/2]).

-record(apireq, { 	
					url,
					name,
					orgname,
					from,
					to,
					rsm
				}).


init(Req0, Opts) ->
	Method	= cowboy_req:method(Req0),
	MapQs	= maps:from_list(cowboy_req:parse_qs(Req0)),
	ApiReq	= #apireq{	
						url = "/bsmsc-" ++ string:to_lower(binary:bin_to_list(  maps:get(<<"orgname">>, MapQs, undefined) )) ++ "-*/entity_state/_search",
						name = maps:get( <<"name">>, MapQs, undefined ),
						orgname = maps:get( <<"orgname">>, MapQs, undefined ),
						from = maps:get( <<"from">>, MapQs, undefined ),
						to = maps:get( <<"to">>, MapQs, undefined ),
						rsm = maps:get( <<"rsm">>, MapQs, undefined )
				},
	Req	= handle_api(Method, ApiReq, Req0),
	{ok, Req, Opts}.

handle_api(<<"GET">>, undefined, Req) ->
	cowboy_req:reply(400, #{}, <<"Missing parameter name.">>, Req);
handle_api(<<"GET">>, {apireq, Url, <<"services">>, OrgName, _From, _To, _Rsm }, Req) when OrgName =/= undefined ->
	% ElasticReq = getElasticRequest({last_ts}),
	%% JsonReply = espool:es_post(pool1, Url, ElasticReq ),
    cowboy_req:reply(200, #{<<"content-type">> => <<"text/plain; charset=utf-8">>}, OrgName, Req);
handle_api(_, _, Req) ->
	%% Method not allowed.
	cowboy_req:reply(405, Req).


getElasticRequest({relation}) ->
        #{
				<<"size">> => 10000
        };
getElasticRequest({entity_filtered, RsmName, LastTs }) ->
		#{
        		<<"size">> => 10000,
        		<<"query">> => #{
		                <<"bool">> => #{
	              	             <<"must">> => [
                                      		#{ <<"match_phrase">> => #{ <<"service">> => RsmName } },
                                      		#{ <<"match">> => #{ <<"timestamp">> => LastTs  } }
                           	     ]
                		}
       			}
  		};
getElasticRequest({services_filtered, LastTs}) ->
        #{
				<<"size">> => 10000,
				<<"query">> => #{
					<<"bool">> => #{
						<<"must">> => [
								#{ <<"match">> => #{ <<"timestamp">> => LastTs } },
								#{ <<"match">> => #{ <<"type">> => <<"service">>  } }
						]

					}
				}
  		};
getElasticRequest({last_ts}) ->
		#{
				<<"size">> => 0,
				<<"aggs">> => #{
						<<"max_ts">> => #{
								<<"max">> => #{
										<<"field">> => <<"timestamp">>
								}
						}
				}
		};
getElasticRequest({packetbeat_agg_data}) ->
        #{
                <<"size">> => 0,
                <<"aggs">> => #{
						<<"max_ts">> => #{
								<<"max">> => #{
										<<"field">> => <<"timestamp">>
								}
						}
				}
        };
getElasticRequest(_) -> undefined.