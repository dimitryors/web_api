-module(api_handler).

-export([init/2]).

-record(apireq, { 	
			name,
			type,
			orgname,
			from,
			to,
			rsm,
			granularity,
			agg1,
			agg2,
			columns1,
			columns2,
			url
		}).


init(Req0, Opts) ->
	Method	= cowboy_req:method(Req0),
	MapQs	= maps:from_list(cowboy_req:parse_qs(Req0)),
	ApiReq	= #apireq{
				name = maps:get( <<"name">>, MapQs, undefined ),	
				type = maps:get( <<"type">>, MapQs, undefined ),
				orgname = maps:get( <<"orgname">>, MapQs, undefined ),
				from = maps:get( <<"from">>, MapQs, undefined ),
				to = maps:get( <<"to">>, MapQs, undefined ),
				rsm = maps:get( <<"rsm">>, MapQs, undefined ),
				granularity = maps:get( <<"granularity">>, MapQs, undefined ),
				agg1 = maps:get( <<"agg1">>, MapQs, undefined ),
				agg2 = maps:get( <<"agg2">>, MapQs, undefined ),
				columns1 = maps:get( <<"columns1">>, MapQs, undefined ),
				columns2 = maps:get( <<"columns2">>, MapQs, undefined ),
				url = "/bsmsc-" ++ string:to_lower(binary:bin_to_list(  maps:get(<<"orgname">>, MapQs, <<"orgname">>) )) ++ "-*/entity_state/_search"
			},
	Req = handle_api(Method, ApiReq, Req0),
	{ok, Req, Opts}.

handle_api(<<"GET">>, undefined, Req) ->
	cowboy_req:reply(400, #{}, <<"Missing parameter name.">>, Req);
%% Entity
handle_api(<<"GET">>, {apireq, <<"entity">>, _Type, OrgName, _From, _To, Rsm, _Granularity, _Agg1, _Agg2, _Columns1, _Columns2, Url }, Req) when OrgName =/= undefined, Rsm =/= undefined ->
	LastTsReq = elastic_lib:getElasticRequest({last_ts}),
	LastTsReply = espool:es_post(pool1, Url, LastTsReq ),
	{[{_,_}, {_,_}, {_, {[{_,_}, {_,_}, {_,_}]}}, {_, {[{_,_}, {_,_}, {_,_}]}}, {_,{[{<<"max_ts">>, {[{_,_}, {<<"value_as_string">>, LastTs }]}}]}}]} = jiffy:decode( LastTsReply ),
	EntityFilteredReq = elastic_lib:getElasticRequest({entity_filtered, Rsm, LastTs }),
	EntityFilteredReply = espool:es_post(pool1, Url, EntityFilteredReq ),
	{[{_,_}, {_,_}, {_,{[{_,_}, {_,_}, {_,_}]}}, {<<"hits">>, {[{_,_}, {_,_}, {<<"hits">>, Filtered }]}}]} = jiffy:decode( EntityFilteredReply ),
    	cowboy_req:reply(200, #{<<"content-type">> => <<"text/plain; charset=utf-8">>}, jiffy:encode(Filtered), Req);
%% Relation
handle_api(<<"GET">>, {apireq, <<"relation">>, _Type, _OrgName, _From, _To, _Rsm, _Granularity, _Agg1, _Agg2, _Columns1, _Columns2, _Url }, Req) ->
        RelationUrl = "/bsmsc1/relation/_search",
	RelationReq = elastic_lib:getElasticRequest({relation}),
        RelationReply = espool:es_post(pool1, RelationUrl, RelationReq ),
	{[{_,_}, {_,_}, {_,{[{_,_}, {_,_}, {_,_}]}}, {<<"hits">>, {[{_,_}, {_,_}, {<<"hits">>, Filtered }]}}]} = jiffy:decode( RelationReply ),
	cowboy_req:reply(200, #{<<"content-type">> => <<"text/plain; charset=utf-8">>}, jiffy:encode(Filtered), Req);
%% Services
handle_api(<<"GET">>, {apireq, <<"services">>, _Type, OrgName, _From, _To, _Rsm, _Granularity, _Agg1, _Agg2, _Columns1, _Columns2, Url }, Req) when OrgName =/= undefined ->
	LastTsReq = elastic_lib:getElasticRequest({last_ts}),
	LastTsReply = espool:es_post(pool1, Url, LastTsReq ),
	{[{_,_}, {_,_}, {_, {[{_,_}, {_,_}, {_,_}]}}, {_, {[{_,_}, {_,_}, {_,_}]}}, {_,{[{<<"max_ts">>, {[{_,_}, {<<"value_as_string">>, LastTs }]}}]}}]} = jiffy:decode( LastTsReply ),
	ServiceFilteredReq = elastic_lib:getElasticRequest({services_filtered, LastTs}),
	ServiceFilteredReply = espool:es_post(pool1, Url, ServiceFilteredReq ),
	{[{_,_}, {_,_}, {_,{[{_,_}, {_,_}, {_,_}]}}, {<<"hits">>, {[{_,_}, {_,_}, {<<"hits">>, Filtered }]}}]} = jiffy:decode( ServiceFilteredReply ),
        cowboy_req:reply(200, #{<<"content-type">> => <<"text/plain; charset=utf-8">>}, jiffy:encode(Filtered), Req);
%% Kpi Week
handle_api(<<"GET">>, {apireq, <<"kpi">>, _Type, OrgName, From, To, _Rsm, <<"week">>, _Agg1, _Agg2, _Columns1, _Columns2, Url }, Req) when OrgName =/= undefined, From =/= undefined, To =/= undefined ->
        KpiWeekReq = elastic_lib:getElasticRequest({kpi_week, From, To}),
	KpiWeekReply =  espool:es_post(pool1, Url, KpiWeekReq ),
	{[{_,_}, {_,_}, {_,{[{_,_}, {_,_}, {_,_}]}}, {<<"hits">>,{[{_,_}, {_,_}, {<<"hits">>, _Filtered }]}}, {<<"aggregations">>, Aggregations } ]} = jiffy:decode( KpiWeekReply ),
	cowboy_req:reply(200, #{<<"content-type">> => <<"text/plain; charset=utf-8">>}, jiffy:encode(Aggregations), Req);
%% Kpi Day
handle_api(<<"GET">>, {apireq, <<"kpi">>, _Type, OrgName, From, To, _Rsm, <<"day">>, _Agg1, _Agg2, _Columns1, _Columns2, Url }, Req) when OrgName =/= undefined, From =/= undefined, To =/= undefined ->
        KpiDayReq = elastic_lib:getElasticRequest({kpi_day, From, To}),
        KpiDayReply =  espool:es_post(pool1, Url, KpiDayReq ),
        {[{_,_}, {_,_}, {_,{[{_,_}, {_,_}, {_,_}]}}, {<<"hits">>,{[{_,_}, {_,_}, {<<"hits">>, _Filtered }]}}, {<<"aggregations">>, Aggregations } ]} = jiffy:decode( KpiDayReply ),
        cowboy_req:reply(200, #{<<"content-type">> => <<"text/plain; charset=utf-8">>}, jiffy:encode(Aggregations), Req);
%% Kpi Hour
handle_api(<<"GET">>, {apireq, <<"kpi">>, _Type, OrgName, From, To, _Rsm, <<"hour">>, _Agg1, _Agg2, _Columns1, _Columns2, Url }, Req) when OrgName =/= undefined, From =/= undefined, To =/= undefined ->
        KpiHourReq = elastic_lib:getElasticRequest({kpi_day, From, To}),
        KpiHourReply =  espool:es_post(pool1, Url, KpiHourReq ),
        {[{_,_}, {_,_}, {_,{[{_,_}, {_,_}, {_,_}]}}, {<<"hits">>,{[{_,_}, {_,_}, {<<"hits">>, _Filtered }]}}, {<<"aggregations">>, Aggregations } ]} = jiffy:decode( KpiHourReply ),
        cowboy_req:reply(200, #{<<"content-type">> => <<"text/plain; charset=utf-8">>}, jiffy:encode(Aggregations), Req);
%% Kpi Default
handle_api(<<"GET">>, {apireq, <<"kpi">>, _Type, OrgName, From, To, _Rsm, undefined, _Agg1, _Agg2, _Columns1, _Columns2, Url }, Req) when OrgName =/= undefined, From =/= undefined, To =/= undefined ->
        KpiReq = elastic_lib:getElasticRequest({kpi_default, From, To}),
        KpiReply =  espool:es_post(pool1, Url, KpiReq ),
        {[{_,_}, {_,_}, {_,{[{_,_}, {_,_}, {_,_}]}}, {<<"hits">>,{[{_,_}, {_,_}, {<<"hits">>, _Filtered }]}}, {<<"aggregations">>, Aggregations } ]} = jiffy:decode( KpiReply ),
        cowboy_req:reply(200, #{<<"content-type">> => <<"text/plain; charset=utf-8">>}, jiffy:encode(Aggregations), Req);
%% Heatmap Service
handle_api(<<"GET">>, {apireq, <<"heatmap">>, undefined, OrgName, From, To, undefined, _Granularity, _Agg1, _Agg2, _Columns1, _Columns2, Url }, Req) when OrgName =/= undefined, From =/= undefined, To =/= undefined ->
        HeatmapServiceReq = elastic_lib:getElasticRequest({heatmap_service, From, To}),
        HeatmapServiceReply =  espool:es_post(pool1, Url, HeatmapServiceReq ),
        {[{_,_}, {_,_}, {_,{[{_,_}, {_,_}, {_,_}]}}, {<<"hits">>,{[{_,_}, {_,_}, {<<"hits">>, _Filtered }]}}, {<<"aggregations">>, Aggregations } ]} = jiffy:decode( HeatmapServiceReply ),
        cowboy_req:reply(200, #{<<"content-type">> => <<"text/plain; charset=utf-8">>}, jiffy:encode(Aggregations), Req);
%% Heatmap Application
handle_api(<<"GET">>, {apireq, <<"heatmap">>, <<"application">>, OrgName, From, To, Rsm, _Granularity, _Agg1, _Agg2, _Columns1, _Columns2, Url }, Req) when OrgName =/= undefined, From =/= undefined, To =/= undefined, Rsm =/= undefined ->
        HeatmapApplicationReq = elastic_lib:getElasticRequest({heatmap_app_node, From, To, Rsm, <<"application">>}),
        HeatmapApplicationReply =  espool:es_post(pool1, Url, HeatmapApplicationReq ),
        {[{_,_}, {_,_}, {_,{[{_,_}, {_,_}, {_,_}]}}, {<<"hits">>,{[{_,_}, {_,_}, {<<"hits">>, _Filtered }]}}, {<<"aggregations">>, Aggregations } ]} = jiffy:decode( HeatmapApplicationReply ),
        cowboy_req:reply(200, #{<<"content-type">> => <<"text/plain; charset=utf-8">>}, jiffy:encode(Aggregations), Req);
%% Heatmap Node
handle_api(<<"GET">>, {apireq, <<"heatmap">>, <<"node">>, OrgName, From, To, Rsm, _Granularity, _Agg1, _Agg2, _Columns1, _Columns2, Url }, Req) when OrgName =/= undefined, From =/= undefined, To =/= undefined, Rsm =/= undefined ->
        HeatmapNodeReq = elastic_lib:getElasticRequest({heatmap_app_node, From, To, Rsm, <<"node">>}),
        HeatmapNodeReply =  espool:es_post(pool1, Url, HeatmapNodeReq ),
        {[{_,_}, {_,_}, {_,{[{_,_}, {_,_}, {_,_}]}}, {<<"hits">>,{[{_,_}, {_,_}, {<<"hits">>, _Filtered }]}}, {<<"aggregations">>, Aggregations } ]} = jiffy:decode( HeatmapNodeReply ),
        cowboy_req:reply(200, #{<<"content-type">> => <<"text/plain; charset=utf-8">>}, jiffy:encode(Aggregations), Req);
%% Histogram request
handle_api(<<"GET">>, {apireq, <<"packetbeat">>, _Type, _OrgName, From, To, _Rsm, _Granularity, Agg1, _Agg2, Columns1, _Columns2, _Url }, Req) when From =/= undefined, To =/= undefined, Agg1 =/= undefined, Columns1 =/= undefined ->
        PacketbeatUrl = "/packetbeat-*/_search",
	PacketbeatReq = elastic_lib:getElasticRequest({packetbeat_request, From, To, Agg1, Columns1}),
        PacketbeatReply = espool:es_post(pool1, PacketbeatUrl, PacketbeatReq ),
	io:format("~n", PacketbeatReply),
        {[{_,_}, {_,_}, {_,{[{_,_}, {_,_}, {_,_}]}}, {<<"hits">>,{[{_,_}, {_,_}, {<<"hits">>, _Filtered }]}}, {<<"aggregations">>, Aggregations } ]} = jiffy:decode( PacketbeatReply ),
        cowboy_req:reply(200, #{<<"content-type">> => <<"text/plain; charset=utf-8">>}, jiffy:encode(Aggregations), Req);

handle_api(_, _, Req) ->
	%% Method not allowed.
	cowboy_req:reply(405, Req).