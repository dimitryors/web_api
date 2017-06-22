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
	LastTsReq = getElasticRequest({last_ts}),
	LastTsReply = espool:es_post(pool1, Url, LastTsReq ),
	{[{_,_}, {_,_}, {_, {[{_,_}, {_,_}, {_,_}]}}, {_, {[{_,_}, {_,_}, {_,_}]}}, {_,{[{<<"max_ts">>, {[{_,_}, {<<"value_as_string">>, LastTs }]}}]}}]} = jiffy:decode( LastTsReply ),
	EntityFilteredReq = getElasticRequest({entity_filtered, Rsm, LastTs }),
	EntityFilteredReply = espool:es_post(pool1, Url, EntityFilteredReq ),
	{[{_,_}, {_,_}, {_,{[{_,_}, {_,_}, {_,_}]}}, {<<"hits">>, {[{_,_}, {_,_}, {<<"hits">>, Filtered }]}}]} = jiffy:decode( EntityFilteredReply ),
    	cowboy_req:reply(200, #{<<"content-type">> => <<"text/plain; charset=utf-8">>}, jiffy:encode(Filtered), Req);
%% Relation
handle_api(<<"GET">>, {apireq, <<"relation">>, _Type, _OrgName, _From, _To, _Rsm, _Granularity, _Agg1, _Agg2, _Columns1, _Columns2, _Url }, Req) ->
        RelationUrl = "/bsmsc1/relation/_search",
	RelationReq = getElasticRequest({relation}),
        RelationReply = espool:es_post(pool1, RelationUrl, RelationReq ),
	{[{_,_}, {_,_}, {_,{[{_,_}, {_,_}, {_,_}]}}, {<<"hits">>, {[{_,_}, {_,_}, {<<"hits">>, Filtered }]}}]} = jiffy:decode( RelationReply ),
	cowboy_req:reply(200, #{<<"content-type">> => <<"text/plain; charset=utf-8">>}, jiffy:encode(Filtered), Req);
%% Services
handle_api(<<"GET">>, {apireq, <<"services">>, _Type, OrgName, _From, _To, _Rsm, _Granularity, _Agg1, _Agg2, _Columns1, _Columns2, Url }, Req) when OrgName =/= undefined ->
	LastTsReq = getElasticRequest({last_ts}),
	LastTsReply = espool:es_post(pool1, Url, LastTsReq ),
	{[{_,_}, {_,_}, {_, {[{_,_}, {_,_}, {_,_}]}}, {_, {[{_,_}, {_,_}, {_,_}]}}, {_,{[{<<"max_ts">>, {[{_,_}, {<<"value_as_string">>, LastTs }]}}]}}]} = jiffy:decode( LastTsReply ),
	ServiceFilteredReq = getElasticRequest({services_filtered, LastTs}),
	ServiceFilteredReply = espool:es_post(pool1, Url, ServiceFilteredReq ),
	{[{_,_}, {_,_}, {_,{[{_,_}, {_,_}, {_,_}]}}, {<<"hits">>, {[{_,_}, {_,_}, {<<"hits">>, Filtered }]}}]} = jiffy:decode( ServiceFilteredReply ),
        cowboy_req:reply(200, #{<<"content-type">> => <<"text/plain; charset=utf-8">>}, jiffy:encode(Filtered), Req);
%% Kpi Week
handle_api(<<"GET">>, {apireq, <<"kpi">>, _Type, OrgName, From, To, _Rsm, <<"week">>, _Agg1, _Agg2, _Columns1, _Columns2, Url }, Req) when OrgName =/= undefined, From =/= undefined, To =/= undefined ->
        KpiWeekReq = getElasticRequest({kpi_week, From, To}),
	KpiWeekReply =  espool:es_post(pool1, Url, KpiWeekReq ),
	{[{_,_}, {_,_}, {_,{[{_,_}, {_,_}, {_,_}]}}, {<<"hits">>,{[{_,_}, {_,_}, {<<"hits">>, _Filtered }]}}, {<<"aggregations">>, Aggregations } ]} = jiffy:decode( KpiWeekReply ),
	cowboy_req:reply(200, #{<<"content-type">> => <<"text/plain; charset=utf-8">>}, jiffy:encode(Aggregations), Req);
%% Kpi Day
handle_api(<<"GET">>, {apireq, <<"kpi">>, _Type, OrgName, From, To, _Rsm, <<"day">>, _Agg1, _Agg2, _Columns1, _Columns2, Url }, Req) when OrgName =/= undefined, From =/= undefined, To =/= undefined ->
        KpiDayReq = getElasticRequest({kpi_day, From, To}),
        KpiDayReply =  espool:es_post(pool1, Url, KpiDayReq ),
        {[{_,_}, {_,_}, {_,{[{_,_}, {_,_}, {_,_}]}}, {<<"hits">>,{[{_,_}, {_,_}, {<<"hits">>, _Filtered }]}}, {<<"aggregations">>, Aggregations } ]} = jiffy:decode( KpiDayReply ),
        cowboy_req:reply(200, #{<<"content-type">> => <<"text/plain; charset=utf-8">>}, jiffy:encode(Aggregations), Req);
%% Kpi Hour
handle_api(<<"GET">>, {apireq, <<"kpi">>, _Type, OrgName, From, To, _Rsm, <<"hour">>, _Agg1, _Agg2, _Columns1, _Columns2, Url }, Req) when OrgName =/= undefined, From =/= undefined, To =/= undefined ->
        KpiHourReq = getElasticRequest({kpi_day, From, To}),
        KpiHourReply =  espool:es_post(pool1, Url, KpiHourReq ),
        {[{_,_}, {_,_}, {_,{[{_,_}, {_,_}, {_,_}]}}, {<<"hits">>,{[{_,_}, {_,_}, {<<"hits">>, _Filtered }]}}, {<<"aggregations">>, Aggregations } ]} = jiffy:decode( KpiHourReply ),
        cowboy_req:reply(200, #{<<"content-type">> => <<"text/plain; charset=utf-8">>}, jiffy:encode(Aggregations), Req);
%% Kpi Default
handle_api(<<"GET">>, {apireq, <<"kpi">>, _Type, OrgName, From, To, _Rsm, undefined, _Agg1, _Agg2, _Columns1, _Columns2, Url }, Req) when OrgName =/= undefined, From =/= undefined, To =/= undefined ->
        KpiReq = getElasticRequest({kpi_default, From, To}),
        KpiReply =  espool:es_post(pool1, Url, KpiReq ),
        {[{_,_}, {_,_}, {_,{[{_,_}, {_,_}, {_,_}]}}, {<<"hits">>,{[{_,_}, {_,_}, {<<"hits">>, _Filtered }]}}, {<<"aggregations">>, Aggregations } ]} = jiffy:decode( KpiReply ),
        cowboy_req:reply(200, #{<<"content-type">> => <<"text/plain; charset=utf-8">>}, jiffy:encode(Aggregations), Req);
%% Heatmap Service
handle_api(<<"GET">>, {apireq, <<"heatmap">>, undefined, OrgName, From, To, undefined, _Granularity, _Agg1, _Agg2, _Columns1, _Columns2, Url }, Req) when OrgName =/= undefined, From =/= undefined, To =/= undefined ->
        HeatmapServiceReq = getElasticRequest({heatmap_service, From, To}),
        HeatmapServiceReply =  espool:es_post(pool1, Url, HeatmapServiceReq ),
        {[{_,_}, {_,_}, {_,{[{_,_}, {_,_}, {_,_}]}}, {<<"hits">>,{[{_,_}, {_,_}, {<<"hits">>, _Filtered }]}}, {<<"aggregations">>, Aggregations } ]} = jiffy:decode( HeatmapServiceReply ),
        cowboy_req:reply(200, #{<<"content-type">> => <<"text/plain; charset=utf-8">>}, jiffy:encode(Aggregations), Req);
%% Heatmap Application
handle_api(<<"GET">>, {apireq, <<"heatmap">>, <<"application">>, OrgName, From, To, Rsm, _Granularity, _Agg1, _Agg2, _Columns1, _Columns2, Url }, Req) when OrgName =/= undefined, From =/= undefined, To =/= undefined, Rsm =/= undefined ->
        HeatmapApplicationReq = getElasticRequest({heatmap_app_node, From, To, Rsm, <<"application">>}),
        HeatmapApplicationReply =  espool:es_post(pool1, Url, HeatmapApplicationReq ),
        {[{_,_}, {_,_}, {_,{[{_,_}, {_,_}, {_,_}]}}, {<<"hits">>,{[{_,_}, {_,_}, {<<"hits">>, _Filtered }]}}, {<<"aggregations">>, Aggregations } ]} = jiffy:decode( HeatmapApplicationReply ),
        cowboy_req:reply(200, #{<<"content-type">> => <<"text/plain; charset=utf-8">>}, jiffy:encode(Aggregations), Req);
%% Heatmap Node
handle_api(<<"GET">>, {apireq, <<"heatmap">>, <<"node">>, OrgName, From, To, Rsm, _Granularity, _Agg1, _Agg2, _Columns1, _Columns2, Url }, Req) when OrgName =/= undefined, From =/= undefined, To =/= undefined, Rsm =/= undefined ->
        HeatmapNodeReq = getElasticRequest({heatmap_app_node, From, To, Rsm, <<"node">>}),
        HeatmapNodeReply =  espool:es_post(pool1, Url, HeatmapNodeReq ),
        {[{_,_}, {_,_}, {_,{[{_,_}, {_,_}, {_,_}]}}, {<<"hits">>,{[{_,_}, {_,_}, {<<"hits">>, _Filtered }]}}, {<<"aggregations">>, Aggregations } ]} = jiffy:decode( HeatmapNodeReply ),
        cowboy_req:reply(200, #{<<"content-type">> => <<"text/plain; charset=utf-8">>}, jiffy:encode(Aggregations), Req);
%% Histogram request
handle_api(<<"GET">>, {apireq, <<"packetbeat">>, _Type, _OrgName, From, To, _Rsm, _Granularity, Agg1, _Agg2, Columns1, _Columns2, _Url }, Req) when From =/= undefined, To =/= undefined, Agg1 =/= undefined, Columns1 =/= undefined ->
        PacketbeatUrl = "/packetbeat-*/_search",
	PacketbeatReq = getElasticRequest({packetbeat_request, From, To, Agg1, Columns1}),
        PacketbeatReply = espool:es_post(pool1, PacketbeatUrl, PacketbeatReq ),
	io:format("~n", PacketbeatReply),
        {[{_,_}, {_,_}, {_,{[{_,_}, {_,_}, {_,_}]}}, {<<"hits">>,{[{_,_}, {_,_}, {<<"hits">>, _Filtered }]}}, {<<"aggregations">>, Aggregations } ]} = jiffy:decode( PacketbeatReply ),
        cowboy_req:reply(200, #{<<"content-type">> => <<"text/plain; charset=utf-8">>}, jiffy:encode(Aggregations), Req);

handle_api(_, _, Req) ->
	%% Method not allowed.
	cowboy_req:reply(405, Req).







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
getElasticRequest({relation}) ->
        	#{
			<<"size">> => 10000
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
getElasticRequest({kpi_week, From, To}) ->
                #{
                        <<"size">> => 0,
                        <<"query">> => #{
                                <<"bool">> => #{
                                        <<"must">> => [
                                                #{ <<"match">> => #{ <<"type">> => <<"service">>  } },
						#{ <<"range">> => #{ <<"timestamp">> => #{ <<"gt">> => From, <<"lt">> => To } } }
                                        ]
                                }
                        },
			
			<<"aggs">> => #{
				<<"byWeek">> => #{
					 <<"date_histogram">> => #{
						<<"field">> => <<"timestamp">>,
						<<"interval">> => <<"week">>
					},

					<<"aggs">> => #{
						<<"byEntity">> => #{
							<<"terms">> => #{ 
								<<"field">> => <<"name">> 
							},

							<<"aggs">> => #{
								<<"severity">> => #{
									<<"scripted_metric">> => #{ 
										<<"lang">> => <<"groovy">>,
										<<"init_script">> => <<"_agg[\"tempArray\"] = [];">>,
										<<"map_script">> => <<"if (doc.severity.value == 5) { _agg.tempArray.add(0); } else { _agg.tempArray.add(1); } ">>,
										<<"combine_script">> => <<"exRevenue = []; for (i in _agg.tempArray) { exRevenue += i }; return exRevenue;">>,
										<<"reduce_script">> => <<"exRevenue = []; for (j in _aggs) { exRevenue += j }; return ( ( exRevenue.sum() / exRevenue.size() ) * 100 );">>
									}
								}
							}
						}
					}
				}
			}
                };
getElasticRequest({kpi_day, From, To}) ->
                #{
                        <<"size">> => 0,
                        <<"query">> => #{
                                <<"bool">> => #{
                                        <<"must">> => [
                                                #{ <<"match">> => #{ <<"type">> => <<"service">>  } },
                                                #{ <<"range">> => #{ <<"timestamp">> => #{ <<"gt">> => From, <<"lt">> => To } } }
                                        ]
                                }
                        },

			 <<"aggs">> => #{
                                <<"byDay">> => #{
                                         <<"date_histogram">> => #{
                                                <<"field">> => <<"timestamp">>,
                                                <<"interval">> => <<"day">>
                                        },

                                        <<"aggs">> => #{
                                                <<"byEntity">> => #{
                                                        <<"terms">> => #{
                                                                <<"field">> => <<"name">>
                                                        },

							<<"aggs">> => #{
                                                                <<"severity">> => #{
                                                                        <<"scripted_metric">> => #{
                                                                                <<"lang">> => <<"groovy">>,
                                                                                <<"init_script">> => <<"_agg[\"tempArray\"] = [];">>,
                                                                                <<"map_script">> => <<"if (doc.severity.value == 5) { _agg.tempArray.add(0); } else { _agg.tempArray.add(1); } ">>,
                                                                                <<"combine_script">> => <<"exRevenue = []; for (i in _agg.tempArray) { exRevenue += i }; return exRevenue;">>,
                                                                                <<"reduce_script">> => <<"exRevenue = []; for (j in _aggs) { exRevenue += j }; return ( ( exRevenue.sum() / exRevenue.size() ) * 100 );">>
                                                                        }
                                                                }
                                                        }
                                                }
                                        }
                                }
                        }
                };
getElasticRequest({kpi_hour, From, To}) ->
                #{
                        <<"size">> => 0,
                        <<"query">> => #{
                                <<"bool">> => #{
                                        <<"must">> => [
                                                #{ <<"match">> => #{ <<"type">> => <<"service">>  } },
                                                #{ <<"range">> => #{ <<"timestamp">> => #{ <<"gt">> => From, <<"lt">> => To } } }
                                        ]
                                }
                        },

                         <<"aggs">> => #{
                                <<"byHour">> => #{
                                         <<"date_histogram">> => #{
                                                <<"field">> => <<"timestamp">>,
                                                <<"interval">> => <<"hour">>
                                        },

                                        <<"aggs">> => #{
                                                <<"byEntity">> => #{
                                                        <<"terms">> => #{
                                                                <<"field">> => <<"name">>
                                                        },
	
							<<"aggs">> => #{
                                                                <<"severity">> => #{
                                                                        <<"scripted_metric">> => #{
                                                                                <<"lang">> => <<"groovy">>,
                                                                                <<"init_script">> => <<"_agg[\"tempArray\"] = [];">>,
                                                                                <<"map_script">> => <<"if (doc.severity.value == 5) { _agg.tempArray.add(0); } else { _agg.tempArray.add(1); } ">>,
                                                                                <<"combine_script">> => <<"exRevenue = []; for (i in _agg.tempArray) { exRevenue += i }; return exRevenue;">>,
                                                                                <<"reduce_script">> => <<"exRevenue = []; for (j in _aggs) { exRevenue += j }; return ( ( exRevenue.sum() / exRevenue.size() ) * 100 );">>
                                                                        }
                                                                }
                                                        }
                                                }
                                        }
                                }
                        }
                };
getElasticRequest({kpi_default, From, To}) ->
		#{
                        <<"size">> => 0,
                        <<"query">> => #{
                                <<"bool">> => #{
                                        <<"must">> => [
                                                #{ <<"match">> => #{ <<"type">> => <<"service">>  } },
                                                #{ <<"range">> => #{ <<"timestamp">> => #{ <<"gt">> => From, <<"lt">> => To } } }
                                        ]
                                }
                        },

			<<"aggs">> => #{

				<<"byHour">> => #{
                                         <<"date_histogram">> => #{
                                                <<"field">> => <<"timestamp">>,
                                                <<"interval">> => <<"hour">>
                                        },

                                        <<"aggs">> => #{
                                                <<"byEntity">> => #{
                                                        <<"terms">> => #{
                                                                <<"field">> => <<"name">>
                                                        },

                                                        <<"aggs">> => #{
                                                                <<"severity">> => #{
                                                                        <<"scripted_metric">> => #{
                                                                                <<"lang">> => <<"groovy">>,
                                                                                <<"init_script">> => <<"_agg[\"tempArray\"] = [];">>,
                                                                                <<"map_script">> => <<"if (doc.severity.value == 5) { _agg.tempArray.add(0); } else { _agg.tempArray.add(1); } ">>,
                                                                                <<"combine_script">> => <<"exRevenue = []; for (i in _agg.tempArray) { exRevenue += i }; return exRevenue;">>,
                                                                                <<"reduce_script">> => <<"exRevenue = []; for (j in _aggs) { exRevenue += j }; return ( ( exRevenue.sum() / exRevenue.size() ) * 100 );">>
                                                                        }
                                                                }
                                                        }
                                                }
                                        }
                                },

				<<"byDay">> => #{
                                         <<"date_histogram">> => #{
                                                <<"field">> => <<"timestamp">>,
                                                <<"interval">> => <<"day">>
                                        },

                                        <<"aggs">> => #{
                                                <<"byEntity">> => #{
                                                        <<"terms">> => #{
                                                                <<"field">> => <<"name">>
                                                        },

                                                        <<"aggs">> => #{
                                                                <<"severity">> => #{
                                                                        <<"scripted_metric">> => #{
                                                                                <<"lang">> => <<"groovy">>,
                                                                                <<"init_script">> => <<"_agg[\"tempArray\"] = [];">>,
                                                                                <<"map_script">> => <<"if (doc.severity.value == 5) { _agg.tempArray.add(0); } else { _agg.tempArray.add(1); } ">>,
                                                                                <<"combine_script">> => <<"exRevenue = []; for (i in _agg.tempArray) { exRevenue += i }; return exRevenue;">>,
                                                                                <<"reduce_script">> => <<"exRevenue = []; for (j in _aggs) { exRevenue += j }; return ( ( exRevenue.sum() / exRevenue.size() ) * 100 );">>
                                                                        }
                                                                }
                                                        }
                                                }
                                        }
                                },

				<<"byWeek">> => #{
                                         <<"date_histogram">> => #{
                                                <<"field">> => <<"timestamp">>,
                                                <<"interval">> => <<"week">>
                                        },

                                        <<"aggs">> => #{
                                                <<"byEntity">> => #{
                                                        <<"terms">> => #{
                                                                <<"field">> => <<"name">>
                                                        },

                                                        <<"aggs">> => #{
                                                                <<"severity">> => #{
                                                                        <<"scripted_metric">> => #{
                                                                                <<"lang">> => <<"groovy">>,
                                                                                <<"init_script">> => <<"_agg[\"tempArray\"] = [];">>,
                                                                                <<"map_script">> => <<"if (doc.severity.value == 5) { _agg.tempArray.add(0); } else { _agg.tempArray.add(1); } ">>,
                                                                                <<"combine_script">> => <<"exRevenue = []; for (i in _agg.tempArray) { exRevenue += i }; return exRevenue;">>,
                                                                                <<"reduce_script">> => <<"exRevenue = []; for (j in _aggs) { exRevenue += j }; return ( ( exRevenue.sum() / exRevenue.size() ) * 100 );">>
                                                                        }
                                                                }
                                                        }
                                                }
                                        }
                                }

			}
		};
getElasticRequest({heatmap_service, From, To}) ->
                #{
                        <<"size">> => 0,
                        <<"query">> => #{
                                <<"bool">> => #{
                                        <<"must">> => [
                                                #{ <<"match">> => #{ <<"type">> => <<"service">>  } },
                                                #{ <<"range">> => #{ <<"timestamp">> => #{ <<"gt">> => From, <<"lt">> => To } } }
                                        ]
                                }
                        },

			<<"aggs">> => #{
				<<"byTs">> => #{
                                         <<"date_histogram">> => #{
                                                <<"field">> => <<"timestamp">>,
                                                <<"interval">> => <<"hour">>
                                        },

                                        <<"aggs">> => #{
                                                <<"byEntity">> => #{
                                                        <<"terms">> => #{
                                                                <<"field">> => <<"name">>
                                                        },

                                                        <<"aggs">> => #{
								<<"maxSeverity">> => #{
									<<"max">> => #{
										<<"field">> => <<"severity">>
									}
								}	
                                                        }
                                                }
                                        }
                                },

				<<"entList">> => #{
					<<"terms">> => #{
						<<"field">> => <<"name">>
					}
				}


			}
		};
getElasticRequest({heatmap_app_node, From, To, Rsm, Type}) ->
                #{
			<<"size">> => 0,
                        <<"query">> => #{
                                <<"bool">> => #{
                                        <<"must">> => [
                                                #{ <<"match">> => #{ <<"type">> => Type  } },
						#{ <<"match_phrase">> => #{ <<"service">> => Rsm } },
                                                #{ <<"range">> => #{ <<"timestamp">> => #{ <<"gt">> => From, <<"lt">> => To } } }
                                        ]
                                }
                        },

			<<"aggs">> => #{
				<<"byTs">> => #{
					<<"date_histogram">> => #{
                                                <<"field">> => <<"timestamp">>,
                                                <<"interval">> => <<"hour">>
                                        },

					<<"aggs">> => #{
						<<"byEntity">> => #{
                                                        <<"terms">> => #{
                                                                <<"script">> => #{
									<<"inline">> => <<"doc['type'].value == 'application' ? doc['name'].value + '_' + doc['appid'].value : doc['name'].value">>,
									<<"lang">> => <<"groovy">>
								}
                                                        },
                                                        <<"aggs">> => #{
                                                                <<"maxSeverity">> => #{
                                                                        <<"max">> => #{
                                                                                <<"field">> => <<"severity">>
                                                                        }
                                                                }
                                                        }
                                                }

					}
				},
				
				<<"entList">> => #{
                                        <<"terms">> => #{
                                                <<"script">> => #{
							<<"inline">> => <<"doc['type'].value == 'application' ? doc['name'].value + '_' + doc['appid'].value : doc['name'].value">>,
							<<"lang">> => <<"groovy">>
						}
                                        }
                                }

			}
		};

getElasticRequest({packetbeat_request, From, To, Agg1, Columns1}) ->
        #{
		<<"query">> => #{
			<<"filtered">> => #{
				<<"query">> => #{
					<<"query_string">> => #{
						<<"analyze_wildcard">> => <<"true">>,
                				<<"query">> => <<"*">>
              				}
				},
				<<"filter">> => #{
              				<<"bool">> => #{
						<<"must">> => [
                  					#{
                    						<<"range">> => #{
                      							<<"@timestamp">> => #{ <<"gte">> => From, <<"lte">> => To, <<"format">> => <<"epoch_millis">> }
                    						}
                  					}
                				],
                				<<"must_not">> => []
              				}
            			}
			}
		},

		<<"size">> => 0,
		<<"aggs">> => #{
			<<"agg1">> => #{
				<<"terms">> => #{
					<<"field">> => Agg1,
					<<"size">> => Columns1,
              				<<"order">> => #{
						<<"_count">> => <<"desc">>
					}
				}


			}
		}

        };
getElasticRequest({packetbeat_agg_data, From, To, Agg1, Columns1, Agg2, Columns2}) ->
        #{
                <<"query">> => #{
                        <<"filtered">> => #{
                                <<"query">> => #{
                                        <<"query_string">> => #{
                                                <<"analyze_wildcard">> => <<"true">>,
                                                <<"query">> => <<"*">>
                                        }
                                },
                                <<"filter">> => #{
                                        <<"bool">> => #{
                                                <<"must">> => [
                                                        #{
                                                                <<"range">> => #{
                                                                        <<"@timestamp">> => #{ <<"gte">> => From, <<"lte">> => To, <<"format">> => <<"epoch_millis">> }
                                                                }
                                                        }
                                                ],
                                                <<"must_not">> => []
                                        }
                                }
                        }
                },

		<<"size">> => 0,
                <<"aggs">> => #{
                        <<"agg1">> => #{
                                <<"terms">> => #{
                                        <<"field">> => Agg1,
                                        <<"size">> => Columns1,
                                        <<"order">> => #{
                                                <<"_count">> => <<"desc">>
                                        }
                                },
                                <<"aggs">> => #{
                                        <<"agg2">> => #{
                                                <<"terms">> => #{
                                                        <<"field">> => Agg2,
                                                        <<"size">> => Columns2,
                                                        <<"order">> => #{
                                                                <<"_count">> => <<"desc">>
                                                        }
                                                }
                                        }
                                }

                        }
                }

        };
getElasticRequest(_) -> undefined.
