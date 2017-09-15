-module(elastic_lib).
-export([getElasticRequest/1, groupEntityByType/1, parseService/1, getRsmData/1, itos/1]).

%%
% GA Main Dashboard
%%
groupEntityByType({EntityList}) ->
        groupEntityByType({EntityList, []});
groupEntityByType({[EntityListHead|EntityListTail], Acc}) ->
        {[ _Index, _Type, _Id, _Score, {<<"_source">>, {[ _Organization, _Name, _Timestamp, Img, Color, _Service, _IdInSrc, _Appid, _Appgroup, Severity, Type, _Events]} }]} = EntityListHead,
        % Declare EntityA 
        EntityA = {[ Type, Severity, Img, Color ]},

        case lists:member(Type, [ TypeType || {[ TypeType, _Severity, _Img, _Color ]} <- Acc ] ) of
                true  -> 
                         % Get entity with current type
                         [ EntityB ] = [ {[ TypeB, SeverityB, ImgB, ColorB ]} || {[ TypeB, SeverityB, ImgB, ColorB ]} <- Acc, TypeB =:= Type ],
                         % Delete entity with current type from Acc
                         ListWithoutType = lists:delete(EntityB, Acc),
                         % Compare New Entity and Old Entity to choose max severity
                         EntityMax = lists:max([ EntityA, EntityB ]),
                         % Loop groupEntityByType
                         groupEntityByType({ EntityListTail, [ EntityMax | ListWithoutType ] });
                false ->
                         groupEntityByType({ EntityListTail, [ EntityA | Acc] })
        end;
groupEntityByType({[], Acc}) -> Acc.
        % lists:sum(MembersAvailability) / erlang:length(MembersAvailability) * 100
%        [ {[ Type, Severity, Img, Color, {<<"MembersAvlArr">>, MembersAvailability }, {<<"MembersAvlPct">>, lists:sum(MembersAvailability) / erlang:length(MembersAvailability) * 100 } ]} 
%                 || {[ Type, Severity, Img, Color, MembersAvailability ]} <- Acc 
%                 ].

%%
% GA Main Dashboard availabilityBySeverity
%%
% availabilityBySeverity(Severity) when Severity == 5 -> 0;
% availabilityBySeverity(_Severity)                   -> 1.
%%
% GA parse Service
%%
parseService({Services}) ->
        parseService({Services, []});
parseService({[ServicesHead|ServicesTail], Acc}) ->
        {[_Index, _Type, _Id, _Score, {<<"_source">>, {[ _Organization, {_SvcId, SvcName}, _Timestamp, _Img, _Color, _Service, _IdInSrc, _Appid, _Appgroup, _Severity, _TypeType, _Events ]}}]} = ServicesHead,
        parseService({ ServicesTail, [ SvcName | Acc ] });
parseService({[], Acc}) -> Acc.
%%
% GA Get RSM for each service
%%
getRsmData({Url, LastTs, Services}) ->
        getRsmData({Url, LastTs, Services, [] });
getRsmData({Url, LastTs, [ServicesHead|ServicesTail], Acc }) ->

        %%%%
        %% Service block
        %%%
        %% For each service get query for Status
        EntityFilteredReq = getElasticRequest({entity_filtered, ServicesHead, LastTs }),
        %% Execute each query for Status
        EntityFilteredReply = espool:es_post(pool1, Url, EntityFilteredReq ),
        %% Parse result of each query for Status
        {[{_,_}, {_,_}, {_,{[{_,_}, {_,_}, {_,_}]}}, {<<"hits">>, {[{_,_}, {_,_}, {<<"hits">>, Filtered }]}}]} = jiffy:decode( EntityFilteredReply ),

        %%%%
        %% Heatmap block
        %%%
        {{Year,Month,Day},{Hour,Minutes,_Seconds}} = calendar:local_time(),
        From = binary:list_to_bin( itos(Year) ++ "-" ++ itos(Month) ++ "-" ++ itos(Day - 1) ++ " " ++ itos(Hour) ++ ":" ++ itos(Minutes) ++ ":59" ),
        To   = binary:list_to_bin( itos(Year) ++ "-" ++ itos(Month) ++ "-" ++ itos(Day) ++ " " ++ itos(Hour) ++ ":" ++ itos(Minutes) ++ ":59"),

        %% For each service get query for Heatmap
        HeatmapApplicationReq = elastic_lib:getElasticRequest({heatmap_app_node, From, To, ServicesHead, <<"service">> }),
        %% Execute each query for Heatmap
        HeatmapApplicationReply =  espool:es_post(pool1, Url, HeatmapApplicationReq ),
        %% Parse result of each query for Heatmap
        {[{_,_}, {_,_}, {_,{[{_,_}, {_,_}, {_,_}]}}, {<<"hits">>,{[{_,_}, {_,_}, {<<"hits">>, _Filtered }]}}, {<<"aggregations">>, HeatmapAggregations } ]} = jiffy:decode( HeatmapApplicationReply ),

        %%%%
        %% Finish block
        %%%
        %% Put query result to json clause
        Service = {[ { <<"service">>, ServicesHead }, { <<"ke">>, groupEntityByType({Filtered}) }, { <<"heatmap">>, HeatmapAggregations } ]},
        getRsmData({Url, LastTs, ServicesTail, [ Service | Acc  ] });
getRsmData({_Url, _LastTs, [], Acc }) -> Acc.

itos(Val) when Val < 10 -> "0" ++ erlang:integer_to_list(Val);
itos(Val)               -> erlang:integer_to_list(Val).


%%
% Requests to Elasticsearch
%%
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
getElasticRequest({entity_filtered, RsmName, Type, LastTs }) ->
                #{
                        <<"size">> => 10000,
                        <<"query">> => #{
                                <<"bool">> => #{
                                     <<"must">> => [
                                                #{ <<"match_phrase">> => #{ <<"service">> => RsmName } },
                                                #{ <<"match">> => #{ <<"type">> => Type  } },
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
getElasticRequest({entity_type_grouped}) ->
        	#{
			<<"size">> => 0,
			<<"aggs">> => #{
				<<"group_by_state">> => #{
					<<"terms">> => #{
                                                <<"field">> => <<"type">>
                                        }
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
getElasticRequest({kpi_default, From, To, Type}) ->
		#{
                        <<"size">> => 0,
                        <<"query">> => #{
                                <<"bool">> => #{
                                        <<"must">> => [
                                                #{ <<"match">> => #{ <<"type">> => Type  } },
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

%% TEST EXAMPLE
%
% Query = #{<<"size">> => 1}.
% httpc:request(post, {"http://localhost:9200/packetbeat-*/_search", [], "", jiffy:encode(Query)}, [], []).
%