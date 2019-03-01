%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_peer_discovery_dns).
-behaviour(rabbit_peer_discovery_backend).

-include("rabbit.hrl").

-export([list_nodes/0, supports_registration/0, register/0, unregister/0,
         post_registration/0, lock/1, unlock/1]).
%% for tests
-export([discover_nodes/2, discover_hostnames/2]).

%%
%% API
%%

-spec list_nodes() ->
    {ok, {Nodes :: [node()], rabbit_types:node_type()}}.

list_nodes() ->
    case application:get_env(rabbit, cluster_formation) of
      undefined         ->
            {ok, {[], disc}};
      {ok, ClusterFormation} ->
        case proplists:get_value(peer_discovery_dns, ClusterFormation) of
            undefined ->
              rabbit_log:warning("Peer discovery backend is set to ~s "
                                 "but final config does not contain rabbit.cluster_formation.peer_discovery_dns. "
                                 "Cannot discover any nodes because seed hostname is not configured!",
                                 [?MODULE]),
              {ok, {[], disc}};
            Proplist  ->
              Hostname = rabbit_data_coercion:to_list(proplists:get_value(hostname, Proplist)),

              {ok, {discover_nodes(Hostname, net_kernel:longnames()), rabbit_peer_discovery:node_type()}}
        end
    end.


-spec supports_registration() -> boolean().

supports_registration() ->
    false.


-spec register() -> ok.

register() ->
    ok.

-spec unregister() -> ok.

unregister() ->
    ok.

-spec post_registration() -> ok.

post_registration() ->
    ok.

-spec lock(Node :: atom()) -> not_supported.

lock(_Node) ->
    not_supported.

-spec unlock(Data :: term()) -> ok.

unlock(_Data) ->
    ok.

%%
%% Implementation
%%

discover_nodes(SeedHostname, LongNamesUsed) ->
    [list_to_atom(rabbit_peer_discovery:append_node_prefix(H)) ||
        H <- discover_hostnames(SeedHostname, LongNamesUsed)].

discover_hostnames(SeedHostname, LongNamesUsed) ->
    lookup(SeedHostname, LongNamesUsed, ipv4) ++
    lookup(SeedHostname, LongNamesUsed, ipv6).

decode_record(ipv4) ->
    a;
decode_record(ipv6) ->
    aaaa.

lookup(SeedHostname, LongNamesUsed, IPv) ->
    IPs   = inet_res:lookup(SeedHostname, in, decode_record(IPv)),
    rabbit_log:info("Addresses discovered via ~s records of ~s: ~s",
		    [string:to_upper(atom_to_list(decode_record(IPv))),
		     SeedHostname,
		     string:join([inet_parse:ntoa(IP) || IP <- IPs], ", ")]),
    Hosts = [extract_host(inet:gethostbyaddr(A), LongNamesUsed, A) ||
		A <- IPs],
    lists:filter(fun(E) -> E =/= error end, Hosts).


%% long node names are used
extract_host({ok, {hostent, FQDN, _, _, _, _}}, true, _Address) ->
  FQDN;
%% short node names are used
extract_host({ok, {hostent, FQDN, _, _, _, _}}, false, _Address) ->
  lists:nth(1, string:tokens(FQDN, "."));
extract_host({error, Error}, _, Address) ->
  rabbit_log:error("Reverse DNS lookup for address ~s failed: ~p",
                   [inet_parse:ntoa(Address), Error]),
  error.
