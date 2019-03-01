%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_amqqueue_sup_sup).

-behaviour(supervisor2).

-export([start_link/0, start_queue_process/3]).
-export([start_for_vhost/1, stop_for_vhost/1,
         find_for_vhost/2, find_for_vhost/1]).

-export([init/1]).

-include("rabbit.hrl").

-define(SERVER, ?MODULE).

%%----------------------------------------------------------------------------

-spec start_link() -> rabbit_types:ok_pid_or_error().

start_link() ->
    supervisor2:start_link(?MODULE, []).

-spec start_queue_process
        (node(), amqqueue:amqqueue(), 'declare' | 'recovery' | 'slave') ->
            pid().

start_queue_process(Node, Q, StartMode) ->
    #resource{virtual_host = VHost} = amqqueue:get_name(Q),
    {ok, Sup} = find_for_vhost(VHost, Node),
    {ok, _SupPid, QPid} = supervisor2:start_child(Sup, [Q, StartMode]),
    QPid.

init([]) ->
    {ok, {{simple_one_for_one, 10, 10},
          [{rabbit_amqqueue_sup, {rabbit_amqqueue_sup, start_link, []},
            temporary, ?SUPERVISOR_WAIT, supervisor, [rabbit_amqqueue_sup]}]}}.

-spec find_for_vhost(rabbit_types:vhost()) -> {ok, pid()} | {error, term()}.
find_for_vhost(VHost) ->
    find_for_vhost(VHost, node()).

-spec find_for_vhost(rabbit_types:vhost(), atom()) -> {ok, pid()} | {error, term()}.
find_for_vhost(VHost, Node) ->
    {ok, VHostSup} = rabbit_vhost_sup_sup:get_vhost_sup(VHost, Node),
    case supervisor2:find_child(VHostSup, rabbit_amqqueue_sup_sup) of
        [QSup] -> {ok, QSup};
        Result -> {error, {queue_supervisor_not_found, Result}}
    end.

-spec start_for_vhost(rabbit_types:vhost()) -> {ok, pid()} | {error, term()}.
start_for_vhost(VHost) ->
    case rabbit_vhost_sup_sup:get_vhost_sup(VHost) of
        {ok, VHostSup} ->
            supervisor2:start_child(
              VHostSup,
              {rabbit_amqqueue_sup_sup,
               {rabbit_amqqueue_sup_sup, start_link, []},
               transient, infinity, supervisor, [rabbit_amqqueue_sup_sup]});
        %% we can get here if a vhost is added and removed concurrently
        %% e.g. some integration tests do it
        {error, {no_such_vhost, VHost}} ->
            rabbit_log:error("Failed to start a queue process supervisor for vhost ~s: vhost no longer exists!",
                             [VHost]),
            {error, {no_such_vhost, VHost}}
    end.

-spec stop_for_vhost(rabbit_types:vhost()) -> ok.
stop_for_vhost(VHost) ->
    case rabbit_vhost_sup_sup:get_vhost_sup(VHost) of
        {ok, VHostSup} ->
            ok = supervisor2:terminate_child(VHostSup, rabbit_amqqueue_sup_sup),
            ok = supervisor2:delete_child(VHostSup, rabbit_amqqueue_sup_sup);
        %% see start/1
        {error, {no_such_vhost, VHost}} ->
            rabbit_log:error("Failed to stop a queue process supervisor for vhost ~s: vhost no longer exists!",
                             [VHost]),
            ok
    end.
