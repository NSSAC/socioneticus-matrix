%% -*- erlang-indent-level: 4;indent-tabs-mode: nil -*-
%% ex: ts=4 sw=4 et
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

-module(code_server_cache).

-behaviour(gen_server).

%% API
-export([start_link/0,
         maybe_call_mfa/4]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {
    modules = #{} :: #{atom() => boolean()}
}).

%% API
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

maybe_call_mfa(Module, Function, Args, Default) ->
    gen_server:call(?MODULE, {maybe_call_mfa, {Module, Function, Args, Default}}).

%% gen_server callbacks

init([]) ->
    {ok, #state{}}.

handle_call({maybe_call_mfa, {Mod, _F, _A, _D} = MFA}, _From, #state{modules = ModuleMap} = State0) ->
    Value = maps:get(Mod, ModuleMap, true),
    {ok, Reply, State1} = handle_maybe_call_mfa(Value, MFA, State0),
    {reply, Reply, State1};
handle_call(_Request, _From, State) ->
    {reply, ignored, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Internal functions

handle_maybe_call_mfa(false, {_M, _F, _A, Default}, State) ->
    {ok, Default, State};
handle_maybe_call_mfa(true, {Module, Function, Args, Default}, State) ->
    try
        Reply = erlang:apply(Module, Function, Args),
        {ok, Reply, State}
    catch
        error:undef ->
            handle_maybe_call_mfa_error(Module, Default, State);
        Err:Reason ->
            rabbit_log:error("Calling ~p:~p failed: ~p:~p~n",
                             [Module, Function, Err, Reason]),
            handle_maybe_call_mfa_error(Module, Default, State)
    end.

handle_maybe_call_mfa_error(Module, Default, #state{modules = ModuleMap0} = State0) ->
    ModuleMap1 = maps:put(Module, false, ModuleMap0),
    State1 = State0#state{modules = ModuleMap1},
    {ok, Default, State1}.
