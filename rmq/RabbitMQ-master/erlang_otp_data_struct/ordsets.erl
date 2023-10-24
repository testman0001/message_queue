%%
%% %CopyrightBegin%
%%
%% Copyright Ericsson AB 1996-2011. All Rights Reserved.
%%
%% The contents of this file are subject to the Erlang Public License,
%% Version 1.1, (the "License"); you may not use this file except in
%% compliance with the License. You should have received a copy of the
%% Erlang Public License along with this software. If not, it can be
%% retrieved online at http://www.erlang.org/.
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and limitations
%% under the License.
%%
%% %CopyrightEnd%
%%

-module(ordsets_test).

-export([new/0,is_set/ 1 ,size/1 ,to_list/1,from_list/ 1 ]).
-export([is_element/2,add_element/ 2 ,del_element/2 ]).
-export([union/2,union/ 1 ,intersection/2 ,intersection/1]).
-export([is_disjoint/2]).
-export([subtract/2,is_subset/ 2 ]).
-export([fold/3,filter/ 2 ]).

-export_type([ordset/1]).

-type ordset(T) :: [T].

%% new() -> Set.
%%  Return a new empty ordered set.

-spec new() -> [].
%% 创建新的ordsets数据结构
new() -> [].

%% is_set(Term) -> boolean().
%%  Return 'true' if Set is an ordered set of elements, else 'false'.

-spec is_set( Ordset ) -> boolean() when
      Ordset :: term().
%% 判断传入的数据是否是ordsets数据结构
is_set([E | Es]) -> is_set( Es , E );

is_set([]) -> true;

is_set(_) -> false.


is_set([E2 | Es], E1) when E1 < E2 ->
      is_set( Es , E2 );

is_set([_ | _], _) -> false;

is_set([], _) -> true.

%% size(OrdSet) -> int().
%%  Return the number of elements in OrdSet.

-spec size( Ordset ) -> non_neg_integer() when
      Ordset :: ordset( _ ).
%% 得到ordsets数据结构的长度
size(S) -> length( S ).

%% to_list(OrdSet) -> [Elem].
%%  Return the elements in OrdSet as a list.

-spec to_list( Ordset ) -> List when
      Ordset :: ordset( T ),
      List :: [ T ].
%% 将ordsets数据结构转化为列表形式
to_list(S) -> S.

%% from_list([Elem]) -> Set.
%%  Build an ordered set from the elements in List.

-spec from_list( List ) -> Ordset when
      List :: [ T ],
      Ordset :: ordset( T ).
%% 通过传入的列表形成ordsets数据结构
from_list(L) ->
      lists:usort( L ).

%% is_element(Element, OrdSet) -> boolean().
%%  Return 'true' if Element is an element of OrdSet, else 'false'.

-spec is_element( Element , Ordset ) -> boolean() when
      Element :: term(),
      Ordset :: ordset( _ ).
%% 判断传入的元素是否是ordsets数据结构中的元素
is_element(E, [H | Es]) when E > H -> is_element( E , Es );

is_element(E, [H | _]) when E < H -> false;

is_element(_E, [_H | _]) -> true;                %E == H

is_element(_, []) -> false.

%% add_element(Element, OrdSet) -> OrdSet.
%%  Return OrdSet with Element inserted in it.

-spec add_element( Element , Ordset1 ) -> Ordset2 when
      Element :: E ,
      Ordset1 :: ordset( T ),
      Ordset2 :: ordset( T | E ).

%-spec add_element(E, ordset(T)) -> [T | E,...].
%% 向ordsets数据结构中添加元素
add_element(E, [H | Es]) when E > H -> [H | add_element( E , Es )];

add_element(E, [H | _] = Set) when E < H -> [E | Set];

add_element(_E, [_H | _] = Set) -> Set;          %E == H

add_element(E, []) -> [E].

%% del_element(Element, OrdSet) -> OrdSet.
%%  Return OrdSet but with Element removed.

-spec del_element( Element , Ordset1 ) -> Ordset2 when
      Element :: term(),
      Ordset1 :: ordset( T ),
      Ordset2 :: ordset( T ).
%% 删除ordsets数据结构中的元素
del_element(E, [H | Es]) when E > H -> [H | del_element( E , Es )];

del_element(E, [H | _] = Set) when E < H -> Set;

del_element(_E, [_H | Es]) -> Es;                %E == H

del_element(_, []) -> [].

%% union(OrdSet1, OrdSet2) -> OrdSet
%%  Return the union of OrdSet1 and OrdSet2.

-spec union( Ordset1 , Ordset2 ) -> Ordset3 when
      Ordset1 :: ordset( T1 ),
      Ordset2 :: ordset( T2 ),
      Ordset3 :: ordset( T1 | T2 ).
%% 合并两个ordsets数据结构
union([E1 | Es1], [E2 | _] = Set2) when E1 < E2 ->
      [ E1 | union(Es1 , Set2)];

union([E1 | _] = Set1, [E2 | Es2]) when E1 > E2 ->
      [ E2 | union(Es2 , Set1)];                   % switch arguments!

union([E1 | Es1], [_E2 | Es2]) ->                %E1 == E2
      [ E1 | union(Es1 , Es2)];

union([], Es2) -> Es2;

union(Es1, []) -> Es1 .

%% union([OrdSet]) -> OrdSet
%%  Return the union of the list of ordered sets.

-spec union( OrdsetList ) -> Ordset when
      OrdsetList :: [ordset( T )],
      Ordset :: ordset( T ).
%% 合并多个ordsets数据结构
union([S1, S2 | Ss]) ->
      union1(union( S1 , S2 ), Ss);

union([S]) -> S;

union([]) -> [].

union1(S1, [S2 | Ss]) -> union1(union( S1 , S2 ), Ss);

union1(S1, []) -> S1 .

%% intersection(OrdSet1, OrdSet2) -> OrdSet.
%%  Return the intersection of OrdSet1 and OrdSet2.

-spec intersection( Ordset1 , Ordset2 ) -> Ordset3 when
      Ordset1 :: ordset( _ ),
      Ordset2 :: ordset( _ ),
      Ordset3 :: ordset( _ ).
%% 两个ordsets数据结构的合集
intersection([E1 | Es1], [E2 | _] = Set2) when E1 < E2 ->
      intersection( Es1 , Set2 );

intersection([E1 | _] = Set1, [E2 | Es2]) when E1 > E2 ->
      intersection( Es2 , Set1 );                   % switch arguments!

intersection([E1 | Es1], [_E2 | Es2]) ->         %E1 == E2
      [ E1 | intersection(Es1 , Es2)];

intersection([], _) ->
      [];

intersection(_, []) ->
      [].

%% intersection([OrdSet]) -> OrdSet.
%%  Return the intersection of the list of ordered sets.

-spec intersection( OrdsetList ) -> Ordset when
      OrdsetList :: [ordset( _ ),...],
      Ordset :: ordset( _ ).
%% 多个ordsets数据结构求合集
intersection([S1, S2 | Ss]) ->
      intersection1(intersection( S1 , S2 ), Ss);

intersection([S]) -> S.

intersection1(S1, [S2 | Ss]) ->
      intersection1(intersection( S1 , S2 ), Ss);

intersection1(S1, []) -> S1 .

%% is_disjoint(OrdSet1, OrdSet2) -> boolean().
%%  Check whether OrdSet1 and OrdSet2 are disjoint.

-spec is_disjoint( Ordset1 , Ordset2 ) -> boolean() when
      Ordset1 :: ordset( _ ),
      Ordset2 :: ordset( _ ).
%% 判断两个ordsets数据结构是否有相同的元素
is_disjoint([E1 | Es1], [E2 | _] = Set2) when E1 < E2 ->
      is_disjoint( Es1 , Set2 );

is_disjoint([E1 | _] = Set1, [E2 | Es2]) when E1 > E2 ->
      is_disjoint( Es2 , Set1 );              % switch arguments!

is_disjoint([_E1 | _Es1], [ _E2 | _Es2 ]) ->             %E1 == E2
      false;

is_disjoint([], _) ->
      true;

is_disjoint(_, []) ->
      true.

%% subtract(OrdSet1, OrdSet2) -> OrdSet.
%%  Return all and only the elements of OrdSet1 which are not also in
%%  OrdSet2.

-spec subtract( Ordset1 , Ordset2 ) -> Ordset3 when
      Ordset1 :: ordset( _ ),
      Ordset2 :: ordset( _ ),
      Ordset3 :: ordset( _ ).
%% 两个ordsets数据结构求差集
subtract([E1 | Es1], [E2 | _] = Set2) when E1 < E2 ->
    [E1 | subtract( Es1 , Set2 )];

subtract([E1 | _] = Set1, [E2 | Es2]) when E1 > E2 ->
    subtract(Set1, Es2);

subtract([_E1 | Es1], [_E2 | Es2]) ->            %E1 == E2
    subtract(Es1, Es2);

subtract([], _) -> [];

subtract(Es1, []) -> Es1 .

%% is_subset(OrdSet1, OrdSet2) -> boolean().
%%  Return 'true' when every element of OrdSet1 is also a member of
%%  OrdSet2, else 'false'.

-spec is_subset( Ordset1 , Ordset2 ) -> boolean() when
      Ordset1 :: ordset( _ ),
      Ordset2 :: ordset( _ ).
%% 判断传入的第一个ordsets数据结构中的每一个元素是否都在第二个ordsets数据结构中
is_subset([E1 | _], [E2 | _]) when E1 < E2 ->    %E1 not in Set2
      false;

is_subset([E1 | _] = Set1, [E2 | Es2]) when E1 > E2 ->
      is_subset( Set1 , Es2 );

is_subset([_E1 | Es1], [_E2 | Es2]) ->           %E1 == E2
      is_subset( Es1 , Es2 );

is_subset([], _) -> true;

is_subset(_, []) -> false.

%% fold(Fun, Accumulator, OrdSet) -> Accumulator.
%%  Fold function Fun over all elements in OrdSet and return Accumulator.

-spec fold( Function , Acc0 , Ordset) -> Acc1 when
      Function :: fun ((Element :: T, AccIn :: term()) -> AccOut :: term()),
      Ordset :: ordset( T ),
      Acc0 :: term(),
      Acc1 :: term().
%% 对ordsets数据结构执行foldl操作
fold(F, Acc, Set) ->
      lists:foldl( F , Acc , Set).

%% filter(Fun, OrdSet) -> OrdSet.
%%  Filter OrdSet with Fun.

-spec filter( Pred , Ordset1 ) -> Ordset2 when
      Pred :: fun ((Element :: T) -> boolean()),
      Ordset1 :: ordset( T ),
      Ordset2 :: ordset( T ).
%% 对ordsets数据结构执行过滤操作
filter(F, Set) ->
      lists:filter( F , Set ).

