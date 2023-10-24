%%
%% %CopyrightBegin%
%%
%% Copyright Ericsson AB 2001-2011. All Rights Reserved.
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
%% =====================================================================
%% Ordered Sets implemented as General Balanced Trees
%%
%% Copyright (C) 1999-2001 Richard Carlsson
%%
%% An implementation of ordered sets using Prof. Arne Andersson's
%% General Balanced Trees. This can be much more efficient than using
%% ordered lists, for larger sets, but depends on the application. See
%% notes below for details.
%% ---------------------------------------------------------------------
%% Notes:
%%
%% The complexity on set operations is bounded by either O(|S|) or O(|T|
%% * log(|S|)), where S is the largest given set, depending on which is
%% fastest for any particular function call. For operating on sets of
%% almost equal size, this implementation is about 3 times slower than
%% using ordered-list sets directly. For sets of very different sizes,
%% however, this solution can be arbitrarily much faster; in practical
%% cases, often between 10 and 100 times. This implementation is
%% particularly suited for ackumulating elements a few at a time,
%% building up a large set (more than 100-200 elements), and repeatedly
%% testing for membership in the current set.
%%
%% As with normal tree structures, lookup (membership testing),
%% insertion and deletion have logarithmic complexity.
%%
%% Operations:
%%
%% - empty(): returns empty set.
%%
%%   Alias: new(), for compatibility with `sets'.
%%
%% - is_empty(S): returns 'true' if S is an empty set, and 'false'
%%   otherwise.
%%
%% - size(S): returns the number of nodes in the set as an integer.
%%   Returns 0 (zero) if the set is empty.
%%
%% - singleton(X): returns a set containing only the element X.
%%
%% - is_member(X, S): returns `true' if element X is a member of set S,
%%   and `false' otherwise.
%%
%%   Alias: is_element(), for compatibility with `sets'.
%%
%% - insert(X, S): inserts element X into set S; returns the new set.
%%   *Assumes that the element is not present in S.*
%%
%% - add(X, S): adds element X to set S; returns the new set. If X is
%%   already an element in S, nothing is changed.
%%
%%   Alias: add_element(), for compatibility with `sets'.
%%
%% - delete(X, S): removes element X from set S; returns new set.
%%   Assumes that the element exists in the set.
%%
%% - delete_any(X, S): removes key X from set S if the key is present
%%   in the set, otherwise does nothing; returns new set.
%%
%%   Alias: del_element(), for compatibility with `sets'.
%%
%% - balance(S): rebalances the tree representation of S. Note that this
%%   is rarely necessary, but may be motivated when a large number of
%%   elements have been deleted from the tree without further
%%   insertions. Rebalancing could then be forced in order to minimise
%%   lookup times, since deletion only does not rebalance the tree.
%%
%% - union(S1, S2): returns a new set that contains each element that is
%%   in either S1 or S2 or both, and no other elements.
%%
%% - union(Ss): returns a new set that contains each element that is in
%%   at least one of the sets in the list Ss, and no other elements.
%%
%% - intersection(S1, S2): returns a new set that contains each element
%%   that is in both S1 and S2, and no other elements.
%%
%% - intersection(Ss): returns a new set that contains each element that
%%   is in all of the sets in the list Ss, and no other elements.
%%
%% - is_disjoint(S1, S2): returns `true' if none of the elements in S1
%%   occurs in S2.
%%
%% - difference(S1, S2): returns a new set that contains each element in
%%   S1 that is not also in S2, and no other elements.
%%
%%   Alias: subtract(), for compatibility with `sets'.
%%
%% - is_subset(S1, S2): returns `true' if each element in S1 is also a
%%   member of S2, and `false' otherwise.
%%
%% - to_list(S): returns an ordered list of all elements in set S. The
%%   list never contains duplicates.
%%
%% - from_list(List): creates a set containing all elements in List,
%%   where List may be unordered and contain duplicates.
%%
%% - from_ordset(L): turns an ordered-set list L into a set. The list
%%   must not contain duplicates.
%%
%% - smallest(S): returns the smallest element in set S. Assumes that
%%   the set S is nonempty.
%%
%% - largest(S): returns the largest element in set S. Assumes that the
%%   set S is nonempty.
%%
%% - take_smallest(S): returns {X, S1}, where X is the smallest element
%%   in set S, and S1 is the set S with element X deleted. Assumes that
%%   the set S is nonempty.
%%
%% - take_largest(S): returns {X, S1}, where X is the largest element in
%%   set S, and S1 is the set S with element X deleted. Assumes that the
%%   set S is nonempty.
%%
%% - iterator(S): returns an iterator that can be used for traversing
%%   the entries of set S; see `next'. The implementation of this is
%%   very efficient; traversing the whole set using `next' is only
%%   slightly slower than getting the list of all elements using
%%   `to_list' and traversing that. The main advantage of the iterator
%%   approach is that it does not require the complete list of all
%%   elements to be built in memory at one time.
%%
%% - next(T): returns {X, T1} where X is the smallest element referred
%%   to by the iterator T, and T1 is the new iterator to be used for
%%   traversing the remaining elements, or the atom `none' if no
%%   elements remain.
%%
%% - filter(P, S): Filters set S using predicate function P. Included
%%   for compatibility with `sets'.
%%
%% - fold(F, A, S): Folds function F over set S with A as the initial
%%   ackumulator. Included for compatibility with `sets'.
%%
%% - is_set(S): returns 'true' if S appears to be a set, and 'false'
%%   otherwise. Not recommended; included for compatibility with `sets'.

-module(gb_sets_test).

-export([empty/0, is_empty/ 1 , size/1 , singleton/1, is_member/ 2 ,
       insert/ 2 , add/2 , delete/2, delete_any/ 2 , balance/1 , union/2,
       union/ 1 , intersection/2 , intersection/1, is_disjoint/ 2 , difference/2 ,
       is_subset/ 2 , to_list/1 , from_list/1, from_ordset/ 1 , smallest/1 ,
       largest/ 1 , take_smallest/1 , take_largest/1, iterator/ 1 , next/1 ,
       filter/ 2 , fold/3 , is_set/1]).

%% `sets' compatibility aliases:

-export([new/0, is_element/ 2 , add_element/2 , del_element/2,
       subtract/ 2 ]).

%% GB-trees adapted from Sven-Olof Nystr�m's implementation for
%% representation of sets.
%%
%% Data structures:
%% - {Size, Tree}, where `Tree' is composed of nodes of the form:
%% - {Key, Smaller, Bigger}, and the "empty tree" node:
%% - nil.
%%
%% No attempt is made to balance trees after deletions. Since deletions
%% don't increase the height of a tree, this should be OK.
%%
%% Original balance condition h(T) <= ceil(c * log(|T|)) has been
%% changed to the similar (but not quite equivalent) condition 2 ^ h(T)
%% <= |T| ^ c. This should also be OK.
%%
%% Behaviour is logarithmic (as it should be).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Some macros.

-define(p, 2). % It seems that p = 2 is optimal for sorted keys

-define(pow(A, _), A * A). % correct with exponent as defined above.

-define(div2(X), X bsr 1 ).                       %% 对X值除以2

-define(mul2(X), X bsl 1 ).                       %% 对X值乘以2

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Some types.

-type gb_set_node() :: 'nil' | {term(), _ , _ }.
-opaque iter() :: [gb_set_node()].

%% A declaration equivalent to the following is currently hard-coded
%% in erl_types.erl
%%
%% -opaque gb_set() :: {non_neg_integer(), gb_set_node()}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec empty() -> Set when
      Set :: gb_set().
%% 创建gb_sets空的数据结构
empty() ->
      { 0 , nil}.

-spec new() -> Set when
      Set :: gb_set().
%% 创建gb_sets空的数据结构
new() -> empty().

-spec is_empty( Set ) -> boolean() when
      Set :: gb_set().
%% 判断gb_sets数据结构是否为空
is_empty({0, nil}) ->
      true;

is_empty(_) ->
      false.

-spec size( Set ) -> non_neg_integer() when
      Set :: gb_set().
%% 得到gb_sets数据结构的大小
size({Size, _}) ->
       Size .

-spec singleton( Element ) -> gb_set() when
      Element :: term().
%% 创建一个gb_sets结构的单例，即只有一个元素的gb_sets数据结构
singleton(Key) ->
      { 1 , {Key , nil, nil}}.

-spec is_element( Element , Set ) -> boolean() when
      Element :: term(),
      Set :: gb_set().
%% 判断Key是否是gb_sets结构中的元素
is_element(Key, S) ->
      is_member( Key , S ).

-spec is_member( Element , Set ) -> boolean() when
      Element :: term(),
      Set :: gb_set().
%% 判断Key是否是gb_sets结构中的元素
is_member(Key, {_, T}) ->
      is_member_1( Key , T ).


%% 判断当前Key是否gb_sets结构中的元素
is_member_1(Key, {Key1, Smaller, _ }) when Key < Key1 ->
      is_member_1( Key , Smaller );

is_member_1(Key, {Key1, _, Bigger}) when Key > Key1 ->
      is_member_1( Key , Bigger );

is_member_1(_, {_, _, _}) ->
      true;

is_member_1(_, nil) ->
      false.

-spec insert( Element , Set1 ) -> Set2 when
      Element :: term(),
      Set1 :: gb_set(),
      Set2 :: gb_set().
%% 向gb_sets结构中插入Key元素
insert(Key, {S, T}) ->
       %% 得到最新的数据元素个数
       S1 = S + 1,
       %% 传入的值为节点数 * 节点数
      { S1 , insert_1(Key , T, ?pow(S1, ?p))}.


%% 此处是要插入的Key小于当前节点的Key1
insert_1(Key, {Key1, Smaller, Bigger }, S ) when Key < Key1 ->
       case insert_1(Key , Smaller, ?div2 (S )) of
            { T1 , H1 , S1} when is_integer( H1 ) ->
                   %% 成功的将Key对应的键值对插入到Key1对应节点的左子树下，组装该节点对应的平衡二叉树
                   T = {Key1 , T1, Bigger},
                   %% 得到右子树的高度和节点数
                  { H2 , S2 } = count(Bigger),
                   %% 将得到的左子树的高度和右子树高度的最大值乘以二
                   H = ?mul2 (erlang:max(H1, H2)),
                   %% 得到当前节点对应的二叉树对应的节点数
                   SS = S1 + S2 + 1,
                   %% 得到当前节点的节点数二次方值
                   P = ?pow (SS, ?p),
                   if
                         %% 如果当前节点对应的二叉树的2^高度 * 2 大于 当前节点数的二次方值则对该节点进行重新平衡操作
                         H > P ->
                               %% 平衡Key1该节点的左右子树
                              balance( T , SS );
                        true ->
                               %% 否则返回当前节点对应的二叉树以及当前二叉树的2^高度以及节点数
                              { T , H , SS}
                   end ;
             T1 ->
                   %% 成功的将Key对应的键值对插入到Key1对应节点的左子树下
                  { Key1 , T1 , Bigger}
       end ;

%% 此处是要插入的Key大于当前节点的Key1
insert_1(Key, {Key1, Smaller, Bigger }, S ) when Key > Key1 ->
       case insert_1(Key , Bigger, ?div2 (S )) of
            { T1 , H1 , S1} when is_integer( H1 ) ->
                   %% 成功的将Key对应的键值对插入到Key1对应节点的左子树下，组装该节点对应的平衡二叉树
                   T = {Key1 , Smaller, T1 },
                   %% 得到左子树的高度和节点数
                  { H2 , S2 } = count(Smaller),
                   %% 将得到的左子树的高度和右子树高度的最大值乘以二
                   H = ?mul2 (erlang:max(H1, H2)),
                   %% 得到当前节点对应的二叉树对应的节点数
                   SS = S1 + S2 + 1,
                   %% 得到当前节点的节点数二次方值
                   P = ?pow (SS, ?p),
                   if
                         %% 如果当前节点对应的二叉树的2^高度 * 2 大于 当前节点数的二次方值则对该节点进行重新平衡操作
                         H > P ->
                               %% 平衡Key1该节点的左右子树
                              balance( T , SS );
                        true ->
                               %% 否则返回当前节点对应的二叉树以及当前二叉树的2^高度以及节点数
                              { T , H , SS}
                   end ;
             T1 ->
                   %% 成功的将Key对应的键值对插入到Key1对应节点的右子树下
                  { Key1 , Smaller , T1}
       end ;

%% 如果S为0同时要插入的二叉树为nil，则直接返回该节点值以及高度和节点数
insert_1(Key, nil, 0 ) ->
      {{ Key , nil, nil}, 1 , 1};

%% S不为0则不进行重新平衡操作，直接返回节点二叉树
insert_1(Key, nil, _ ) ->
      { Key , nil, nil};

insert_1(Key, _, _) ->
      erlang:error({key_exists, Key }).


%% 得到gb_trees平衡二叉树的2^平衡二叉树的高度以及平衡二叉树的key-value键值对的数量
%% 例如平衡二叉树的高度是5，键值对数为7，则返回{2^5 = 32, 7}
count({_, nil, nil}) ->
      { 1 , 1 };

count({_, Sm, Bi}) ->
      { H1 , S1 } = count(Sm),
      { H2 , S2 } = count(Bi),
      { ?mul2 (erlang:max(H1 , H2)), S1 + S2 + 1};

count(nil) ->
      { 1 , 0 }.

-spec balance( Set1 ) -> Set2 when
      Set1 :: gb_set(),
      Set2 :: gb_set().
%% 对gb_sets数据结构进行重新平衡操作
balance({S, T}) ->
      { S , balance(T , S)}.


balance(T, S) ->
      balance_list(to_list_1( T ), S ).


balance_list(L, S) ->
      { T , _ } = balance_list_1(L, S),
       T .


balance_list_1(L, S) when S > 1 ->
       %% 减一表示父节点
       Sm = S - 1,
       %% 整个节点数减一后整除2后得到右子树的节点数
       S2 = Sm div 2 ,
       %% 剩下的节点表示左子树的节点数
       S1 = Sm - S2,
       %% 首先根据左子树的节点数S1创建左子树，返回后得到左子树，同时拿到当前节点的key-value键值对
      { T1 , [K | L1]} = balance_list_1( L , S1 ),
       %% 除去该节点的key-value键值对后，然后根据右子树的节点数和剩下的节点数创建该节点的右子树
      { T2 , L2 } = balance_list_1(L1, S2),
       T = {K , T1, T2},
      { T , L2 };

%% 在平分划分后的节点数为一的时候则直接创建节点(即表示该节点只有一个节点，则当前节点没有左右子节点，则直接同过该节点创建二叉树，虽然该二叉树只有一个节点)
balance_list_1([Key | L], 1) ->
      {{ Key , nil, nil}, L };

%% 在平分划分后的节点数为零的时候表示已经没有节点(表示该节点没有一个节点，所以直接返回nil)
balance_list_1(L, 0) ->
      {nil, L }.

-spec add_element( Element , Set1 ) -> Set2 when
      Element :: term(),
      Set1 :: gb_set(),
      Set2 :: gb_set().
%% gb_sets数据结构进行增加元素的操作
add_element(X, S) ->
      add( X , S ).

-spec add( Element , Set1 ) -> Set2 when
      Element :: term(),
      Set1 :: gb_set(),
      Set2 :: gb_set().
%% gb_sets数据结构进行增加元素的操作
add(X, S) ->
       %% 先判断当前X元素是否存在于gb_sets数据结构中
       case is_member(X , S) of
            true ->
                   S ;    % we don't have to do anything here
            false ->
                   %% 如果X元素没有存在于gb_sets结构中，则将该元素插入gb_sets数据结构中
                  insert( X , S )
       end .

-spec from_list( List ) -> Set when
      List :: [term()],
      Set :: gb_set().
%% 通过列表创建gb_sets数据结构
from_list(L) ->
      from_ordset(ordsets:from_list( L )).

-spec from_ordset( List ) -> Set when
      List :: [term()],
      Set :: gb_set().
%% 通过ordsets数据结构生成新的gb_sets数据结构
from_ordset(L) ->
       S = length(L ),
      { S , balance_list(L , S)}.

-spec del_element( Element , Set1 ) -> Set2 when
      Element :: term(),
      Set1 :: gb_set(),
      Set2 :: gb_set().
%% 删除gb_sets中的元素
del_element(Key, S) ->
      delete_any( Key , S ).

-spec delete_any( Element , Set1 ) -> Set2 when
      Element :: term(),
      Set1 :: gb_set(),
      Set2 :: gb_set().
%% 删除gb_sets中Key这个元素，如果没有存在于gb_sets中，则不进行任何操作，直接返回gb_sets结构
delete_any(Key, S) ->
       %% 先判断当前X元素是否存在于gb_sets数据结构中
       case is_member(Key , S) of
            true ->
                   %% 没有存在于gb_sets结构中，则进行删除操作
                  delete( Key , S );
            false ->
                   S
       end .

-spec delete( Element , Set1 ) -> Set2 when
      Element :: term(),
      Set1 :: gb_set(),
      Set2 :: gb_set().
%% 删除Key这个在gb_sets中的的元素
delete(Key, {S, T}) ->
      { S - 1 , delete_1(Key, T)}.


%% gb_sets结构中实际删除Key元素的操作函数
delete_1(Key, {Key1, Smaller, Larger }) when Key < Key1 ->
       %% 删除Key1节点左子树中Key
       Smaller1 = delete_1(Key , Smaller),
      { Key1 , Smaller1 , Larger};

delete_1(Key, {Key1, Smaller, Bigger }) when Key > Key1 ->
       %% 删除Key1节点右子树中Key
       Bigger1 = delete_1(Key , Bigger),
      { Key1 , Smaller , Bigger1};

delete_1(_, {_, Smaller, Larger }) ->
      merge( Smaller , Larger ).


%% gb_trees平衡二叉树的节点删除后，需要根据它的左右子树组装成一个新的平衡二叉树(此处的方法是得到右子树的最小节点做为该左右子树的父节点)
merge(Smaller, nil) ->
       Smaller ;

merge(nil, Larger) ->
       Larger ;

merge(Smaller, Larger ) ->
      { Key , Larger1 } = take_smallest1(Larger),
      { Key , Smaller , Larger1}.

-spec take_smallest( Set1 ) -> {Element, Set2 } when
      Set1 :: gb_set(),
      Set2 :: gb_set(),
      Element :: term().
%% 从gb_sets数据结构中取得元素值最小的元素
take_smallest({S, T}) ->
      { Key , Larger } = take_smallest1(T),
      { Key , {S - 1, Larger}}.


%% 实际取最小元素的操作函数
take_smallest1({Key, nil, Larger }) ->
       %% 取得了最小值的元素后，将该节点对应的右子树作为上一个节点的左子树
      { Key , Larger };

take_smallest1({Key, Smaller, Larger }) ->
      { Key1 , Smaller1 } = take_smallest1(Smaller),
      { Key1 , {Key , Smaller1, Larger }}.

-spec smallest( Set ) -> term() when
      Set :: gb_set().
%% 从gb_sets数据结构中得到最小值元素，但是不将该节点元素从平衡二叉树中删除掉
smallest({_, T}) ->
      smallest_1( T ).


smallest_1({Key, nil, _Larger }) ->
       Key ;

smallest_1({_Key, Smaller, _Larger }) ->
      smallest_1( Smaller ).

-spec take_largest( Set1 ) -> {Element, Set2 } when
      Set1 :: gb_set(),
      Set2 :: gb_set(),
      Element :: term().
%% 从gb_sets数据结构中取得最大值的元素，然后将该值从平衡二叉树中删除掉
take_largest({S, T}) ->
      { Key , Smaller } = take_largest1(T),
      { Key , {S - 1, Smaller}}.


%% 在此处删除最大值元素对应的节点后，将该节点的左子树作为上一个节点的右子树
take_largest1({Key, Smaller, nil}) ->
      { Key , Smaller };

take_largest1({Key, Smaller, Larger }) ->
      { Key1 , Larger1 } = take_largest1(Larger),
      { Key1 , {Key , Smaller, Larger1 }}.

-spec largest( Set ) -> term() when
      Set :: gb_set().
%% 取得gb_sets数据结构的最大值，但是不将该节点从gb_sets结构中删除掉
largest({_, T}) ->
      largest_1( T ).

largest_1({Key, _Smaller, nil}) ->
       Key ;

largest_1({_Key, _Smaller, Larger }) ->
      largest_1( Larger ).

-spec to_list( Set ) -> List when
      Set :: gb_set(),
      List :: [term()].
%% 将gb_sets数据结构中的元素转化为列表形式
to_list({_, T}) ->
      to_list( T , []).


to_list_1(T) -> to_list( T , []).


to_list({Key, Small, Big}, L) ->
      to_list( Small , [Key | to_list(Big, L)]);

to_list(nil, L) -> L.

-spec iterator( Set ) -> Iter when
      Set :: gb_set(),
      Iter :: iter().
%% 得到gb_sets平衡二叉树的迭代器(通过迭代器遍历平衡二叉树，实际上是通过先序遍历二叉树的方式遍历gb_sets)
iterator({_, T}) ->
      iterator( T , []).

%% The iterator structure is really just a list corresponding to the
%% call stack of an in-order traversal. This is quite fast.

iterator({_, nil, _ } = T , As) ->
      [ T | As ];

iterator({_, L, _} = T, As) ->
      iterator( L , [T | As]);

iterator(nil, As) ->
       As .

-spec next( Iter1 ) -> {Element, Iter2 } | 'none' when
      Iter1 :: iter(),
      Iter2 :: iter(),
      Element :: term().
%% 使用iterator接口得到的迭代器进行遍历平衡二叉树，该接口是得到平衡二叉树的下一个元素
next([{X, _, T} | As]) ->
      { X , iterator(T , As)};

next([]) ->
      none.


%% Set operations:


%% If |X| < |Y|, then we traverse the elements of X. The cost for
%% testing a single random element for membership in a tree S is
%% proportional to log(|S|); thus, if |Y| / |X| < c * log(|Y|), for some
%% c, it is more efficient to scan the ordered sequence of elements of Y
%% while traversing X (under the same ordering) in order to test whether
%% elements of X are already in Y. Since the `math' module does not have
%% a `log2'-function, we rewrite the condition to |X| < |Y| * c1 *
%% ln(|X|), where c1 = c / ln 2.

-define(c, 1.46).    % 1 / ln 2; this appears to be best

%% If the sets are not very different in size, i.e., if |Y| / |X| >= c *
%% log(|Y|), then the fastest way to do union (and the other similar set
%% operations) is to build the lists of elements, traverse these lists
%% in parallel while building a reversed ackumulator list, and finally
%% rebuild the tree directly from the ackumulator. Other methods of
%% traversing the elements can be devised, but they all have higher
%% overhead.

-spec union( Set1 , Set2 ) -> Set3 when
      Set1 :: gb_set(),
      Set2 :: gb_set(),
      Set3 :: gb_set().
%% 求两个gb_sets数据结构的合集
union({N1, T1}, {N2, T2}) when N2 < N1 ->
      union(to_list_1( T2 ), N2 , T1, N1);

union({N1, T1}, {N2, T2}) ->
      union(to_list_1( T1 ), N1 , T2, N2).

%% We avoid the expensive mathematical(数学) computations(计算) if there is little
%% chance at saving at least the same amount of time by making the right
%% choice of strategy. Recall that N1 < N2 here.
%% gb_sets数据结构实际合并的操作
union(L, N1, T2, N2) when N2 < 10 ->
       %% Break even is about 7 for N1 = 1 and 10 for N1 = 2
      union_2( L , to_list_1(T2 ), N1 + N2);

union(L, N1, T2, N2) ->
       X = N1 * round(?c * math:log( N2 )),
       if N2 < X ->
               %% 将T2gb_sets数据结构展开的代价较小
               union_2( L , to_list_1(T2 ), N1 + N2);
         true ->
               %% 不必将T2gb_sets数据结构展开，直接将T1中的数据插入到T2，由插入操作去避免重复操作，N2 >= X的情况下次操作代价较小
               union_1( L , mk_set(N2 , T2))
       end .

-spec mk_set(non_neg_integer(), gb_set_node()) -> gb_set().
%% 通过平衡二叉树以及元素的个数组装gb_sets数据结构
mk_set(N, T) ->
      { N , T }.

%% If the length of the list is in proportion with the size of the
%% target set, this version spends too much time doing lookups, compared
%% to the below version.
%% 不必将T2gb_sets数据结构展开，直接将T1中的数据插入到T2，由插入操作去避免重复操作
union_1([X | Xs], S) ->
      union_1( Xs , add(X , S));

union_1([], S) ->
       S .


%% If the length of the first list is too small in comparison with the
%% size of the target set, this version spends too much time scanning
%% the element list of the target set for possible membership, compared
%% with the above version.

%% Some notes on sequential scanning of ordered lists
%%
%% 1) We want to put the equality case last, if we can assume that the
%% probability for overlapping elements is relatively low on average.
%% Doing this also allows us to completely skip the (arithmetic)
%% equality test, since the term order is arithmetically total.
%%
%% 2) We always test for `smaller than' first, i.e., whether the head of
%% the left list is smaller than the head of the right list, and if the
%% `greater than' test should instead turn out to be true, we switch
%% left and right arguments in the recursive call under the assumption
%% that the same is likely to apply to the next element also,
%% statistically reducing the number of failed tests and automatically
%% adapting to cases of lists having very different lengths. This saves
%% 10-40% of the traversation time compared to a "fixed" strategy,
%% depending on the sizes and contents of the lists.
%%
%% 3) A tail recursive version using `lists:reverse/2' is about 5-10%
%% faster than a plain recursive version using the stack, for lists of
%% more than about 20 elements and small stack frames. For very short
%% lists, however (length < 10), the stack version can be several times
%% faster. As stack frames grow larger, the advantages of using
%% `reverse' could get greater.
%% 实际的求合集的接口，S表示两个gb_sets元素的总大小(将要合并的两个gb_sets结构都展开进行合并的操作)
union_2(Xs, Ys, S) ->
      union_2( Xs , Ys , [], S).    % S is the sum of the sizes here


union_2([X | Xs1], [Y | _] = Ys, As, S) when X < Y ->
      union_2( Xs1 , Ys , [X | As], S);

union_2([X | _] = Xs, [Y | Ys1], As, S) when X > Y ->
      union_2( Ys1 , Xs , [Y | As], S);

%% 将相同的元素的个数减去，只得到一个元素
union_2([X | Xs1], [_ | Ys1], As, S) ->
      union_2( Xs1 , Ys1 , [X | As], S - 1);

union_2([], Ys, As, S) ->
      { S , balance_revlist(push(Ys , As), S)};

union_2(Xs, [], As , S ) ->
      { S , balance_revlist(push(Xs , As), S)}.


push([X | Xs], As) ->
      push( Xs , [X | As]);

push([], As) ->
       As .


%% 将L列表进行冲新平衡得到一个新的gb_sets数据结构
balance_revlist(L, S) ->
      { T , _ } = balance_revlist_1(L, S),
       T .


balance_revlist_1(L, S) when S > 1 ->
       %% 减一表示父节点
       Sm = S - 1,
       %% 整个节点数减一后整除2后得到右子树的节点数
       S2 = Sm div 2 ,
       %% 剩下的节点表示左子树的节点数
       S1 = Sm - S2,
       %% 首先根据左子树的节点数S1创建左子树，返回后得到左子树，同时拿到当前节点的元素
      { T2 , [K | L1]} = balance_revlist_1( L , S1 ),
       %% 除去该节点的值对后，然后根据右子树的节点数和剩下的节点数创建该节点的右子树
      { T1 , L2 } = balance_revlist_1(L1, S2),
       T = {K , T1, T2},
      { T , L2 };

%% 在平分划分后的节点数为一的时候则直接创建节点(即表示该节点只有一个节点，则当前节点没有左右子节点，则直接同过该节点创建二叉树，虽然该二叉树只有一个节点)
balance_revlist_1([Key | L], 1) ->
      {{ Key , nil, nil}, L };

%% 在平分划分后的节点数为零的时候表示已经没有节点(表示该节点没有一个节点，所以直接返回nil)
balance_revlist_1(L, 0) ->
      {nil, L }.

-spec union( SetList ) -> Set when
      SetList :: [gb_set(),...],
      Set :: gb_set().
%% 对多个gb_sets数据结构进行合并操作
union([S | Ss]) ->
      union_list( S , Ss );

union([]) -> empty().


%% 实际的合并多个gb_sets数据结构的函数
union_list(S, [S1 | Ss]) ->
      union_list(union( S , S1 ), Ss);

union_list(S, []) -> S.


%% The rest is modelled on the above.

-spec intersection( Set1 , Set2 ) -> Set3 when
      Set1 :: gb_set(),
      Set2 :: gb_set(),
      Set3 :: gb_set().
%% 对两个gb_sets数据结构求交集的操作
intersection({N1, T1}, {N2, T2}) when N2 < N1 ->
      intersection(to_list_1( T2 ), N2 , T1, N1);

intersection({N1, T1}, {N2, T2}) ->
      intersection(to_list_1( T1 ), N1 , T2, N2).


%% T2小于10个元素则直接将T2展开进行求交集的操作
intersection(L, _N1, T2, N2) when N2 < 10 ->
      intersection_2( L , to_list_1(T2 ));

%% 如果T2的元素个数小于阀值则将T2展开进行求交集的操作，否则通过is_memeber操作来求交集的操作
intersection(L, N1, T2, N2) ->
       X = N1 * round(?c * math:log( N2 )),
       if N2 < X ->
               %% 将T2展开进行求交集的操作
               intersection_2( L , to_list_1(T2 ));
         true ->
               %% 通过is_memeber操作来求交集的操作
               intersection_1( L , T2 )
       end .

%% We collect the intersecting elements in an accumulator list and count
%% them at the same time so we can balance the list afterwards.
%% 通过is_memeber操作来求交集的操作
intersection_1(Xs, T) ->
      intersection_1( Xs , T , [], 0).


%% 实际通过is_memeber接口来求交集的操作函数
intersection_1([X | Xs], T, As, N) ->
       case is_member_1(X , T) of
            true ->
                  intersection_1( Xs , T , [X | As], N + 1);
            false ->
                  intersection_1( Xs , T , As, N)
       end ;

intersection_1([], _, As, N) ->
       %% 将得到的交集组装成一个gb_sets数据结构
      { N , balance_revlist(As , N)}.


%% 将T2展开进行求交集的操作
intersection_2(Xs, Ys) ->
      intersection_2( Xs , Ys , [], 0).


intersection_2([X | Xs1], [Y | _] = Ys, As, S) when X < Y ->
      intersection_2( Xs1 , Ys , As, S);

intersection_2([X | _] = Xs, [Y | Ys1], As, S) when X > Y ->
      intersection_2( Ys1 , Xs , As, S);

intersection_2([X | Xs1], [_ | Ys1], As, S) ->
      intersection_2( Xs1 , Ys1 , [X | As], S + 1);

intersection_2([], _, As, S) ->
      { S , balance_revlist(As , S)};

intersection_2(_, [], As, S) ->
      { S , balance_revlist(As , S)}.

-spec intersection( SetList ) -> Set when
      SetList :: [gb_set(),...],
      Set :: gb_set().
%% 对多个gb_sets数据结构进行求交集的操作
intersection([S | Ss]) ->
      intersection_list( S , Ss ).


%% 实际的对多个gb_sets结构求交集的操作
intersection_list(S, [S1 | Ss]) ->
      intersection_list(intersection( S , S1 ), Ss);

intersection_list(S, []) -> S.

-spec is_disjoint( Set1 , Set2 ) -> boolean() when
      Set1 :: gb_set(),
      Set2 :: gb_set().
%% 判断两个gb_sets结构是否有相同的元素
is_disjoint({N1, T1}, {N2, T2}) when N1 < N2 ->
      is_disjoint_1( T1 , T2 );

is_disjoint({_, T1}, {_, T2}) ->
      is_disjoint_1( T2 , T1 ).


is_disjoint_1({K1, Smaller1, Bigger }, {K2 , Smaller2, _ } = Tree ) when K1 < K2 ->
       not is_member_1(K1 , Smaller2) andalso
            is_disjoint_1( Smaller1 , Smaller2 ) andalso
            is_disjoint_1( Bigger , Tree );

is_disjoint_1({K1, Smaller, Bigger1 }, {K2 , _, Bigger2} = Tree ) when K1 > K2 ->
       not is_member_1(K1 , Bigger2) andalso
            is_disjoint_1( Bigger1 , Bigger2 ) andalso
            is_disjoint_1( Smaller , Tree );

is_disjoint_1({_K1, _, _}, {_K2, _, _}) ->       %K1 == K2
      false;

is_disjoint_1(nil, _) ->
      true;

is_disjoint_1(_, nil) ->
      true.

%% Note that difference is not symmetric. We don't use `delete' here,
%% since the GB-trees implementation does not rebalance after deletion
%% and so we could end up with very unbalanced trees indeed depending on
%% the sets. Therefore, we always build a new tree, and thus we need to
%% traverse the whole element list of the left operand.

-spec subtract( Set1 , Set2 ) -> Set3 when
      Set1 :: gb_set(),
      Set2 :: gb_set(),
      Set3 :: gb_set().
%% 求两个gb_sets数据结构的差集
subtract(S1, S2) ->
      difference( S1 , S2 ).

-spec difference( Set1 , Set2 ) -> Set3 when
      Set1 :: gb_set(),
      Set2 :: gb_set(),
      Set3 :: gb_set().

difference({N1, T1}, {N2, T2}) ->
      difference(to_list_1( T1 ), N1 , T2, N2).


%% 如果T2元素个数小于10则直接将T2直接展开求差集
difference(L, N1, T2, N2) when N2 < 10 ->
      difference_2( L , to_list_1(T2 ), N1);

%% 如果小于阀值，则直接将T2直接展开求差集，否则通过is_memeber接口来进行求差集
difference(L, N1, T2, N2) ->
       X = N1 * round(?c * math:log( N2 )),
       if N2 < X ->
               %% 直接将T2直接展开求差集
               difference_2( L , to_list_1(T2 ), N1);
         true ->
               %% 通过is_memeber接口来进行求差集
               difference_1( L , T2 )
       end .


%% 通过is_memeber接口来进行求差集
difference_1(Xs, T) ->
      difference_1( Xs , T , [], 0).


difference_1([X | Xs], T, As, N) ->
       case is_member_1(X , T) of
            true ->
                  difference_1( Xs , T , As, N);
            false ->
                  difference_1( Xs , T , [X | As], N + 1)
       end ;

difference_1([], _, As, N) ->
       %% 将得到的差集组装成一个gb_sets数据结构
      { N , balance_revlist(As , N)}.


%% 直接将T2直接展开求差集
difference_2(Xs, Ys, S) ->
      difference_2( Xs , Ys , [], S).    % S is the size of the left set


difference_2([X | Xs1], [Y | _] = Ys, As, S) when X < Y ->
      difference_2( Xs1 , Ys , [X | As], S);

difference_2([X | _] = Xs, [Y | Ys1], As, S) when X > Y ->
      difference_2( Xs , Ys1 , As, S);

difference_2([_X | Xs1], [_Y | Ys1], As, S) ->
      difference_2( Xs1 , Ys1 , As, S - 1);

difference_2([], _Ys, As, S) ->
      { S , balance_revlist(As , S)};

difference_2(Xs, [], As , S ) ->
       %% 将得到的差集组装成一个gb_sets数据结构
      { S , balance_revlist(push(Xs , As), S)}.


%% Subset testing is much the same thing as set difference, but
%% without the construction of a new set.

-spec is_subset( Set1 , Set2 ) -> boolean() when
      Set1 :: gb_set(),
      Set2 :: gb_set().
%% 判断第一个gb_sets中的元素是否存在于第二个gb_sets结构中
is_subset({N1, T1}, {N2, T2}) ->
      is_subset(to_list_1( T1 ), N1 , T2, N2).


%% 如果小于阀值，则直接将T2直接展开来判断，否则通过is_memeber接口来进行判断
is_subset(L, _N1, T2, N2) when N2 < 10 ->
      is_subset_2( L , to_list_1(T2 ));

is_subset(L, N1, T2, N2) ->
       X = N1 * round(?c * math:log( N2 )),
       if N2 < X ->
               %% 直接将T2直接展开来判断
               is_subset_2( L , to_list_1(T2 ));
         true ->
               %% 通过is_memeber接口来进行判断
               is_subset_1( L , T2 )
       end .


%% 通过is_memeber接口来进行判断
is_subset_1([X | Xs], T) ->
       case is_member_1(X , T) of
            true ->
                  is_subset_1( Xs , T );
            false ->
                  false
       end ;

is_subset_1([], _) ->
      true.


%% 直接将T2直接展开来判断
is_subset_2([X | _], [Y | _]) when X < Y ->
      false;

is_subset_2([X | _] = Xs, [Y | Ys1]) when X > Y ->
      is_subset_2( Xs , Ys1 );

is_subset_2([_ | Xs1], [_ | Ys1]) ->
      is_subset_2( Xs1 , Ys1 );

is_subset_2([], _) ->
      true;

is_subset_2(_, []) ->
      false.


%% For compatibility with `sets':

-spec is_set( Term ) -> boolean() when
      Term :: term().
%% 判断传入进来的数据结构是gb_sets数据结构
is_set({0, nil}) -> true;

is_set({N, {_, _, _}}) when is_integer( N ), N >= 0 -> true;

is_set(_) -> false.

-spec filter( Pred , Set1 ) -> Set2 when
      Pred :: fun ((E :: term()) -> boolean()),
      Set1 :: gb_set(),
      Set2 :: gb_set().
%% 对gb_sets数据结构进行过滤操作
filter(F, S) ->
      from_ordset([ X || X <- to_list(S), F(X)]).

-spec fold( Function , Acc0 , Set) -> Acc1 when
      Function :: fun ((E :: term(), AccIn) -> AccOut ),
      Acc0 :: term(),
      Acc1 :: term(),
      AccIn :: term(),
      AccOut :: term(),
      Set :: gb_set().
%% 对gb_sets数据机构进行foldl操作(是通过先序遍历进行foldl操作)
fold(F, A, {_, T}) when is_function( F , 2 ) ->
      fold_1( F , A , T).


fold_1(F, Acc0, {Key, Small, Big}) ->
       Acc1 = fold_1(F , Acc0, Small),
       Acc = F (Key, Acc1),
      fold_1( F , Acc , Big);

fold_1(_, Acc, _) ->
       Acc .

