### 1. 启动流程

rabbit_networking.erl:boot() --> start_tcp_listener() --> supervisor:start_child(rabbit_sup, Spec)



rabbit_reader->init() -->start_connection()

![img](https://pic1.xuehuaimg.com/proxy/csdn/https://img-blog.csdn.net/20180112212000246?watermark/2/text/aHR0cDovL2Jsb2cuY3Nkbi5uZXQvaG5jc2N3Yw==/font/5a6L5L2T/fontsize/400/fill/I0JBQkFCMA==/dissolve/70/gravity/SouthEast)

### 2. publish流程

rabbit_reader:

start_connection() --> system_continue() --> mainloop() --> recvloop() --> handle_input() --> handle_frame() -->

 --> process_frame() --> rabbit_channel:do_flow() --> gen_server2:cast()

--> handle_method0 --> rabbit_channel:handle_method() -->

handle_method(#'basic.publish'{exchange   = ExchangeNameBin,

​                routing_key = RoutingKey,

​                mandatory  = Mandatory},

1. 检查exchange 可写。 check_write_permitted(ExchangeName, User),

2. 查找exchange是否存在。 Exchange = rabbit_exchange:lookup_or_die(ExchangeName),

3. 检查是否是internal exchange。 check_internal_exchange(Exchange),

4. 检查当前用户是否允许publish到这个topic。 check_write_permitted_on_topic(Exchange, User, ConnPid, RoutingKey, ChSrc),

5. 检查消息ttl 是否超时。 check_expiration_header(Props),

   **查找rk对应的queue和exchange**

--> rabbit_exchange:route()

​	**trace point**

​	判断channel是否出于commiting过程中，如果是，消息不路由到queue或exchange，而是直接放入channel的queue

```erlang
{noreply, case Tx of
​             none     -> deliver_to_queues(DQ, State1);
​             {Msgs, Acks} -> Msgs1 = queue:in(DQ, Msgs),
​                     State1#ch{tx = {Msgs1, Acks}}
​           end};
```

deliver to queue() --> rabbit_amqqueue:deliver()

```erlang
deliver(Qs, Delivery = #delivery{flow = Flow}) ->
	%% 查找queue的master和slave pid信息
    {MPids, SPids} = qpids(Qs),
    QPids = MPids ++ SPids,
    %% We use up two credits to send to a slave since the message
    %% arrives at the slave from two directions. We will ack one when
    %% the slave receives the message direct from the channel, and the
    %% other when it receives it via GM.
    case Flow of
        %% Here we are tracking messages sent by the rabbit_channel
        %% process. We are accessing the rabbit_channel process
        %% dictionary.
        flow   -> [credit_flow:send(QPid) || QPid <- QPids],
                  [credit_flow:send(QPid) || QPid <- SPids];
        noflow -> ok
    end,

    %% We let slaves know that they were being addressed as slaves at
    %% the time - if they receive such a message from the channel
    %% after they have become master they should mark the message as
    %% 'delivered' since they do not know what the master may have
    %% done with it.
    MMsg = {deliver, Delivery, false},
    SMsg = {deliver, Delivery, true},
    %% 发送到队列对应的pid, 最终被rabbit_amqqueue_process handle_cast/2 接收处理
    delegate:invoke_no_result(MPids, {gen_server2, cast, [MMsg]}),
    delegate:invoke_no_result(SPids, {gen_server2, cast, [SMsg]}),
    QPids.
```

rabbit_amqqueue_process:handle_cast/2

```erlang
handle_cast({deliver,
                Delivery = #delivery{sender = Sender,
                                     flow   = Flow},
                SlaveWhenPublished},
            State = #q{senders = Senders}) ->
    Senders1 = case Flow of
    %% In both credit_flow:ack/1 we are acking messages to the channel
    %% process that sent us the message delivery. See handle_ch_down
    %% for more info.
                   %% ack rabbit_amqqueue:deliver()的credit_flow:send()
                   flow   -> credit_flow:ack(Sender),
                             case SlaveWhenPublished of
                                 true  -> credit_flow:ack(Sender); %% [0]
                                 false -> ok
                             end,
                             pmon:monitor(Sender, Senders);
                   noflow -> Senders
               end,
    State1 = State#q{senders = Senders1},
    %% 直接到subscriber 或 enqueue
    noreply(maybe_deliver_or_enqueue(Delivery, SlaveWhenPublished, State1));

maybe_deliver_or_enqueue(Delivery, Delivered, State = #q{overflow = Overflow}) ->
    send_mandatory(Delivery), %% must do this before confirms
    %% 丢消息点，队列满
    case {will_overflow(Delivery, State), Overflow} of
        {true, 'reject-publish'} ->
            %% Drop publish and nack to publisher
            send_reject_publish(Delivery, Delivered, State);
        _ ->
            %% Enqueue and maybe drop head later
            deliver_or_enqueue(Delivery, Delivered, State)
    end.

deliver_or_enqueue(Delivery = #delivery{message = Message,
                                        sender  = SenderPid,
                                        flow    = Flow},
                   Delivered, State = #q{backing_queue       = BQ,
                                         backing_queue_state = BQS}) ->
	%% 队列或者消息 持久化，或确认
    {Confirm, State1} = send_or_record_confirm(Delivery, State),
    Props = message_properties(Message, Confirm, State1),
    %% 检查消息是否重复，重复的检查方法是
    {IsDuplicate, BQS1} = BQ:is_duplicate(Message, BQS),
    State2 = State1#q{backing_queue_state = BQS1},
    case IsDuplicate orelse attempt_delivery(Delivery, Props, Delivered,
                                             State2) of
        true ->
            State2;
        {delivered, State3} ->
            State3;
        %% The next one is an optimisation
        {undelivered, State3 = #q{ttl = 0, dlx = undefined,
                                  backing_queue_state = BQS2,
                                  msg_id_to_channel   = MTC}} ->
            {BQS3, MTC1} = discard(Delivery, BQ, BQS2, MTC),
            State3#q{backing_queue_state = BQS3, msg_id_to_channel = MTC1};
        {undelivered, State3 = #q{backing_queue_state = BQS2}} ->

            BQS3 = BQ:publish(Message, Props, Delivered, SenderPid, Flow, BQS2),
            {Dropped, State4 = #q{backing_queue_state = BQS4}} =
                maybe_drop_head(State3#q{backing_queue_state = BQS3}),
            QLen = BQ:len(BQS4),
            %% optimisation: it would be perfectly safe to always
            %% invoke drop_expired_msgs here, but that is expensive so
            %% we only do that if a new message that might have an
            %% expiry ends up at the head of the queue. If the head
            %% remains unchanged, or if the newly published message
            %% has no expiry and becomes the head of the queue then
            %% the call is unnecessary.
            case {Dropped, QLen =:= 1, Props#message_properties.expiry} of
                {false, false,         _} -> State4;
                {true,  true,  undefined} -> State4;
                {_,     _,             _} -> drop_expired_msgs(State4)
            end
    end.
```

### 4. 异常处理流程

