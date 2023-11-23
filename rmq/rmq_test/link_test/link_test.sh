#!/bin/bash
# 用来三节点定时随机断网一台，模拟异常场景
. /etc/profile
kdf=/opt/basecloud/shells
. $kdf/basecloudfunctions

log_path=/tmp/test.log
exec 1>>${log_path} 2>&1
host=`hostname`

ilog "start..."

# 定义地址列表
host_list=("35ce6f76-77ae-11ee-b24b-000c29af87ae" "65adbb2a-7268-11ee-a59e-000c29af87ae" "655a1286-7268-11ee-a59e-000c29af87ae")

while true;do
		random_host=$(/opt/midware/redis/bin/redis-cli -h 127.0.0.1 -p 6384 -a Keda\!Redis_47 get "test_host")
		if [ x"${random_host}" != x"$host" ];then
			ilog "skip, sleep 10"
			sleep 10
            continue
        fi
		
        ilog "link down..."
        ifconfig eth0 down
        # 等脑裂彻底发生
        sleep 90
        ifconfig eth0 up
        ilog "link up, sleep 10s..."
        # 留20s的启动恢复时间
        sleep 20

        #状态检测
        ilog "check status"
        softfiles=$(cat /opt/data/config/basecloud/luban/basecloud.txt)
        for sfile in ${softfiles}
        do
                TYPE=${sfile%.*}
                ilog "type:$TYPE, sfile:$sfile"
                if [ x"${TYPE}" == x"upu" ];then
                        continue
                fi
                export RABBITMQ_CONF_ENV_FILE=/opt/data/config/basecloud/rabbitmq/rabbitmq_${TYPE}/rabbitmq-env.config
                #统计启动次数，看是否出现反复重启，预期是每次本机网口up后才会启动一次
				count=$(cat /opt/log/basecloud/rabbitmq/${TYPE}/${TYPE}\@${host}.log|grep "startup complete"|wc -l)
                ilog "type:$TYPE, conut:${count}"
                #查看集群状态，正常应该是三节点无异常
                /opt/midware/rabbitmq/server/sbin/rabbitmqctl cluster_status
        done

        # 十分钟后继续下一轮
        sleep 600
		random_index=$((RANDOM % 3))
		random_host=${host_list[random_index]}
		/opt/midware/redis/bin/redis-cli -h 127.0.0.1 -p 6384 -a Keda\!Redis_47 set "test_host" "$random_host"
		echo "下一个重启网口的机器是: ${random_host}"
done