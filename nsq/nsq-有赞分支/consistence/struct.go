//        file: consistence/struct.go
// description: struct of nsq etcd

//      author: reezhou
//       email: reechou@gmail.com
//   copyright: youzan

package consistence

// this is default value
var NSQ_ROOT_DIR = "NSQMetaData"

const (
	NSQ_TOPIC_DIR              = "Topics"
	NSQ_TOPIC_META             = "TopicMeta"
	NSQ_TOPIC_REPLICA_INFO     = "ReplicaInfo"
	NSQ_TOPIC_LEADER_SESSION   = "LeaderSession"
	NSQ_NODE_DIR               = "NsqdNodes"
	NSQ_LOOKUPD_DIR            = "NsqlookupdInfo"
	NSQ_LOOKUPD_NODE_DIR       = "NsqlookupdNodes"
	NSQ_LOOKUPD_LEADER_SESSION = "LookupdLeaderSession"
)

const (
	ETCD_LOCK_NSQ_NAMESPACE = "nsq"
)

type TopicReplicasInfo struct {
	Leader string
	ISR    []string
}

type TopicCatchupInfo struct {
	CatchupList []string
}

type TopicChannelsInfo struct {
	Channels []string
}
