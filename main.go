package main

import (
	"github.com/centrifuge/go-substrate-rpc-client/v4/types"
	contract "github.com/cerebellum-network/cere-ddc-sdk-go/contract/pkg"
	"github.com/cerebellum-network/cere-ddc-sdk-go/contract/pkg/bucket"
	log "github.com/sirupsen/logrus"
)

const (
	DDC_SC_ADDRESS     = "6QHgFfxkYyMdUqMw2rpykwGbKbtiof5bYvFMg17swct85vvX"
	BLOCKCHAIN_RPC_URL = "wss://archive.devnet.cere.network/ws"
)

func main() {
	bucketContract := createContract()

	getNode(*bucketContract)
	getCdnNode(*bucketContract)
	getCluster(*bucketContract)
	getNodeList(*bucketContract)
	getCdnNodeList(*bucketContract)
	getClustersList(*bucketContract)
	getBucket(*bucketContract)
	getBucketsList(*bucketContract)

	listenToEvents(*bucketContract)

	select {}
}

func createContract() *bucket.DdcBucketContract {
	var client contract.BlockchainClient = nil
	client = contract.CreateBlockchainClient(BLOCKCHAIN_RPC_URL)

	c := ddcBucketContract(client)

	if err := client.SetEventDispatcher(DDC_SC_ADDRESS, c.GetEventDispatcher()); err != nil {
		log.WithError(err).Fatal("Unable to set event dispatcher")
	}

	return &c
}

func ddcBucketContract(client contract.BlockchainClient) bucket.DdcBucketContract {
	return bucket.CreateDdcBucketContract(client, DDC_SC_ADDRESS)
}

func listenToEvents(bucketContract bucket.DdcBucketContract) {
	log.Print("Registring event handler ...")

	bucketContract.AddContractEventHandler(bucket.NodeCreatedEventId, func(raw interface{}) {
		log.Printf("NodeCreatedEvent raw: %v", raw)
		nodeCreatedEvent := raw.(*bucket.NodeCreatedEvent)
		log.Printf("NodeCreatedEvent: %v", nodeCreatedEvent)
	})

	log.Print("Waiting for events ...")
}

func getCluster(bucketContract bucket.DdcBucketContract) {
	var clusterId types.U32 = 0
	cluster, err := bucketContract.ClusterGet(clusterId)
	if err != nil {
		log.WithError(err).Fatal("Cannot get a cluster")
	} else {
		log.Printf("Cluster - %v", cluster)
	}
}

func getNode(bucketContract bucket.DdcBucketContract) {
	key, err := types.NewAccountIDFromHexString("0xd43593c715fdd31c61141abd04a99fd6822c8558854ccde39a5684e7a56da27d")
	if err != nil {
		log.Printf("Error parsing node key - %v", key)
	}

	node, err := bucketContract.NodeGet(*key)
	if err != nil {
		log.WithError(err).Fatal("Cannot get a node")
	} else {
		log.Printf("Storage Node - %v", node)
	}
}

func getCdnNode(bucketContract bucket.DdcBucketContract) {
	key, err := types.NewAccountIDFromHexString("0xfe65717dad0447d715f660a0a58411de509b42e6efb8375f562f58a554d5860e")
	if err != nil {
		log.Printf("Error parsing node key - %v", key)
	}

	node, err := bucketContract.CdnNodeGet(*key)
	if err != nil {
		log.WithError(err).Fatal("Cannot get a node")
	} else {
		log.Printf("Cdn Node - %v", node)
	}
}

func getBucket(bucketContract bucket.DdcBucketContract) {
	bucket, err := bucketContract.BucketGet(types.NewU32(0))
	if err != nil {
		log.WithError(err).Fatal("Cannot get a bucket")
	} else {
		log.Printf("Bucket - %v", bucket)
	}
}

func getNodeList(bucketContract bucket.DdcBucketContract) {
	nodes, err := bucketContract.NodeList(0, 10, types.OptionAccountID{})
	if err != nil {
		log.WithError(err).Fatal("Cannot get storage nodes list")
	} else {
		log.Printf("Storage Nodes - %v", nodes)
	}
}

func getCdnNodeList(bucketContract bucket.DdcBucketContract) {
	nodes, err := bucketContract.CdnNodeList(0, 10, types.OptionAccountID{})
	if err != nil {
		log.WithError(err).Fatal("Cannot get cdn nodes list")
	} else {
		log.Printf("CDN Nodes - %v", nodes)
	}
}

func getClustersList(bucketContract bucket.DdcBucketContract) {
	clusters, err := bucketContract.ClusterList(0, 10, types.OptionAccountID{})
	if err != nil {
		log.WithError(err).Fatal("Cannot get clusters list")
	} else {
		log.Printf("Clusters - %v", clusters)
	}
}

func getBucketsList(bucketContract bucket.DdcBucketContract) {
	buckets, err := bucketContract.BucketList(0, 10, types.OptionAccountID{})
	if err != nil {
		log.WithError(err).Fatal("Cannot get buckets list")
	} else {
		log.Printf("Buckets - %v", buckets)
	}
}
