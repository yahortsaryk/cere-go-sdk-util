package main

import (
	"time"

	"github.com/centrifuge/go-substrate-rpc-client/v4/types"
	contract "github.com/cerebellum-network/cere-ddc-sdk-go/contract/pkg"
	"github.com/cerebellum-network/cere-ddc-sdk-go/contract/pkg/bucket"
	"github.com/cerebellum-network/cere-ddc-sdk-go/contract/pkg/cache"
	log "github.com/sirupsen/logrus"
)

const (
	DDC_SC_ADDRESS     = "6Thd2amHgEWgwTM9P1KfzqXkyKEbQZKV2eNoChJA19Jeks4q"
	BLOCKCHAIN_RPC_URL = "ws://127.0.0.1:9944"
)

func main() {
	bucketContract := createContracts()
	getNode(bucketContract)
	getCdnNode(bucketContract)
	getCluster(bucketContract)
	getNodeList(bucketContract)
	getCdnNodeList(bucketContract)
	getClustersList(bucketContract)
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
	key, err := types.NewAccountIDFromHexString("0x00f9a6e388598a10527dc3b203f1293df6cfa865610e04c60a4eb76b535beb3e")
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
	key, err := types.NewAccountIDFromHexString("0x7473fa5f3b345ebc8968879533fc690be483cd402153d3ccbb500c22a8f6726d")
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

func createContracts() bucket.DdcBucketContract {
	var client contract.BlockchainClient = nil
	client = contract.CreateBlockchainClient(BLOCKCHAIN_RPC_URL)

	c := ddcBucketContract(client)
	if err := c.HookContractEvents(); err != nil {
		log.WithError(err).Fatal("Unable to hook events")
	}

	if err := client.SetEventDispatcher(DDC_SC_ADDRESS, c.GetEventDispatcher()); err != nil {
		log.WithError(err).Fatal("Unable to set event dispatcher")
	}

	return c
}

func ddcBucketContract(client contract.BlockchainClient) cache.DdcBucketContractCache {
	cleanupInterval := 1 * time.Hour
	params := cache.BucketCacheParameters{
		BucketCacheExpiration: 1 * time.Hour,
		BucketCacheCleanUp:    cleanupInterval,

		NodeCacheExpiration: 2 * time.Hour,
		NodeCacheCleanUp:    cleanupInterval,

		AccountCacheExpiration: 30 * time.Minute,
		AccountCacheCleanUp:    cleanupInterval,
	}

	return cache.CreateDdcBucketContractCache(bucket.CreateDdcBucketContract(client, DDC_SC_ADDRESS), params)
}
