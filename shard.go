package main

import (
	"database/sql"
	"fmt"
	"hash/crc32"
)

type ShardManager struct {
	DB         *sql.DB
	NumShards  int
	TableNames []string
}

func shardForKey(key string, numShards int) int {
	h := crc32.ChecksumIEEE([]byte(key))
	return int(h % uint32(numShards))
}

func newShardManager(db *sql.DB, numShards int, baseTableName string) *ShardManager {
	tables := make([]string, numShards)
	for i := 0; i < numShards; i++ {
		tables[i] = fmt.Sprintf("%s_%d", baseTableName, i)
	}
	return &ShardManager{
		DB:         db,
		NumShards:  numShards,
		TableNames: tables,
	}
}

func (s *ShardManager) TableForKey(key string) string {
	shard := shardForKey(key, s.NumShards)
	return s.TableNames[shard]
}
