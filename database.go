package main

var (
	redisDBDriver = &RedisDBDriver{}
)

// DBDriver ...
type DBDriver interface {
	FindTunnel(key string)
}

// DB ...
type DB struct {
	Driver *DBDriver
}

// NewDB ...
func NewDB() *DB {
	return &DB{}
}

// RedisDBDriver ...
type RedisDBDriver struct {
	// Client redis.NewPool()
}

// FindTunnel ...
func (db *RedisDBDriver) FindTunnel(key string) {
	// results, _ := rdb.Get("tunnels:maps:" + key).Result()
	// if results == "" {
	// 	return nil, errs.NewErrTunnelNotFound()
	// }
	//
	// model, _ := redis.HVals("tunnels:" + results).Result()
	// if len(model) == 0 {
	// 	return nil, errs.NewErrTunnelNotFound()
	// }
	//
	// tunnel := NewTunnel(model[0], model[1], 100)
	// return tunnel, nil
}
