package redlock_go


import (
	"errors"
	"fmt"
	"github.com/go-redis/redis"
	"time"
)

var errNotObtainLock = errors.New("Cannot obtain lock")
var errMultipleLock = errors.New("Get multiple lock")

type redisAttribute struct{
	redisHost string
	redisPassword string
	redisDB int
}

type redisOptions struct {
	retry_count int
	retry_delay time.Duration
	ttl int

}

type redisLockObject struct {
	validity int
	resource string
	val string
}

func RedisNewClient(attribute redisAttribute) (*redis.Client, error) {

	client := redis.NewClient(&redis.Options{
		Addr:attribute.redisHost,
		Password:attribute.redisPassword,
		DB:attribute.redisDB,
	})
	_, err := client.Ping().Result()
	if err != nil {
		panic(err)
	}
	return client, err
}

func InitRedisLockEnvironment(connect_list []redisAttribute) ([]*redis.Client, error){
	//unknown type client
	var servers []*redis.Client
	var connect_info redisAttribute
	var err error
	for _,connect_info = range connect_list {
		var server *redis.Client
		server, err = RedisNewClient(connect_info)
		if err != nil {
			return servers, err
		}
		servers = append(servers, server)
	}

	quorum := len(connect_list) / 2 + 1
	if len(servers) < quorum {
		fmt.Println(errNotObtainLock)
		return  servers, errNotObtainLock
	}

	return servers, err

}

func lock_instance(client *redis.Client, resource string, val string, ttl time.Duration) (bool, error){
	set, err := client.SetNX(resource, val, ttl).Result()
	return set, err
}

func unlock_instance(client *redis.Client, resource []string, val []string) {
	unlockScript := ""
	client.Eval(unlockScript, resource, val)
}

func getUniqueId() (randomString string) {
	return "randomString"
}

func lock(resource string, ttl int, options redisOptions, servers []*redis.Client, quorum int) (object redisLockObject, err error){
	retry := 0
	clockDriftFactor := 0.01
	var lockObject redisLockObject
	val := getUniqueId()
	retryCount := options.retry_count
	retryDelay := options.retry_delay
	drift := int(float64(ttl) * clockDriftFactor) + 2
	for ;retry < retryCount;retry++{
		n := 0
		startTime := int(time.Now().UnixNano() * 1000)
		var server *redis.Client
		for _, server = range servers {
			var resourceArray []string
			var valArray []string
			resourceArray = append(resourceArray, resource)
			valArray = append(valArray, val)
			n++
		}
		elapsedTime := int(time.Now().UnixNano() * 1000) - startTime
		validity := int(ttl - elapsedTime - drift)
		if validity > 0 && n >= quorum {
			lockObject.validity = validity
			lockObject.resource = resource
			lockObject.val = val
			return lockObject, nil
		} else {
			var server *redis.Client
			for _, server = range servers {
				var resourceArray []string
				var valArray []string
				resourceArray = append(resourceArray, resource)
				valArray = append(valArray, val)
				unlock_instance(server, resourceArray, valArray)
			}
		}
		time.Sleep(retryDelay)
	}
	return lockObject, errNotObtainLock
}

func unlock(lock redisLockObject, servers []*redis.Client) {
	var server *redis.Client
	var key string
	var val string
	key = lock.resource
	val = lock.val
	for _,server = range servers {
		var resourceArray []string
		var valArray []string
		resourceArray = append(resourceArray, key)
		valArray = append(valArray, val)
		unlock_instance(server, resourceArray, valArray)
	}
}