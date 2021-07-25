package registry

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/rcrowley/go-metrics"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// EtcdV3Registry implements etcd registry.
type EtcdV3Registry struct {
	// service address, for example, 127.0.0.1:8972
	ServiceAddress string
	// etcd addresses
	EtcdServers []string
	// base path for http server, for example example/http
	BasePath string
	// Registered services
	services    map[string]service
	serviceLock sync.Mutex
	// 租约时长(秒)
	Lease     int64
	metasLock sync.RWMutex
	metas     map[string]string

	// Metrics 监控
	Metrics        metrics.Meter
	ShowMetricsLog bool

	// etcd client
	kv *clientv3.Client
}

// service 注册服务类型
type service struct {
	// 服务标识key
	key string
	// 服务host
	host string
	// 租约ID
	leaseID clientv3.LeaseID
	// 租约KeepAlive
	keepAliveChan <-chan *clientv3.LeaseKeepAliveResponse
}

// initEtcd 初始化etcd连接
func (registry *EtcdV3Registry) initEtcd() error {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   registry.EtcdServers,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return err
	}

	registry.kv = cli

	return nil
}

// Init 初始化Etcd注册
func (registry *EtcdV3Registry) Init() error {
	if registry.kv == nil {
		err := registry.initEtcd()
		if err != nil {
			return err
		}
	}
	if registry.services == nil {
		registry.services = make(map[string]service)
	}
	if registry.metas == nil {
		registry.metas = make(map[string]string)
	}

	return nil
}

// Stop 停止服务注册
func (registry *EtcdV3Registry) Stop() error {
	// 注销所有服务
	for serviceName := range registry.services {
		_ = registry.Unregister(serviceName)
	}

	return registry.kv.Close()
}

// Register 服务注册
func (registry *EtcdV3Registry) Register(serviceName string, metadata string) error {
	// 设置租约时间
	resp, err := registry.kv.Grant(context.Background(), registry.Lease)
	if err != nil {
		return err
	}

	serviceName = "/" + registry.BasePath + "/" + serviceName + "/" + registry.ServiceAddress

	registry.serviceLock.Lock()
	defer func() {
		registry.serviceLock.Unlock()
	}()

	// 注册服务并绑定租约
	_, err = registry.kv.Put(context.Background(), serviceName, registry.ServiceAddress, clientv3.WithLease(resp.ID))
	if err != nil {
		return err
	}

	// 设置续租 并定期发送续租请求(心跳)
	leaseRespChan, err := registry.kv.KeepAlive(context.Background(), resp.ID)

	if err != nil {
		return err
	}

	if registry.services == nil {
		registry.services = make(map[string]service)
	}
	registry.services[serviceName] = service{
		host:          registry.ServiceAddress,
		key:           serviceName,
		leaseID:       resp.ID,
		keepAliveChan: leaseRespChan,
	}

	go func() {
		for {
			<-leaseRespChan
		}
	}()

	registry.metasLock.Lock()
	if registry.metas == nil {
		registry.metas = make(map[string]string)
	}
	registry.metas[serviceName] = metadata
	registry.metasLock.Unlock()

	log.Printf("register http service: %s", serviceName)

	return nil
}

// Unregister 注销服务
func (registry *EtcdV3Registry) Unregister(serviceName string) error {
	if "" == strings.TrimSpace(serviceName) {
		return errors.New("register service `name` can't be empty")
	}

	if registry.kv == nil {
		err := registry.initEtcd()
		if err != nil {
			return err
		}
	}

	var ser service
	var ok bool

	if ser, ok = registry.services[serviceName]; !ok {
		return errors.New(fmt.Sprintf("service `%s` not registered", serviceName))
	}

	registry.serviceLock.Lock()
	defer func() {
		registry.serviceLock.Unlock()
	}()

	// 撤销租约
	if _, err := registry.kv.Revoke(context.Background(), ser.leaseID); err != nil {
		return err
	}

	log.Printf("unregister service: %s", serviceName)

	return nil
}

// GetMetrics 获取Meter
func (registry *EtcdV3Registry) GetMetrics() metrics.Meter {
	return registry.Metrics
}

// IsShowMetricsLog 是否显示监控日志
func (registry *EtcdV3Registry) IsShowMetricsLog() bool {
	return registry.ShowMetricsLog
}
