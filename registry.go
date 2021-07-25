package httpgateway

import (
	"log"

	"github.com/dysodeng/http-gateway/registry"
	"github.com/pkg/errors"
)

type Registry struct {
	// registry 服务注册器
	registry registry.Registry
}

// NewRegistry new Registry
func NewRegistry(registry registry.Registry) *Registry {
	r := &Registry{
		registry: registry,
	}

	err := r.registry.Init()
	if err != nil {
		log.Panicln(err)
	}

	return r
}

// Register 服务注册
func (r *Registry) Register(serviceName string) error {
	if serviceName == "" {
		return errors.New("service name empty")
	}

	return r.registry.Register(serviceName, "")
}

func (r *Registry) Shop() error {
	return r.registry.Stop()
}
