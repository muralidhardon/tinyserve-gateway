// Package discovery keeps the gateway's replica pool in sync with a
// Kubernetes Service, by watching the Service's EndpointSlices.
//
// Only ready endpoints are routed to. When a pod starts terminating its
// endpoint stops being ready, so the gateway sends it no new streams while
// its open ones finish: that, plus the pod's preStop delay and TinyServe's
// graceful shutdown, is what lets a rolling update drop no requests.
package discovery

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"slices"
	"strconv"

	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	"github.com/muralidhardon/tinyserve-gateway/internal/pool"
)

type Config struct {
	Namespace string
	Service   string
	// PortName picks the Service port replicas serve on; empty means the
	// slice's first port.
	PortName string
	Logger   *slog.Logger
}

// Start fills p from the Service's EndpointSlices and keeps it current until
// ctx is done. It returns once the initial list has been loaded, so the
// gateway doesn't start serving with an empty pool.
func Start(ctx context.Context, client kubernetes.Interface, cfg Config, p *pool.Pool) error {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	selector := labels.Set{discoveryv1.LabelServiceName: cfg.Service}.String()
	factory := informers.NewSharedInformerFactoryWithOptions(client, 0,
		informers.WithNamespace(cfg.Namespace),
		informers.WithTweakListOptions(func(o *metav1.ListOptions) { o.LabelSelector = selector }))
	slicesInformer := factory.Discovery().V1().EndpointSlices()
	lister := slicesInformer.Lister().EndpointSlices(cfg.Namespace)

	sync := func() {
		all, err := lister.List(labels.Everything())
		if err != nil {
			cfg.Logger.Error("listing endpointslices", "err", err)
			return
		}
		p.Set(URLs(all, cfg.PortName))
	}
	_, err := slicesInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(any) { sync() },
		UpdateFunc: func(any, any) { sync() },
		DeleteFunc: func(any) { sync() },
	})
	if err != nil {
		return err
	}
	factory.Start(ctx.Done())
	for typ, ok := range factory.WaitForCacheSync(ctx.Done()) {
		if !ok {
			return fmt.Errorf("discovery: %v cache did not sync", typ)
		}
	}
	sync()
	cfg.Logger.Info("discovering replicas", "namespace", cfg.Namespace, "service", cfg.Service,
		"replicas", len(p.Backends()))
	return nil
}

// URLs lists the base URLs of the ready endpoints in eps, sorted and
// without duplicates.
func URLs(eps []*discoveryv1.EndpointSlice, portName string) []string {
	var out []string
	for _, s := range eps {
		if s.AddressType == discoveryv1.AddressTypeFQDN {
			continue
		}
		port := -1
		for _, sp := range s.Ports {
			if sp.Port != nil && (portName == "" || (sp.Name != nil && *sp.Name == portName)) {
				port = int(*sp.Port)
				break
			}
		}
		if port < 0 {
			continue
		}
		for _, e := range s.Endpoints {
			// Ready is nil when unknown, which the API says to treat as ready.
			// A terminating pod's endpoint is not ready: drain it.
			if (e.Conditions.Ready != nil && !*e.Conditions.Ready) || len(e.Addresses) == 0 {
				continue
			}
			out = append(out, "http://"+net.JoinHostPort(e.Addresses[0], strconv.Itoa(port)))
		}
	}
	slices.Sort(out)
	return slices.Compact(out)
}
