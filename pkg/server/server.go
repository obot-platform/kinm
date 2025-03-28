package server

import (
	"context"
	"fmt"
	"net"
	"net/http"

	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authentication/request/anonymous"
	"k8s.io/apiserver/pkg/authentication/request/union"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/apiserver/pkg/endpoints/openapi"
	"k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/server/filters"
	"k8s.io/apiserver/pkg/server/healthz"
	"k8s.io/apiserver/pkg/server/options"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	openapicommon "k8s.io/kube-openapi/pkg/common"
	netutils "k8s.io/utils/net"
)

const MinkHeaderKey = "X-Mink-Server"

type Server struct {
	config           *Config
	Config           *server.RecommendedConfig
	GenericAPIServer *server.GenericAPIServer
	Loopback         *rest.Config
	started          chan struct{}
}

type Config struct {
	Name                  string
	Version               string
	Authenticator         authenticator.Request
	Authorization         authorizer.Authorizer
	HTTPListenPort        int
	Listener              net.Listener
	HTTPSListenPort       int
	LongRunningVerbs      []string
	LongRunningResources  []string
	OpenAPIConfig         openapicommon.GetOpenAPIDefinitions
	Scheme                *runtime.Scheme
	CodecFactory          *serializer.CodecFactory
	APIGroups             []*server.APIGroupInfo
	Middleware            []func(http.Handler) http.Handler
	PostStartFunc         server.PostStartHookFunc
	SupportAPIAggregation bool
	DefaultOptions        *options.RecommendedOptions
	AuditConfig           *options.AuditOptions
	IgnoreStartFailure    bool
	ReadinessCheckers     []healthz.HealthChecker
}

func (c *Config) complete() {
	if c.HTTPListenPort == 0 {
		c.HTTPListenPort = 8080
	}
	if c.HTTPSListenPort == 0 {
		c.HTTPSListenPort = c.HTTPListenPort + 1
	}
	if len(c.LongRunningVerbs) == 0 {
		c.LongRunningVerbs = []string{"watch", "proxy"}
	}
	if c.Scheme == nil {
		c.Scheme = scheme.Scheme
	}
	if c.CodecFactory == nil {
		codec := serializer.NewCodecFactory(c.Scheme)
		c.CodecFactory = &codec
	}
	if c.Name == "" {
		c.Name = "mink"
	}
	if c.DefaultOptions == nil {
		c.DefaultOptions = DefaultOpts()
		if c.AuditConfig != nil {
			c.DefaultOptions.Audit = c.AuditConfig
		}
	}
}

func DefaultOpts() *options.RecommendedOptions {
	opts := options.NewRecommendedOptions("", nil)
	opts.Audit = nil
	opts.Etcd = nil
	opts.CoreAPI = nil
	opts.Authorization = nil
	opts.Features = nil
	opts.Admission = nil
	return opts
}

func New(config *Config) (*Server, error) {
	config.complete()

	opts := config.DefaultOptions
	opts.SecureServing.Listener = config.Listener
	opts.SecureServing.BindPort = config.HTTPSListenPort
	opts.Authentication.SkipInClusterLookup = !config.SupportAPIAggregation
	opts.Authentication.RemoteKubeConfigFileOptional = !config.SupportAPIAggregation

	if err := opts.SecureServing.MaybeDefaultWithSelfSignedCerts("localhost", nil, []net.IP{netutils.ParseIPSloppy("127.0.0.1")}); err != nil {
		return nil, fmt.Errorf("error creating self-signed certificates: %v", err)
	}

	serverConfig := server.NewRecommendedConfig(*config.CodecFactory)
	serverConfig.ClientConfig = generateDummyKubeconfig()
	serverConfig.OpenAPIConfig = server.DefaultOpenAPIConfig(config.OpenAPIConfig, openapi.NewDefinitionNamer(config.Scheme))
	serverConfig.OpenAPIConfig.Info.Title = config.Name
	serverConfig.OpenAPIConfig.Info.Version = config.Version
	serverConfig.OpenAPIV3Config = server.DefaultOpenAPIV3Config(config.OpenAPIConfig, openapi.NewDefinitionNamer(config.Scheme))
	serverConfig.OpenAPIV3Config.Info.Title = config.Name
	serverConfig.OpenAPIV3Config.Info.Version = config.Version
	serverConfig.LongRunningFunc = filters.BasicLongRunningRequestCheck(
		sets.NewString(config.LongRunningVerbs...),
		sets.NewString(config.LongRunningResources...),
	)

	if errs := opts.Validate(); len(errs) > 0 {
		return nil, errors.NewAggregate(errs)
	}

	if err := opts.ApplyTo(serverConfig); err != nil {
		return nil, err
	}

	if err := options.NewServerRunOptions().ApplyTo(&serverConfig.Config); err != nil {
		return nil, err
	}

	if config.Authenticator != nil {
		serverConfig.Authentication.Authenticator = union.New(config.Authenticator, anonymous.NewAuthenticator(nil))
	}
	if config.Authorization != nil {
		serverConfig.Authorization.Authorizer = config.Authorization
	}

	if config.PostStartFunc != nil {
		serverConfig.AddPostStartHookOrDie(config.Name, func(context server.PostStartHookContext) error {
			err := config.PostStartFunc(context)
			if err != nil {
				logrus.Fatalf("failed to run post startup hook: %v", err)
			}
			return err
		})
	}

	var result = Server{
		config:  config,
		Config:  serverConfig,
		started: make(chan struct{}),
	}

	err := serverConfig.AddPostStartHook("save loopback", func(context server.PostStartHookContext) error {
		result.Loopback = context.LoopbackClientConfig
		close(result.started)
		return nil

	})
	if err != nil {
		return nil, err
	}

	serverConfig.AddReadyzChecks(config.ReadinessCheckers...)

	server, err := serverConfig.Complete().New(config.Name, server.NewEmptyDelegate())
	if err != nil {
		return nil, err
	}

	result.GenericAPIServer = server

	for _, apiGroup := range config.APIGroups {
		legacy := false
		for _, gv := range apiGroup.PrioritizedVersions {
			if gv.Group == "" {
				legacy = true
				break
			}
		}
		if legacy {
			if err := server.InstallLegacyAPIGroup("/api", apiGroup); err != nil {
				return nil, err
			}
		} else if err := server.InstallAPIGroups(apiGroup); err != nil {
			return nil, err
		}
	}

	return &result, nil
}

func (s *Server) Handler(ctx context.Context) http.Handler {
	readyServer := s.GenericAPIServer.PrepareRun()

	go func() {
		err := readyServer.RunWithContext(ctx)
		if err != nil {
			if s.config.IgnoreStartFailure {
				logrus.Errorf("Failed to run api server: %v", err)
			} else {
				logrus.Fatalf("Failed to run api server: %v", err)
			}
		}
	}()

	<-s.started

	handler := addResponseHeader(readyServer.Handler)
	for i := len(s.config.Middleware) - 1; i >= 0; i-- {
		handler = s.config.Middleware[i](handler)
	}

	return handler
}

func (s *Server) Run(ctx context.Context) error {
	address := fmt.Sprintf("0.0.0.0:%d", s.config.HTTPListenPort)
	handler := s.Handler(ctx)

	httpServer := &http.Server{
		Handler: handler,
		Addr:    address,
	}

	go func() {
		logrus.Infof("Listening on %s", address)
		if err := httpServer.ListenAndServe(); err != nil {
			if s.config.IgnoreStartFailure {
				logrus.Errorf("Failed to run http api server: %v", err)
			} else {
				logrus.Fatalf("Failed to run http api server: %v", err)
			}
		}
	}()

	go func() {
		<-ctx.Done()
		httpServer.Close()
	}()

	return nil
}

func generateDummyKubeconfig() *rest.Config {
	return &rest.Config{}
}

func addResponseHeader(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// This is to indicate that the response actually came from a mink server.
		// One day we might consider adding a request ID or something here.
		w.Header().Add(MinkHeaderKey, "true")
		handler.ServeHTTP(w, r)
	})
}
