package main

import (
	"fmt"
	"io/ioutil"
	_ "net/http/pprof"
	"os"
	"strconv"

	"code.cloudfoundry.org/clock"
	"code.cloudfoundry.org/consuladapter"
	"code.cloudfoundry.org/lager"
	"code.cloudfoundry.org/locket"

	apirunner "github.com/pivotal/lts-switchboard/runner/api"
	apiaggregatorrunner "github.com/pivotal/lts-switchboard/runner/apiaggregator"

	consulapi "github.com/hashicorp/consul/api"
	"github.com/pivotal/lts-switchboard/api"
	"github.com/pivotal/lts-switchboard/apiaggregator"
	"github.com/pivotal/lts-switchboard/config"
	"github.com/pivotal/lts-switchboard/domain"
	"github.com/pivotal/lts-switchboard/runner/bridge"
	"github.com/pivotal/lts-switchboard/runner/health"
	"github.com/pivotal/lts-switchboard/runner/monitor"
	"github.com/tedsuo/ifrit"
	"github.com/tedsuo/ifrit/grouper"
	"github.com/tedsuo/ifrit/sigmon"
)

func main() {
	rootConfig, err := config.NewConfig(os.Args)

	logger := rootConfig.Logger

	err = rootConfig.Validate()
	if err != nil {
		logger.Fatal("Error validating config:", err, lager.Data{"config": rootConfig})
	}

	if _, err := os.Stat(rootConfig.StaticDir); os.IsNotExist(err) {
		logger.Fatal(fmt.Sprintf("staticDir: %s does not exist", rootConfig.StaticDir), nil)
	}

	backends := domain.NewBackends(rootConfig.Proxy.Backends, logger)

	activeNodeClusterMonitor := monitor.NewClusterMonitor(
		backends,
		rootConfig.Proxy.HealthcheckTimeout(),
		logger,
		true,
	)

	activeNodeBridgeRunner := bridge.NewRunner(rootConfig.Proxy.Port, rootConfig.Proxy.ShutdownDelay(), logger)
	clusterStateManager := api.NewClusterAPI(logger)

	activeNodeClusterMonitor.RegisterBackendSubscriber(activeNodeBridgeRunner.ActiveBackendChan)
	activeNodeClusterMonitor.RegisterBackendSubscriber(clusterStateManager.ActiveBackendChan)

	clusterStateManager.RegisterTrafficEnabledChan(activeNodeBridgeRunner.TrafficEnabledChan)
	go clusterStateManager.ListenForActiveBackend()

	apiHandler := api.NewHandler(clusterStateManager, backends, logger, rootConfig.API, rootConfig.StaticDir)
	aggregatorHandler := apiaggregator.NewHandler(logger, rootConfig.API)

	members := grouper.Members{
		{
			Name:   "active-node-bridge",
			Runner: activeNodeBridgeRunner,
		},
		{
			Name:   "api-aggregator",
			Runner: apiaggregatorrunner.NewRunner(rootConfig.API.AggregatorPort, aggregatorHandler),
		},
		{
			Name:   "api",
			Runner: apirunner.NewRunner(rootConfig.API.Port, apiHandler),
		},
		{
			Name:   "active-node-monitor",
			Runner: monitor.NewRunner(activeNodeClusterMonitor, logger),
		},
	}

	if rootConfig.HealthPort != rootConfig.API.Port {
		members = append(members, grouper.Member{
			Name:   "health",
			Runner: health.NewRunner(rootConfig.HealthPort),
		})
	}

	if rootConfig.ConsulCluster != "" {
		writePid(logger, rootConfig.PidFile)

		if rootConfig.ConsulServiceName == "" {
			rootConfig.ConsulServiceName = "mysql"
		}

		clock := clock.NewClock()
		consulClient, err := consuladapter.NewClientFromUrl(rootConfig.ConsulCluster)
		if err != nil {
			logger.Fatal("new-consul-client-failed", err)
		}

		registrationRunner := locket.NewRegistrationRunner(logger,
			&consulapi.AgentServiceRegistration{
				Name:  rootConfig.ConsulServiceName,
				Port:  int(rootConfig.Proxy.Port),
				Check: &consulapi.AgentServiceCheck{TTL: "3s"},
			},
			consulClient, locket.RetryInterval, clock)

		members = append(members, grouper.Member{"registration", registrationRunner})
	}

	group := grouper.NewOrdered(os.Interrupt, members)
	process := ifrit.Invoke(sigmon.New(group))

	logger.Info("Proxy started", lager.Data{"proxyConfig": rootConfig.Proxy})

	if rootConfig.ConsulCluster == "" {
		writePid(logger, rootConfig.PidFile)
	}

	err = <-process.Wait()
	if err != nil {
		logger.Fatal("Switchboard exited unexpectedly", err, lager.Data{"proxyConfig": rootConfig.Proxy})
	}
}

func writePid(logger lager.Logger, pidFile string) {
	err := ioutil.WriteFile(pidFile, []byte(strconv.Itoa(os.Getpid())), 0644)
	if err == nil {
		logger.Info(fmt.Sprintf("Wrote pidFile to %s", pidFile))
	} else {
		logger.Fatal("Cannot write pid to file", err, lager.Data{"pidFile": pidFile})
	}
}
