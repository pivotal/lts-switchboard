package monitor_test

import (
	"os"

	"code.cloudfoundry.org/lager/lagertest"
	"github.com/pivotal/lts-switchboard/runner/monitor"
	"github.com/pivotal/lts-switchboard/runner/monitor/monitorfakes"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/tedsuo/ifrit"
)

var _ = Describe("Monitor Runner", func() {
	It("shuts down gracefully when signalled", func() {
		m := new(monitorfakes.FakeMonitor)

		logger := lagertest.NewTestLogger("Monitor Runner test")
		monitorRunner := monitor.NewRunner(m, logger)
		monitorProcess := ifrit.Invoke(monitorRunner)

		shutdownChan := m.MonitorArgsForCall(0)
		Consistently(shutdownChan).ShouldNot(BeClosed())

		monitorProcess.Signal(os.Kill)
		Eventually(monitorProcess.Wait()).Should(Receive())

		Expect(shutdownChan).To(BeClosed())
	})
})
