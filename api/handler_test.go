package api_test

import (
	"net/http"

	"github.com/cloudfoundry-incubator/switchboard/api"
	apifakes "github.com/cloudfoundry-incubator/switchboard/api/fakes"
	"github.com/cloudfoundry-incubator/switchboard/config"
	"github.com/cloudfoundry-incubator/switchboard/domain"
	domainfakes "github.com/cloudfoundry-incubator/switchboard/domain/fakes"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pivotal-golang/lager/lagertest"
)

var _ = Describe("Handler", func() {
	var handler http.Handler

	JustBeforeEach(func() {
		backends := &domainfakes.FakeBackends{}
		logger := lagertest.NewTestLogger("Handler Test")
		config := config.API{}
		staticDir := ""
		handler = api.NewHandler(backends, logger, config, staticDir)
	})

	Context("when a request panics", func() {
		var (
			realBackendsIndex func(backends domain.Backends) http.Handler
			responseWriter    *apifakes.FakeResponseWriter
			request           *http.Request
		)

		BeforeEach(func() {
			realBackendsIndex = api.BackendsIndex
			api.BackendsIndex = func(domain.Backends) http.Handler {
				return http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
					panic("fake request panic")
				})
			}

			responseWriter = &apifakes.FakeResponseWriter{}
			var err error
			request, err = http.NewRequest("GET", "/v0/backends", nil)
			Expect(err).NotTo(HaveOccurred())
		})

		AfterEach(func() {
			api.BackendsIndex = realBackendsIndex
		})

		It("recovers from panics and responds with an internal server error", func() {
			handler.ServeHTTP(responseWriter, request) // should not panic

			Expect(responseWriter.WriteHeaderCallCount()).To(Equal(1))
			Expect(responseWriter.WriteHeaderArgsForCall(0)).To(Equal(http.StatusInternalServerError))
		})
	})
})
