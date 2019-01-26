package apiaggregator_test

import (
	"net/http"
	"net/http/httptest"

	"code.cloudfoundry.org/lager/lagertest"
	"github.com/pivotal/lts-switchboard/api"
	"github.com/pivotal/lts-switchboard/api/apifakes"
	"github.com/pivotal/lts-switchboard/apiaggregator"
	"github.com/pivotal/lts-switchboard/config"
	"github.com/pivotal/lts-switchboard/domain"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Handler", func() {
	var (
		handler          http.Handler
		responseRecorder *httptest.ResponseRecorder
		cfg              config.API
	)

	JustBeforeEach(func() {
		logger := lagertest.NewTestLogger("Handler Test")

		handler = apiaggregator.NewHandler(
			logger,
			cfg,
		)
	})

	Context("when a request panics", func() {
		var (
			realBackendsIndex func([]*domain.Backend, api.ClusterManager) http.Handler
			responseWriter    *apifakes.FakeResponseWriter
			request           *http.Request
		)

		BeforeEach(func() {
			cfg = config.API{
				ForceHttps: false,
				Username:   "foo",
				Password:   "bar",
			}
			realBackendsIndex = api.BackendsIndex
			api.BackendsIndex = func([]*domain.Backend, api.ClusterManager) http.Handler {
				return http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
					panic("fake request panic")
				})
			}

			responseWriter = new(apifakes.FakeResponseWriter)
			var err error
			request, err = http.NewRequest("GET", "/v0/backends", nil)
			Expect(err).NotTo(HaveOccurred())
			request.SetBasicAuth("foo", "bar")
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

	Context("when request does not contain https header", func() {
		var request *http.Request

		BeforeEach(func() {
			cfg = config.API{
				ForceHttps: true,
			}
			responseRecorder = httptest.NewRecorder()
			request, _ = http.NewRequest("GET", "http://localhost/foo/bar", nil)
			request.Header.Set("X-Forwarded-Proto", "http")
		})

		It("redirects to https", func() {
			handler.ServeHTTP(responseRecorder, request)

			Expect(responseRecorder.Code).To(Equal(http.StatusFound))
			Expect(responseRecorder.HeaderMap.Get("Location")).To(Equal("https://localhost/foo/bar"))
		})
	})
})
