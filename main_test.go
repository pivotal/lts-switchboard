package main_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"code.cloudfoundry.org/consuladapter"
	"code.cloudfoundry.org/consuladapter/consulrunner"

	consulapi "github.com/hashicorp/consul/api"
	. "github.com/onsi/ginkgo"
	ginkgoconf "github.com/onsi/ginkgo/config"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/types"
	"github.com/pivotal/lts-switchboard/api"
	"github.com/pivotal/lts-switchboard/config"
	"github.com/pivotal/lts-switchboard/dummies"
	"github.com/tedsuo/ifrit"
	"github.com/tedsuo/ifrit/ginkgomon"
	"github.com/tedsuo/ifrit/grouper"
	"github.com/tedsuo/ifrit/sigmon"
)

type Response struct {
	BackendPort  uint
	BackendIndex uint
	Message      string
}

func allowTraffic(allow bool, port uint) {
	var url string
	if allow {
		url = fmt.Sprintf(
			"http://localhost:%d/v0/cluster?trafficEnabled=%t",
			port,
			allow,
		)
	} else {
		url = fmt.Sprintf(
			"http://localhost:%d/v0/cluster?trafficEnabled=%t&message=%s",
			port,
			allow,
			"main%20test%20is%20disabling%20traffic",
		)
	}

	req, err := http.NewRequest("PATCH", url, nil)
	Expect(err).NotTo(HaveOccurred())
	req.SetBasicAuth("username", "password")

	client := &http.Client{}
	resp, err := client.Do(req)
	Expect(err).NotTo(HaveOccurred())
	Expect(resp.StatusCode).To(Equal(http.StatusOK))
}

func getClusterFromAPI(req *http.Request) map[string]interface{} {
	client := &http.Client{}
	resp, err := client.Do(req)
	Expect(err).NotTo(HaveOccurred())
	Expect(resp.StatusCode).To(Equal(http.StatusOK))

	returnedCluster := map[string]interface{}{}

	decoder := json.NewDecoder(resp.Body)
	err = decoder.Decode(&returnedCluster)
	Expect(err).NotTo(HaveOccurred())
	return returnedCluster
}

func sendData(conn net.Conn, data string) (Response, error) {
	_, _ = conn.Write([]byte(data))

	buffer := make([]byte, 1024)
	n, err := conn.Read(buffer)
	if err != nil {
		return Response{}, err.(error)
	} else {
		response := Response{}
		err := json.Unmarshal(buffer[:n], &response)
		if err != nil {
			return Response{}, err
		}
		return response, nil
	}
}

func verifyHeaderContains(header http.Header, key, valueSubstring string) {
	found := false
	for k, v := range header {
		if k == key {
			for _, value := range v {
				if strings.Contains(value, valueSubstring) {
					found = true
				}
			}
		}
	}
	Expect(found).To(BeTrue(), fmt.Sprintf("%s: %s not found in header", key, valueSubstring))
}

func getBackendsFromApi(req *http.Request) []map[string]interface{} {
	client := &http.Client{}
	resp, err := client.Do(req)
	Expect(err).NotTo(HaveOccurred())
	Expect(resp.StatusCode).To(Equal(http.StatusOK))

	returnedBackends := []map[string]interface{}{}

	decoder := json.NewDecoder(resp.Body)
	err = decoder.Decode(&returnedBackends)
	Expect(err).NotTo(HaveOccurred())
	return returnedBackends
}

func matchConnectionDisconnect() types.GomegaMatcher {
	//exact error depends on environment
	return MatchError(
		MatchRegexp(
			"%s|%s",
			io.EOF.Error(),
			syscall.ECONNRESET.Error(),
		),
	)
}

const startupTimeout = 10 * time.Second

var _ = Describe("Switchboard", func() {
	var (
		process                                      ifrit.Process
		initialActiveBackend, initialInactiveBackend config.Backend
		healthcheckRunners                           []*dummies.HealthcheckRunner
		healthcheckWaitDuration                      time.Duration

		proxyPort                    uint
		proxyInactiveNodePort        uint
		switchboardAPIPort           uint
		switchboardAPIAggregatorPort uint
		switchboardHealthPort        uint
		backends                     []config.Backend
		rootConfig                   config.Config
		proxyConfig                  config.Proxy
		apiConfig                    config.API
		pidFile                      string
		staticDir                    string
	)

	BeforeEach(func() {
		tempDir, err := ioutil.TempDir(os.TempDir(), "switchboard")
		Expect(err).NotTo(HaveOccurred())

		testDir := getDirOfCurrentFile()
		staticDir = filepath.Join(testDir, "static")

		pidFileFile, _ := ioutil.TempFile(tempDir, "switchboard.pid")
		_ = pidFileFile.Close()
		pidFile = pidFileFile.Name()
		_ = os.Remove(pidFile)

		proxyPort = uint(10000 + GinkgoParallelNode())
		proxyInactiveNodePort = uint(10600 + GinkgoParallelNode())
		switchboardAPIPort = uint(10100 + GinkgoParallelNode())
		switchboardAPIAggregatorPort = uint(10800 + GinkgoParallelNode())
		switchboardHealthPort = uint(6160 + GinkgoParallelNode())

		backend1 := config.Backend{
			Host:           "localhost",
			Port:           uint(10200 + GinkgoParallelNode()),
			StatusPort:     uint(10300 + GinkgoParallelNode()),
			StatusEndpoint: "api/v1/status",
			Name:           "backend-0",
		}

		backend2 := config.Backend{
			Host:           "localhost",
			Port:           uint(10400 + GinkgoParallelNode()),
			StatusPort:     uint(10500 + GinkgoParallelNode()),
			StatusEndpoint: "api/v1/status",
			Name:           "backend-1",
		}

		backends = []config.Backend{backend1, backend2}

		proxyConfig = config.Proxy{
			Backends:                 backends,
			HealthcheckTimeoutMillis: 500,
			Port:                     proxyPort,
			InactiveMysqlPort:        proxyInactiveNodePort,
		}

		apiConfig = config.API{
			AggregatorPort: switchboardAPIAggregatorPort,
			Port:           switchboardAPIPort,
			Username:       "username",
			Password:       "password",
			ProxyURIs:      []string{"some-proxy-uri-0", "some-proxy-uri-1"},
		}

		rootConfig = config.Config{
			Proxy:      proxyConfig,
			API:        apiConfig,
			HealthPort: switchboardHealthPort,
			PidFile:    pidFile,
			StaticDir:  staticDir,
		}
		healthcheckWaitDuration = 3 * proxyConfig.HealthcheckTimeout()
	})

	JustBeforeEach(func() {
		b, err := json.Marshal(rootConfig)
		Expect(err).NotTo(HaveOccurred())

		healthcheckRunners = []*dummies.HealthcheckRunner{
			dummies.NewHealthcheckRunner(backends[0], 0),
			dummies.NewHealthcheckRunner(backends[1], 1),
		}

		logLevel := "debug"
		switchboardRunner := ginkgomon.New(ginkgomon.Config{
			Command: exec.Command(
				switchboardBinPath,
				fmt.Sprintf("-config=%s", string(b)),
				fmt.Sprintf("-logLevel=%s", logLevel),
			),
			Name:              fmt.Sprintf("switchboard"),
			StartCheck:        "started",
			StartCheckTimeout: startupTimeout,
		})

		group := grouper.NewOrdered(os.Interrupt, grouper.Members{
			{Name: "backend-0", Runner: dummies.NewBackendRunner(0, backends[0])},
			{Name: "backend-1", Runner: dummies.NewBackendRunner(1, backends[1])},
			{Name: "healthcheck-0", Runner: healthcheckRunners[0]},
			{Name: "healthcheck-1", Runner: healthcheckRunners[1]},
			{Name: "switchboard", Runner: switchboardRunner},
		})
		process = ifrit.Invoke(sigmon.New(group))
	})

	AfterEach(func() {
		ginkgomon.Interrupt(process, 5*time.Second)
	})

	Context("When consul is not configured", func() {
		Context("and switchboard starts successfully", func() {
			JustBeforeEach(func() {
				var response Response

				Eventually(func() error {
					url := fmt.Sprintf("http://localhost:%d/v0/cluster", switchboardAPIPort)
					req, err := http.NewRequest("GET", url, nil)
					if err != nil {
						return err
					}
					req.SetBasicAuth("username", "password")

					client := &http.Client{}
					resp, err := client.Do(req)
					if err != nil {
						return err
					}

					var returnedCluster api.ClusterJSON
					decoder := json.NewDecoder(resp.Body)
					err = decoder.Decode(&returnedCluster)
					if err != nil {
						return err
					}
					if returnedCluster.ActiveBackend == nil || returnedCluster.ActiveBackend.Port != backends[0].Port {
						return errors.New("Expected backend not active yet")
					}

					return err
				}, startupTimeout).Should(Succeed())

				Eventually(func() error {
					url := fmt.Sprintf("http://localhost:%d/v0/backends", switchboardAPIPort)
					req, err := http.NewRequest("GET", url, nil)
					if err != nil {
						return err
					}
					req.SetBasicAuth("username", "password")

					client := &http.Client{}
					resp, err := client.Do(req)
					if err != nil {
						return err
					}

					var returnedBackends []api.V0BackendResponse
					decoder := json.NewDecoder(resp.Body)
					err = decoder.Decode(&returnedBackends)
					if err != nil {
						return err
					}

					for _, backend := range returnedBackends {
						if backend.Healthy == true && backend.Active == false {
							return nil
						}
					}

					return errors.New("Inactive backend never became healthy")
				}, startupTimeout).Should(Succeed())

				Eventually(func() error {
					conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%d", proxyPort))
					if err != nil {
						return err
					}
					defer conn.Close()

					response, err = sendData(conn, "detect active")
					return err
				}, startupTimeout).Should(Succeed())

				initialActiveBackend = backends[response.BackendIndex]
				initialInactiveBackend = backends[(response.BackendIndex+1)%2]
			})

			It("writes its PidFile", func() {
				finfo, err := os.Stat(pidFile)
				Expect(err).NotTo(HaveOccurred())
				Expect(finfo.Mode().Perm()).To(Equal(os.FileMode(0644)))
			})

			Describe("Health", func() {
				var acceptsAndClosesTCPConnections = func() {
					var err error
					var conn net.Conn
					Eventually(func() error {
						conn, err = net.Dial("tcp", fmt.Sprintf("localhost:%d", rootConfig.HealthPort))
						if err != nil {
							return err
						}
						return nil

					}, startupTimeout).Should(Succeed())
					defer conn.Close()

				}

				It("accepts and immediately closes TCP connections on HealthPort", func() {
					acceptsAndClosesTCPConnections()
				})

				Context("when HealthPort == API.Port", func() {
					BeforeEach(func() {
						rootConfig.HealthPort = rootConfig.API.Port
					})

					It("operates normally", func() {
						acceptsAndClosesTCPConnections()
					})
				})
			})

			Describe("API Aggregator", func() {
				Describe("/", func() {
					var url string

					BeforeEach(func() {
						url = fmt.Sprintf("http://localhost:%d/", switchboardAPIAggregatorPort)
					})

					It("prompts for Basic Auth creds when they aren't provided", func() {
						resp, err := http.Get(url)
						Expect(err).NotTo(HaveOccurred())
						Expect(resp.StatusCode).To(Equal(http.StatusUnauthorized))
						Expect(resp.Header.Get("WWW-Authenticate")).To(Equal(`Basic realm="Authorization Required"`))
					})

					It("does not accept bad Basic Auth creds", func() {
						req, err := http.NewRequest("GET", url, nil)
						req.SetBasicAuth("bad_username", "bad_password")
						client := &http.Client{}
						resp, err := client.Do(req)

						Expect(err).NotTo(HaveOccurred())
						Expect(resp.StatusCode).To(Equal(http.StatusUnauthorized))
					})

					It("responds with 200 and contains proxy URIs when authorized", func() {
						req, err := http.NewRequest("GET", url, nil)
						req.SetBasicAuth("username", "password")
						client := &http.Client{}
						resp, err := client.Do(req)
						Expect(err).NotTo(HaveOccurred())
						Expect(resp.StatusCode).To(Equal(http.StatusOK))

						Expect(resp.Body).ToNot(BeNil())
						defer resp.Body.Close()
						body, err := ioutil.ReadAll(resp.Body)
						Expect(len(body)).To(BeNumerically(">", 0), "Expected body to not be empty")

						Expect(string(body)).To(ContainSubstring(apiConfig.ProxyURIs[0]))
						Expect(string(body)).To(ContainSubstring(apiConfig.ProxyURIs[1]))
					})
				})
			})

			Describe("UI", func() {
				Describe("/", func() {
					var url string

					BeforeEach(func() {
						url = fmt.Sprintf("http://localhost:%d/", switchboardAPIPort)
					})

					It("prompts for Basic Auth creds when they aren't provided", func() {
						resp, err := http.Get(url)
						Expect(err).NotTo(HaveOccurred())
						Expect(resp.StatusCode).To(Equal(http.StatusUnauthorized))
						Expect(resp.Header.Get("WWW-Authenticate")).To(Equal(`Basic realm="Authorization Required"`))
					})

					It("does not accept bad Basic Auth creds", func() {
						req, err := http.NewRequest("GET", url, nil)
						req.SetBasicAuth("bad_username", "bad_password")
						client := &http.Client{}
						resp, err := client.Do(req)

						Expect(err).NotTo(HaveOccurred())
						Expect(resp.StatusCode).To(Equal(http.StatusUnauthorized))
					})

					It("responds with 200 and contains non-zero body when authorized", func() {
						req, err := http.NewRequest("GET", url, nil)
						req.SetBasicAuth("username", "password")
						client := &http.Client{}
						resp, err := client.Do(req)
						Expect(err).NotTo(HaveOccurred())
						Expect(resp.StatusCode).To(Equal(http.StatusOK))

						Expect(resp.Body).ToNot(BeNil())
						defer resp.Body.Close()
						body, err := ioutil.ReadAll(resp.Body)
						Expect(len(body)).To(BeNumerically(">", 0), "Expected body to not be empty")
					})
				})
			})

			Describe("api", func() {
				Describe("/v0/backends/", func() {
					var url string

					BeforeEach(func() {
						url = fmt.Sprintf("http://localhost:%d/v0/backends", switchboardAPIPort)
					})

					It("prompts for Basic Auth creds when they aren't provided", func() {
						resp, err := http.Get(url)
						Expect(err).NotTo(HaveOccurred())
						Expect(resp.StatusCode).To(Equal(http.StatusUnauthorized))
						Expect(resp.Header.Get("WWW-Authenticate")).To(Equal(`Basic realm="Authorization Required"`))
					})

					It("does not accept bad Basic Auth creds", func() {
						req, err := http.NewRequest("GET", url, nil)
						req.SetBasicAuth("bad_username", "bad_password")
						client := &http.Client{}
						resp, err := client.Do(req)

						Expect(err).NotTo(HaveOccurred())
						Expect(resp.StatusCode).To(Equal(http.StatusUnauthorized))
					})

					Context("When authorized", func() {
						var req *http.Request

						BeforeEach(func() {
							var err error
							req, err = http.NewRequest("GET", url, nil)
							Expect(err).NotTo(HaveOccurred())
							req.SetBasicAuth("username", "password")
						})

						It("returns correct headers", func() {
							client := &http.Client{}
							resp, err := client.Do(req)
							Expect(err).NotTo(HaveOccurred())
							Expect(resp.StatusCode).To(Equal(http.StatusOK))
							verifyHeaderContains(resp.Header, "Content-Type", "application/json")
						})

						It("returns valid JSON in body", func() {
							returnedBackends := getBackendsFromApi(req)

							Expect(len(returnedBackends)).To(Equal(2))

							Expect(returnedBackends[0]["host"]).To(Equal("localhost"))
							Expect(returnedBackends[0]["healthy"]).To(BeTrue(), "Expected backends[0] to be healthy")

							Expect(returnedBackends[1]["host"]).To(Equal("localhost"))
							Expect(returnedBackends[1]["healthy"]).To(BeTrue(), "Expected backends[1] to be healthy")

							if returnedBackends[0]["active"] == true {
								Expect(returnedBackends[1]["active"]).To(BeFalse())
							} else {
								Expect(returnedBackends[1]["active"]).To(BeTrue())
							}

							switch returnedBackends[0]["name"] {

							case backends[0].Name:
								Expect(returnedBackends[0]["port"]).To(BeNumerically("==", backends[0].Port))
								Expect(returnedBackends[1]["port"]).To(BeNumerically("==", backends[1].Port))
								Expect(returnedBackends[1]["name"]).To(Equal(backends[1].Name))

							case backends[1].Name: // order reversed in response
								Expect(returnedBackends[1]["port"]).To(BeNumerically("==", backends[0].Port))
								Expect(returnedBackends[0]["port"]).To(BeNumerically("==", backends[1].Port))
								Expect(returnedBackends[0]["name"]).To(Equal(backends[1].Name))
							default:
								Fail(fmt.Sprintf("Invalid backend name: %s", returnedBackends[0]["name"]))
							}
						})

						It("returns session count for backends", func() {
							var err error
							var conn net.Conn
							Eventually(func() error {
								conn, err = net.Dial("tcp", fmt.Sprintf("localhost:%d", proxyPort))
								if err != nil {
									return err
								}
								return nil

							}, startupTimeout).Should(Succeed())
							defer conn.Close()

							connData, err := sendData(conn, "success")
							Expect(err).ToNot(HaveOccurred())
							Expect(connData.Message).To(Equal("success"))

							returnedBackends := getBackendsFromApi(req)

							Eventually(func() interface{} {
								return getBackendsFromApi(req)[0]["currentSessionCount"]
							}).Should(BeNumerically("==", 1), "Expected active backend to have SessionCount == 1")

							Expect(returnedBackends[1]["currentSessionCount"]).To(BeNumerically("==", 0), "Expected inactive backend to have SessionCount == 0")
						})
					})
				})
			})

			Describe("/v0/cluster", func() {
				Describe("GET", func() {
					It("returns valid JSON in body", func() {
						url := fmt.Sprintf("http://localhost:%d/v0/cluster", switchboardAPIPort)
						req, err := http.NewRequest("GET", url, nil)
						Expect(err).NotTo(HaveOccurred())
						req.SetBasicAuth("username", "password")

						returnedCluster := getClusterFromAPI(req)

						Expect(returnedCluster["trafficEnabled"]).To(BeTrue())
					})
				})

				Describe("PATCH", func() {
					It("returns valid JSON in body", func() {
						url := fmt.Sprintf("http://localhost:%d/v0/cluster?trafficEnabled=true", switchboardAPIPort)
						req, err := http.NewRequest("PATCH", url, nil)
						Expect(err).NotTo(HaveOccurred())
						req.SetBasicAuth("username", "password")

						returnedCluster := getClusterFromAPI(req)

						Expect(returnedCluster["trafficEnabled"]).To(BeTrue())
						Expect(returnedCluster["lastUpdated"]).NotTo(BeEmpty())
					})

					It("persists the provided value of enableTraffic", func() {
						url := fmt.Sprintf("http://localhost:%d/v0/cluster?trafficEnabled=false&message=some-reason", switchboardAPIPort)
						req, err := http.NewRequest("PATCH", url, nil)
						Expect(err).NotTo(HaveOccurred())
						req.SetBasicAuth("username", "password")

						returnedCluster := getClusterFromAPI(req)

						Expect(returnedCluster["trafficEnabled"]).To(BeFalse())

						url = fmt.Sprintf("http://localhost:%d/v0/cluster?trafficEnabled=true", switchboardAPIPort)
						req, err = http.NewRequest("PATCH", url, nil)
						Expect(err).NotTo(HaveOccurred())
						req.SetBasicAuth("username", "password")

						returnedCluster = getClusterFromAPI(req)

						Expect(returnedCluster["trafficEnabled"]).To(BeTrue())
					})
				})
			})

			Describe("proxy", func() {
				Context("when connecting to the active port", func() {

					Context("when there are multiple concurrent clients", func() {
						It("proxies all the connections to the lowest indexed backend", func() {
							var doneArray = make([]chan interface{}, 3)
							var dataMessages = make([]Response, 3)

							for i := 0; i < 3; i++ {
								doneArray[i] = make(chan interface{})
								go func(index int) {
									defer GinkgoRecover()
									defer close(doneArray[index])

									var err error
									var conn net.Conn

									Eventually(func() error {
										conn, err = net.Dial("tcp", fmt.Sprintf("localhost:%d", proxyPort))
										return err
									}, startupTimeout).ShouldNot(HaveOccurred())

									data, err := sendData(conn, fmt.Sprintf("test%d", index))
									Expect(err).ToNot(HaveOccurred())
									dataMessages[index] = data
								}(i)
							}

							for _, done := range doneArray {
								<-done
							}

							for i, dataResponse := range dataMessages {
								Expect(dataResponse.Message).Should(Equal(fmt.Sprintf("test%d", i)))
								Expect(dataResponse.BackendIndex).To(BeEquivalentTo(0))
							}
						})
					})

					Context("when other clients disconnect", func() {
						var conn net.Conn
						var connToDisconnect net.Conn

						It("maintains a long-lived connection", func() {
							Eventually(func() error {
								var err error
								conn, err = net.Dial("tcp", fmt.Sprintf("localhost:%d", proxyPort))
								return err
							}, startupTimeout).Should(Succeed())

							Eventually(func() error {
								var err error
								connToDisconnect, err = net.Dial("tcp", fmt.Sprintf("localhost:%d", proxyPort))
								return err
							}, "5s").Should(Succeed())

							dataBeforeDisconnect, err := sendData(conn, "data before disconnect")
							Expect(err).ToNot(HaveOccurred())
							Expect(dataBeforeDisconnect.Message).Should(Equal("data before disconnect"))

							_ = connToDisconnect.Close()

							dataAfterDisconnect, err := sendData(conn, "data after disconnect")
							Expect(err).ToNot(HaveOccurred())
							Expect(dataAfterDisconnect.Message).Should(Equal("data after disconnect"))
						})
					})

					Context("when the healthcheck succeeds", func() {
						It("checks health again after the specified interval", func() {
							var client net.Conn
							Eventually(func() error {
								var err error
								client, err = net.Dial("tcp", fmt.Sprintf("localhost:%d", proxyPort))
								return err
							}, startupTimeout).Should(Succeed())

							data, err := sendData(client, "data around first healthcheck")
							Expect(err).NotTo(HaveOccurred())
							Expect(data.Message).To(Equal("data around first healthcheck"))

							Consistently(func() error {
								_, err = sendData(client, "data around subsequent healthcheck")
								return err
							}, 3*time.Second, 500*time.Millisecond).Should(Succeed())
						})
					})

					Context("when the cluster is down", func() {
						Context("when the healthcheck reports a 503", func() {
							It("disconnects client connections", func() {
								var conn net.Conn
								Eventually(func() error {
									var err error
									conn, err = net.Dial("tcp", fmt.Sprintf("localhost:%d", proxyPort))
									return err
								}, startupTimeout).Should(Succeed())

								dataWhileHealthy, err := sendData(conn, "data while healthy")
								Expect(err).ToNot(HaveOccurred())
								Expect(dataWhileHealthy.Message).To(Equal("data while healthy"))

								if initialActiveBackend == backends[0] {
									healthcheckRunners[0].SetStatusCode(http.StatusServiceUnavailable)
								} else {
									healthcheckRunners[1].SetStatusCode(http.StatusServiceUnavailable)
								}

								Eventually(func() error {
									_, err := sendData(conn, "data when unhealthy")
									return err
								}, healthcheckWaitDuration).Should(matchConnectionDisconnect())
							})
						})

						Context("when a backend goes down", func() {
							var conn net.Conn
							var data Response

							JustBeforeEach(func() {
								Eventually(func() (err error) {
									conn, err = net.Dial("tcp", fmt.Sprintf("localhost:%d", proxyPort))
									return err
								}, startupTimeout).Should(Succeed())

								data, err := sendData(conn, "data before hang")
								Expect(err).ToNot(HaveOccurred())
								Expect(data.Message).To(Equal("data before hang"))

								if initialActiveBackend == backends[0] {
									healthcheckRunners[0].SetHang(true)
								} else {
									healthcheckRunners[1].SetHang(true)
								}
							})

							It("disconnects existing client connections", func() {
								Eventually(func() error {
									_, err := sendData(conn, "data after hang")
									return err
								}, healthcheckWaitDuration).Should(matchConnectionDisconnect())
							})

							It("proxies new connections to another backend", func() {
								var err error
								Eventually(func() (uint, error) {
									conn, err = net.Dial("tcp", fmt.Sprintf("localhost:%d", proxyPort))
									if err != nil {
										return 0, err
									}

									data, err = sendData(conn, "test")
									return data.BackendPort, err
								}, healthcheckWaitDuration).Should(Equal(initialInactiveBackend.Port))

								Expect(data.Message).To(Equal("test"))
							})
						})

						Context("when all backends are down", func() {
							JustBeforeEach(func() {
								for _, hr := range healthcheckRunners {
									hr.SetHang(true)
								}
							})

							It("rejects any new connections that are attempted", func() {
								Eventually(func() error {
									conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%d", proxyPort))
									if err != nil {
										return err
									}
									_, err = sendData(conn, "write that should fail")
									return err
								}, healthcheckWaitDuration, 200*time.Millisecond).Should(matchConnectionDisconnect())
							})
						})
					})

					Context("when traffic is disabled", func() {
						It("disconnects client connections", func() {
							var conn net.Conn
							Eventually(func() error {
								var err error
								conn, err = net.Dial("tcp", fmt.Sprintf("localhost:%d", proxyPort))
								return err
							}, startupTimeout).Should(Succeed())
							defer conn.Close()

							dataWhileHealthy, err := sendData(conn, "data while healthy")
							Expect(err).ToNot(HaveOccurred())
							Expect(dataWhileHealthy.Message).To(Equal("data while healthy"))

							allowTraffic(false, switchboardAPIPort)

							Eventually(func() error {
								_, err := sendData(conn, "data when unhealthy")
								return err
							}, healthcheckWaitDuration).Should(matchConnectionDisconnect())
						})

						It("severs new connections", func() {
							allowTraffic(false, switchboardAPIPort)
							Eventually(func() error {
								conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%d", proxyPort))
								if err != nil {
									return err
								}
								defer conn.Close()
								_, err = sendData(conn, "write that should fail")

								return err
							}).Should(matchConnectionDisconnect())
						})

						It("permits new connections again after re-enabling traffic", func() {
							allowTraffic(false, switchboardAPIPort)
							allowTraffic(true, switchboardAPIPort)

							Eventually(func() error {
								var err error
								_, err = net.Dial("tcp", fmt.Sprintf("localhost:%d", proxyPort))
								return err
							}, "5s").Should(Succeed())
						})
					})
				})

				Context("when connecting to the inactive port", func() {
					JustBeforeEach(func() {
						Eventually(func() error {
							conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%d", proxyInactiveNodePort))
							if err != nil {
								return err
							}
							_, err = sendData(conn, "checking inactive port is ready")
							return err
						}, startupTimeout).ShouldNot(HaveOccurred(), "Switchboard inactive mysql port never became ready")
					})

					Context("when there are multiple concurrent clients", func() {
						It("proxies all the connections to the highest indexed backend", func() {
							var doneArray = make([]chan interface{}, 3)
							var dataMessages = make([]Response, 3)

							for i := 0; i < 3; i++ {
								doneArray[i] = make(chan interface{})
								go func(index int) {
									defer GinkgoRecover()
									defer close(doneArray[index])

									var err error
									var conn net.Conn

									Eventually(func() error {
										conn, err = net.Dial("tcp", fmt.Sprintf("localhost:%d", proxyInactiveNodePort))
										return err
									}, startupTimeout).ShouldNot(HaveOccurred())

									data, err := sendData(conn, fmt.Sprintf("test%d", index))
									Expect(err).ToNot(HaveOccurred())
									dataMessages[index] = data
								}(i)
							}

							for _, done := range doneArray {
								<-done
							}

							for i, dataResponse := range dataMessages {
								Expect(dataResponse.Message).Should(Equal(fmt.Sprintf("test%d", i)))
								Expect(dataResponse.BackendIndex).To(BeEquivalentTo(1))
							}
						})
					})

					Context("when other clients disconnect", func() {
						var conn net.Conn
						var connToDisconnect net.Conn

						It("maintains a long-lived connection when other clients disconnect", func() {
							Eventually(func() error {
								var err error
								conn, err = net.Dial("tcp", fmt.Sprintf("localhost:%d", proxyInactiveNodePort))
								return err
							}, startupTimeout).Should(Succeed())

							Eventually(func() error {
								var err error
								connToDisconnect, err = net.Dial("tcp", fmt.Sprintf("localhost:%d", proxyInactiveNodePort))
								return err
							}, "5s").Should(Succeed())

							dataBeforeDisconnect, err := sendData(conn, "data before disconnect")
							Expect(err).ToNot(HaveOccurred())
							Expect(dataBeforeDisconnect.Message).Should(Equal("data before disconnect"))

							_ = connToDisconnect.Close()

							dataAfterDisconnect, err := sendData(conn, "data after disconnect")
							Expect(err).ToNot(HaveOccurred())
							Expect(dataAfterDisconnect.Message).Should(Equal("data after disconnect"))
						})
					})

					Context("when the healthcheck succeeds", func() {
						It("checks health again after the specified interval", func() {
							var client net.Conn
							Eventually(func() error {
								var err error
								client, err = net.Dial("tcp", fmt.Sprintf("localhost:%d", proxyInactiveNodePort))
								return err
							}, startupTimeout).Should(Succeed())

							data, err := sendData(client, "data around first healthcheck")
							Expect(err).NotTo(HaveOccurred())
							Expect(data.Message).To(Equal("data around first healthcheck"))

							Consistently(func() error {
								_, err = sendData(client, "data around subsequent healthcheck")
								return err
							}, 3*time.Second, 500*time.Millisecond).Should(Succeed())
						})
					})

					Context("when the cluster is down", func() {
						Context("when the healthcheck reports a 503", func() {
							It("disconnects client connections", func() {
								var conn net.Conn
								Eventually(func() error {
									var err error
									conn, err = net.Dial("tcp", fmt.Sprintf("localhost:%d", proxyInactiveNodePort))
									return err
								}, startupTimeout).Should(Succeed())

								dataWhileHealthy, err := sendData(conn, "data while healthy")
								Expect(err).ToNot(HaveOccurred())
								Expect(dataWhileHealthy.Message).To(Equal("data while healthy"))

								if initialInactiveBackend == backends[0] {
									healthcheckRunners[0].SetStatusCode(http.StatusServiceUnavailable)
								} else {
									healthcheckRunners[1].SetStatusCode(http.StatusServiceUnavailable)
								}

								Eventually(func() error {
									_, err := sendData(conn, "data when unhealthy")
									return err
								}, healthcheckWaitDuration).Should(matchConnectionDisconnect())
							})
						})

						Context("when a backend goes down", func() {
							var conn net.Conn
							var data Response

							JustBeforeEach(func() {
								Eventually(func() (err error) {
									conn, err = net.Dial("tcp", fmt.Sprintf("localhost:%d", proxyInactiveNodePort))
									return err
								}, startupTimeout).Should(Succeed())

								data, err := sendData(conn, "data before hang")
								Expect(err).ToNot(HaveOccurred())
								Expect(data.Message).To(Equal("data before hang"))

								if initialInactiveBackend == backends[0] {
									healthcheckRunners[0].SetHang(true)
								} else {
									healthcheckRunners[1].SetHang(true)
								}
							})

							It("disconnects existing client connections", func() {
								Eventually(func() error {
									_, err := sendData(conn, "data after hang")
									return err
								}, healthcheckWaitDuration).Should(matchConnectionDisconnect())
							})

							It("proxies new connections to another backend", func() {
								var err error
								Eventually(func() (uint, error) {
									conn, err = net.Dial("tcp", fmt.Sprintf("localhost:%d", proxyPort))
									if err != nil {
										return 0, err
									}

									data, err = sendData(conn, "test")
									return data.BackendPort, err
								}, healthcheckWaitDuration).Should(Equal(initialActiveBackend.Port))

								Expect(data.Message).To(Equal("test"))
							})
						})

						Context("when all backends are down", func() {
							JustBeforeEach(func() {
								for _, hr := range healthcheckRunners {
									hr.SetHang(true)
								}
							})

							It("rejects any new connections that are attempted", func() {

								Eventually(func() error {
									conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%d", proxyInactiveNodePort))
									if err != nil {
										return err
									}
									_, err = sendData(conn, "write that should fail")
									return err
								}, healthcheckWaitDuration, 200*time.Millisecond).Should(matchConnectionDisconnect())
							})
						})
					})

					Context("when traffic is disabled", func() {
						It("disconnects client connections", func() {
							var conn net.Conn
							Eventually(func() error {
								var err error
								conn, err = net.Dial("tcp", fmt.Sprintf("localhost:%d", proxyInactiveNodePort))
								return err
							}, startupTimeout).Should(Succeed())
							defer conn.Close()

							dataWhileHealthy, err := sendData(conn, "data while healthy")
							Expect(err).ToNot(HaveOccurred())
							Expect(dataWhileHealthy.Message).To(Equal("data while healthy"))

							allowTraffic(false, switchboardAPIPort)

							Eventually(func() error {
								_, err := sendData(conn, "data when unhealthy")
								return err
							}, healthcheckWaitDuration).Should(matchConnectionDisconnect())
						})

						It("severs new connections", func() {
							allowTraffic(false, switchboardAPIPort)
							Eventually(func() error {
								conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%d", proxyInactiveNodePort))
								if err != nil {
									return err
								}
								defer conn.Close()
								_, err = sendData(conn, "write that should fail")

								return err
							}).Should(matchConnectionDisconnect())
						})

						It("permits new connections again after re-enabling traffic", func() {
							allowTraffic(false, switchboardAPIPort)
							allowTraffic(true, switchboardAPIPort)

							Eventually(func() error {
								var err error
								_, err = net.Dial("tcp", fmt.Sprintf("localhost:%d", proxyInactiveNodePort))
								return err
							}, "5s").Should(Succeed())
						})
					})
				})

				Context("when inactive port is not configured", func() {
					BeforeEach(func() {
						rootConfig.Proxy.InactiveMysqlPort = 0
					})

					It("does not crash", func() {
						Eventually(func() error {
							conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%d", proxyInactiveNodePort))
							if err != nil {
								return err
							}
							_, err = sendData(conn, "write that should fail")
							return err
						}, healthcheckWaitDuration, 200*time.Millisecond).Should(MatchError(ContainSubstring("connection refused")))
					})

				})
			})
		})

		Context("and switchboard is failing", func() {
			BeforeEach(func() {
				rootConfig.StaticDir = "this is totally invalid so switchboard won't start"
			})

			It("does not write the PidFile", func() {
				Consistently(func() error {
					_, err := os.Stat(pidFile)
					return err
				}).Should(HaveOccurred())
			})
		})
	})

	Describe("consul", func() {
		var (
			consulRunner *consulrunner.ClusterRunner
			consulClient consuladapter.Client
		)

		BeforeEach(func() {
			consulConfig := consulrunner.ClusterRunnerConfig{
				StartingPort: 9001 + ginkgoconf.GinkgoConfig.ParallelNode*consulrunner.PortOffsetLength,
				NumNodes:     1,
				Scheme:       "http",
				CACert:       "",
				ClientCert:   "",
				ClientKey:    "",
			}

			consulRunner = consulrunner.NewClusterRunner(consulConfig)
			consulRunner.Start()
			consulRunner.WaitUntilReady()

			rootConfig.ConsulCluster = consulRunner.ConsulCluster()
			rootConfig.ConsulServiceName = "test_mysql"
			consulClient = consulRunner.NewClient()
		})

		AfterEach(func() {
			_ = consulRunner.Reset()
			consulRunner.Stop()
			rootConfig.ConsulCluster = ""
		})

		It("immediately writes its PidFile", func() {
			Eventually(func() os.FileMode {
				finfo, err := os.Stat(pidFile)
				if err != nil {
					return 0
				}
				return finfo.Mode().Perm()
			}, startupTimeout).Should(Equal(os.FileMode(0644)))
		})

		It("registers itself with consul", func() {
			Eventually(func() map[string]*consulapi.AgentService {
				services, _ := consulClient.Agent().Services()
				return services
			}, startupTimeout).Should(HaveKeyWithValue("test_mysql",
				&consulapi.AgentService{
					Service: "test_mysql",
					ID:      "test_mysql",
					Port:    int(proxyPort),
					Tags:    nil,
				}))
		})
	})
})
