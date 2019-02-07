/**
 * Copyright 2016 Comcast Cable Communications Management, LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package main

import (
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"

	"github.com/Comcast/webpa-common/concurrent"
	"github.com/Comcast/webpa-common/device"
	"github.com/Comcast/webpa-common/logging"
	"github.com/Comcast/webpa-common/logging/logginghttp"
	"github.com/Comcast/webpa-common/server"
	"github.com/Comcast/webpa-common/service"
	"github.com/Comcast/webpa-common/service/monitor"
	"github.com/Comcast/webpa-common/service/servicecfg"
	"github.com/Comcast/webpa-common/service/servicehttp"
	"github.com/Comcast/webpa-common/xhttp/gate"
	"github.com/Comcast/webpa-common/xhttp/xcontext"
	"github.com/go-kit/kit/log/level"
	"github.com/justinas/alice"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

const (
	applicationName       = "petasos"
	release               = "Developer"
	defaultVnodeCount int = 211

	baseURI = "/api"
	version = "v1"
)

// petasos is the driver function for Petasos.  It performs everything main() would do,
// except for obtaining the command-line arguments (which are passed to it).
func petasos(arguments []string) int {
	//
	// Initialize the server environment: command-line flags, Viper, logging, and the WebPA instance
	//

	var (
		f = pflag.NewFlagSet(applicationName, pflag.ContinueOnError)
		v = viper.New()

		logger, metricsRegistry, webPA, err = server.Initialize(applicationName, arguments, f, v, service.Metrics)
		infoLog                             = logging.Info(logger)
		errorLog                            = logging.Error(logger)
	)

	if err != nil {
		errorLog.Log(logging.MessageKey(), "Unable to initialize Viper environment", logging.ErrorKey(), err)
		return 1
	}

	//
	// Now, initialize the service discovery infrastructure
	//

	e, err := servicecfg.NewEnvironment(logger, v.Sub("service"))
	if err != nil {
		errorLog.Log(logging.MessageKey(), "Unable to initialize service discovery environment", logging.ErrorKey(), err)
		return 2
	} else if e == nil {
		errorLog.Log(logging.MessageKey(), "Petasos requires service discovery")
		return 2
	}

	infoLog.Log("configurationFile", v.ConfigFileUsed())

	var (
		accessor = service.NewLayeredAccesor(service.DefaultTrafficRouter(), service.DefaultOrder())

		redirectHandler = &servicehttp.RedirectHandler{
			KeyFunc:      device.IDHashParser,
			Accessor:     accessor,
			RedirectCode: http.StatusTemporaryRedirect,
		}

		requestFunc      = logginghttp.SetLogger(logger, logginghttp.Header("X-Webpa-Device-Name", "device_id"), logginghttp.Header("Authorization", "authorization"))
		decoratedHandler = alice.New(xcontext.Populate(0, requestFunc)).Then(redirectHandler)

		_, petasosServer, done = webPA.Prepare(logger, nil, metricsRegistry, decoratedHandler)
		signals                = make(chan os.Signal, 1)

		controlRegions = make(map[string]gate.Interface)
	)

	g := gate.New(true, gate.WithGauge(metricsRegistry.NewGauge("gate_status")))

	vNodeCount := v.Sub("service").GetInt("vnodeCount")
	if vNodeCount < 1 {
		vNodeCount = defaultVnodeCount
	}

	_, err = monitor.New(
		monitor.WithLogger(logger),
		monitor.WithEnvironment(e),
		monitor.WithListeners(
			monitor.NewMetricsListener(metricsRegistry),
			monitor.NewAccessorListener(service.NewConsistentAccessorFactoryWithGate(vNodeCount, g), accessor.UpdatePrimary),
		),
	)

	controlRegions["primary"] = g

	if err != nil {
		errorLog.Log(logging.MessageKey(), "Unable to start service discovery monitor", logging.ErrorKey(), err)
		return 3
	}

	redundancy := v.GetStringMap("redundancy")
	for region := range redundancy {
		region := strings.TrimSpace(region)
		if len(region) == 0 {
			errorLog.Log(logging.MessageKey(), "Unable to initialize empty region")
			continue
		}
		redundancyEnv, err := servicecfg.NewEnvironment(logger, v.Sub("redundancy").Sub(region))
		vNodeCount := v.Sub("redundancy").Sub(region).GetInt("vnodeCount")
		if vNodeCount < 1 {
			vNodeCount = defaultVnodeCount
		}
		if err != nil {
			errorLog.Log(logging.MessageKey(), "Unable to initialize service discovery environment", logging.ErrorKey(), err, "region", region)
			continue
		}
		g := gate.New(true, gate.WithGauge(metricsRegistry.NewGauge("gate_"+region+"_status")))

		_, err = monitor.New(
			monitor.WithLogger(logging.Debug(logger, "region", region)),
			monitor.WithEnvironment(redundancyEnv),
			monitor.WithListeners(
				monitor.NewKeyAccessorListener(service.NewConsistentAccessorFactoryWithGate(vNodeCount, g), region, accessor.UpdateFailOver),
			))
		if err != nil {
			errorLog.Log(logging.MessageKey(), "Unable to start service discovery monitor", logging.ErrorKey(), err, "region", region)
			continue
		}

		infoLog.Log(logging.MessageKey(), "Successfully started service monitor", "region", region)
		// create Gate

		controlRegions[region] = g
	}

	err = StartControlServer(logger, controlRegions, v, webPA)
	if err != nil {
		logger.Log(level.Key(), level.ErrorValue(), logging.MessageKey(), "Unable to create control server", logging.ErrorKey(), err)
		return 3
	}

	//
	// Execute the runnable, which runs all the servers, and wait for a signal
	//
	waitGroup, shutdown, err := concurrent.Execute(petasosServer)
	if err != nil {
		errorLog.Log(logging.MessageKey(), "Unable to start petasos", logging.ErrorKey(), err)
		return 4
	}

	signal.Notify(signals)
	for exit := false; !exit; {
		select {
		case s := <-signals:
			if s != os.Kill && s != os.Interrupt {
				logger.Log(level.Key(), level.InfoValue(), logging.MessageKey(), "ignoring signal", "signal", s)
			} else {
				logger.Log(level.Key(), level.ErrorValue(), logging.MessageKey(), "exiting due to signal", "signal", s)
				exit = true
			}

		case <-done:
			logger.Log(level.Key(), level.ErrorValue(), logging.MessageKey(), "one or more servers exited")
			exit = true
		}
	}

	close(shutdown)
	waitGroup.Wait()

	return 0
}

func main() {
	os.Exit(petasos(os.Args))
}
