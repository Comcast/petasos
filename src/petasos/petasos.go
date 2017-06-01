package main

import (
	"fmt"
	"github.com/Comcast/webpa-common/concurrent"
	"github.com/Comcast/webpa-common/device"
	"github.com/Comcast/webpa-common/server"
	"github.com/Comcast/webpa-common/service"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"net/http"
	_ "net/http/pprof"
	"os"
)

const (
	applicationName       = "petasos"
	release               = "Developer"
	defaultVnodeCount int = 211
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

		logger, webPA, err = server.Initialize(applicationName, arguments, f, v)
	)

	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to initialize Viper environment: %s\n", err)
		return 1
	}

	logger.Info("Using configuration file: %s", v.ConfigFileUsed())

	//
	// Now, initialize the service discovery infrastructure
	//

	serviceOptions, registrar, _, err := service.Initialize(logger, nil, v.Sub(service.DiscoveryKey))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to initialize service discovery: %s\n", err)
		return 2
	}

	logger.Info("Service options: %#v", serviceOptions)

	var (
		accessor     = service.NewUpdatableAccessor(serviceOptions, nil)
		subscription = service.Subscription{
			Logger:    logger,
			Registrar: registrar,
			Listener:  accessor.Update,
		}

		redirectHandler = service.NewRedirectHandler(
			accessor,
			http.StatusTemporaryRedirect,
			device.IDHashParser,
			logger,
		)

		_, runnable = webPA.Prepare(logger, redirectHandler)
		signals     = make(chan os.Signal, 1)
	)

	if err := subscription.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "Unable to run subscription: %s", err)
		return 3
	}

	//
	// Execute the runnable, which runs all the servers, and wait for a signal
	//

	if err := concurrent.Await(runnable, signals); err != nil {
		fmt.Fprintf(os.Stderr, "Error when starting %s: %s", applicationName, err)
		return 4
	}

	return 0
}

func main() {
	os.Exit(petasos(os.Args))
}
