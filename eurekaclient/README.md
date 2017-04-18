#Sample Plugin - Eureka

##Overview
This plugin integrates with [NetFlix's Eureka](https://github.com/Netflix/eureka). Eureka is a REST (Representational State Transfer) based service that is primarily used in the AWS cloud for locating services for the purpose of load balancing and failover of middle-tier servers.


##Enable the plugin
Include the plugin the in plugin sequence of {org}-{env}-config.yaml file:
```
  plugins:
    sequence:
      - oauth
      - eurekaclient
```

##Configure the plugin
The plugin configuration has three parts:
* (instance) Defining the microgateway instance. This registers microgateway with Eureka
* (eureka) Provide the endpoint details to where Eureka is hosted
* (lookup) See below for details
```
eurekaclient:
	instance:
	    app: microgateway
	    vipAddress: microgateway
	    dataCenterInfo: 
	    	name: MyOwn
	eureka:
	    host: localhost
	    port: 8761
	    servicePath: /eureka/apps/
	lookup:
		-
		    uri: /book
		    app: BOOK
		    secure: false
		-
		    uri: /httpbin
		    app: BOOK
		    secure: false	
```

###Lookup Configuration
Eureka registers apps (BOOK in the example above). There needs to be a mapping to map the API Proxy basePath to an app registered on Eureka. In the example above, the basePath /httpbin and /book maps to the BOOK app.

NOTE: If a mapping is not found, then the plugins forwards the API traffic as configured in the proxy.


