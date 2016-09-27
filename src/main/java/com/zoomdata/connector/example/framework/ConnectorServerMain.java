/**
 * Copyright (C) Zoomdata, Inc. 2012-2016. All rights reserved.
 */
package com.zoomdata.connector.example.framework;

import com.zoomdata.connector.example.framework.annotation.Connector;
import com.zoomdata.connector.example.framework.provider.ConnectorBeanNameGenerator;
import com.zoomdata.connector.example.framework.service.ZoomdataConnectorService;
import com.zoomdata.gen.edc.ConnectorService;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServlet;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.annotation.*;

/*
 * This example uses Spring Boot with embedded Jetty to quickly and easily provide a HTTP server for serving
 * requests, but it's not mandatory. Any implementation can be used to provide HTTP or socket responses in any
 * coding language.
 */

@Configuration
@SpringBootApplication
@ComponentScan(basePackages = "com.zoomdata",
        includeFilters = {@ComponentScan.Filter(type = FilterType.ANNOTATION, value = Connector.class)},
        nameGenerator=ConnectorBeanNameGenerator.class)
@PropertySource("classpath:framework.properties")
public abstract class ConnectorServerMain {

    public ConnectorServerMain() {
    }

    public static void main(String[] args) {
        SpringApplication app = new SpringApplication(ConnectorServerMain.class);
        app.run(args);
    }

    @Bean
    public TProtocolFactory thriftProtocolFactory() {
        return new TCompactProtocol.Factory();
    }

    @Bean
    public ServletRegistrationBean connector(TProtocolFactory protocolFactory, ZoomdataConnectorService handler){
        return new ServletRegistrationBean(new TServlet(new ConnectorService.Processor<>(handler), protocolFactory),
            "/connector/*");
    }
}
