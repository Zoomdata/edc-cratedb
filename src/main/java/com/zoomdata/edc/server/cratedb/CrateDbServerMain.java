/**
 * Copyright (C) Zoomdata, Inc. 2012-2016. All rights reserved.
 */
package com.zoomdata.edc.server.cratedb;

import com.zoomdata.gen.edc.ConnectorService;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 *  This is entry point to your server
 */
public final class CrateDbServerMain {

    // logger is not required, but helps you to detect problems
    public static final Logger log = LoggerFactory.getLogger(CrateDbServerMain.class);

    private static final ExecutorService executor = Executors.newCachedThreadPool();

    public static void main(String[] args) {
        // Parse input arguments and extract port
        if (args.length != 1) {
            log.error("Usage: <port>");
            System.exit(1);
        }
        int port = Integer.parseInt(args[0]);

        TServer server = null;
        try {

            // Create DataProvider, this is implementation of ConnectorService.IFace
            // This class is responsible to process your thrift calls
            final CrateDbDataProvider provider = new CrateDbDataProvider();

            // Create TProcessor, thrift level abstraction that wraps
            // your interface implementation
            final TProcessor processor = new ConnectorService.Processor<>(provider);

            //  Create Server by passing processor to it
            server = createServer(port, processor);

            // Start serving
            log.info("Server started on port " + port);
            server.serve();
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            executor.shutdown();
            if (server != null) {
                server.stop();
            }
        }

    }

    private static TServer createServer(int port, TProcessor processor) throws TTransportException {
        TServerTransport serverTransport = new TServerSocket(port);
        TServer server = new TThreadPoolServer(
                new TThreadPoolServer.Args(serverTransport)
                        .protocolFactory(new TCompactProtocol.Factory())
                        .processor(processor)
                        .executorService(executor)
        );
        return server;
    }
}
