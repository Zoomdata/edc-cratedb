/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.common.utils;

import com.zoomdata.gen.edc.ConnectorService;
import com.zoomdata.gen.edc.request.CollectionInfo;
import com.zoomdata.gen.edc.types.Field;
import com.zoomdata.gen.edc.types.FieldType;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.THttpClient;
import org.apache.thrift.transport.TSSLTransportFactory;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.util.Collections;
import java.util.Date;

public final class ThriftUtils {
    public ThriftUtils() { }

    private static final String HTTP_PROTOCOL = "http";
    private static final String HTTPS_PROTOCOL = "https";
    private static final String HTTP_CLIENT_FORMAT = "%s://%s:%s/connector/";
    private static DateTimeFormatter DATE_TIME_FORMATTER = ISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC);


    public static ConnectorService.Client createHttpClient(String host, int port, boolean useSSL) throws TTransportException {
        TTransport transport = new THttpClient(
                String.format(HTTP_CLIENT_FORMAT, (useSSL) ? HTTPS_PROTOCOL : HTTP_PROTOCOL , host, port));
        TProtocol protocol = new TCompactProtocol.Factory().getProtocol(transport);
        return new ConnectorService.Client(protocol);
    }

    public static ConnectorService.Client createSocketClient(String host, int port, boolean useSSL) throws TTransportException {
        TTransport transport;
        if (useSSL) {
            TSSLTransportFactory.TSSLTransportParameters params = new TSSLTransportFactory.TSSLTransportParameters();
            params.setTrustStore(System.getProperty("ssl.truststore"), System.getProperty("ssl.truststore.password"));
            transport = TSSLTransportFactory.getClientSocket(host, port, 0, params);
        } else {
            transport = new TSocket(host, port);
        }
        TProtocol protocol = new TCompactProtocol.Factory().getProtocol(transport);
        ConnectorService.Client client = new ConnectorService.Client(protocol);
        if (!transport.isOpen()) {
            transport.open();
        }
        return client;
    }

    public static Long getInteger(Field value) {
        return value.isIsNull() ? null : Long.parseLong(value.getValue());
    }

    public static Double getDouble(Field value) {
        return value.isIsNull() ? null : Double.parseDouble(value.getValue());
    }

    public static String getString(Field value) {
        return value.isIsNull() ? null : value.getValue();
    }

    public static DateTime getDateTime(Field value) {
        return value.isIsNull() ? null : DATE_TIME_FORMATTER.parseDateTime(value.getValue());
    }

    public static FieldType detectType(Object value) {
        if (value instanceof Short || value instanceof Integer || value instanceof Long) {
            return FieldType.INTEGER;
        }
        if (value instanceof Number) {
            return FieldType.DOUBLE;
        }
        if (value instanceof Date) {
            return FieldType.DATE;
        }
        return FieldType.STRING;
    }

    public static CollectionInfo getCollectionInfo(String schema, String tableName) {
        CollectionInfo ci = new CollectionInfo();
        ci.setSchema(schema);
        ci.setCollection(tableName);
        ci.setParams(Collections.emptyMap());
        return ci;
    }
}
