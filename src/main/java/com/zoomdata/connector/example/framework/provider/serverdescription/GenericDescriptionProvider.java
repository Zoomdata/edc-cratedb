/**
 * Copyright (C) Zoomdata, Inc. 2012-2016. All rights reserved.
 */
package com.zoomdata.connector.example.framework.provider.serverdescription;

import com.zoomdata.connector.example.common.utils.CollectionUtils;
import com.zoomdata.connector.example.framework.api.IDescriptionProvider;
import com.zoomdata.connector.example.framework.provider.serverdescription.connectionparameters.IConnectionParameter;
import com.zoomdata.connector.example.framework.provider.serverdescription.connectionparameters.IConnectionParameterBuilder;
import com.zoomdata.gen.edc.request.RequestInfo;
import com.zoomdata.gen.edc.request.serverdescription.BinaryIconProperties;
import com.zoomdata.gen.edc.request.serverdescription.Icon;
import com.zoomdata.gen.edc.request.serverdescription.IconType;
import com.zoomdata.gen.edc.request.serverdescription.ServerDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.IOUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;

public class GenericDescriptionProvider implements IDescriptionProvider {
    private static final Logger LOG = LoggerFactory.getLogger(GenericDescriptionProvider.class);

    private final String storageType;
    private String minVersion;
    private String maxVersion;
    private Optional<Icon> iconFile = Optional.empty();
    private List<IConnectionParameter> parameters = Collections.emptyList();

    public GenericDescriptionProvider(String storageType) {
        this.storageType = storageType;
    }


    @Override
    public void validate(RequestInfo requestInfo) {
        Map<String, String> actualValues = requestInfo.getDataSourceInfo().getParams();
        if (CollectionUtils.isNotEmpty(parameters)) {
            parameters.forEach(param -> param.validate(actualValues.get(param.getName())));
        }
    }


    @Override
    public ServerDescription describe() {
        ServerDescription description = new ServerDescription()
            .setStorageType(storageType)
            .setMinSupportedVersion(minVersion)
            .setMaxSupportedVersion(maxVersion)
            .setConnectionParameters(parameters.stream().map(param -> param.toThrift()).collect(Collectors.toList()));
        iconFile.ifPresent(value -> description.setIcon(value));
        return description;
    }

    public GenericDescriptionProvider minVersion(String minVersion) {
        this.minVersion = minVersion;
        return this;
    }

    public GenericDescriptionProvider maxVersion(String maxVersion) {
        this.maxVersion = maxVersion;
        return this;
    }

    public GenericDescriptionProvider pngIcon(String iconFile) {
        try {
            byte[] image = IOUtils.readFully(this.getClass().getResourceAsStream(iconFile), -1, true);
            this.iconFile = Optional.of(
                new Icon().setIconType(IconType.BINARY)
                    .setBinaryIconProperties(
                        new BinaryIconProperties()
                            .setIcon(image)
                            .setImageType("image/png")));
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
        return this;
    }

    public GenericDescriptionProvider svgIcon(String iconFile) {
        try {
            byte[] image = IOUtils.readFully(this.getClass().getResourceAsStream(iconFile), -1, true);
            this.iconFile = Optional.of(
                new Icon().setIconType(IconType.BINARY)
                    .setBinaryIconProperties(
                        new BinaryIconProperties()
                            .setIcon(image)
                            .setImageType("image/svg+xml")));
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
        return this;
    }
    public GenericDescriptionProvider addParameters(IConnectionParameterBuilder... input) {
        return addParameters(asList(input));
    }

    public GenericDescriptionProvider addParameters(List<IConnectionParameterBuilder> input) {
        List<IConnectionParameter> newParams = input.stream().map(p -> p.build()).collect(Collectors.toList());
        this.parameters = CollectionUtils.isEmpty(parameters) ? newParams : CollectionUtils.union(this.parameters, newParams);
        return this;

    }

}
