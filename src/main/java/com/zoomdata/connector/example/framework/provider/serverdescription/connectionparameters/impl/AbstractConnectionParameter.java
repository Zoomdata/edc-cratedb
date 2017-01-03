/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.framework.provider.serverdescription.connectionparameters.impl;

import com.zoomdata.connector.example.framework.provider.serverdescription.connectionparameters.IConnectionParameter;
import com.zoomdata.connector.example.framework.provider.serverdescription.connectionparameters.IConnectionParameterBuilder;
import com.zoomdata.gen.edc.request.serverdescription.ConnectionParameter;
import com.zoomdata.gen.edc.request.serverdescription.ParameterType;

import java.util.Optional;

public class AbstractConnectionParameter implements IConnectionParameter {
    protected ConnectionParameter parameter;

    protected AbstractConnectionParameter(ParameterType type, String name, boolean isRequired, Optional<String> description) {
        parameter = new ConnectionParameter().setType(type).setName(name).setIsRequired(isRequired);
        description.ifPresent(value -> parameter.setDescription(value));
    }

    @Override
    public ConnectionParameter toThrift() {
        return parameter;
    }

    @Override
    public void validate(String value) {
        if (parameter.isIsRequired() && value == null) {
            throw new IllegalArgumentException(String.format("Parameter %s is required", parameter.getName()));
        }
    }

    @Override
    public String getName() {
        return parameter.getName();
    }

    public static abstract class AbstractConnectionParameterBuilder implements IConnectionParameterBuilder {
        protected String name;
        protected Optional<String> description = Optional.empty();
        protected boolean isRequired;

        protected AbstractConnectionParameterBuilder(String name) {
            this.name = name;
        }

        public AbstractConnectionParameterBuilder description(String input) {
            this.description = Optional.of(input);
            return this;
        }

        public AbstractConnectionParameterBuilder isRequired(boolean isRequired) {
            this.isRequired = isRequired;
            return this;
        }
    }
}
