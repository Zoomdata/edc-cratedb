/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.framework.provider.serverdescription.connectionparameters.impl;

import com.zoomdata.gen.edc.request.serverdescription.ParameterType;

import java.util.Optional;

public class PasswordConnectionParameter extends AbstractConnectionParameter {
    private PasswordConnectionParameter(String name, boolean isRequired, Optional<String> description) {
        super(ParameterType.PASSWORD, name, isRequired, description);
    }


    public static class PasswordConnectionParameterBuilder extends AbstractConnectionParameter.AbstractConnectionParameterBuilder {

        protected PasswordConnectionParameterBuilder(String name) {
            super(name);
        }

        public static PasswordConnectionParameterBuilder passwordParameter(String name) {
            return new PasswordConnectionParameterBuilder(name);
        }

        @Override
        public PasswordConnectionParameter build() {
            return new PasswordConnectionParameter(name, isRequired, description);
        }
    }
}
