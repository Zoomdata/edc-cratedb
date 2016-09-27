/**
 * Copyright (C) Zoomdata, Inc. 2012-2016. All rights reserved.
 */
package com.zoomdata.connector.example.framework.provider.serverdescription.connectionparameters.impl;

import com.mysema.commons.lang.Assert;
import com.zoomdata.gen.edc.request.serverdescription.ParameterType;

import java.util.Optional;
import java.util.regex.Pattern;

public class BooleanConnectionParameter extends AbstractConnectionParameter {
    private static final String BOOLEAN_VALUE_PATTERN = "(true)|(false)";

    private BooleanConnectionParameter(String name, boolean isRequired, Optional<String> description) {
        super(ParameterType.BOOLEAN, name, isRequired, description);
    }

    @Override
    public void validate(String value) {
        super.validate(value);
        if (value != null) {
            Assert.isTrue(Pattern.matches(BOOLEAN_VALUE_PATTERN, value),
                String.format("Value %s of parameter %s is not valid boolean value", value, parameter.getName()));
        }
    }

    public static class BooleanConnectionParameterBuilder extends AbstractConnectionParameter.AbstractConnectionParameterBuilder {

        protected BooleanConnectionParameterBuilder(String name) {
            super(name);
        }

        public static BooleanConnectionParameterBuilder booleanParameter(String name) {
            return new BooleanConnectionParameterBuilder(name);
        }

        @Override
        public BooleanConnectionParameter build() {
            return new BooleanConnectionParameter(name, isRequired, description);
        }
    }
}
