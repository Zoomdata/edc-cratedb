/**
 * Copyright (C) Zoomdata, Inc. 2012-2016. All rights reserved.
 */
package com.zoomdata.connector.example.framework.provider.serverdescription.connectionparameters.impl;

import com.zoomdata.gen.edc.request.serverdescription.ParameterString;
import com.zoomdata.gen.edc.request.serverdescription.ParameterType;

import java.util.Optional;

public class StringConnectionParameter extends AbstractConnectionParameter {

    private StringConnectionParameter(String name,
                                      boolean isRequired,
                                      Optional<String> description,
                                      Optional<Integer> maxLength) {
        super(ParameterType.STRING, name, isRequired, description);
        parameter.setParameterString(new ParameterString());
        maxLength.ifPresent(value -> parameter.getParameterString().setMaxLength(value));
    }

    @Override
    public void validate(String value) {
        super.validate(value);
        if (parameter.getParameterString().isSetMaxLength() && value != null) {
            int maxLength = parameter.getParameterString().getMaxLength();
            if (value.length() > maxLength) {
                throw new IllegalArgumentException(
                    String.format("Maximum length of parameter %s is %s symbols", parameter.getName(), maxLength)
                );
            }
        }
    }

    public static class StringConnectionParameterBuilder extends AbstractConnectionParameter.AbstractConnectionParameterBuilder {

        private Optional<Integer> maxLength = Optional.empty();

        protected StringConnectionParameterBuilder(String name) {
            super(name);
        }

        public static StringConnectionParameterBuilder stringParameter(String name) {
            return new StringConnectionParameterBuilder(name);
        }

        public StringConnectionParameterBuilder maxLength(int input) {
            this.maxLength = Optional.of(input);
            return this;
        }

        @Override
        public StringConnectionParameter build() {
            return new StringConnectionParameter(name, isRequired, description, maxLength);
        }
    }
}
