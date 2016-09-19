/**
 * Copyright (C) Zoomdata, Inc. 2012-2016. All rights reserved.
 */
package com.zoomdata.connector.example.framework.provider.serverdescription.connectionparameters.impl;

import com.mysema.commons.lang.Assert;
import com.zoomdata.connector.example.framework.provider.serverdescription.connectionparameters.IConnectionParameter;
import com.zoomdata.gen.edc.request.serverdescription.ParameterInteger;
import com.zoomdata.gen.edc.request.serverdescription.ParameterType;

import java.util.Optional;
import java.util.regex.Pattern;

public class IntegerConnectionParameter extends AbstractConnectionParameter {
    private static String NUMBER_PATTERN = "^[-+]?\\d+$";

    protected IntegerConnectionParameter(String name,
                                         boolean isRequired,
                                         Optional<String> description,
                                         Optional<Integer> min,
                                         Optional<Integer> max) {
        super(ParameterType.INTEGER, name, isRequired, description);
        parameter.setParameterInteger(new ParameterInteger());
        min.ifPresent(value -> parameter.getParameterInteger().setMin(value));
        max.ifPresent(value -> parameter.getParameterInteger().setMax(value));
    }

    @Override
    public void validate(String value) {
        super.validate(value);
        if (value != null) {
            Assert.isTrue(Pattern.matches(NUMBER_PATTERN, value),
                String.format("Value %s of parameter %s is not valid integer", value, parameter.getName()));
            int valueInt = Integer.parseInt(value);
            if (parameter.getParameterInteger().isSetMin()) {
                int min = parameter.getParameterInteger().getMin();
                Assert.isTrue(valueInt >= min,
                    String.format("Value %s is less than minimum allowed value %s for parameter %s", value,
                        valueInt, min, parameter.getName()));
            }
            if (parameter.getParameterInteger().isSetMax()) {
                int max = parameter.getParameterInteger().getMax();
                Assert.isTrue(valueInt <= max,
                    String.format("Value %s is greater than maxinum allowed value %s for parameter %s", value,
                        valueInt, max, parameter.getName()));
            }
        }
    }

    public static class IntegerConnectionParameterBuilder extends AbstractConnectionParameter.AbstractConnectionParameterBuilder {

        private Optional<Integer> min = Optional.empty();
        private Optional<Integer> max = Optional.empty();

        protected IntegerConnectionParameterBuilder(String name) {
            super(name);
        }

        public static IntegerConnectionParameterBuilder intParameter(String name) {
            return new IntegerConnectionParameterBuilder(name);
        }

        public IntegerConnectionParameterBuilder min(int input) {
            this.min = Optional.of(input);
            return this;
        }

        public IntegerConnectionParameterBuilder max(int input) {
            this.max = Optional.of(input);
            return this;
        }

        @Override
        public IConnectionParameter build() {
            return new IntegerConnectionParameter(name, isRequired, description, min, max);
        }
    }
}
