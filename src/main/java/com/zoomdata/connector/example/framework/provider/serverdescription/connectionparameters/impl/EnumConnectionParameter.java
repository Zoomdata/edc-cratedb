/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.framework.provider.serverdescription.connectionparameters.impl;

import com.zoomdata.gen.edc.request.serverdescription.ParameterEnum;
import com.zoomdata.gen.edc.request.serverdescription.ParameterType;

import java.util.List;
import java.util.Optional;

public class EnumConnectionParameter extends AbstractConnectionParameter {

    private EnumConnectionParameter(String name,
                                    boolean isRequired,
                                    Optional<String> description,
                                    List<String> values) {
        super(ParameterType.ENUM, name, isRequired, description);
        parameter.setParameterEnum(new ParameterEnum().setValues(values));
    }

    @Override
    public void validate(String value) {
        super.validate(value);
        if (parameter.getParameterEnum().isSetValues() && value != null) {
            List<String> values = parameter.getParameterEnum().getValues();
            if (!values.contains(value)) {
                throw new IllegalArgumentException(
                    String.format("Allowed values for parameter %s are %s", parameter.getName(), String.join(", ", values))
                );
            }
        }
    }

    public static class EnumConnectionParameterBuilder extends AbstractConnectionParameter.AbstractConnectionParameterBuilder {

        private final List<String> values;

        protected EnumConnectionParameterBuilder(String name, List<String> values) {
            super(name);
            this.values = values;
        }

        public static EnumConnectionParameterBuilder enumParameter(String name, List<String> values) {
            return new EnumConnectionParameterBuilder(name, values);
        }

        @Override
        public EnumConnectionParameter build() {
            return new EnumConnectionParameter(name, isRequired, description, values);
        }
    }
}
