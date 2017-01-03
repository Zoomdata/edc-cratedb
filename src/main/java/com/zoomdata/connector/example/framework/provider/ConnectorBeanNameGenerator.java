/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.framework.provider;

import com.zoomdata.connector.example.framework.annotation.Connector;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.annotation.AnnotationBeanNameGenerator;
import org.springframework.core.annotation.AnnotationUtils;

public class ConnectorBeanNameGenerator extends AnnotationBeanNameGenerator {
    @Override
    public String generateBeanName(BeanDefinition definition, BeanDefinitionRegistry registry) {
        Connector connector;
        try {
            connector = AnnotationUtils.getAnnotation(Class.forName(definition.getBeanClassName()), Connector.class);
        } catch (ClassNotFoundException e) {
            throw new InstantiationError("Could not instantiate connector " + definition.getBeanClassName());
        }
        return connector == null || connector.value().isEmpty() ? super.generateBeanName(definition, registry) :
                connector.value();
    }
}
