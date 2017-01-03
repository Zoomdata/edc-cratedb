/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.framework.common.sql.impl;

import com.zoomdata.connector.example.framework.common.sql.IAliasGenerator;

import javax.validation.constraints.NotNull;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class AliasGenerator implements IAliasGenerator {
    private final Map<String, Integer> aliasCounts = new HashMap<>();

    private static final int MAX_ALIAS_COUNT_PER_FIELD = 99_999;

    private final Integer fieldPartLength;

    private AliasGenerator(Builder builder) {
        this.fieldPartLength = Optional.ofNullable(builder.maxAliasLength)
                .map(length -> length - ("_" + MAX_ALIAS_COUNT_PER_FIELD).length())
                .orElse(null);
    }

    public static Builder newAliasGenerator() {
        return new Builder();
    }

    @Override
    public String generate(String fieldName) {
        String shortenedFieldName = shortenFieldNameForAlias(fieldName);
        int counter = aliasCounts.getOrDefault(shortenedFieldName, 0);
        aliasCounts.put(shortenedFieldName, increaseCounter(counter));
        return shortenedFieldName + (counter > 0 ? "_" + counter : "");
    }

    @Override
    public String generateForPercentiles(String fieldName) {
        return shortenFieldNameForAlias(fieldName);
    }

    private int increaseCounter(int counter) {
        if (fieldPartLength != null && counter >= MAX_ALIAS_COUNT_PER_FIELD) {
            throw new UnsupportedOperationException(
                    "Can't generate new aliases. Limit for field is exceeded: Limit=" + MAX_ALIAS_COUNT_PER_FIELD
            );
        }
        return counter + 1;
    }

    private String shortenFieldNameForAlias(@NotNull String fieldName) {
        return Optional.ofNullable(fieldPartLength).map(length ->
                fieldName.substring(
                        0,
                        Math.min(
                                fieldPartLength,
                                fieldName.length())
                )
        ).orElse(fieldName);
    }

    public static final class Builder {
        private Integer maxAliasLength;

        private Builder() {
        }

        public AliasGenerator build() {
            return new AliasGenerator(this);
        }

        public Builder maxAliasLength(Integer maxAliasLength) {
            this.maxAliasLength = maxAliasLength;
            return this;
        }
    }
}

