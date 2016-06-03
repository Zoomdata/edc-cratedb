/**
 * Copyright (C) Zoomdata, Inc. 2012-2016. All rights reserved.
 */
package com.zoomdata.edc.server.core.sql;

import com.zoomdata.edc.server.core.utils.StringUtils;

public class Percentile {
    private String field;
    private double percentile;

    public Percentile(String field, double percentile) {
        if (StringUtils.isEmpty(field)) {
            throw new IllegalAccessError("Percentile field must have a non-empty value.");
        }
        if (percentile < 0 || percentile > 100) {
            throw new IllegalArgumentException("Percentile value must be in range [0; 100].");
        }

        this.field = field;
        this.percentile = percentile;
    }

    public String getField() {
        return field;
    }

    public double getPercentile() {
        return percentile;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Percentile that = (Percentile) o;

        return percentile == that.percentile && field.equals(that.field);

    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = field != null ? field.hashCode() : 0;
        temp = Double.doubleToLongBits(percentile);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }
}
