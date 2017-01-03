/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.framework.common.sql.filter;

import com.querydsl.core.types.Predicate;
import com.zoomdata.gen.edc.filter.Filter;

public interface FilterProcessor {

    Predicate processFilter(Filter filter);

}
