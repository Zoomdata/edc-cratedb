/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.provider.cratedb.sql;

import com.querydsl.core.types.Path;
import com.zoomdata.connector.example.framework.common.sql.FiltersProcessor;
import com.zoomdata.connector.example.framework.common.sql.filter.type.DefaultFilterTypeService;
import com.zoomdata.connector.example.framework.common.sql.filter.type.FilterTypeService;
import com.zoomdata.connector.example.framework.common.sql.impl.DefaultSQLQueryBuilder;

import java.util.Collections;
import java.util.Optional;

public class CrateDBSQLQueryBuilder extends DefaultSQLQueryBuilder {

    @Override
    public FiltersProcessor createFiltersProcessor(Path<?> table) {
        return new CrateDBFiltersProcessor(table, Optional.ofNullable(fieldMetadata).orElse(Collections.emptyMap()),
                createFilterTypeService());
    }

    protected FilterTypeService createFilterTypeService() {
        return new DefaultFilterTypeService();
    }
}
