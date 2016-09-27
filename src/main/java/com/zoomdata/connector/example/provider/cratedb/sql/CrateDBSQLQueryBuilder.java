/**
 * Copyright (C) Zoomdata, Inc. 2012-2016. All rights reserved.
 */
package com.zoomdata.connector.example.provider.cratedb.sql;

import com.zoomdata.connector.example.framework.common.sql.FiltersProcessor;
import com.zoomdata.connector.example.framework.common.sql.impl.DefaultSQLQueryBuilder;

public class CrateDBSQLQueryBuilder extends DefaultSQLQueryBuilder {

    @Override
    public FiltersProcessor createFiltersProcessor() {
        return new CrateDBFiltersProcessor();
    }
}
