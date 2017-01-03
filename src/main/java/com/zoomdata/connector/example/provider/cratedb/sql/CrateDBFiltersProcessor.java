/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.provider.cratedb.sql;

import com.querydsl.core.types.Path;
import com.zoomdata.connector.example.framework.common.sql.filter.type.FilterTypeService;
import com.zoomdata.connector.example.framework.common.sql.impl.DefaultFiltersProcessor;
import com.zoomdata.gen.edc.filter.FilterFunction;
import com.zoomdata.gen.edc.types.FieldMetadata;

import java.util.Map;

public class CrateDBFiltersProcessor extends DefaultFiltersProcessor {

    public CrateDBFiltersProcessor(Path<?> table, Map<String, FieldMetadata> metadata,
                                   FilterTypeService filterTypeService) {
        super(table, metadata, filterTypeService);
        addFilterProcessor(FilterFunction.NOT, new CrateDbNotFilterProcessor(this));
    }
}
