/**
 * Copyright (C) Zoomdata, Inc. 2012-2016. All rights reserved.
 */
package com.zoomdata.edc.server.cratedb;

import org.junit.Test;

public class CrateDbServerMainTest {

    @Test
    public void testMain() throws Exception {
        final String args[] = {"3333"};
        CrateDbServerMain.main(args);
    }
}