/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.common.utils;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public final class FileUtils {
    private FileUtils() { }

    public static String readFile(String path) throws IOException {
        byte[] encoded = Files.readAllBytes(Paths.get(path));
        return new String(encoded, "UTF-8");
    }

    public static String readFileFromClassPath(String file) throws IOException {
        try {
            URL resourceUrl = FileUtils.class.getResource(file);
            Path resourcePath = Paths.get(resourceUrl.toURI());
            byte[] encoded = Files.readAllBytes(resourcePath);
            return new String(encoded, "UTF-8");
        } catch (URISyntaxException e) {
            // FIXME wrong! do nothing
            return "";
        }
    }
}
