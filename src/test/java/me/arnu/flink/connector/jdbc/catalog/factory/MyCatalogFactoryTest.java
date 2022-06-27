/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package me.arnu.flink.connector.jdbc.catalog.factory;

import me.arnu.flink.catalog.MyCatalog;
import me.arnu.flink.catalog.factory.MyCatalogFactory;
import me.arnu.flink.catalog.factory.MyCatalogFactoryOptions;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.flink.table.factories.FactoryUtil;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Test for {@link MyCatalogFactory}. */
public class MyCatalogFactoryTest {

    protected static String url;
    protected static MyCatalog catalog;

    protected static final String TEST_CATALOG_NAME = "mysql-catalog";
    protected static final String TEST_USERNAME = "flink_metastore";
    protected static final String TEST_PWD = "flink_metastore";

    @BeforeClass
    public static void setup() throws SQLException {
        // jdbc:postgresql://localhost:50807/postgres?user=postgres
        // String embeddedJdbcUrl = pg.getEmbeddedPostgres().getJdbcUrl(TEST_USERNAME, TEST_PWD);
        // jdbc:postgresql://localhost:50807/
        url = "jdbc:mysql://localhost:3306/flink_metastore?useUnicode=true&characterEncoding=utf8&serverTimezone=UTC";

        catalog =
                new MyCatalog(
                        TEST_CATALOG_NAME,
                        MyCatalog.DEFAULT_DATABASE,
                        TEST_USERNAME,
                        TEST_PWD,
                        url);
    }

    @Test
    public void test() {
        final Map<String, String> options = new HashMap<>();
        options.put(CommonCatalogOptions.CATALOG_TYPE.key(), MyCatalogFactoryOptions.IDENTIFIER);
        options.put(
                MyCatalogFactoryOptions.DEFAULT_DATABASE.key(), MyCatalog.DEFAULT_DATABASE);
        options.put(MyCatalogFactoryOptions.USERNAME.key(), TEST_USERNAME);
        options.put(MyCatalogFactoryOptions.PASSWORD.key(), TEST_PWD);
        options.put(MyCatalogFactoryOptions.URL.key(), url);

        final Catalog actualCatalog =
                FactoryUtil.createCatalog(
                        TEST_CATALOG_NAME,
                        options,
                        null,
                        Thread.currentThread().getContextClassLoader());

        checkEquals(catalog, (MyCatalog) actualCatalog);

        assertTrue( actualCatalog instanceof MyCatalog);
    }

    private static void checkEquals(MyCatalog c1, MyCatalog c2) {
        assertEquals(c1.getName(), c2.getName());
        assertEquals(c1.getDefaultDatabase(), c2.getDefaultDatabase());
        assertEquals(c1.getUser(), c2.getUser());
        assertEquals(c1.getPwd(), c2.getPwd());
        assertEquals(c1.getUrl(), c2.getUrl());
    }
}
