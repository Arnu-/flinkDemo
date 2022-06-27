///*
//
//#     __
//#    /  |  ____ ___  _
//#   / / | / __//   // / /
//#  /_/`_|/_/  / /_//___/
//create @ 2022/5/23
//*/
//package me.arnu.flink.catalog;
//
//import com.opentable.db.postgres.junit.EmbeddedPostgresRules;
//import com.opentable.db.postgres.junit.SingleInstancePostgresRule;
//import org.junit.ClassRule;
//
//import org.junit.Test;
//
//public class TestCatalog {
//    protected static final String TEST_USERNAME = "postgres";
//    protected static final String TEST_PWD = "postgres";
//    @ClassRule
//    public static SingleInstancePostgresRule pg = EmbeddedPostgresRules.singleInstance();
//
//    @Test
//    public void testEmbeddedPG(){
//        {
//            // public static SingleInstancePostgresRule pg = EmbeddedPostgresRules.singleInstance();
//            SingleInstancePostgresRule pg = EmbeddedPostgresRules.singleInstance();
//            // jdbc:postgresql://localhost:50807/postgres?user=postgres
//            String embeddedJdbcUrl = pg.getEmbeddedPostgres().getJdbcUrl(TEST_USERNAME, TEST_PWD);
//            // jdbc:postgresql://localhost:50807/
//            String baseUrl = embeddedJdbcUrl.substring(0, embeddedJdbcUrl.lastIndexOf("/"));
//            System.out.println(baseUrl);
//        }
//
//    }
//}
