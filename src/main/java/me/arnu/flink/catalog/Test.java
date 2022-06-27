/*

#     __                        
#    /  |  ____ ___  _          
#   / / | / __//   // / /       
#  /_/`_|/_/  / /_//___/        
create @ 2022/5/23                                
*/
package me.arnu.flink.catalog;

import me.arnu.utils.ArnuSign;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import java.util.List;

/**
 * 测试一下
 */
public class Test {
    public static void main(String[] args) {
        ArnuSign.printSign();
        //1\. 获取上下文环境 table的环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        String catalogName = "mysql-catalog";
        String defaultDatabase = "default_database";
        String username = "flink_metastore";
        String password = "flink_metastore";
        String jdbcUrl = "jdbc:mysql://localhost:3306/flink_metastore?useUnicode=true&characterEncoding=utf8&serverTimezone=UTC";


        //使用自定义的 mysql catalog
        // MysqlCatalog catalog = new MysqlCatalog(catalogName, defaultDatabase, jdbcUrl, username, password);
        MyCatalog catalog = new MyCatalog(catalogName, defaultDatabase, jdbcUrl, username, password);
        tableEnv.registerCatalog(catalogName, catalog);
        tableEnv.useCatalog(catalogName);
        // 创建database
        tableEnv.executeSql("create database if not exists default_database");
        //2\. 读取score.csv

        String csvFile = "D:\\code\\1\\flinksql\\src\\main\\resources\\score.csv";
        String createTable = "CREATE TABLE IF NOT EXISTS player_data\n" +
                "( season varchar,\n" +
                "  player varchar,\n" +
                "  play_num varchar,\n" +
                "  first_court int,\n" +
                "  `time` double,\n" +
                "  assists double,\n" +
                "  steals double,\n" +
                "  blocks double,\n" +
                "  scores double\n" +
                ") WITH ( \n" +
                "    'connector' = 'filesystem',\n" +
                "    'path' = '" + csvFile + " ',\n" +
                "    'format' = 'csv'\n" +
                ")";

        tableEnv.executeSql(createTable);

        String createView= "CREATE VIEW IF NOT EXISTS test_view " +
                " (player, play_num" +
                " ,sumaabb)" +
                " COMMENT 'test view' " +
                " AS SELECT player, play_num, assists + steals as sumaabb FROM player_data";

        tableEnv.executeSql(createView);
/*

        String createSinkTable = "CREATE TABLE IF NOT EXISTS mysql_player_from_view\n" +
                "( " +
                "  player varchar,\n" +
                "  play_num varchar,\n" +
                "  sumaabb double\n" +
                ") WITH ( \n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:mysql://localhost:3306/a01_rep_db',\n" +
                "    'table-name' = 'mysql_player_from_view',\n" +
                "    'username' = 'root',\n" +
                "    'password' = '123456'\n" +
                ")";
*/

        String createSinkTable = "CREATE TABLE IF NOT EXISTS mysql_player\n" +
                "( season varchar,\n" +
                "  player varchar,\n" +
                "  play_num varchar,\n" +
                "  first_court int,\n" +
                "  `time` double,\n" +
                "  assists double,\n" +
                "  steals double,\n" +
                "  blocks double,\n" +
                "  scores double\n" +
                ") WITH ( \n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:mysql://localhost:3306/a01_rep_db',\n" +
                "    'table-name' = 'mysql_player',\n" +
                "    'username' = 'root',\n" +
                "    'password' = '123456'\n" +
                ")";

        tableEnv.executeSql(createSinkTable);
        /*tableEnv.executeSql("Insert into mysql_player_from_view\n" +
                "SELECT \n" +
                        "player ,\n" +
                "  play_num ,\n" +
                        "  sumaabb \n" +
                "FROM test_view");*/

        List<Row> results =
                CollectionUtil.iteratorToList(
                        tableEnv.sqlQuery("select * from mysql_player")
                                .execute()
                                .collect());

        List<Row> tresults =
                CollectionUtil.iteratorToList(
                        tableEnv.sqlQuery("select * from test_view")
                                .execute()
                                .collect());

        List<Row> presults =
                CollectionUtil.iteratorToList(
                        tableEnv.sqlQuery("select * from player_data")
                                .execute()
                                .collect());
        //4\. 编写sql 然后提交执行
        //select player, count(season) as num from score group by player order by num desc;
        /*Table queryResult = tableEnv.sqlQuery("select 't' as t" +
                ", player" +
                //", count(season) as num" +
                " from player_data\n" +
                // " group by player" +
                // " order by num desc limit 3" +
                "");

        //5\. 结果进行打印
        DataStream<Row> resultStream = tableEnv.toDataStream(queryResult);
        resultStream.print();*/
    }
}
