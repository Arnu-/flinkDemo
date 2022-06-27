/*

#     __                        
#    /  |  ____ ___  _          
#   / / | / __//   // / /       
#  /_/`_|/_/  / /_//___/        
create @ 2022/5/24                                
*/
package me.arnu.flink.catalog;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.exceptions.*;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * 这个类的目标是通过 mysql 来保存所有的 flink sql ddl信息，
 * 这样就可以持久化，统一的管理 动态表，且可以跨任务使用表信息。
 */
public class MysqlCatalog extends AbstractCatalog {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    public static final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";

    static {
        try {
            Class.forName(MYSQL_DRIVER);
        } catch (ClassNotFoundException e) {
            throw new CatalogException("未加载 mysql 驱动！", e);
        }
    }

    public static final String MYSQL_TYPE_STRING = "string";
    public static final String MYSQL_TYPE_DOUBLE = "double";
    public static final String MYSQL_TYPE_TIMESTAMP = "timestamp";
    public static final String MYSQL_TYPE_BIGINT = "bigint";
    public static final String MYSQL_TYPE_INT = "int";

    /**
     * 数据库用户名
     */
    private final String user;
    /**
     * 数据库密码
     */
    private final String pwd;
    /**
     * 数据库连接
     */
    private final String url;

    public MysqlCatalog(String name,
                        String defaultDatabase,
                        String url,
                        String user,
                        String pwd) {
        super(name, defaultDatabase);
        this.url = url;
        this.user = user;
        this.pwd = pwd;
    }


    @Override
    public void open() throws CatalogException {
        // todo: 补充完成该方法。
    }

    @Override
    public void close() throws CatalogException {
// todo: 补充完成该方法。
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        List<String> myDatabases = new ArrayList<>();
        String querySql = "SELECT database_name FROM old_metadata_database";
        try (Connection conn = DriverManager.getConnection(url, user, pwd);
             PreparedStatement ps = conn.prepareStatement(querySql)) {

            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
                String dbName = rs.getString(1);
                myDatabases.add(rs.getString(1));

            }

            return myDatabases;
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed listing database in catalog %s", getName()), e);
        }
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException, CatalogException {
        String querySql = "SELECT id, database_name,description FROM old_metadata_database where database_name=?";
        try (Connection conn = DriverManager.getConnection(url, user, pwd);
             PreparedStatement ps = conn.prepareStatement(querySql)) {
            ps.setString(1, databaseName);
            ResultSet rs = ps.executeQuery();

            if (rs.next()) {
                int id = rs.getInt("id");
                String description = rs.getString("description");
                Map<String, String> properties = getPropertiesFromMysql(conn, id, "database");
                return new CatalogDatabaseImpl(properties, description);
            }
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed get database in catalog %s", getName()), e);
        }
        throw new DatabaseNotExistException(getName(), databaseName);

    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        return getDatabaseId(databaseName) != null;
    }

    private Integer getDatabaseId(String databaseName) {
        String querySql = "select id from old_metadata_database where database_name=?";
        try (Connection conn = DriverManager.getConnection(url, user, pwd);
             PreparedStatement ps = conn.prepareStatement(querySql)) {
            ps.setString(1, databaseName);
            ResultSet rs = ps.executeQuery();
            boolean multiDB = false;
            Integer id = null;
            while (rs.next()) {
                if (!multiDB) {
                    id = rs.getInt(1);
                    multiDB = true;
                } else {
                    throw new CatalogException("存在多个同名database: " + databaseName);
                }
            }
            return id;
        } catch (SQLException e) {
            logger.error("获取database信息失败：", e);
            return null;
        }
    }

    @Override
    public void createDatabase(String databaseName, CatalogDatabase db, boolean ignoreIfExists) throws DatabaseAlreadyExistException, CatalogException {

        checkArgument(!StringUtils.isNullOrWhitespaceOnly(databaseName));
        checkNotNull(db);

        if (databaseExists(databaseName)) {
            if (!ignoreIfExists) {
                throw new DatabaseAlreadyExistException(getName(), databaseName);
            }
        } else {
            // 在这里实现创建库的代码
            try (Connection conn = DriverManager.getConnection(url, user, pwd)) {
                // 启动事务
                Integer id = null;
                conn.setAutoCommit(false);
                String insertSql = "insert into old_metadata_database(database_name, description) values(?,?)";
                PreparedStatement stat = conn.prepareStatement(insertSql, Statement.RETURN_GENERATED_KEYS);
                stat.setString(1, databaseName);
                stat.setString(2, db.getComment());
                stat.executeUpdate();
                ResultSet idRs = stat.getGeneratedKeys();
                if (idRs.next() && db.getProperties() != null && db.getProperties().size() > 0) {
                    id = idRs.getInt(1);
                    StringBuilder sb = new StringBuilder("insert into old_metadata_properties(object_id, object_type," +
                            "`key`,`value`) values (?,?,?,?)");
                    for (int i = 1; i < db.getProperties().size(); i++) {
                        sb.append(",(?,?,?,?)");
                    }

                    String propInsertSql = sb.toString();
                    PreparedStatement pstat = conn.prepareStatement(propInsertSql);
                    int i = 1;
                    for (Map.Entry<String, String> entry : db.getProperties().entrySet()) {
                        pstat.setInt(i++, id);
                        pstat.setString(i++, "database");
                        pstat.setString(i++, entry.getKey());
                        pstat.setString(i++, entry.getValue());
                    }
                    int rc = pstat.executeUpdate();
                    if (rc != db.getProperties().size()) {
                        logger.warn("创建 database 写入 properties 数量不一致，db id：{} 对象中数量：{}， 插入数据库数量：{}",
                                id, db.getProperties().size(), rc);
                    }
                }
            } catch (SQLException e) {
                logger.error("创建 database 信息失败：", e);
            }
        }
    }

    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade) throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
        // 1、取出db id，
        Integer id = getDatabaseId(name);
        if (id == null) {
            if (!ignoreIfNotExists) {
                throw new DatabaseNotExistException(getName(), name);
            }
            return;
        }

        // todo: 这里需要做事务处理。
        try (Connection conn = DriverManager.getConnection(url, user, pwd)) {
            // 查询是否有表
            List<String> tables = listTables(name);
            if (tables.size() > 0) {
                if (!cascade) {
                    // 有表，不做级联删除。
                    throw new CatalogException("database 不为空，不可删除。");
                }
                // 做级联删除
                for (String table : tables) {
                    try {
                        dropTable(ObjectPath.fromString(name + "." + table), true);
                    } catch (TableNotExistException t) {
                        logger.warn("表{}不存在", name + "." + table);
                    }
                }
            }
            // todo: 现在是真实删除，后续设计是否做记录保留。
            String deletePropSql = "delete from old_metadata_properties where object_type='database' and object_id=?";
            PreparedStatement dStat = conn.prepareStatement(deletePropSql);
            dStat.setInt(1, id);
            dStat.executeUpdate();
            dStat.close();
            String deleteDbSql = "delete from old_metadata_database where id=?";
            dStat = conn.prepareStatement(deleteDbSql);
            dStat.setInt(1, id);
            dStat.executeUpdate();
            dStat.close();
        } catch (SQLException e) {
            logger.error("删除 database 信息失败：", e);
        }
    }

    @Override
    public void alterDatabase(String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists) throws DatabaseNotExistException, CatalogException {
        // todo: 补充完成该方法。
        throw new UnsupportedOperationException("该方法尚未完成");
    }

    @Override
    public List<String> listTables(String databaseName) throws DatabaseNotExistException, CatalogException {
        Integer databaseId = getDatabaseId(databaseName);
        if (null == databaseId) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }

        // get all schemas
        String querySql = "SELECT table_name FROM old_metadata_table mt\n" +
                " where mt.database_id = ?";
        try (Connection conn = DriverManager.getConnection(url, user, pwd);
             PreparedStatement ps = conn.prepareStatement(querySql)) {
            ps.setInt(1, databaseId);
            ResultSet rs = ps.executeQuery();

            List<String> tables = new ArrayList<>();

            while (rs.next()) {
                String table = rs.getString(1);
                tables.add(table);
            }
            return tables;
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed listing database in catalog %s", getName()), e);
        }
    }

    @Override
    public List<String> listViews(String databaseName) throws DatabaseNotExistException, CatalogException {
        // todo: 补充完成该方法。
        throw new UnsupportedOperationException("该方法尚未完成");
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {
        Schema.Builder builder = Schema.newBuilder();
        String databaseName = tablePath.getDatabaseName();
        String tableName = tablePath.getObjectName();
        int tableId = 0;
        String comment = "";
        String pk = "";
        String watermarkRowTimeAttribute = "";
        String watermarkExpression = "";
        Map<String, String> props = new HashMap<>();
        String querySql = "select" +
                " tb.id," +
                " db.database_name," +
                " tb.table_name," +
                " tb.description," +
                " tb.primary_key," +
                " tb.watermark_row_time_attribute," +
                " tb.watermark_expression," +
                " cl.column_name," +
                " cl.column_type," +
                " cl.expr" +
                " FROM old_metadata_database db" +
                " JOIN old_metadata_table tb ON db.id = tb.database_id" +
                " JOIN old_metadata_column cl ON tb.id = cl.table_id" +
                " WHERE database_name =? AND table_name =?";
        try (Connection conn = DriverManager.getConnection(url, user, pwd);
             PreparedStatement ps = conn.prepareStatement(querySql)) {
            ps.setString(1, databaseName);
            ps.setString(2, tableName);
            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
                if (rs.isFirst()) {
                    tableId = rs.getInt("id");
                    comment = rs.getString("description");
                    pk = rs.getString("primary_key");
                    watermarkRowTimeAttribute = rs.getString("watermark_row_time_attribute");
                    watermarkExpression = rs.getString("watermark_expression");
                }
                String columnName = rs.getString("column_name");
                String columnType = rs.getString("column_type");
                String expr = rs.getString("expr");
                if (expr == null || "".equals(expr)) {
                    builder.column(columnName, mappingType(columnType));
                } else {
                    builder.columnByExpression(columnName, expr);
                }
            }

            props = getPropertiesFromMysql(conn, tableId, "table");

        } catch (Exception e) {
            logger.error("get table fail", e);
        }

        //设置主键
        if (pk != null && !"".equals(pk)) {
            builder.primaryKey(pk.split(";"));
        }
        //设置watermark
        if (!StringUtils.isNullOrWhitespaceOnly(watermarkExpression)) {
            builder.watermark(watermarkRowTimeAttribute, watermarkExpression);
        }

        Schema schema = builder.build();
        // todo: 增加对partition的支持。
        return CatalogTable.of(schema, comment, Collections.emptyList(), props);
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        Integer id = getTableId(tablePath);
        return id != null;
    }

    private Integer getTableId(ObjectPath tablePath) {
        // 获取id
        String getIdSql = "select mt.id from old_metadata_table mt\n" +
                "join old_metadata_database md on mt.database_id = md.id\n" +
                " where table_name=? and database_name=?";
        try (Connection conn = DriverManager.getConnection(url, user, pwd);
             PreparedStatement gStat = conn.prepareStatement(getIdSql)) {
            gStat.setString(1, tablePath.getObjectName());
            gStat.setString(2, tablePath.getDatabaseName());
            ResultSet rs = gStat.executeQuery();
            if (rs.next()) {
                int id = rs.getInt(1);
                return id;
            }
        } catch (SQLException e) {
            logger.error("get table fail", e);
            throw new CatalogException("get table fail.", e);
        }
        return null;
    }

    @Override
    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        Integer id = getTableId(tablePath);
        if (id == null) {
            throw new TableNotExistException(getName(), tablePath);
        }
        try (Connection conn = DriverManager.getConnection(url, user, pwd)) {
            // todo: 现在是真实删除，后续设计是否做记录保留。
            String deletePropSql = "delete from old_metadata_properties where object_type='table' and object_id=?";
            PreparedStatement dStat = conn.prepareStatement(deletePropSql);
            dStat.setInt(1, id);
            dStat.executeUpdate();
            dStat.close();
            String deleteColSql = "delete from old_metadata_column where table_id=?";
            dStat = conn.prepareStatement(deleteColSql);
            dStat.setInt(1, id);
            dStat.executeUpdate();
            dStat.close();
            String deleteDbSql = "delete from old_metadata_database where id=?";
            dStat = conn.prepareStatement(deleteDbSql);
            dStat.setInt(1, id);
            dStat.executeUpdate();
            dStat.close();
        } catch (SQLException e) {
            logger.error("drop table fail", e);
            throw new CatalogException("drop table fail.", e);
        }
    }

    @Override
    public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists) throws TableNotExistException, TableAlreadyExistException, CatalogException {
// todo: 补充完成该方法。
        throw new UnsupportedOperationException("该方法尚未完成");
    }

    @Override
    public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists) throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {

        Integer dbId = getDatabaseId(tablePath.getDatabaseName());
        if (dbId == null) {
            throw new DatabaseNotExistException(getName(), tablePath.getDatabaseName());
        }
        if (tableExists(tablePath)) {
            if (!ignoreIfExists) {
                throw new TableAlreadyExistException(getName(), tablePath);
            }
            return;
        }
        // 插入表
        try (Connection conn = DriverManager.getConnection(url, user, pwd)) {
            conn.setAutoCommit(false);
            String insertSql = "insert into old_metadata_table(\n" +
                    " table_name," +
                    " primary_key," +
                    " watermark_row_time_attribute," +
                    " watermark_expression," +
                    " database_id)" +
                    " values(?,?,?,?,?)";
            PreparedStatement iStat = conn.prepareStatement(insertSql, Statement.RETURN_GENERATED_KEYS);
            iStat.setInt(5, dbId);
            String tableName = tablePath.getObjectName();
            iStat.setString(1, tableName);
            // todo: 支持多pk
            String primaryKey = null;
            if (table.getUnresolvedSchema().getPrimaryKey().isPresent()) {
                Schema.UnresolvedPrimaryKey pk = table.getUnresolvedSchema().getPrimaryKey().get();
                if (pk.getColumnNames().size() > 0) {
                    primaryKey = pk.getColumnNames().get(0);
                    iStat.setString(2, primaryKey);
                } else {
                    iStat.setObject(2, null);
                }
            } else {
                iStat.setObject(2, null);
            }
            // todo: 支持多watermark
            String watermarkRowTimeAttribute = null;
            String watermarkExpression = null;
            List<Schema.UnresolvedWatermarkSpec> w = table.getUnresolvedSchema().getWatermarkSpecs();
            if (w != null && w.size() > 0) {
                Schema.UnresolvedWatermarkSpec w0 = w.get(0);
                watermarkRowTimeAttribute = w0.getColumnName();
                watermarkExpression = w0.getWatermarkExpression().asSummaryString();
                iStat.setString(3, watermarkRowTimeAttribute);
                iStat.setString(4, watermarkExpression);
            } else {
                iStat.setObject(3, null);
                iStat.setObject(4, null);
            }
            iStat.executeUpdate();
            ResultSet idRs = iStat.getGeneratedKeys();
            if (idRs.next()) {
                int id = idRs.getInt(1);
                // 插入属性和列
                if (table.getOptions().size() > 0) {
                    StringBuilder opInsertSqlSb = new StringBuilder("insert into old_metadata_properties(object_type, object_id, `key`, `value`)\n" +
                            " values(?,?,?,?)");
                    for (int i = 1; i < table.getOptions().size(); i++) {
                        opInsertSqlSb.append(",(?,?,?,?)");
                    }
                    String opInsertSql = opInsertSqlSb.toString();
                    PreparedStatement opIStat = conn.prepareStatement(opInsertSql);
                    int i = 1;
                    for (Map.Entry<String, String> entry : table.getOptions().entrySet()) {
                        opIStat.setString(i++, "table");
                        opIStat.setInt(i++, id);
                        opIStat.setString(i++, entry.getKey());
                        opIStat.setString(i++, entry.getValue());
                    }
                    int rc = opIStat.executeUpdate();
                    if (rc != table.getOptions().size()) {
                        logger.warn("创建 table 写入 properties 数量不一致，table ：{} 对象中数量：{}， 插入数据库数量：{}",
                                id, table.getOptions().size(), rc);
                    }
                }

                // 插入属性和列
                if (table.getUnresolvedSchema().getColumns().size() > 0) {
                    StringBuilder colInsertSqlSb = new StringBuilder("insert into old_metadata_column(table_id," +
                            " column_name, column_type, `expr`)\n" +
                            " values(?,?,?,?)");
                    for (int i = 1; i < table.getUnresolvedSchema().getColumns().size(); i++) {
                        colInsertSqlSb.append(",(?,?,?,?)");
                    }
                    String colInsertSql = colInsertSqlSb.toString();
                    PreparedStatement colIStat = conn.prepareStatement(colInsertSql);
                    int i = 1;
                    // todo: 这两种不同类型的列，需要进行特殊的实现。
                    for (Schema.UnresolvedColumn column : table.getUnresolvedSchema().getColumns()) {
                        colIStat.setInt(i++, id);
                        colIStat.setString(i++, column.getName());
                        if (column instanceof Schema.UnresolvedPhysicalColumn) {
                            Schema.UnresolvedPhysicalColumn pCol = (Schema.UnresolvedPhysicalColumn) column;
                            colIStat.setString(i++, mappingDataType(pCol.getDataType()));
                            colIStat.setObject(i++, null);
                        } else if (column instanceof Schema.UnresolvedComputedColumn) {
                            Schema.UnresolvedComputedColumn cCol = (Schema.UnresolvedComputedColumn) column;
                            colIStat.setObject(i++, null);
                            colIStat.setString(i++, cCol.getExpression().asSummaryString());
                        }
                    }
                    int rc = colIStat.executeUpdate();
                    if (rc != table.getUnresolvedSchema().getColumns().size()) {
                        logger.warn("创建 table 写入 column 数量不一致，table ：{} 对象中数量：{}， 插入数据库数量：{}",
                                id, table.getUnresolvedSchema().getColumns().size(), rc);
                    }
                }
            }
            conn.commit();
        } catch (SQLException ex) {
            logger.error("插入数据库失败", ex);
            throw new CatalogException("插入数据库失败", ex);
        }
    }

    @Override
    public void alterTable(ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {
// todo: 补充完成该方法。
        throw new UnsupportedOperationException("该方法尚未完成");
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath) throws TableNotExistException, TableNotPartitionedException, CatalogException {
        // todo: 补充完成该方法。
        throw new UnsupportedOperationException("该方法尚未完成");
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException, CatalogException {
        // todo: 补充完成该方法。
        throw new UnsupportedOperationException("该方法尚未完成");
    }

    @Override
    public List<CatalogPartitionSpec> listPartitionsByFilter(ObjectPath tablePath, List<Expression> filters) throws TableNotExistException, TableNotPartitionedException, CatalogException {
        // todo: 补充完成该方法。
        throw new UnsupportedOperationException("该方法尚未完成");
    }

    @Override
    public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws PartitionNotExistException, CatalogException {
        // todo: 补充完成该方法。
        throw new UnsupportedOperationException("该方法尚未完成");
    }

    @Override
    public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws CatalogException {
        // todo: 补充完成该方法。
        throw new UnsupportedOperationException("该方法尚未完成");
    }

    @Override
    public void createPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogPartition partition, boolean ignoreIfExists) throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException, PartitionAlreadyExistsException, CatalogException {
// todo: 补充完成该方法。
        throw new UnsupportedOperationException("该方法尚未完成");
    }

    @Override
    public void dropPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {
// todo: 补充完成该方法。
        throw new UnsupportedOperationException("该方法尚未完成");
    }

    @Override
    public void alterPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogPartition newPartition, boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {
// todo: 补充完成该方法。
        throw new UnsupportedOperationException("该方法尚未完成");
    }

    @Override
    public List<String> listFunctions(String dbName) throws DatabaseNotExistException, CatalogException {
        // todo: 补充完成该方法。
        throw new UnsupportedOperationException("该方法尚未完成");
    }

    @Override
    public CatalogFunction getFunction(ObjectPath functionPath) throws FunctionNotExistException, CatalogException {
        // todo: 补充完成该方法。
        // throw new UnsupportedOperationException("该方法尚未完成");
        throw new FunctionNotExistException("该方法尚未完成", functionPath);
    }

    @Override
    public boolean functionExists(ObjectPath functionPath) throws CatalogException {
        // todo: 补充完成该方法。
        throw new UnsupportedOperationException("该方法尚未完成");
    }

    @Override
    public void createFunction(ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists) throws FunctionAlreadyExistException, DatabaseNotExistException, CatalogException {
// todo: 补充完成该方法。
        throw new UnsupportedOperationException("该方法尚未完成");
    }

    @Override
    public void alterFunction(ObjectPath functionPath, CatalogFunction newFunction, boolean ignoreIfNotExists) throws FunctionNotExistException, CatalogException {
// todo: 补充完成该方法。
        throw new UnsupportedOperationException("该方法尚未完成");
    }

    @Override
    public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists) throws FunctionNotExistException, CatalogException {
// todo: 补充完成该方法。
        throw new UnsupportedOperationException("该方法尚未完成");
    }

    @Override
    public CatalogTableStatistics getTableStatistics(ObjectPath tablePath) throws TableNotExistException, CatalogException {
        // todo: 补充完成该方法。
        checkNotNull(tablePath);

        if (!tableExists(tablePath)) {
            throw new TableNotExistException(getName(), tablePath);
        }
        /*if (!isPartitionedTable(tablePath)) {
            CatalogTableStatistics result = tableStats.get(tablePath);
            return result != null ? result.copy() : CatalogTableStatistics.UNKNOWN;
        } else {
            return CatalogTableStatistics.UNKNOWN;
        }*/
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath) throws TableNotExistException, CatalogException {
        // todo: 补充完成该方法。
        checkNotNull(tablePath);

        if (!tableExists(tablePath)) {
            throw new TableNotExistException(getName(), tablePath);
        }

        // CatalogColumnStatistics result = tableColumnStats.get(tablePath);
        // return result != null ? result.copy() : CatalogColumnStatistics.UNKNOWN;
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public CatalogTableStatistics getPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws PartitionNotExistException, CatalogException {
        // todo: 补充完成该方法。
        throw new UnsupportedOperationException("该方法尚未完成");
    }

    @Override
    public CatalogColumnStatistics getPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws PartitionNotExistException, CatalogException {
        // todo: 补充完成该方法。
        throw new UnsupportedOperationException("该方法尚未完成");
    }

    @Override
    public void alterTableStatistics(ObjectPath tablePath, CatalogTableStatistics tableStatistics, boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {
// todo: 补充完成该方法。
        throw new UnsupportedOperationException("该方法尚未完成");
    }

    @Override
    public void alterTableColumnStatistics(ObjectPath tablePath, CatalogColumnStatistics columnStatistics, boolean ignoreIfNotExists) throws TableNotExistException, CatalogException, TablePartitionedException {
// todo: 补充完成该方法。
        throw new UnsupportedOperationException("该方法尚未完成");
    }

    @Override
    public void alterPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogTableStatistics partitionStatistics, boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {
// todo: 补充完成该方法。
        throw new UnsupportedOperationException("该方法尚未完成");
    }

    @Override
    public void alterPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogColumnStatistics columnStatistics, boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {
// todo: 补充完成该方法。
        throw new UnsupportedOperationException("该方法尚未完成");
    }

    /**
     * 根据数据库中存储的数据类型，返回flink中的数据类型
     *
     * @param dataType
     * @return
     */
    private <T extends AbstractDataType<T>> String mappingDataType(AbstractDataType<T> dataType) {
        if (DataTypes.BIGINT().equals(dataType)) {
            return MYSQL_TYPE_BIGINT;
        } else if (DataTypes.DOUBLE().equals(dataType)) {
            return MYSQL_TYPE_DOUBLE;
        } else if (DataTypes.INT().equals(dataType)) {
            return MYSQL_TYPE_INT;
        } else if (DataTypes.STRING().equals(dataType)) {
            return MYSQL_TYPE_STRING;
        } else if (DataTypes.TIMESTAMP(3).equals(dataType)) {
            return MYSQL_TYPE_TIMESTAMP;
        }
        throw new UnsupportedOperationException("current not support " + dataType);
    }

    /**
     * 根据数据库中存储的数据类型，返回flink中的数据类型
     *
     * @param mysqlType
     * @return
     */
    private DataType mappingType(String mysqlType) {
        switch (mysqlType) {
            case MYSQL_TYPE_BIGINT:
                return DataTypes.BIGINT();
            case MYSQL_TYPE_DOUBLE:
                return DataTypes.DOUBLE();
            case MYSQL_TYPE_STRING:
                return DataTypes.STRING();
            case MYSQL_TYPE_TIMESTAMP:
                return DataTypes.TIMESTAMP(3);
            case MYSQL_TYPE_INT:
                return DataTypes.INT();
            default:
                throw new UnsupportedOperationException("current not support " + mysqlType);
        }
    }


    private Map<String, String> getPropertiesFromMysql(Connection conn, int objectId, String objectType) {
        Map<String, String> map = new HashMap<>();

        String sql = "select `key`,`value` " +
                "from old_metadata_properties " +
                "where object_type=? and object_id=?";
        try {
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setString(1, objectType);
            ps.setInt(2, objectId);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                map.put(rs.getString("key"), rs.getString("value"));
            }
        } catch (Exception e) {
            logger.error("get properties fail", e);
        }

        return map;
    }

}
