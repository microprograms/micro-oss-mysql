package com.github.microprograms.micro_oss_mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.microprograms.micro_oss_core.MicroOssProvider;
import com.github.microprograms.micro_oss_core.Transaction;
import com.github.microprograms.micro_oss_core.exception.MicroOssException;
import com.github.microprograms.micro_oss_core.model.Entity;
import com.github.microprograms.micro_oss_core.model.FieldDefinition;
import com.github.microprograms.micro_oss_core.model.ddl.CreateTableCommand;
import com.github.microprograms.micro_oss_core.model.ddl.DropTableCommand;
import com.github.microprograms.micro_oss_core.model.dml.DeleteCommand;
import com.github.microprograms.micro_oss_core.model.dml.InsertCommand;
import com.github.microprograms.micro_oss_core.model.dml.SelectCommand;
import com.github.microprograms.micro_oss_core.model.dml.SelectCountCommand;
import com.github.microprograms.micro_oss_core.model.dml.UpdateCommand;
import com.github.microprograms.micro_oss_mysql.model.ddl.PrimaryKeyDefinition;
import com.github.microprograms.micro_oss_mysql.model.ddl.TableColumnDefinition;
import com.github.microprograms.micro_oss_mysql.model.ddl.TableElementDefinition;
import com.github.microprograms.micro_oss_mysql.utils.MysqlUtils;

public class MysqlMicroOssProvider implements MicroOssProvider {
    private static final Logger log = LoggerFactory.getLogger(MysqlMicroOssProvider.class);
    private Config config;

    public MysqlMicroOssProvider(Config config) {
        this.config = config;
        try {
            Class.forName(config.getDriver());
        } catch (ClassNotFoundException e) {
            log.error("canot load jdbc driver", e);
        }
    }

    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection(config.getUrl(), config.getUser(), config.getPassword());
    }

    @Override
    public void createTable(CreateTableCommand command) throws MicroOssException {
        try (Connection conn = getConnection()) {
            StringBuffer sb = new StringBuffer("CREATE TABLE IF NOT EXISTS ");
            sb.append(command.getTableDefinition().getTableName());
            sb.append("(");
            PrimaryKeyDefinition primaryKeyDefinition = new PrimaryKeyDefinition();
            List<TableElementDefinition> tableElementDefinitions = new ArrayList<>();
            for (FieldDefinition fieldDefinition : command.getTableDefinition().getFields()) {
                boolean isPrimaryKey = fieldDefinition.getPrimaryKey() > 0;
                if (isPrimaryKey) {
                    primaryKeyDefinition.getFiledNames().put(fieldDefinition.getPrimaryKey(), fieldDefinition.getName());
                }
                tableElementDefinitions.add(new TableColumnDefinition(fieldDefinition.getName(), TableColumnDefinition.getIgniteType(fieldDefinition.getType(), isPrimaryKey)));
            }
            tableElementDefinitions.add(primaryKeyDefinition);
            sb.append(StringUtils.join(tableElementDefinitions, ","));
            sb.append(");");
            String sql = sb.toString();
            log.debug("executeUpdate> {}", sql);
            conn.createStatement().executeUpdate(sql);
        } catch (Exception e) {
            throw new MicroOssException(e);
        }
    }

    @Override
    public void dropTable(DropTableCommand command) throws MicroOssException {
        try (Connection conn = getConnection()) {
            String sql = String.format("DROP TABLE IF EXISTS %s", command.getTableName());
            log.debug("executeUpdate> {}", sql);
            conn.createStatement().executeUpdate(sql);
        } catch (Exception e) {
            throw new MicroOssException(e);
        }
    }

    @Override
    public long deleteObject(DeleteCommand command) throws MicroOssException {
        try (Connection conn = getConnection()) {
            String sql = MysqlUtils.buildSql(command);
            log.debug("executeUpdate> {}", sql);
            return conn.createStatement().executeUpdate(sql);
        } catch (Exception e) {
            throw new MicroOssException(e);
        }
    }

    @Override
    public long insertObject(InsertCommand command) throws MicroOssException {
        try (Connection conn = getConnection()) {
            String sql = MysqlUtils.buildSql(command);
            log.debug("executeUpdate> {}", sql);
            return conn.createStatement().executeUpdate(sql);
        } catch (Exception e) {
            throw new MicroOssException(e);
        }
    }

    @Override
    public List<Entity> selectObject(SelectCommand command) throws MicroOssException {
        try (Connection conn = getConnection()) {
            String sql = MysqlUtils.buildSql(command);
            log.debug("executeQuery> {}", sql);
            return MysqlUtils.getEntityList(command.getTableName(), conn.createStatement().executeQuery(sql));
        } catch (Exception e) {
            throw new MicroOssException(e);
        }
    }

    @Override
    public long selectCount(SelectCountCommand command) throws MicroOssException {
        try (Connection conn = getConnection()) {
            StringBuffer sb = new StringBuffer("SELECT COUNT(*) AS count FROM ").append(command.getTableName());
            String where = MysqlUtils.parse(command.getWhere());
            if (StringUtils.isNotBlank(where)) {
                sb.append(" WHERE ").append(where);
            }
            String sql = sb.toString();
            log.debug("executeQuery> {}", sql);
            return MysqlUtils.getCount(conn.createStatement().executeQuery(sql));
        } catch (Exception e) {
            throw new MicroOssException(e);
        }
    }

    @Override
    public long updateObject(UpdateCommand command) throws MicroOssException {
        try (Connection conn = getConnection()) {
            String sql = MysqlUtils.buildSql(command);
            log.debug("executeUpdate> {}", sql);
            return conn.createStatement().executeUpdate(sql);
        } catch (Exception e) {
            throw new MicroOssException(e);
        }
    }

    @Override
    public Transaction beginTransaction() throws MicroOssException {
        try {
            Connection connection = getConnection();
            connection.setAutoCommit(false);
            return new MysqlTransaction(connection);
        } catch (Exception e) {
            throw new MicroOssException(e);
        }
    }

    @Override
    public void close() throws MicroOssException {
        log.debug("close");
    }
}
