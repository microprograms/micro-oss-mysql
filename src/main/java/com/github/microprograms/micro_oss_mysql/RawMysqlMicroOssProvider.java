package com.github.microprograms.micro_oss_mysql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import javax.sql.DataSource;

import com.github.microprograms.micro_oss_core.MicroOssConfig;
import com.github.microprograms.micro_oss_core.QueryResult;
import com.github.microprograms.micro_oss_core.Transaction;
import com.github.microprograms.micro_oss_core.exception.MicroOssException;
import com.github.microprograms.micro_oss_core.model.Entity;
import com.github.microprograms.micro_oss_core.model.Field;
import com.github.microprograms.micro_oss_core.model.ddl.CreateTableCommand;
import com.github.microprograms.micro_oss_core.model.ddl.DropTableCommand;
import com.github.microprograms.micro_oss_core.model.dml.query.Condition;
import com.github.microprograms.micro_oss_core.model.dml.query.Join;
import com.github.microprograms.micro_oss_core.model.dml.query.PagerRequest;
import com.github.microprograms.micro_oss_core.model.dml.query.SelectCommand;
import com.github.microprograms.micro_oss_core.model.dml.query.SelectCountCommand;
import com.github.microprograms.micro_oss_core.model.dml.query.Sort;
import com.github.microprograms.micro_oss_core.model.dml.update.DeleteCommand;
import com.github.microprograms.micro_oss_core.model.dml.update.InsertCommand;
import com.github.microprograms.micro_oss_core.model.dml.update.UpdateCommand;
import com.github.microprograms.micro_oss_core.utils.MicroOssUtils;
import com.github.microprograms.micro_oss_mysql.utils.MysqlUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class RawMysqlMicroOssProvider {
	private static final Logger log = LoggerFactory.getLogger(RawMysqlMicroOssProvider.class);

	private MicroOssConfig config;

	public RawMysqlMicroOssProvider(MicroOssConfig config) {
		this.config = config;
	}

	public MicroOssConfig getConfig() {
		return config;
	}

	private String _getTableName(Class<?> clz) {
		return MicroOssUtils.getTableName(clz);
	}

	private String _getTableNameWithPrefix(String tableName) {
		return MicroOssUtils.getTableNameWithPrefix(tableName, config.getTablePrefix());
	}

	private Entity _buildEntity(Object javaObject) {
		return MicroOssUtils.buildEntity(javaObject);
	}

	public void createTable(Connection conn, CreateTableCommand command) throws Exception {
		command.getTableDefinition().setTableName(_getTableNameWithPrefix(command.getTableDefinition().getTableName()));
		String sql = MysqlUtils.buildSql(command);
		log.debug("createTable> {}", sql);
		conn.createStatement().executeUpdate(sql);
	}

	public void dropTable(Connection conn, DropTableCommand command) throws Exception {
		command.setTableName(_getTableNameWithPrefix(command.getTableName()));
		String sql = MysqlUtils.buildSql(command);
		log.debug("dropTable> {}", sql);
		conn.createStatement().executeUpdate(sql);
	}

	public int insertObject(Connection conn, InsertCommand command) throws Exception {
		command.getEntity().setTableName(_getTableNameWithPrefix(command.getEntity().getTableName()));
		String sql = MysqlUtils.buildSql(command);
		log.debug("executeUpdate> {}", sql);
		return conn.createStatement().executeUpdate(sql);
	}

	public int insertObject(Connection conn, Object object) throws Exception {
		return insertObject(conn, new InsertCommand(_buildEntity(object)));
	}

	public int updateObject(Connection conn, UpdateCommand command) throws Exception {
		command.setTableName(_getTableNameWithPrefix(command.getTableName()));
		String sql = MysqlUtils.buildSql(command);
		log.debug("executeUpdate> {}", sql);
		return conn.createStatement().executeUpdate(sql);
	}

	public int updateObject(Connection conn, Class<?> clz, List<Field> fields, Condition where) throws Exception {
		return updateObject(conn, new UpdateCommand(_getTableName(clz), fields, where));
	}

	public int deleteObject(Connection conn, DeleteCommand command) throws Exception {
		command.setTableName(_getTableNameWithPrefix(command.getTableName()));
		String sql = MysqlUtils.buildSql(command);
		log.debug("executeUpdate> {}", sql);
		return conn.createStatement().executeUpdate(sql);
	}

	public int deleteObject(Connection conn, Class<?> clz, Condition where) throws Exception {
		return deleteObject(conn, new DeleteCommand(_getTableName(clz), where));
	}

	public int queryCount(Connection conn, SelectCountCommand command) throws Exception {
		command.setTableName(_getTableNameWithPrefix(command.getTableName()));
		if (command.getJoins() != null) {
			for (Join x : command.getJoins()) {
				x.setTableName(_getTableNameWithPrefix(x.getTableName()));
			}
		}
		String sql = MysqlUtils.buildSql(command);
		log.debug("executeQuery> {}", sql);
		ResultSet rs = conn.createStatement().executeQuery(sql);
		rs.next();
		return rs.getObject("count", Integer.class);
	}

	public int queryCount(Connection conn, Class<?> clz, Condition where) throws Exception {
		return queryCount(conn, new SelectCountCommand(_getTableName(clz), where));
	}

	public <T> QueryResult<T> query(Connection conn, SelectCommand command) throws Exception {
		command.setTableName(_getTableNameWithPrefix(command.getTableName()));
		if (command.getJoins() != null) {
			for (Join x : command.getJoins()) {
				x.setTableName(_getTableNameWithPrefix(x.getTableName()));
			}
		}
		String sql = MysqlUtils.buildSql(command);
		log.debug("executeQuery> {}", sql);
		List<Entity> entities = MysqlUtils.getEntityList(command.getTableName(),
				conn.createStatement().executeQuery(sql));
		return new QueryResult<>(entities);
	}

	public <T> QueryResult<T> query(Connection conn, Class<T> clz, List<String> fieldNames, Condition where,
			List<Sort> sorts, PagerRequest pager) throws Exception {
		QueryResult<T> queryResult = query(conn,
				new SelectCommand(_getTableName(clz), fieldNames, where, sorts, pager));
		return queryResult.clz(clz);
	}

	public void execute(DataSource dataSource, Transaction transaction) throws MicroOssException {
		Connection conn = null;
		try {
			log.debug("transaction execute> {}", transaction.getTransactionId());
			conn = dataSource.getConnection();
			conn.setAutoCommit(false);
			transaction.execute(new TransactionMysqlMicroOssProvider(conn, dataSource, getConfig()));
			conn.commit();
			log.debug("transaction commit> {}", transaction.getTransactionId());
		} catch (Exception e) {
			try {
				conn.rollback();
				log.debug("transaction rollback> {}", transaction.getTransactionId());
			} catch (SQLException sqlException) {
				// ignore
				log.warn("", sqlException);
			}
			throw new MicroOssException(e);
		} finally {
			try {
				conn.close();
			} catch (SQLException sqlException) {
				// ignore
				log.warn("", sqlException);
			}
		}
	}
}
