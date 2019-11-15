package com.github.microprograms.micro_oss_mysql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.github.microprograms.micro_oss_core.MicroOssConfig;
import com.github.microprograms.micro_oss_core.QueryResult;
import com.github.microprograms.micro_oss_core.Transaction;
import com.github.microprograms.micro_oss_core.exception.MicroOssException;
import com.github.microprograms.micro_oss_core.model.Entity;
import com.github.microprograms.micro_oss_core.model.Field;
import com.github.microprograms.micro_oss_core.model.ddl.CreateTableCommand;
import com.github.microprograms.micro_oss_core.model.ddl.DropTableCommand;
import com.github.microprograms.micro_oss_core.model.dml.query.Condition;
import com.github.microprograms.micro_oss_core.model.dml.query.PagerRequest;
import com.github.microprograms.micro_oss_core.model.dml.query.SelectCommand;
import com.github.microprograms.micro_oss_core.model.dml.query.SelectCountCommand;
import com.github.microprograms.micro_oss_core.model.dml.query.Sort;
import com.github.microprograms.micro_oss_core.model.dml.update.DeleteCommand;
import com.github.microprograms.micro_oss_core.model.dml.update.InsertCommand;
import com.github.microprograms.micro_oss_core.model.dml.update.UpdateCommand;
import com.github.microprograms.micro_oss_mysql.utils.MysqlUtils;

public class RawMysqlMicroOssProvider {
	private static final Logger log = LoggerFactory.getLogger(RawMysqlMicroOssProvider.class);

	private MicroOssConfig config;

	public RawMysqlMicroOssProvider(MicroOssConfig config) {
		this.config = config;
	}

	public MicroOssConfig getConfig() {
		return config;
	}

	protected String getTableName(Class<?> clz) {
		if (StringUtils.isBlank(config.getTablePrefix())) {
			return clz.getSimpleName();
		}
		return config.getTablePrefix() + clz.getSimpleName();
	}

	protected Entity buildEntity(Object javaObject) {
		JSONObject json = (JSONObject) JSON.toJSON(javaObject);
		List<Field> fields = new ArrayList<>();
		for (String key : json.keySet()) {
			fields.add(new Field(key, json.get(key)));
		}
		return new Entity(getTableName(javaObject.getClass()), fields);
	}

	public void createTable(Connection conn, CreateTableCommand command) throws Exception {
		String sql = MysqlUtils.buildSql(command);
		log.debug("createTable> {}", sql);
		conn.createStatement().executeUpdate(sql);
	}

	public void dropTable(Connection conn, DropTableCommand command) throws Exception {
		String sql = String.format("DROP TABLE IF EXISTS %s;", command.getTableName());
		log.debug("dropTable> {}", sql);
		conn.createStatement().executeUpdate(sql);
	}

	public int insertObject(Connection conn, InsertCommand command) throws Exception {
		String sql = MysqlUtils.buildSql(command);
		log.debug("executeUpdate> {}", sql);
		return conn.createStatement().executeUpdate(sql);
	}

	public int insertObject(Connection conn, Object object) throws Exception {
		return insertObject(conn, new InsertCommand(buildEntity(object)));
	}

	public int updateObject(Connection conn, UpdateCommand command) throws Exception {
		String sql = MysqlUtils.buildSql(command);
		log.debug("executeUpdate> {}", sql);
		return conn.createStatement().executeUpdate(sql);
	}

	public int updateObject(Connection conn, Class<?> clz, List<Field> fields, Condition where) throws Exception {
		return updateObject(conn, new UpdateCommand(getTableName(clz), fields, where));
	}

	public int deleteObject(Connection conn, DeleteCommand command) throws Exception {
		String sql = MysqlUtils.buildSql(command);
		log.debug("executeUpdate> {}", sql);
		return conn.createStatement().executeUpdate(sql);
	}

	public int deleteObject(Connection conn, Class<?> clz, Condition where) throws Exception {
		return deleteObject(conn, new DeleteCommand(getTableName(clz), where));
	}

	public int queryCount(Connection conn, SelectCountCommand command) throws Exception {
		StringBuffer sb = new StringBuffer("SELECT COUNT(*) AS count FROM ").append(command.getTableName());
		String whereCondition = MysqlUtils.parseCondition(command.getWhere());
		if (StringUtils.isNotBlank(whereCondition)) {
			sb.append(" WHERE ").append(whereCondition).append(";");
		}
		String sql = sb.toString();
		log.debug("executeQuery> {}", sql);
		ResultSet rs = conn.createStatement().executeQuery(sql);
		rs.next();
		return rs.getObject("count", Integer.class);
	}

	public int queryCount(Connection conn, Class<?> clz, Condition where) throws Exception {
		return queryCount(conn, new SelectCountCommand(getTableName(clz), where));
	}

	public QueryResult<?> query(Connection conn, SelectCommand command) throws Exception {
		String sql = MysqlUtils.buildSql(command);
		log.debug("executeQuery> {}", sql);
		List<Entity> entities = MysqlUtils.getEntityList(command.getTableName(),
				conn.createStatement().executeQuery(sql));
		return new QueryResult<>(entities);
	}

	public <T> QueryResult<T> query(Connection conn, Class<T> clz, List<String> fieldNames, Condition where,
			List<Sort> sorts, PagerRequest pager) throws Exception {
		SelectCommand command = new SelectCommand(getTableName(clz), fieldNames, where, sorts, pager);
		String sql = MysqlUtils.buildSql(command);
		log.debug("executeQuery> {}", sql);
		List<Entity> entities = MysqlUtils.getEntityList(command.getTableName(),
				conn.createStatement().executeQuery(sql));
		return new QueryResult<>(entities, clz);
	}

	public void execute(DataSource dataSource, Transaction transaction) throws MicroOssException {
		Connection conn = null;
		try {
			conn = dataSource.getConnection();
			conn.setAutoCommit(false);
			transaction.execute(new TransactionMysqlMicroOssProvider(conn, dataSource, getConfig()));
			conn.commit();
		} catch (Exception e) {
			try {
				conn.rollback();
			} catch (SQLException sqlException) {
				// ignore
			}
			throw new MicroOssException(e);
		} finally {
			try {
				conn.close();
			} catch (SQLException sqlException) {
				// ignore
			}
		}
	}
}
