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
import com.github.microprograms.micro_oss_core.MicroOssProvider;
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

public class MysqlMicroOssProvider implements MicroOssProvider {
	private static final Logger log = LoggerFactory.getLogger(MysqlMicroOssProvider.class);

	private DataSource dataSource;

	public MysqlMicroOssProvider(DataSource dataSource) {
		this.dataSource = dataSource;
	}

	protected String getTableName(Class<?> clz) {
		return clz.getSimpleName();
	}

	protected Entity buildEntity(Object javaObject) {
		JSONObject json = (JSONObject) JSON.toJSON(javaObject);
		List<Field> fields = new ArrayList<>();
		for (String key : json.keySet()) {
			fields.add(new Field(key, json.get(key)));
		}
		return new Entity(getTableName(javaObject.getClass()), fields);
	}

	@Override
	public void createTable(CreateTableCommand command) throws MicroOssException {
		try (Connection conn = dataSource.getConnection()) {
			String sql = MysqlUtils.buildSql(command);
			log.debug("createTable> {}", sql);
			conn.createStatement().executeUpdate(sql);
		} catch (Exception e) {
			throw new MicroOssException(e);
		}
	}

	@Override
	public void dropTable(DropTableCommand command) throws MicroOssException {
		try (Connection conn = dataSource.getConnection()) {
			String sql = String.format("DROP TABLE IF EXISTS %s;", command.getTableName());
			log.debug("dropTable> {}", sql);
			conn.createStatement().executeUpdate(sql);
		} catch (Exception e) {
			throw new MicroOssException(e);
		}
	}

	@Override
	public int insertObject(Object object) throws MicroOssException {
		try (Connection conn = dataSource.getConnection()) {
			String sql = MysqlUtils.buildSql(new InsertCommand(buildEntity(object)));
			log.debug("executeUpdate> {}", sql);
			return conn.createStatement().executeUpdate(sql);
		} catch (Exception e) {
			throw new MicroOssException(e);
		}
	}

	@Override
	public int updateObject(Class<?> clz, List<Field> fields, Condition where) throws MicroOssException {
		try (Connection conn = dataSource.getConnection()) {
			String sql = MysqlUtils.buildSql(new UpdateCommand(getTableName(clz), fields, where));
			log.debug("executeUpdate> {}", sql);
			return conn.createStatement().executeUpdate(sql);
		} catch (Exception e) {
			throw new MicroOssException(e);
		}
	}

	@Override
	public int deleteObject(Class<?> clz, Condition where) throws MicroOssException {
		try (Connection conn = dataSource.getConnection()) {
			String sql = MysqlUtils.buildSql(new DeleteCommand(getTableName(clz), where));
			log.debug("executeUpdate> {}", sql);
			return conn.createStatement().executeUpdate(sql);
		} catch (Exception e) {
			throw new MicroOssException(e);
		}
	}

	@Override
	public int queryCount(Class<?> clz, Condition where) throws MicroOssException {
		try (Connection conn = dataSource.getConnection()) {
			SelectCountCommand command = new SelectCountCommand(getTableName(clz), where);
			StringBuffer sb = new StringBuffer("SELECT COUNT(*) AS count FROM ").append(command.getTableName());
			String whereCondition = MysqlUtils.parse(command.getWhere());
			if (StringUtils.isNotBlank(whereCondition)) {
				sb.append(" WHERE ").append(whereCondition).append(";");
			}
			String sql = sb.toString();
			log.debug("executeQuery> {}", sql);
			ResultSet rs = conn.createStatement().executeQuery(sql);
			rs.next();
			return rs.getObject("count", Integer.class);
		} catch (Exception e) {
			throw new MicroOssException(e);
		}
	}

	@Override
	public <T> QueryResult<T> query(Class<T> clz, Condition where) throws MicroOssException {
		return query(clz, null, where, null, null);
	}

	@Override
	public <T> QueryResult<T> query(Class<T> clz, Condition where, List<Sort> sorts) throws MicroOssException {
		return query(clz, null, where, sorts, null);
	}

	@Override
	public <T> QueryResult<T> query(Class<T> clz, Condition where, List<Sort> sorts, PagerRequest pager)
			throws MicroOssException {
		return query(clz, null, where, sorts, pager);
	}

	@Override
	public <T> QueryResult<T> query(Class<T> clz, List<String> fieldNames, Condition where, List<Sort> sorts,
			PagerRequest pager) throws MicroOssException {
		try (Connection conn = dataSource.getConnection()) {
			SelectCommand command = new SelectCommand(getTableName(clz), fieldNames, where, sorts, pager);
			String sql = MysqlUtils.buildSql(command);
			log.debug("executeQuery> {}", sql);
			List<Entity> entities = MysqlUtils.getEntityList(command.getTableName(),
					conn.createStatement().executeQuery(sql));
			return new QueryResult<>(entities, clz);
		} catch (Exception e) {
			throw new MicroOssException(e);
		}
	}

	@Override
	public void execute(Transaction transaction) throws MicroOssException {
		Connection conn = null;
		try {
			conn = dataSource.getConnection();
			conn.setAutoCommit(false);
			transaction.execute(this);
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
