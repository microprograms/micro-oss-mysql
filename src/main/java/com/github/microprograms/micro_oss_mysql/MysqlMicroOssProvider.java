package com.github.microprograms.micro_oss_mysql;

import java.sql.Connection;
import java.util.List;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.microprograms.micro_oss_core.MicroOssConfig;
import com.github.microprograms.micro_oss_core.MicroOssProvider;
import com.github.microprograms.micro_oss_core.QueryResult;
import com.github.microprograms.micro_oss_core.Transaction;
import com.github.microprograms.micro_oss_core.exception.MicroOssException;
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

public class MysqlMicroOssProvider extends RawMysqlMicroOssProvider implements MicroOssProvider {
	private static final Logger log = LoggerFactory.getLogger(MysqlMicroOssProvider.class);

	private DataSource dataSource;

	public MysqlMicroOssProvider(DataSource dataSource, MicroOssConfig config) {
		super(config);
		this.dataSource = dataSource;
	}

	@Override
	public void createTable(CreateTableCommand command) throws MicroOssException {
		try (Connection conn = dataSource.getConnection()) {
			createTable(conn, command);
		} catch (Exception e) {
			throw new MicroOssException(e);
		}
	}

	@Override
	public void dropTable(DropTableCommand command) throws MicroOssException {
		try (Connection conn = dataSource.getConnection()) {
			dropTable(conn, command);
		} catch (Exception e) {
			throw new MicroOssException(e);
		}
	}

	@Override
	public int insertObject(InsertCommand command) throws MicroOssException {
		try (Connection conn = dataSource.getConnection()) {
			return insertObject(conn, command);
		} catch (Exception e) {
			throw new MicroOssException(e);
		}
	}

	@Override
	public int updateObject(UpdateCommand command) throws MicroOssException {
		try (Connection conn = dataSource.getConnection()) {
			return updateObject(conn, command);
		} catch (Exception e) {
			throw new MicroOssException(e);
		}
	}

	@Override
	public int deleteObject(DeleteCommand command) throws MicroOssException {
		try (Connection conn = dataSource.getConnection()) {
			return deleteObject(conn, command);
		} catch (Exception e) {
			throw new MicroOssException(e);
		}
	}

	@Override
	public int queryCount(SelectCountCommand command) throws MicroOssException {
		try (Connection conn = dataSource.getConnection()) {
			return queryCount(conn, command);
		} catch (Exception e) {
			throw new MicroOssException(e);
		}
	}

	@Override
	public QueryResult<?> query(SelectCommand command) throws MicroOssException {
		try (Connection conn = dataSource.getConnection()) {
			return query(conn, command);
		} catch (Exception e) {
			throw new MicroOssException(e);
		}
	}

	@Override
	public int insertObject(Object object) throws MicroOssException {
		try (Connection conn = dataSource.getConnection()) {
			return insertObject(conn, object);
		} catch (Exception e) {
			throw new MicroOssException(e);
		}
	}

	@Override
	public int updateObject(Class<?> clz, List<Field> fields, Condition where) throws MicroOssException {
		try (Connection conn = dataSource.getConnection()) {
			return updateObject(conn, clz, fields, where);
		} catch (Exception e) {
			throw new MicroOssException(e);
		}
	}

	@Override
	public int deleteObject(Class<?> clz, Condition where) throws MicroOssException {
		try (Connection conn = dataSource.getConnection()) {
			return deleteObject(conn, clz, where);
		} catch (Exception e) {
			throw new MicroOssException(e);
		}
	}

	@Override
	public int queryCount(Class<?> clz, Condition where) throws MicroOssException {
		try (Connection conn = dataSource.getConnection()) {
			return queryCount(conn, clz, where);
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
			return query(conn, clz, fieldNames, where, sorts, pager);
		} catch (Exception e) {
			throw new MicroOssException(e);
		}
	}

	@Override
	public void execute(Transaction transaction) throws MicroOssException {
		execute(dataSource, transaction);
	}

}
