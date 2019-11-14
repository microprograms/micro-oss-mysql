package com.github.microprograms.micro_oss_mysql;

import java.sql.Connection;
import java.util.List;

import javax.sql.DataSource;

import com.github.microprograms.micro_oss_core.MicroOssConfig;
import com.github.microprograms.micro_oss_core.MicroOssProvider;
import com.github.microprograms.micro_oss_core.QueryResult;
import com.github.microprograms.micro_oss_core.Transaction;
import com.github.microprograms.micro_oss_core.exception.MicroOssException;
import com.github.microprograms.micro_oss_core.model.Field;
import com.github.microprograms.micro_oss_core.model.ddl.CreateTableCommand;
import com.github.microprograms.micro_oss_core.model.dml.query.Condition;
import com.github.microprograms.micro_oss_core.model.dml.query.PagerRequest;
import com.github.microprograms.micro_oss_core.model.dml.query.Sort;

public class TransactionMysqlMicroOssProvider extends RawMysqlMicroOssProvider implements MicroOssProvider {

	private Connection conn;
	private DataSource dataSource;

	public TransactionMysqlMicroOssProvider(Connection conn, DataSource dataSource, MicroOssConfig config) {
		super(config);
		this.conn = conn;
		this.dataSource = dataSource;
	}

	@Override
	public void createTable(Class<?> clz, CreateTableCommand command) throws MicroOssException {
		try {
			createTable(conn, clz, command);
		} catch (Exception e) {
			throw new MicroOssException(e);
		}
	}

	@Override
	public void dropTable(Class<?> clz) throws MicroOssException {
		try {
			dropTable(conn, clz);
		} catch (Exception e) {
			throw new MicroOssException(e);
		}
	}

	@Override
	public int insertObject(Object object) throws MicroOssException {
		try {
			return insertObject(conn, object);
		} catch (Exception e) {
			throw new MicroOssException(e);
		}
	}

	@Override
	public int updateObject(Class<?> clz, List<Field> fields, Condition where) throws MicroOssException {
		try {
			return updateObject(conn, clz, fields, where);
		} catch (Exception e) {
			throw new MicroOssException(e);
		}
	}

	@Override
	public int deleteObject(Class<?> clz, Condition where) throws MicroOssException {
		try {
			return deleteObject(conn, clz, where);
		} catch (Exception e) {
			throw new MicroOssException(e);
		}
	}

	@Override
	public int queryCount(Class<?> clz, Condition where) throws MicroOssException {
		try {
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
		try {
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
