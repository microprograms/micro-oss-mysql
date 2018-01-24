package com.github.microprograms.micro_oss_mysql;

import java.sql.Connection;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.microprograms.micro_oss_core.Transaction;
import com.github.microprograms.micro_oss_core.exception.MicroOssTransactionException;
import com.github.microprograms.micro_oss_core.model.dml.DeleteCommand;
import com.github.microprograms.micro_oss_core.model.dml.InsertCommand;
import com.github.microprograms.micro_oss_core.model.dml.UpdateCommand;
import com.github.microprograms.micro_oss_mysql.utils.MysqlUtils;

public class MysqlTransaction implements Transaction {
    private static final Logger log = LoggerFactory.getLogger(MysqlTransaction.class);

    private Connection conn;

    public MysqlTransaction(Connection connection) {
        this.conn = connection;
    }

    @Override
    public long deleteObject(DeleteCommand command) throws MicroOssTransactionException {
        try {
            String sql = MysqlUtils.buildSql(command);
            log.debug("executeUpdate> {}", sql);
            return conn.createStatement().executeUpdate(sql);
        } catch (Exception e) {
            throw new MicroOssTransactionException(e);
        }
    }

    @Override
    public long insertObject(InsertCommand command) throws MicroOssTransactionException {
        try {
            String sql = MysqlUtils.buildSql(command);
            log.debug("executeUpdate> {}", sql);
            return conn.createStatement().executeUpdate(sql);
        } catch (Exception e) {
            throw new MicroOssTransactionException(e);
        }
    }

    @Override
    public long updateObject(UpdateCommand command) throws MicroOssTransactionException {
        try {
            String sql = MysqlUtils.buildSql(command);
            log.debug("executeUpdate> {}", sql);
            return conn.createStatement().executeUpdate(sql);
        } catch (Exception e) {
            throw new MicroOssTransactionException(e);
        }
    }

    @Override
    public void commit() throws MicroOssTransactionException {
        try {
            conn.commit();
        } catch (SQLException e) {
            throw new MicroOssTransactionException(e);
        }
    }

    @Override
    public void rollback() throws MicroOssTransactionException {
        try {
            conn.rollback();
        } catch (SQLException e) {
            throw new MicroOssTransactionException(e);
        }
    }

    @Override
    public void close() throws MicroOssTransactionException {
        try {
            conn.close();
        } catch (SQLException e) {
            throw new MicroOssTransactionException(e);
        }
    }

}
