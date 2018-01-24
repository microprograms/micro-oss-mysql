package com.github.microprograms.micro_oss_mysql.utils;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.github.microprograms.micro_oss_core.model.Entity;
import com.github.microprograms.micro_oss_core.model.Field;
import com.github.microprograms.micro_oss_core.model.dml.ComplexCondition;
import com.github.microprograms.micro_oss_core.model.dml.Condition;
import com.github.microprograms.micro_oss_core.model.dml.DeleteCommand;
import com.github.microprograms.micro_oss_core.model.dml.InsertCommand;
import com.github.microprograms.micro_oss_core.model.dml.PagerRequest;
import com.github.microprograms.micro_oss_core.model.dml.SelectCommand;
import com.github.microprograms.micro_oss_core.model.dml.Sort;
import com.github.microprograms.micro_oss_core.model.dml.UpdateCommand;

public class MysqlUtils {

    public static String parse(Condition where) {
        if (where == null) {
            return null;
        } else if (where instanceof ComplexCondition) {
            ComplexCondition complex = (ComplexCondition) where;
            if (complex.getConditions().isEmpty()) {
                return null;
            }
            List<String> childStringList = new ArrayList<>();
            for (Condition child : complex.getConditions()) {
                if (child == null) {
                    continue;
                }
                String childString = parse(child);
                if (StringUtils.isBlank(childString)) {
                    continue;
                }
                if (child instanceof ComplexCondition) {
                    childStringList.add("(" + childString + ")");
                } else {
                    childStringList.add(childString);
                }
            }
            return StringUtils.join(childStringList, getSeparator(complex.getType()));
        } else {
            return String.format("%s %s", where.getKey(), getSqlValue(where.getValue()));
        }
    }

    private static String getSeparator(ComplexCondition.TypeEnum type) {
        switch (type) {
        case and:
            return " and ";
        case or:
            return " or ";
        default:
            throw new RuntimeException("Unsupported Type");
        }
    }

    private static String getSqlField(String field) {
        return "`" + field + "`";
    }

    private static String getSqlValue(Object value) {
        if (value instanceof String) {
            return "'" + value.toString().replaceAll("'", "''") + "'";
        }
        if (value instanceof Integer) {
            return value.toString();
        }
        if (value instanceof Long) {
            return value.toString();
        }
        return null;
    }

    public static String parseSorts(List<Sort> sorts) {
        if (sorts == null || sorts.isEmpty()) {
            return null;
        }
        List<String> list = new ArrayList<>();
        for (Sort x : sorts) {
            list.add(String.format("%s %s", getSqlField(x.getFieldName()), x.getType()));
        }
        return StringUtils.join(list, ",");
    }

    public static String parse(PagerRequest pagerRequest) {
        if (pagerRequest == null) {
            return null;
        }
        return String.format("LIMIT %s OFFSET %s", pagerRequest.getPageSize(), pagerRequest.getPageIndex() * pagerRequest.getPageSize());
    }

    public static List<Entity> getEntityList(String tableName, ResultSet rs) throws SQLException {
        List<Entity> list = new ArrayList<>();
        ResultSetMetaData rsMetaData = rs.getMetaData();
        while (rs.next()) {
            List<Field> fields = new ArrayList<>();
            for (int i = 1; i <= rsMetaData.getColumnCount(); i++) {
                fields.add(new Field(rsMetaData.getColumnLabel(i), rs.getObject(i)));
            }
            list.add(new Entity(tableName, fields));
        }
        return list;
    }

    public static int getCount(ResultSet rs) throws SQLException {
        rs.next();
        return rs.getObject("count", Integer.class);
    }

    public static void closeQuietly(Closeable closeable) {
        try {
            if (closeable != null) {
                closeable.close();
            }
        } catch (IOException e) {
            // ignore
        }
    }

    public static void closeQuietly(AutoCloseable autoCloseable) {
        try {
            if (autoCloseable != null) {
                autoCloseable.close();
            }
        } catch (Exception e) {
            // ignore
        }
    }

    public static void rollbackQuietly(Connection connection) {
        try {
            if (connection != null) {
                connection.rollback();
            }
        } catch (SQLException e) {
            // ignore
        }
    }

    public static String buildSql(SelectCommand command) {
        StringBuffer sb = new StringBuffer("SELECT ");
        sb.append(command.getFieldNames() == null || command.getFieldNames().isEmpty() ? "*" : StringUtils.join(command.getFieldNames(), ","));
        sb.append(" FROM ").append(command.getTableName());
        if (command.getLeftJoin() != null) {
            sb.append(" LEFT JOIN ").append(command.getLeftJoin().getTableName()).append(" ON ").append(MysqlUtils.parse(command.getLeftJoin().getCondition()));
        }
        String where = MysqlUtils.parse(command.getWhere());
        if (StringUtils.isNotBlank(where)) {
            sb.append(" WHERE ").append(where);
        }
        String sort = MysqlUtils.parseSorts(command.getSorts());
        if (StringUtils.isNotBlank(sort)) {
            sb.append(" ORDER BY ").append(sort);
        }
        String pager = MysqlUtils.parse(command.getPager());
        if (StringUtils.isNotBlank(pager)) {
            sb.append(" ").append(pager);
        }
        return sb.toString();
    }

    public static String buildSql(DeleteCommand command) {
        StringBuffer sb = new StringBuffer("DELETE FROM ").append(command.getTableName());
        String where = MysqlUtils.parse(command.getWhere());
        if (StringUtils.isNoneBlank(where)) {
            sb.append(" WHERE ").append(where);
        }
        return sb.toString();
    }

    public static String buildSql(InsertCommand command) {
        List<String> fieldNames = new ArrayList<>();
        List<String> fieldValues = new ArrayList<>();
        for (Field field : command.getEntity().getFields()) {
            fieldNames.add(getSqlField(field.getName()));
            String value = getSqlValue(field.getValue());
            fieldValues.add(value == null ? "null" : value);
        }
        return String.format("INSERT INTO %s (%s) VALUES (%s)", command.getEntity().getTableName(), StringUtils.join(fieldNames, ","), StringUtils.join(fieldValues, ","));
    }

    public static String buildSql(UpdateCommand command) {
        StringBuffer sb = new StringBuffer("UPDATE ").append(command.getTableName());
        String fields = parseFields(command.getFields());
        if (StringUtils.isNotBlank(fields)) {
            sb.append(" SET ").append(fields);
        }
        String where = MysqlUtils.parse(command.getWhere());
        if (StringUtils.isNotBlank(where)) {
            sb.append(" WHERE ").append(where);
        }
        return sb.toString();
    }

    public static String parseFields(List<Field> fields) {
        if (fields == null || fields.isEmpty()) {
            return null;
        }
        List<String> list = new ArrayList<>();
        for (Field x : fields) {
            list.add(String.format("%s=%s", getSqlField(x.getName()), getSqlValue(x.getValue())));
        }
        return StringUtils.join(list, ",");
    }
}
