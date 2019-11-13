package com.github.microprograms.micro_oss_mysql.utils;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.github.microprograms.micro_oss_core.model.Entity;
import com.github.microprograms.micro_oss_core.model.Field;
import com.github.microprograms.micro_oss_core.model.FieldDefinition;
import com.github.microprograms.micro_oss_core.model.TableDefinition;
import com.github.microprograms.micro_oss_core.model.ddl.CreateTableCommand;
import com.github.microprograms.micro_oss_core.model.dml.query.Condition;
import com.github.microprograms.micro_oss_core.model.dml.query.Condition.ComplexCondition;
import com.github.microprograms.micro_oss_core.model.dml.query.PagerRequest;
import com.github.microprograms.micro_oss_core.model.dml.query.SelectCommand;
import com.github.microprograms.micro_oss_core.model.dml.query.Sort;
import com.github.microprograms.micro_oss_core.model.dml.update.DeleteCommand;
import com.github.microprograms.micro_oss_core.model.dml.update.InsertCommand;
import com.github.microprograms.micro_oss_core.model.dml.update.UpdateCommand;
import com.github.microprograms.micro_oss_mysql.model.ddl.PrimaryKeyDefinition;
import com.github.microprograms.micro_oss_mysql.model.ddl.TableColumnDefinition;
import com.github.microprograms.micro_oss_mysql.model.ddl.TableElementDefinition;

public class MysqlUtils {

	public static String buildSql(CreateTableCommand command) {
		StringBuffer sb = new StringBuffer("CREATE TABLE IF NOT EXISTS ");
		TableDefinition tableDefinition = command.getTableDefinition();
		sb.append(tableDefinition.getTableName());
		sb.append("(");
		PrimaryKeyDefinition primaryKeyDefinition = new PrimaryKeyDefinition();
		List<TableElementDefinition> tableElementDefinitions = new ArrayList<>();
		for (FieldDefinition fieldDefinition : tableDefinition.getFields()) {
			boolean isPrimaryKey = fieldDefinition.getPrimaryKey() > 0;
			if (isPrimaryKey) {
				primaryKeyDefinition.getFiledNames().put(fieldDefinition.getPrimaryKey(), fieldDefinition.getName());
			}
			tableElementDefinitions
					.add(new TableColumnDefinition(fieldDefinition.getName(), fieldDefinition.getComment(),
							TableColumnDefinition.getMysqlDataType(fieldDefinition.getType(), isPrimaryKey)));
		}
		tableElementDefinitions.add(primaryKeyDefinition);
		sb.append(StringUtils.join(tableElementDefinitions, ","));
		String tableComment = StringUtils.isBlank(tableDefinition.getComment()) ? ""
				: tableDefinition.getComment().replaceAll("'", "''");
		sb.append(String.format(") COMMENT='%s';", tableComment));
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
		return String.format("INSERT INTO %s (%s) VALUES (%s);", command.getEntity().getTableName(),
				StringUtils.join(fieldNames, ","), StringUtils.join(fieldValues, ","));
	}

	public static String buildSql(UpdateCommand command) {
		StringBuffer sb = new StringBuffer("UPDATE ").append(command.getTableName());
		List<Field> fields = command.getFields();
		if (null == fields || fields.isEmpty()) {
			return null;
		}
		List<String> pairs = new ArrayList<>();
		for (Field x : fields) {
			pairs.add(String.format("%s=%s", getSqlField(x.getName()), getSqlValue(x.getValue())));
		}
		sb.append(" SET ").append(StringUtils.join(pairs, ","));
		String where = MysqlUtils.parse(command.getWhere());
		if (StringUtils.isNotBlank(where)) {
			sb.append(" WHERE ").append(where);
		}
		return sb.append(";").toString();
	}

	public static String buildSql(DeleteCommand command) {
		StringBuffer sb = new StringBuffer("DELETE FROM ").append(command.getTableName());
		String where = MysqlUtils.parse(command.getWhere());
		if (StringUtils.isNoneBlank(where)) {
			sb.append(" WHERE ").append(where);
		}
		return sb.append(";").toString();
	}

	public static String buildSql(SelectCommand command) {
		StringBuffer sb = new StringBuffer("SELECT ");
		sb.append(command.getFieldNames() == null || command.getFieldNames().isEmpty() ? "*"
				: StringUtils.join(command.getFieldNames(), ","));
		sb.append(" FROM ").append(command.getTableName());
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
		return sb.append(";").toString();
	}

	public static String parse(Condition where) {
		if (null == where) {
			return null;
		} else if (where instanceof ComplexCondition) {
			ComplexCondition complex = (ComplexCondition) where;
			Condition[] conditions = complex.getConditions();
			if (null == conditions || conditions.length == 0) {
				return null;
			}
			List<String> childStringList = new ArrayList<>();
			for (Condition child : complex.getConditions()) {
				if (null == child) {
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
		return value.toString();
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
		if (null == pagerRequest) {
			return null;
		}
		return String.format("LIMIT %s OFFSET %s", pagerRequest.getPageSize(),
				pagerRequest.getPageIndex() * pagerRequest.getPageSize());
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
}
