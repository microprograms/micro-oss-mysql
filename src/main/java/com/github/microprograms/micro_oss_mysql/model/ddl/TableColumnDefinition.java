package com.github.microprograms.micro_oss_mysql.model.ddl;

import org.apache.commons.lang3.StringUtils;

import com.github.microprograms.micro_oss_core.model.FieldDefinition.FieldTypeEnum;

public class TableColumnDefinition implements TableElementDefinition {
	private String name;
	private String comment;
	private String type;

	public TableColumnDefinition(String name, String comment, String type) {
		this.name = name;
		this.comment = comment;
		this.type = type;
	}

	public static String getMysqlDataType(FieldTypeEnum fieldType, boolean isPrimaryKey) {
		switch (fieldType) {
		case int_type:
			return "int";
		case long_type:
			return "bigint";
		case string_type:
			return isPrimaryKey ? "varchar(99)" : "text";
		case datetime_type:
			return "datetime";
		default:
			return null;
		}
	}

	@Override
	public String toText() {
		return String.format("`%s` %s COMMENT '%s'", name, type,
				StringUtils.isBlank(comment) ? "" : comment.replaceAll("'", "''"));
	}

	@Override
	public String toString() {
		return toText();
	}
}
