package com.github.microprograms.micro_oss_mysql.model.ddl;

import com.github.microprograms.micro_oss_core.model.FieldDefinition.FieldTypeEnum;

public class TableColumnDefinition implements TableElementDefinition {
    private String name;
    private String type;

    public TableColumnDefinition(String name, String type) {
        this.name = name;
        this.type = type;
    }

    public static String getIgniteType(FieldTypeEnum fieldType, boolean isPrimaryKey) {
        switch (fieldType) {
        case int_type:
            return "int";
        case long_type:
            return "bigint";
        case string_type:
            return isPrimaryKey ? "varchar(255)" : "text";
        default:
            return null;
        }
    }

    @Override
    public String toText() {
        return "`" + name + "` " + type;
    }

    @Override
    public String toString() {
        return toText();
    }
}
