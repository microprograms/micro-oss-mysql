package com.github.microprograms.micro_oss_mysql.model.ddl;

import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang3.StringUtils;

public class PrimaryKeyDefinition implements TableElementDefinition {
    private Map<Integer, String> filedNames;

    public PrimaryKeyDefinition() {
        this.filedNames = new TreeMap<>();
    }

    public Map<Integer, String> getFiledNames() {
        return filedNames;
    }

    public void setFiledNames(Map<Integer, String> filedNames) {
        this.filedNames = filedNames;
    }

    @Override
    public String toText() {
        return String.format("PRIMARY KEY(%s)", StringUtils.join(filedNames.values(), ","));
    }

    @Override
    public String toString() {
        return toText();
    }
}
