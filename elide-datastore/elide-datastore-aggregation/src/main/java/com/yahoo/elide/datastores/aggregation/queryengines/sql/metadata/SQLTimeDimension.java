/*
 * Copyright 2020, Yahoo Inc.
 * Licensed under the Apache License, Version 2.0
 * See LICENSE file in project root for terms.
 */
package com.yahoo.elide.datastores.aggregation.queryengines.sql.metadata;

import static com.yahoo.elide.datastores.aggregation.queryengines.sql.SQLQueryEngine.generateColumnReference;
import static com.yahoo.elide.datastores.aggregation.queryengines.sql.SQLQueryEngine.getClassAlias;

import com.yahoo.elide.core.EntityDictionary;
import com.yahoo.elide.core.Path;
import com.yahoo.elide.datastores.aggregation.core.JoinPath;
import com.yahoo.elide.datastores.aggregation.metadata.models.Table;
import com.yahoo.elide.datastores.aggregation.metadata.models.TimeDimension;
import com.yahoo.elide.datastores.aggregation.queryengines.sql.annotation.JoinTo;

import lombok.Getter;

/**
 * SQLTimeDimension are time dimension columns with extra physical information.
 */
public class SQLTimeDimension extends TimeDimension implements SQLColumn {
    @Getter
    private final String reference;

    @Getter
    private final JoinPath joinPath;

    @Getter
    private EntityDictionary metadataDictionary;

    public SQLTimeDimension(Table table, String fieldName, EntityDictionary dictionary) {
        super(table, fieldName, dictionary);
        Class<?> tableClass = dictionary.getEntityClass(table.getId());

        JoinTo joinTo = dictionary.getAttributeOrRelationAnnotation(tableClass, JoinTo.class, fieldName);

        if (joinTo == null || joinTo.path().equals("")) {
            this.reference = getClassAlias(tableClass) + "." + dictionary.getAnnotatedColumnName(tableClass, fieldName);
            this.joinPath = null;
        } else {
            JoinPath path = new JoinPath(tableClass, dictionary, joinTo.path());
            this.reference = generateColumnReference(path, dictionary);
            this.joinPath = path;
        }

        this.metadataDictionary = dictionary;
    }

    @Override
    public Path getSourcePath(EntityDictionary metadataDictionary) {
        return joinPath == null ? super.getSourcePath(metadataDictionary) : joinPath;
    }
}
