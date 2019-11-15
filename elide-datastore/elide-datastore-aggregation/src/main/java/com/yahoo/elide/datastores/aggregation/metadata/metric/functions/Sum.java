/*
 * Copyright 2019, Yahoo Inc.
 * Licensed under the Apache License, Version 2.0
 * See LICENSE file in project root for terms.
 */
package com.yahoo.elide.datastores.aggregation.metadata.metric.functions;

import com.yahoo.elide.datastores.aggregation.metadata.models.MetricFunction;

/**
 * Canned SUM metric function.
 */
public class Sum extends MetricFunction {
    public Sum() {
        super("Sum", "Sum", "Calculate sum of a metric column");
    }
}
