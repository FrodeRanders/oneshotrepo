/*
 * Copyright (C) 2024 Frode Randers
 * All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gautelis.repo.search;

import org.gautelis.repo.db.Column;
import org.gautelis.repo.db.Table;

import java.sql.Timestamp;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Objects;

public abstract class CommonDbmsAdapter extends DatabaseAdapter {
    /**
     * Unload issues for this database adapter.
     * <p>
     */
    public void unload() {
        /* Nothing to see here */
    }

    public String getTimePattern() {
        return "yyyy-MM-dd HH:mm:ss.SSS";
    }

    public String asTime(String timeStr) {
        return "{ts '" + timeStr.replace('\'', ' ') + "'}";
    }

    /**
     * Generates compound attribute constraints:  a IN ( b[0] ) AND a IN ( b[1] ) ...
     * thus overriding the default that uses UNION.
     */
    protected StringBuilder in(
            StringBuilder buf, Column column, Collection<StringBuilder> bs
    ) throws IllegalArgumentException {
        Objects.requireNonNull(bs, "bs");

        if (bs.isEmpty()) {
            throw new IllegalArgumentException("Parameter too short");
        }

        boolean doAppend = false;
        for (StringBuilder b : bs) {
            in(buf, column, b, doAppend);
            doAppend = true;
        }
        return buf;
    }

    /**
     * Generates compound attribute constraints:  (a,b) IN ( c[0] ) AND (a,b) IN ( c[1] ) ...
     * thus overriding the default that uses UNION.
     */
    protected StringBuilder in(
            StringBuilder buf, Column[] columns, Collection<StringBuilder> bs
    ) throws IllegalArgumentException {
        Objects.requireNonNull(bs, "bs");

        if (bs.isEmpty()) {
            throw new IllegalArgumentException("Parameter too short");
        }

        boolean doAppend = false;
        for (StringBuilder b : bs) {
            in(buf, columns, b, doAppend);
            doAppend = true;
        }
        return buf;
    }

    protected SearchExpression optimize(
            SearchExpression sex
    ) throws IllegalArgumentException {
        return sex;
    }

    public StringBuilder generateStatement(
            UnitSearchData sd
    ) throws IllegalArgumentException {
        Objects.requireNonNull(sd, "sd");

        SearchExpression expression = optimize(sd.getExpression());
        SearchOrder order = sd.getOrder();

        // Generate an SQL query without any constraints
        StringBuilder buf = sqlPreamble(sd);

        // Add constraints, if requested
        if (null != expression) {
            buf.append(handleExpression(expression));
        }

        // Add ordering, if requested
        if (null != order) {
            buf.append(" ");
            buf.append(handleSearchOrder(order));
        }

        return buf;
    }

    protected StringBuilder sqlPreamble(
            UnitSearchData sd
    ) throws IllegalArgumentException {
        Objects.requireNonNull(sd, "sd");

        StringBuilder buf = new StringBuilder();
        buf.append("SELECT ");

        // Unit identifications
        buf.append(Column.UNIT_TENANTID).append(", ").append(Column.UNIT_UNITID);

        // Some DBMSs demand that we formally list columns used in the ORDER BY part,
        // so it seems to be appropriate to add these columns.
        SearchOrder order = sd.getOrder();
        if (order != null) {
            order.appendColumns(buf);
        } else {
            buf.append(" ");
        }

        buf.append("FROM ").append(Table.UNIT).append(" ");
        buf.append("WHERE ");

        return buf;
    }

    protected StringBuilder handleUnitItem(
            UnitSearchItem<?> sit
    ) throws IllegalArgumentException {

        StringBuilder buf = new StringBuilder();

        switch (sit.getType()) {
            case TIME ->
                    // Adjusting time value search is only relevant for attribute searches.
                    compare(buf, sit.getColumn(), sit.getOperator(), (Timestamp) sit.getValue(), /* append? */ false, /* adjust? */ false);
            case INTEGER ->
                    compare(buf, sit.getColumn(), sit.getOperator(), (Integer) sit.getValue(), /* append? */ false);
            case LONG ->
                    compare(buf, sit.getColumn(), sit.getOperator(), (Long) sit.getValue(), /* append? */ false);
            case STRING ->
                    compare(buf, sit.getColumn(), sit.getOperator(), (String) sit.getValue(), /* append? */ false);
        }
        return buf;
    }

    private StringBuilder attributeSearch(
            AttributeSearchItem<?> sit, Column[] outerColumns
    ) throws IllegalArgumentException {

        StringBuilder buf = new StringBuilder();

        // Assemble SELECT
        buf.append("SELECT ");
        if (outerColumns.length > 1) {
            buf.append(Column.ATTRIBUTE_VALUE_TENANTID).append(", ");
        }
        buf.append(Column.ATTRIBUTE_VALUE_UNITID).append(" ")
           .append("FROM ").append(Table.UNIT).append(" ")
           .append("INNER JOIN ").append(Table.ATTRIBUTE_VALUE).append(" ON (")
                .append(Column.UNIT_TENANTID).append(" = ").append(Column.ATTRIBUTE_VALUE_TENANTID)
                .append(" AND ")
                .append(Column.UNIT_UNITID).append(" = ").append(Column.ATTRIBUTE_VALUE_UNITID)
                .append(") ");

        buf.append("INNER JOIN ");
        switch (sit.getType()) {
            case STRING ->
                    buf.append(Table.ATTRIBUTE_STRING_VALUE_VECTOR);
            case INTEGER ->
                    buf.append(Table.ATTRIBUTE_INTEGER_VALUE_VECTOR);
            case LONG ->
                    buf.append(Table.ATTRIBUTE_LONG_VALUE_VECTOR);
            case TIME ->
                    buf.append(Table.ATTRIBUTE_TIME_VALUE_VECTOR);
            case DOUBLE ->
                    buf.append(Table.ATTRIBUTE_DOUBLE_VALUE_VECTOR);
            case BOOLEAN ->
                    buf.append(Table.ATTRIBUTE_BOOLEAN_VALUE_VECTOR);
        }
        buf.append(" ON (")
                .append(Column.ATTRIBUTE_VALUE_VALUEID).append(" = ").append(Column.ATTRIBUTE_VALUE_VECTOR_VALUEID)
                .append(") ");
        buf.append("WHERE ");

        // Add mandatory constraint
        compare(buf, Column.ATTRIBUTE_VALUE_ATTRID, Operator.EQ, sit.getAttrId(), /* append? */ false);

        // Add further constraints
        switch (sit.getType()) {
            case STRING ->
                    compare(buf, Column.ATTRIBUTE_VALUE_VECTOR_ENTRY, sit.getOperator(), (String) sit.getValue(), /* append? */ true);
            case INTEGER ->
                    compare(buf, Column.ATTRIBUTE_VALUE_VECTOR_ENTRY, sit.getOperator(), (Integer) sit.getValue(), /* append? */ true);
            case LONG ->
                    compare(buf, Column.ATTRIBUTE_VALUE_VECTOR_ENTRY, sit.getOperator(), (Long) sit.getValue(), /* append? */ true);
            case TIME ->
                    // Adjusting time value search is only relevant for attribute searches.
                    compare(buf, Column.ATTRIBUTE_VALUE_VECTOR_ENTRY, sit.getOperator(), (Timestamp) sit.getValue(), /* append? */ true, /* adjust? */ true);
            case DOUBLE ->
                    compare(buf, Column.ATTRIBUTE_VALUE_VECTOR_ENTRY, sit.getOperator(), (Double) sit.getValue(), /* append? */ true);
            case BOOLEAN ->
                    compare(buf, Column.ATTRIBUTE_VALUE_VECTOR_ENTRY, sit.getOperator(), (Boolean) sit.getValue(), /* append? */ true);
        }
        return buf;
    }

    protected StringBuilder handleSingleAttributeItem(
            AttributeSearchItem<?> sit
    ) throws IllegalArgumentException {

        final Column[] columns = {Column.UNIT_UNITID};

        // Assemble search for individual attribute
        StringBuilder b = attributeSearch(sit, columns);

        // Assemble compound search
        StringBuilder buf = new StringBuilder();
        in(buf, columns, b, /* append? */ false);

        return buf;
    }

    protected StringBuilder handleMultipleAttributeItems(
            Collection<AttributeSearchItem<?>> sits
    ) throws IllegalArgumentException {

        final Column[] columns = {Column.UNIT_UNITID};

        // Assemble searches for individual attributes
        Collection<StringBuilder> b = new LinkedList<>();

        for (AttributeSearchItem<?> item : sits) {
            b.add(attributeSearch(item, columns));
        }

        // Assemble compound search
        StringBuilder buf = new StringBuilder();
        in(buf, columns, b);

        return buf;
    }

    protected StringBuilder handleSearchOrder(
            SearchOrder so
    ) throws IllegalArgumentException {
        Objects.requireNonNull(so, "so");

        Column[] columns = so.columns();
        boolean[] ascending = so.ascending();

        StringBuilder buf = new StringBuilder();
        if (columns.length > 0) {
            orderBy(buf, columns, ascending);
        }

        return buf;
    }
}
