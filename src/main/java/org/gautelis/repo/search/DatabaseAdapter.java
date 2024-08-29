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


import org.gautelis.repo.db.Adapter;
import org.gautelis.repo.db.Column;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Database adapter base class implementation.
 */
public abstract class DatabaseAdapter implements Adapter {
    private static final Logger log = LoggerFactory.getLogger(DatabaseAdapter.class);

    public DatabaseAdapter() {
    }

    public abstract String getTimePattern();

    public abstract String asTime(String timeStr);

    /**
     * Generates fragment: a IN ( b )
     */
    protected StringBuilder in(
            StringBuilder buf, Column column, StringBuilder b, boolean append
    ) throws IllegalArgumentException {

        if (append) {
            buf.append("AND ");
        }
        buf.append(column);
        buf.append(" IN ( ");
        buf.append(b);
        buf.append(" ) ");

        return buf;
    }

    /**
     * Generates fragment: (a,b) IN ( c )
     */
    protected StringBuilder in(
            StringBuilder buf, Column[] columns, StringBuilder b, boolean append
    ) throws IllegalArgumentException {

        if (append) {
            buf.append("AND ");
        }

        if (columns.length > 1) {
            buf.append("(");
        }
        for (int i = 0; i < columns.length; i++) {
            buf.append(columns[i]);
            if (i < columns.length - 1)
                buf.append(", ");
        }
        if (columns.length > 1) {
            buf.append(")");
        }
        buf.append(" IN ( ");
        buf.append(b);
        buf.append(" ) ");

        return buf;
    }

    /**
     * Generates fragments (for attribute searching) like:
     * a IN ( b[0] INTERSECT b[1] ... )     -- Oracle
     * a IN ( b[0] ) AND a IN ( b[1] ) ...  -- DB2
     */
    abstract protected StringBuilder in(
            StringBuilder buf, Column column, Collection<StringBuilder> bs
    ) throws IllegalArgumentException;

    /**
     * Generates fragments (for attribute searching) like:
     * (a,b) IN ( c[0] INTERSECT c[1] ... )         -- Oracle
     * (a,b) IN ( c[0] ) AND (a,b) IN ( c[1] ) ...  -- DB2
     */
    abstract protected StringBuilder in(
            StringBuilder buf, Column[] columns, Collection<StringBuilder> bs
    ) throws IllegalArgumentException;

    /**
     * Generates fragment: a OPERATOR b
     * for Strings.
     * <p>
     * Operator is one of =, &gt;=, &gt;, &lt;, &lt;=, &lt;&gt;, LIKE, ...
     */
    protected StringBuilder compare(
            StringBuilder buf, Column column, Operator operator, String b, boolean append
    ) {

        if (append) {
            buf.append("AND ");
        }
        buf.append("LOWER(");
        buf.append(column);
        buf.append(") ");

        // Detect wildcards
        boolean hasWildcards = false;

        if (Operator.EQ == operator || Operator.LIKE == operator) {
            hasWildcards =
                    b.indexOf('*') >= 0
                 || b.indexOf('%') >= 0
                 || b.indexOf('_') >= 0;

            if (hasWildcards) {
                // Replace any '*' with a '%'
                b = b.replace('*', '%');
            }
        }

        if (Operator.LIKE == operator && !hasWildcards) {
            // Use an EQ compare instead.
            buf.append(Operator.EQ.expr());

        } else if (Operator.EQ == operator && hasWildcards) {
            // Use a LIKE compare instead.
            buf.append(Operator.LIKE.expr());

        } else {
            buf.append(operator.expr());
        }

        b = b.replace('\'', ' '); // Protect against escape characters in value.
        buf.append("'");
        buf.append(b.toLowerCase());
        buf.append("' ");

        return buf;
    }

    /**
     * Generates fragment: a OPERATOR b
     * for byte[].
     * <p>
     * Operator is one of =, &gt;=, &gt;, &lt;, &lt;=, &lt;&gt;, LIKE, ...
     */
    protected StringBuilder compare(
            StringBuilder buf, Column column, Operator operator, byte[] b, boolean append
    ) {

        if (append) {
            buf.append("AND ");
        }
        buf.append(column);
        buf.append(operator.expr());
        buf.append(Arrays.toString(b)); // May not be correct
        buf.append(" ");

        return buf;
    }

    /**
     * Generates fragment: a OPERATOR b
     * for java.sql.Timestamp.
     * <p>
     * Operator is one of =, &gt;=, &gt;, &lt;, &lt;=, &lt;&gt;, LIKE, ...
     * <p>
     * The 'adjust' is used when searching for attributes to allow
     * equality matches where the 'b' is not specified down to the
     * individual millis.
     */
    protected StringBuilder compare(
            StringBuilder buf, Column column, Operator operator, java.sql.Timestamp b, boolean append, boolean adjust
    ) {

        if (append) {
            buf.append("AND ");
        }

        // Convert date to a known format
        SimpleDateFormat sdf = (SimpleDateFormat) DateFormat.getDateTimeInstance(
                /* datestyle */ DateFormat.SHORT, /* timestyle */ DateFormat.MEDIUM, Locale.ENGLISH
        );
        sdf.applyPattern(getTimePattern());

        if (adjust && Operator.EQ == operator) {
            // Since we are checking for equality and we are advised to adjust
            // the search (practically indicating that we are comparing dates),
            // we will do so depending on the granularity of the provided timestamp.
            //
            // Otherwise, we will never get a match on a date attribute if we did
            // not have the exact value including all millis. This is probably not
            // what the users of the system would expect.
            //
            // Thus, we try to establish some kind of interval in which we seek
            // a match. If we got a date "2008-01-23 23:12", then we will actually
            // accept matches in the range
            // from (including) "2008-01-23 23:12:00.000000000"
            // to (including) "2008-01-23 23:12:59.999999999"
            //
            // "In the date format for DT_DBTIMESTAMP, fffffffff is a value between 0 and 999999999",
            // but since some DBMS does not record more than 3 fractional seconds, we will use
            // only 3 digits.
            //
            // SELECT TO_TIMESTAMP('2008-01-23 00:00:00.123456789', 'YYYY-MM-DD HH24:MI:SS.FF3') FROM DUAL
            // --> 23-JAN-08 12.00.00.123 AM

            // Match on the time pattern (above)
            String d = "(\\d\\d+)"; // number
            String s = ".*?"; // filler
            Pattern p = Pattern.compile(d + s + d + s + d + s + d + s + d + s + d + s + d, Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

            //
            Locale some24HourLocale = Locale.of("sv", "SE");
            Calendar lower = Calendar.getInstance(some24HourLocale);
            lower.setTimeInMillis(b.getTime());
            Calendar higher = Calendar.getInstance(some24HourLocale);
            higher.setTimeInMillis(b.getTime());
            Calendar bc = Calendar.getInstance(some24HourLocale);
            bc.setTimeInMillis(b.getTime());
            boolean compareWithInterval = false;

            String dateStr = sdf.format(b);
            Matcher m = p.matcher(dateStr);
            if (m.find()) {
                String _year = m.group(1);
                if (null != _year && !_year.isEmpty()) {
                    int year = Integer.parseInt(_year);
                    if (0 == year) {
                        // year not specified
                        lower.set(Calendar.YEAR, lower.getActualMinimum(Calendar.YEAR));
                        higher.set(Calendar.YEAR, higher.getActualMaximum(Calendar.YEAR));
                        compareWithInterval = true;
                    }
                }

                String _month = m.group(2);
                if (compareWithInterval || (null != _month && !_month.isEmpty())) {
                    int month = Integer.parseInt(_month);
                    if (compareWithInterval || 0 == month) {
                        // month not specified
                        lower.set(Calendar.MONTH, lower.getActualMinimum(Calendar.MONTH));
                        higher.set(Calendar.MONTH, higher.getActualMaximum(Calendar.MONTH));
                        compareWithInterval = true;
                    }
                }

                String _day = m.group(3);
                if (compareWithInterval || (null != _day && !_day.isEmpty())) {
                    int day = Integer.parseInt(_day);
                    if (compareWithInterval || 0 == day) {
                        // day not specified
                        lower.set(Calendar.DAY_OF_MONTH, lower.getActualMinimum(Calendar.DAY_OF_MONTH));
                        higher.set(Calendar.DAY_OF_MONTH, higher.getActualMaximum(Calendar.DAY_OF_MONTH));
                        compareWithInterval = true;
                    }
                }

                String _hour = m.group(4);
                if (compareWithInterval || (null != _hour && !_hour.isEmpty())) {
                    int hour = Integer.parseInt(_hour);
                    if (compareWithInterval || 0 == hour) {
                        // hour not specified
                        lower.set(Calendar.HOUR, lower.getActualMinimum(Calendar.HOUR));
                        higher.set(Calendar.HOUR, 23); // higher.getActualMaximum(Calendar.HOUR) returns 11 ???
                        compareWithInterval = true;
                    }
                }

                String _minutes = m.group(5);
                if (compareWithInterval || (null != _minutes && !_minutes.isEmpty())) {
                    int minutes = Integer.parseInt(_minutes);
                    if (compareWithInterval || 0 == minutes) {
                        // minutes not specified
                        lower.set(Calendar.MINUTE, lower.getActualMinimum(Calendar.MINUTE));
                        higher.set(Calendar.MINUTE, higher.getActualMaximum(Calendar.MINUTE));
                        compareWithInterval = true;
                    }
                }

                String _seconds = m.group(6);
                if (compareWithInterval || (null != _seconds && !_seconds.isEmpty())) {
                    int seconds = Integer.parseInt(_seconds);
                    if (compareWithInterval || 0 == seconds) {
                        // seconds not specified
                        lower.set(Calendar.SECOND, lower.getActualMinimum(Calendar.SECOND));
                        higher.set(Calendar.SECOND, higher.getActualMaximum(Calendar.SECOND));
                        compareWithInterval = true;
                    }
                }

                String _millis = m.group(7);
                if (compareWithInterval || (null != _millis && !_millis.isEmpty())) {
                    int millis = Integer.parseInt(_millis);
                    if (compareWithInterval || 0 == millis) {
                        // milliseconds not specified
                        lower.set(Calendar.MILLISECOND, lower.getActualMinimum(Calendar.MILLISECOND));
                        higher.set(Calendar.MILLISECOND, higher.getActualMaximum(Calendar.MILLISECOND));
                        compareWithInterval = true;
                    }
                }
            }
            if (compareWithInterval) {
                buf.append("( ");
                buf.append(column);
                buf.append(Operator.GEQ.expr());
                buf.append(asTime(sdf.format(lower.getTime())));
                buf.append(" AND ");
                buf.append(column);
                buf.append(Operator.LEQ.expr());
                buf.append(asTime(sdf.format(higher.getTime())));
                buf.append(" ) ");
            } else {
                buf.append(column);
                buf.append(operator.expr()); // operator is EQ
                buf.append(asTime(sdf.format(lower.getTime())));
                buf.append(" ");
            }
        } else {
            buf.append(column);
            buf.append(operator.expr());
            buf.append(asTime(sdf.format(b)));
            buf.append(" ");
        }

        return buf;
    }

    /**
     * Generates fragment: a OPERATOR b
     * for integers.
     * <p>
     * Operator is one of =, &gt;=, &gt;, &lt;, &lt;=, &lt;&gt;, LIKE, ...
     */
    protected StringBuilder compare(
            StringBuilder buf, Column column, Operator operator, int b, boolean append
    ) {

        if (append) {
            buf.append("AND ");
        }
        buf.append(column);
        buf.append(operator.expr());
        buf.append(b);
        buf.append(" ");

        return buf;
    }

    /**
     * Generates fragment: a OPERATOR b
     * for booleans.
     * <p>
     * Operator is one of =, &gt;=, &gt;, &lt;, &lt;=, &lt;&gt;, LIKE, ...
     */
    protected StringBuilder compare(
            StringBuilder buf, Column column, Operator operator, boolean b, boolean append
    ) {

        if (append) {
            buf.append("AND ");
        }
        buf.append(column);
        buf.append(operator.expr());
        buf.append(b ? 1 : 0); // Treat as integer
        buf.append(" ");

        return buf;
    }

    /**
     * Generates fragment: a OPERATOR b
     * for float.
     * <p>
     * Operator is one of =, &gt;=, &gt;, &lt;, &lt;=, &lt;&gt;, LIKE, ...
     */
    protected StringBuilder compare(
            StringBuilder buf, Column column, Operator operator, double d, boolean append
    ) throws IllegalArgumentException {

        if (append) {
            buf.append("AND ");
        }
        buf.append(column);
        buf.append(operator.expr());
        buf.append(d);
        buf.append(" ");

        return buf;
    }

    /**
     * Generates fragment:  ( a ) AND ( b ),  ( a ) OR ( b ).
     * AND defines the intersection of elements between sets A and B, while
     * OR defines the union of elements in either A, B or in both sets.
     */
    protected StringBuilder logicOperator(
            StringBuilder buf, StringBuilder a, Operator operator, StringBuilder b
    ) throws IllegalArgumentException {

        if (operator != Operator.AND && operator != Operator.OR) {
            throw new IllegalArgumentException("Unknown operator: " + operator);
        }

        buf.append("( ");
        buf.append(a);
        buf.append(operator.expr());
        buf.append(b);
        buf.append(") ");

        return buf;
    }

    /**
     * Generates fragment: NOT ( a )
     */
    protected StringBuilder logicNegate(
            StringBuilder buf, StringBuilder a
    ) throws IllegalArgumentException {

        buf.append(Operator.NOT.expr()).append("(");
        buf.append(a);
        buf.append(") ");

        if (log.isTraceEnabled()) {
            log.trace(buf.toString());
        }

        return buf;
    }

    /**
     * Conditionally generates fragment: NOT ( a )
     */
    protected StringBuilder logicNegate(
            StringBuilder buf, boolean condition, StringBuilder a
    ) throws IllegalArgumentException {

        if (condition) {
            buf.append(a);
            buf.append(" ");
        } else {
            buf.append(Operator.NOT.expr()).append("(");
            buf.append(a);
            buf.append(") ");
        }

        return buf;
    }

    /**
     * Generates fragment: ORDER BY a ASC, ORDER BY b DESC
     */
    protected StringBuilder orderBy(
            StringBuilder buf, Column column, boolean ascending
    ) throws IllegalArgumentException {

        buf.append("ORDER BY ");
        buf.append(column);
        buf.append(ascending ? " ASC " : " DESC ");

        return buf;
    }

    /**
     * Generates fragment like: ORDER BY a, b, c ASC, ORDER BY a, b, c DESC
     */
    protected StringBuilder orderBy(
            StringBuilder buf, Column[] column, boolean[] ascending
    ) throws IllegalArgumentException {

        int length = column.length;

        if (length < 1) {
            throw new IllegalArgumentException("Parameter array is too short");
        }

        buf.append("ORDER BY ");
        for (int i = 0; i < length; i++) {
            buf.append(column[i]);
            buf.append(ascending[i] ? " ASC" : " DESC");
            if (length - i > 1) {
                buf.append(", ");
            }
        }
        buf.append(" ");

        return buf;
    }

    //---------------------------------------------------------------
    // GENERIC STRUCTURAL
    //---------------------------------------------------------------

    public abstract StringBuilder generateStatement(
            UnitSearchData sd
    ) throws IllegalArgumentException;

    public StringBuilder generateStatement(
            SearchExpression sex
    ) throws IllegalArgumentException {
        return generateStatement(new UnitSearchData(sex));
    }

    protected StringBuilder handleExpression(
            SearchExpression sex
    ) throws IllegalArgumentException {

        StringBuilder buf = new StringBuilder();
        if (null == sex) {
            return buf;
        }

        //  --- Expression data ---
        //  Expression 2 is only valid during AND & OR operations.
        //
        //  Unary NOT and single value expressions always use
        //  Expression 1 or Item. In the latter case
        //  Expression 2 is not even checked.
        //
        //  This behaviour is controlled by the constructors
        //  of SearchExpression that enforce a consistent
        //  use of Expression 1, Expression 2 and Item.
        //  ------------------------------------------------
        SearchExpression expr1 = sex.getExpression1();
        SearchExpression expr2 = sex.getExpression2();
        SearchItem<?> item = sex.getItem();
        Operator op = sex.getOperator();

        switch (op) {
            case NOT:
                if (null != expr1) {
                    logicNegate(buf, handleExpression(expr1));
                } else {
                    logicNegate(buf, handleItem(item));
                }
                break;

            case AND:
            case OR:
                logicOperator(buf, handleExpression(expr1), op, handleExpression(expr2));
                break;

            default:
                if (null != expr1) {
                    buf.append(handleExpression(expr1));
                } else {
                    buf.append(handleItem(item));
                }
                break;
        }

        return buf;
    }

    /**
     * Determines if there are any attribute search items in the expression.
     */
    protected boolean hasAttributeConstraints(
            SearchExpression expression
    ) {
        // Check if search item is attribute constraint kind
        SearchItem<?> item = expression.getItem();
        if (null != item) {
            return switch (item.getVariant()) {
                case ATTRIBUTE -> true;
                default -> false;
            };
        }

        // Search in sub-expressions
        SearchExpression expr1 = expression.getExpression1();
        if (hasAttributeConstraints(expr1)) {
            return true;
        }

        SearchExpression expr2 = expression.getExpression2();
        return hasAttributeConstraints(expr2);
    }

    /**
     * Counts number of attribute search items in expression.
     */
    protected int countAttributeConstraints(
            SearchExpression expression
    ) {
        Objects.requireNonNull(expression, "expression");

        int count = 0;

        // Check if search item is attribute constraint kind
        SearchItem<?> item = expression.getItem();
        if (null != item) {
            if (item.getVariant() == SearchItem.Variant.ATTRIBUTE) {
                count++;
            }
        }

        SearchExpression expr1 = expression.getExpression1();
        if (null != expr1) {
            count += countAttributeConstraints(expr1);
        }

        SearchExpression expr2 = expression.getExpression2();
        if (null != expr2) {
            count += countAttributeConstraints(expr2);
        }

        return count;
    }

    /**
     * Handles search items.
     */
    protected StringBuilder handleItem(
            SearchItem<?> sit
    ) throws IllegalArgumentException {

        StringBuilder buf = new StringBuilder();
        if (null == sit) {
            return buf;
        }

        switch (sit.getVariant()) {
            case UNIT -> buf = handleUnitItem((UnitSearchItem<?>) sit);
            case ATTRIBUTE -> buf = handleSingleAttributeItem((AttributeSearchItem<?>) sit);
        }

        return buf;
    }

    /**
     * Handles search items.
     */
    protected StringBuilder handleItems(
            Collection<AttributeSearchItem<?>> sits
    ) throws IllegalArgumentException {

        StringBuilder buf = new StringBuilder();
        if (null == sits) {
            return buf;
        }

        buf = handleMultipleAttributeItems(sits);

        return buf;
    }

    //---------------------------------------------------------------
    // DBMS SPECIFIC GENERATORS
    //---------------------------------------------------------------
    abstract protected SearchExpression optimize(
            SearchExpression sex
    ) throws IllegalArgumentException;

    abstract protected StringBuilder sqlPreamble(
            UnitSearchData sd
    ) throws IllegalArgumentException;

    abstract protected StringBuilder handleUnitItem(
            UnitSearchItem<?> sit
    ) throws IllegalArgumentException;

    abstract protected StringBuilder handleSingleAttributeItem(
            AttributeSearchItem<?> sit
    ) throws IllegalArgumentException;

    abstract protected StringBuilder handleMultipleAttributeItems(
            Collection<AttributeSearchItem<?>> sits
    ) throws IllegalArgumentException;

    abstract protected StringBuilder handleSearchOrder(
            SearchOrder so
    ) throws IllegalArgumentException;
}
