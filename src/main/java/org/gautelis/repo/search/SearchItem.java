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
import org.gautelis.repo.exceptions.InvalidParameterException;
import org.gautelis.repo.model.Unit;
import org.gautelis.repo.model.attributes.Type;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.Calendar;
import java.util.Locale;
import java.util.Objects;

/**
 *
 */
public abstract class SearchItem<T> {
    //
    public enum Variant {
        UNKNOWN(0),
        UNIT(1),
        ATTRIBUTE(2);

        private final int variant;

        Variant(int variant) {
            this.variant = variant;
        }

        public int getVariant() {
            return variant;
        }
    }

    protected final Variant variant;
    protected final Type type;
    protected final Operator operator;

    protected SearchItem(Variant variant, Type type, Operator operator) {
        Objects.requireNonNull(variant, "variant");
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(operator, "operator");

        this.variant = variant;
        this.type = type;
        this.operator = operator;
    }

    public Variant getVariant() {
        return variant;
    }

    public Type getType() {
        return type;
    }

    public Operator getOperator() {
        return operator;
    }

    public abstract T getValue();

    /**
     * Converts a string to a date given a specific locale.
     *
     * @param date
     * @param locale
     * @return
     * @throws ParseException
     */
    public static java.util.Date string2Date(
            String date, Locale locale
    ) throws ParseException {
        if (null != date && date.length() == 1) {
            Calendar now = Calendar.getInstance(); // locale not important
            now.setTimeInMillis(System.currentTimeMillis());

            switch (date.toLowerCase().charAt(0)) {
                case 'd':
                case 't': {
                    // This date, "today" or whatever
                    Calendar c = Calendar.getInstance(); // locale not important
                    c.clear();
                    c.set(now.get(Calendar.YEAR), now.get(Calendar.MONTH), now.get(Calendar.DATE));
                    return c.getTime();
                }

                case 'n':
                    // This very moment ("now")
                    return now.getTime();
            }
        }

        // Parse the date and possibly time, minding the locale
        try {
            // -----------------------------
            // Format: 2024-08-26 19:15:23
            // -----------------------------
            DateFormat df = DateFormat.getDateTimeInstance(
                    /* date style */ DateFormat.SHORT, /* time style */ DateFormat.MEDIUM, locale
            );
            return df.parse(date);

        } catch (ParseException pe1) {
            try {
                // ---------------------------
                // Format: 2024-08-26 19:15
                // ---------------------------
                DateFormat df = DateFormat.getDateTimeInstance(
                        /* date style */ DateFormat.SHORT, /* time style */ DateFormat.SHORT, locale
                );
                return df.parse(date);

            } catch (ParseException pe2) {
                // ---------------------------
                // Format: 2024-08-26
                // ---------------------------
                DateFormat df = DateFormat.getDateInstance(
                        /* date style */ DateFormat.SHORT, locale
                );
                return df.parse(date);
            }
        }
    }

    /**
     * Returns an appropriate from-date, given a date as a string.
     * <p>
     * The general idea is to specify date + time, time being
     * 00:00:00.000 of that date.
     * <p>
     * Match call with call to #toDate.
     *
     * @param str
     * @param locale
     * @return
     */
    public static Timestamp early(
            String str, Locale locale
    ) throws ParseException {
        Objects.requireNonNull(str, "str");

        java.util.Date d = string2Date(str, locale);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(d);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);

        return new Timestamp(calendar.getTime().getTime());
    }

    /**
     * Returns an appropriate to-date, given a date as a string.
     * <p>
     * The general idea is to specify date + time, time being
     * 23:59:59.999 of that date.
     * <p>
     * Match call with call to #fromDate.
     *
     * @param str
     * @param locale
     * @return
     */
    public static Timestamp late(
            String str, Locale locale
    ) throws ParseException {
        Objects.requireNonNull(str, "str");

        java.util.Date d = string2Date(str, locale);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(d);
        calendar.set(Calendar.HOUR_OF_DAY, 23);
        calendar.set(Calendar.MINUTE, 59);
        calendar.set(Calendar.SECOND, 59);
        calendar.set(Calendar.MILLISECOND, 999);

        return new Timestamp(calendar.getTime().getTime());
    }

    /**
     * Generates constraint "unit has specific status", identified by
     * one of the following constants:
     * <UL>
     * <LI>Unit.Status.PENDING_DELETION</LI>
     * <LI>Unit.Status.PENDING_DISPOSITION</LI>
     * <LI>Unit.Status.OBLITERATED</LI>
     * <LI>Unit.Status.EFFECTIVE</LI>
     * <LI>Unit.Status.ARCHIVED</LI>
     * </UL>
     *
     * @param status
     * @return
     */
    public static IntegerUnitSearchItem constrainToSpecificStatus(Unit.Status status) {
        return new IntegerUnitSearchItem(Column.UNIT_STATUS, Operator.EQ, status.getStatus());
    }

    /**
     * Generates constraint "unit has specific tenant", identified by tenantId.
     */
    public static IntegerUnitSearchItem constrainToSpecificTenant(int tenantId) {
        return new IntegerUnitSearchItem(Column.UNIT_TENANTID, Operator.EQ, tenantId);
    }

    /**
     * Generates constraint "unit is effective/operative/in use"
     */
    public static SearchExpression constrainToEffective() {
        return new SearchExpression(new IntegerUnitSearchItem(Column.UNIT_STATUS, Operator.GEQ, Unit.Status.EFFECTIVE.getStatus()));
    }

    /**
     * Generates a constraint "unit was created before"
     */
    public static TimeUnitSearchItem constrainToCreatedBefore(Timestamp timestamp) {
        Objects.requireNonNull(timestamp, "timestamp");
        return new TimeUnitSearchItem(Column.UNIT_CREATED, Operator.LT, timestamp);
    }

    /**
     * Generates a constraint "unit was created after (inclusive)"
     */
    public static TimeUnitSearchItem constrainToCreatedAfter(Timestamp timestamp) {
        Objects.requireNonNull(timestamp, "timestamp");
        return new TimeUnitSearchItem(Column.UNIT_CREATED, Operator.GEQ, timestamp);
    }
}
