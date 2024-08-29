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
package org.gautelis.repo.model.attributes;

import org.gautelis.repo.exceptions.*;
import org.gautelis.repo.model.Context;
import org.gautelis.repo.model.Unit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

public abstract class Value<T> {
    static final Logger log = LoggerFactory.getLogger(Value.class);

    public static final long ILLEGAL_VALUE_ID = -1L;

    protected final ArrayList<T> values = new ArrayList<>();
    private int initialHashCode;
    private boolean isNew;

    /**
     * Creates a <I>new</I> attribute value.
     * <p>
     * Called from derived objects.
     */
    protected Value() {
        isNew = true;

        // Mark current status, so we can detect changes later...
        initialHashCode = values.hashCode();
    }

    /**
     * Creates an <I>existing</I> attribute value from a resultset.
     * <p>
     * Called from derived objects.
     */
    protected Value(ResultSet rs) throws DatabaseReadException {
        isNew = false;

        try {
            // We will only pick the value vector elements associated with
            // the current (attribute) value.
            final long valueId = rs.getLong("valueid");

            while (/* we have an index (into the value vector) */ !rs.wasNull()) {

                // Get value element from vector
                inflate(rs);

                // Boundary at next valueid
                if (rs.next()) {
                    // Verify that we are still referring to the same valueId,
                    // and that we are referring to the next value index.
                    long nextValueId = rs.getLong("valueid");

                    if (/* we have a next index */ !rs.wasNull() &&
                        /* same valueid */ valueId == nextValueId) {
                        continue;
                    }
                }
                break;
            }

            // Mark current status, so we can detect changes later...
            initialHashCode = values.hashCode();

        } catch (SQLException sqle) {
            throw new DatabaseReadException(sqle);
        }
    }

    /**
     * Creates a <I>new</I> attribute value of type 'type'
     */
    /* package accessible only */
    static <T> Value<T> createValue(Type type) throws AttributeTypeException {
        Value<?> value = switch (type) {
            case STRING -> new StringValue();
            case TIME -> new TimeValue();
            case INTEGER -> new IntegerValue();
            case LONG -> new LongValue();
            case DOUBLE -> new DoubleValue();
            case BOOLEAN -> new BooleanValue();
            case DATA -> new DataValue();
        };

        //noinspection unchecked
        return (Value<T>) value;
    }

    /**
     * Inflates an <I>existing</I> attribute value from a result set.
     * <p>
     * This is a <I>helper method</I> called by Attribute when
     * creating attribute values, since Attribute knows
     * nothing about the various value types (StringValue, ...)
     */
    protected static <T> Value<T> inflateValue(
            Type type,
            ResultSet rs
    ) throws AttributeTypeException, DatabaseReadException {
        Value<?> value = switch (type) {
            case STRING -> new StringValue(rs);
            case TIME -> new TimeValue(rs);
            case INTEGER -> new IntegerValue(rs);
            case LONG -> new LongValue(rs);
            case DOUBLE -> new DoubleValue(rs);
            case BOOLEAN -> new BooleanValue(rs);
            case DATA -> new DataValue(rs);
        };

        //noinspection unchecked
        return (Value<T>) value;
    }

    /**
     * Actually inflate values from the resultset.
     * <p>
     * This method must be overridden by each value type (StringValue, ...)
     */
    protected abstract void inflate(ResultSet rs) throws DatabaseReadException;

    /**
     * Gets type of attribute.
     */
    public abstract Type getType();

    /**
     * Returns dimension information.
     * An attribute is scalar if there only exists
     * one value for the attribute. We view scalar
     * attributes as a special case of the more general
     * situation with multiple values for the attribute.
     * <p>
     * If no values are associated with an attribute
     * we assume scalar.
     */
    public boolean isScalar() {
        return values.size() <= 1;
    }

    public boolean isVector() {
        return values.size() > 1;
    }

    /**
     * Returns number of values associated with attribute
     */
    public int getSize() {
        return values.size();
    }

    /**
     * Mark attribute value as stored (to database)
     */
    protected void setStored() {
        initialHashCode = values.hashCode();
        isNew = false;
    }

    /**
     * Get all values
     */
    public ArrayList<T> get() {
        return values;
    }

    public boolean isNew() {
        return isNew;
    }

    /**
     * Have any values been modified?
     */
    public boolean isModified() {
        return initialHashCode != values.hashCode();
    }

    /**
     * Sets values. The types of the values must
     * be compatible with the attribute type, unless
     * an AttributeTypeException exception is thrown.
     */
    public abstract void set(ArrayList<T> values)
            throws AttributeTypeException;

    /**
     * Treats value as a scalar and returns first element
     * in vector. If no element was found, null is returned.
     */
    public abstract T getScalar();

    /* Package accessible only */
    void copy(Value<T> other) {
        values.addAll(other.values);
    }

    /*--------------------------------------------------------
     * Override the proper setter/getter and ignore the rest
     *-------------------------------------------------------*/

    /**
     * Verifies that value is of right type
     */
    public abstract boolean verify(Object value);

    /**
     * Default setter for values of type: STRING_VALUE.
     */
    public abstract void set(T value);

    /**
     * Store attribute value.
     *
     * @return valueId
     */
    /* package protected */ abstract void store(
            Context ctx,
            Unit unit,
            Attribute<T> attribute,
            long valueId,
            Connection conn
    ) throws AttributeTypeException, AttributeValueException, DatabaseWriteException;

    /**
     * Return String representation of object.
     *
     * @return Returns created String
     */
    public String toString() {
        return values.toString();
    }
}
