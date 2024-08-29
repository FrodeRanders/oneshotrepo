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

import org.gautelis.repo.db.Database;
import org.gautelis.repo.exceptions.*;
import org.gautelis.repo.model.Context;
import org.gautelis.repo.model.KnownAttributes;
import org.gautelis.repo.model.Unit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;

/**
 *
 */
public class Attribute<T> {
    private static final Logger log = LoggerFactory.getLogger(Attribute.class);

    private int attrId;
    private long valueId = -1L; // initially invalid
    private String name;
    private Type type;
    private Value<T> value = null;

    /**
     * Creates a <B>new</B> attribute.
     * <p>
     * Remember that attribute ids are defined among the known attributes,
     * so what we do is to associate an attribute id with a certain version
     * of a unit. Individual values of the attribute may be provided
     * as soon as we have an attribute to contain them.
     */
    /* Should be package accessible only */
    public Attribute(
            int attrId, String name, Type type
    ) throws AttributeTypeException {
        this.attrId = attrId;
        this.name = name.trim();
        this.type = type;

        value = Value.createValue(type);
    }

    public Attribute(
            KnownAttributes.AttributeInfo attributeInfo
    ) throws AttributeTypeException {
        this(attributeInfo.attrId, attributeInfo.attrName, Type.of(attributeInfo.attrType));
    }

    /**
     * Inflates an attribute from a result set.
     */
    /* Should be package accessible only */
    public Attribute(
            ResultSet rs
    ) throws DatabaseReadException, AttributeTypeException {
        readEntry(rs);
    }

    /**
     * Copy constructor.
     * NOTE!
     *   Does not copy field 'valueId' which will be assigned a new value.
     */
    @SuppressWarnings("CopyConstructorMissesField")
    public Attribute(Attribute<T> other) {
        this(other.getAttrId(), other.getName(), other.getType());
        value.copy(other.value);
    }

    /**
     * Gets attribute type
     */
    public Type getType() {
        return value.getType();
    }


    /**
     * Gets attribute value (vector).
     */
    public ArrayList<T> getValue() {
        return value.get();
    }

    /**
     * Gets attribute size.
     */
    public int getSize() {
        return value.getSize();
    }

    /**
     * Gets attribute name.
     *
     * @return String name of attribute
     */
    public String getName() {
        return name;
    }

    /**
     * Gets attribute id.
     *
     * @return int id of attribute
     */
    public int getAttrId() {
        return attrId;
    }

    /**
     * Save modified attribute to persistent database.
     */
    public void store(
            Context ctx, Unit unit, Connection conn
    ) throws AttributeTypeException, AttributeValueException, DatabaseWriteException {
        try {
            if (log.isDebugEnabled()) {
                log.debug("Storing {}", this);
            }

            boolean doStore = /* attribute */ isNew() || /* attribute */ isModified();

            /*--------------------------
             * Store attribute value(s)
             *--------------------------*/
            if (doStore) {
                // The value vector was modified and a new value was created.
                //
                // A special case is if the value vector was empty, in which case
                // no values are written to the database. We will still refer
                // to this empty vector by binding an attribute version to it.
                //
                // In fact, only storing the value id here without storing any
                // value data is what constitutes an empty value vector.
                //
                String[] generatedColumns = { "valueid" };
                try (PreparedStatement pStmt = conn.prepareStatement(ctx.getStatements().attributeInsert(), generatedColumns)) {
                    int i = 0;
                    pStmt.setInt(++i, unit.getTenantId());
                    pStmt.setLong(++i, unit.getUnitId());
                    pStmt.setInt(++i, attrId);

                    int rowCount = Database.executeUpdate(pStmt);
                    if (rowCount != 1) {
                        String info = "Failed to insert attribute->value mapping: " + rowCount + " rows inserted != 1";
                        log.error(info);
                        throw new ConfigurationException(info);
                    }

                    try (ResultSet rs = pStmt.getGeneratedKeys()) {
                        if (rs.next()) {
                            valueId = rs.getLong(1);
                        } else {
                            String info = "Failed to determine auto-generated value ID";
                            log.error(info); // This is nothing we can recover from
                            throw new ConfigurationException(info);
                        }
                    }
                }

                log.trace("New value {}", valueId);

                // Store the value vector.
                //
                // Observe that storing an empty value vector, which results in no value data
                // being written to database, is considered as having "stored" a value. An
                // empty value vector is still a value vector!
                value.store(ctx, unit, this, valueId, conn);
            }
        } catch (SQLException sqle) {
            log.error(Database.squeeze(sqle));
            throw new DatabaseWriteException(sqle);
        }
    }

    private void readEntry(ResultSet rs) throws DatabaseReadException, AttributeTypeException {

        try {
            // Get attribute version information from row
            attrId = rs.getInt("attrid");
            valueId = rs.getLong("valueid");
            name = rs.getString("attrname");
            type = Type.of(rs.getInt("attrtype"));

            // Continue with value information
            value = Value.inflateValue(type, rs);

        } catch (SQLException sqle) {
            throw new DatabaseReadException(sqle);
        }
    }

    /**
     * Checks if attributes were modified
     *
     * @return True if modified, else false
     */
    public boolean isModified() {
        return value.isModified();
    }

    public boolean isNew() {
        return value.isNew() || valueId <= 0;
    }

    /**
     * Overridden method from {@link Object }
     *
     * @return Created String
     */
    public String toString() {
        return attrId + "(" + name + ")" +
                (value.isNew() ? "*" : "") +
                (value.isModified() ? "~" : "") +
                " = " + value.toString();
    }
}



