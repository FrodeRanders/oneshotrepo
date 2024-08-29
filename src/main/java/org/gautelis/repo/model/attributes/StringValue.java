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
import org.gautelis.repo.model.Unit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;

final class StringValue extends Value<String> {
    public final static int MAXSTRLEN = 255;
    private static final Logger log = LoggerFactory.getLogger(StringValue.class);
    private final static String columnName = "stringval";

    /**
     * Creates a <I>new</I> string value
     */
    StringValue() {
    }

    /**
     * Creates an <I>existing</I> string value
     */
    StringValue(ResultSet rs) throws DatabaseReadException {
        super(rs);
    }

    /**
     * Inflate an <I>existing</I> string value from a result set.
     * <p>
     * Called from the Value constructor.
     */
    protected void inflate(ResultSet rs) throws DatabaseReadException {
        try {
            String value = rs.getString(columnName);
            values.add(value); // Oracle will store empty string as SQL NULL

        } catch (SQLException sqle) {
            throw new DatabaseReadException(sqle);
        }
    }

    public Type getType() {
        return Type.STRING;
    }

    public void set(ArrayList<String> values) throws AttributeTypeException {
        if (null == values || values.isEmpty()) {
            this.values.clear();
        } else {
            this.values.addAll(values);
        }
    }

    public boolean verify(Object value) {
        return value instanceof String;
    }

    public void set(String value) {
        values.add(value);
    }

    public String getScalar() {
        return values.getFirst();
    }

    /* package protected */ void store(
            Context ctx,
            Unit unit,
            Attribute<String> attribute,
            long valueId,
            Connection conn
    ) throws AttributeValueException, DatabaseWriteException {
        // Accept bounded strings only!
        for (String s : values) {
            if (null == s)
                continue;

            if (s.length() > MAXSTRLEN) {
                String info = "Incompatible values exceeds maximal length for STRING_VALUE: " + "attribute id=" + attribute.getAttrId() +
                        " name=\"" + attribute.getName() + "\"";
                throw new AttributeValueException(info);
            }
        }

        try {
            if (!values.isEmpty()) {
                // Prepare value vector
                try (PreparedStatement pStmt = conn.prepareStatement(ctx.getStatements().valueStringInsert())) {
                    int index = 0;
                    for (String value : values) {
                        int i = 0;
                        pStmt.setLong(++i, valueId);
                        pStmt.setInt(++i, index++);
                        if (null != value && !value.isEmpty()) {
                            pStmt.setString(++i, value);
                        } else {
                            pStmt.setNull(++i, Types.VARCHAR);
                        }

                        Database.executeUpdate(pStmt);
                    }
                }
            }

            // Reset modification controls
            setStored();

        } catch (SQLException sqle) {
            log.error(Database.squeeze(sqle));
            throw new DatabaseWriteException(sqle);
        }
    }
}

