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
import org.gautelis.repo.exceptions.AttributeTypeException;
import org.gautelis.repo.exceptions.DatabaseReadException;
import org.gautelis.repo.exceptions.DatabaseWriteException;
import org.gautelis.repo.model.Context;
import org.gautelis.repo.model.Unit;

import java.sql.*;
import java.util.ArrayList;

final class BooleanValue extends Value<Boolean> {
    private final static String columnName = "boolval";

    /**
     * Creates a <I>new</I> boolean value
     */
    BooleanValue() {
    }

    /**
     * Creates an <I>existing</I> boolean value
     */
    BooleanValue(ResultSet rs) throws DatabaseReadException {
        super(rs);
    }

    /**
     * Inflate an <I>existing</I> string value from a result set.
     * <p>
     * Called from the Value constructor.
     */
    protected void inflate(ResultSet rs) throws DatabaseReadException {
        try {
            Boolean value = rs.getBoolean(columnName);
            values.add(value);
        } catch (SQLException sqle) {
            throw new DatabaseReadException(sqle);
        }
    }

    public Type getType() {
        return Type.BOOLEAN;
    }

    public void set(ArrayList<Boolean> values) throws AttributeTypeException {
        if (null == values || values.isEmpty()) {
            this.values.clear();
        } else {
            this.values.addAll(values);
        }
    }

    public boolean verify(Object value) {
        return value instanceof Boolean;
    }

    public void set(Boolean value) {
        values.add(value);
    }

    public Boolean getScalar() {
        return values.getFirst();
    }

    /* package protected */ void store(
            Context ctx,
            Unit unit,
            Attribute<Boolean> attribute,
            long valueId,
            Connection conn
    ) throws DatabaseWriteException, AttributeTypeException {
        try {
            if (!values.isEmpty()) {
                try (PreparedStatement pStmt = conn.prepareStatement(ctx.getStatements().valueBooleanInsert())) {
                    int index = 0;
                    for (Boolean value : values) {
                        int i = 0;
                        pStmt.setLong(++i, valueId);
                        pStmt.setInt(++i, index++);
                        if (null != value) {
                            pStmt.setBoolean(++i, value);
                        } else {
                            pStmt.setNull(++i, Types.BOOLEAN);
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
