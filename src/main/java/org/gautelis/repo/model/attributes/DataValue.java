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

final class DataValue extends Value<Object> {
    private final static String columnName = "dataval";

    /**
     * Creates a <I>new</I> data value
     */
    DataValue() {
    }

    /**
     * Creates an <I>existing</I> data value
     */
    DataValue(ResultSet rs) throws DatabaseReadException {
        super(rs);
    }

    /**
     * Inflate an <I>existing</I> string value from a result set.
     * <p>
     * Called from the Value constructor.
     */
    protected void inflate(ResultSet rs) throws DatabaseReadException {
        // This is not a very good implementation, to say the least,
        // and experimental at best.
        //
        // We will in fact cache the blob in memory, which is Not A Good Thing To Do.
        // Consider implementing blob data retrieval as an InputStream
        // data.getBinaryStream() on access - which will of course need a
        // connection.
        //
        // Also, the current SQL type used with Oracle does not match reading
        // with ResultSet.getBlob(). This handling should be synchronized among
        // the supported databases.
        //
        try {
            byte[] value;
            try {
                // SQL Server: Works with the VARBINARY type
                Blob data = rs.getBlob(columnName);
                long length = data.length();
                value = data.getBytes(1, (int) length);

            } catch (Throwable ignore) {
                // Oracle: Works with the RAW type
                value = (byte[]) rs.getObject(columnName);
            }
            values.add(value);
        } catch (SQLException sqle) {
            throw new DatabaseReadException(sqle);
        }
    }

    public Type getType() {
        return Type.DATA;
    }

    public void set(ArrayList<Object> values) throws AttributeTypeException {
        if (null == values || values.isEmpty()) {
            this.values.clear();
        } else {
            // Accept byte[] only!
            for (Object o : values) {
                if (null == o)
                    continue;

                if (!(o instanceof byte[])) {
                    throw new AttributeTypeException("Incompatible values does not match DATA_VALUE");
                }
            }

            // Assign
            this.values.addAll(values);
        }
    }

    public boolean verify(Object value) {
        return value instanceof byte[];
    }

    public void set(Object value) {
        values.add(value);
    }

    public Object getScalar() {
        return values.getFirst();
    }

    /* package protected */ void store(
            Context ctx,
            Unit unit,
            Attribute<Object> attribute,
            long valueId,
            Connection conn
    ) throws DatabaseWriteException, AttributeTypeException {
        // Accept byte[] only!
        for (Object o : values) {
            if (null == o)
                continue;

            if (!(o instanceof byte[])) {
                String info = "Incompatible values does not match DATA_VALUE: " + "Found a " + o.getClass().getName() +
                        " for attribute id=" + attribute.getAttrId() +
                        " name=\"" + attribute.getName() + "\"";
                throw new AttributeTypeException(info);
            }
        }

        try {
            if (!values.isEmpty()) {
                try (PreparedStatement pStmt = conn.prepareStatement(ctx.getStatements().valueDataInsert())) {
                    int index = 0;
                    for (Object _value : values) {
                        byte[] value = (byte[]) _value; // Assumption

                        int i = 0;
                        pStmt.setLong(++i, valueId);
                        pStmt.setInt(++i, index++);
                        if (null != value) {
                            pStmt.setBytes(++i, value);
                        } else {
                            pStmt.setNull(++i, Types.BLOB);
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
