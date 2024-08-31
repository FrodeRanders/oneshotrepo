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
package org.gautelis.repo.model;

import org.gautelis.repo.db.Database;
import org.gautelis.repo.exceptions.DatabaseConnectionException;
import org.gautelis.repo.exceptions.DatabaseReadException;
import org.gautelis.repo.model.utils.TimedExecution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;


/**
 *
 */
public final class KnownAttributes {
    private static final Logger log = LoggerFactory.getLogger(KnownAttributes.class);

    public static class AttributeInfo {
        public int id = 0;
        public String name = null;
        public int type = 0; // illegal initial value
        public boolean forcedScalar = false;
        public Timestamp created = null;
    }

    private static final Map<String, AttributeInfo> attributes = new HashMap<>();

    /**
     * Fetch all data from an attribute pool.
     * <p>
     */
    private static synchronized Map<String, AttributeInfo> fetchAttributes(Context ctx) throws DatabaseConnectionException, DatabaseReadException {

        if (attributes.isEmpty()) {
            if (log.isTraceEnabled()) {
                log.trace("Fetching known attributes");
            }

            TimedExecution.run(ctx.getTimingData(), "fetch known attributes", () -> Database.useReadonlyPreparedStatement(ctx.getDataSource(), ctx.getStatements().attributeGetAll(), pStmt -> {
                try (ResultSet rs = Database.executeQuery(pStmt)) {
                    while (rs.next()) {
                        AttributeInfo info = new AttributeInfo();
                        info.id = rs.getInt("attrid");
                        info.name = rs.getString("attrname");
                        info.type = rs.getInt("attrtype");
                        info.forcedScalar = rs.getBoolean("scalar");
                        info.created = rs.getTimestamp("created");

                        attributes.put(info.name.toLowerCase(), info);
                    }
                }
            }));
        }
        return attributes;
    }

    /**
     * Get attribute identified by id
     *
     * @param attrId id of attribute
     * @return AttributeInfo if attribute exists
     */
    /* package accessible only */
    static Optional<AttributeInfo> getAttribute(Context ctx, int attrId) throws DatabaseConnectionException, DatabaseReadException {

        Map<String, AttributeInfo> data = fetchAttributes(ctx);

        for (AttributeInfo info : data.values()) {
            if (info.id == attrId) {
                return Optional.of(info);
            }
        }
        return Optional.empty();
    }

    /**
     * Get attribute identified by name.
     *
     * @param name name of attribute
     * @return AttributeInfo if attribute exists
     */
    /* package accessible only */
    static Optional<AttributeInfo> getAttribute(Context ctx, String name) throws DatabaseConnectionException, DatabaseReadException {
        Map<String, AttributeInfo> info = fetchAttributes(ctx);
        return Optional.ofNullable(info.get(name.toLowerCase()));
    }
}

