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
import org.gautelis.repo.exceptions.*;
import org.gautelis.repo.model.associations.Association;
import org.gautelis.repo.model.associations.AssociationManager;
import org.gautelis.repo.model.associations.ExternalAssociation;
import org.gautelis.repo.model.associations.InternalRelation;
import org.gautelis.repo.model.attributes.Attribute;
import org.gautelis.repo.model.attributes.Type;
import org.gautelis.repo.model.cache.UnitFactory;
import org.gautelis.repo.model.locks.Lock;
import org.gautelis.repo.model.utils.TimedExecution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;


/**
 */
public class Unit implements Cloneable {

    public enum Status {
        PENDING_DISPOSITION(1),
        PENDING_DELETION(10),
        OBLITERATED(20),
        EFFECTIVE(30),
        ARCHIVED(40);

        private final int status;

        Status(int status) {
            this.status = status;
        }

        static Status of(int status) throws StatusException {
            for (Status s : Status.values()) {
                if (s.status == status) {
                    return s;
                }
            }
            throw new StatusException("Unknown status: " + status);
        }

        public int getStatus() {
            return status;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(Unit.class);
    private final Context ctx;

    // Unit information
    protected int tenantId;
    protected long unitId;
    protected String corrId;
    protected String name = null;
    protected Status status;
    protected Timestamp createdTime = null;

    // Attributes associated with this unit
    private Map<String, Attribute<?>> attributes = null;

    // Predicates
    protected boolean isNew; // true if not yet persisted

    private Unit(Context ctx) {
        this.ctx = ctx;
    }

    /**
     * Creates a <B>new</B> unit.
     */
    /* package accessible only */
    Unit(
            final Context ctx,
            final int tenantId,
            final String name
    ) throws DatabaseConnectionException, DatabaseReadException, DatabaseWriteException, ConfigurationException {
        this(ctx);

        // Adjusting initial values
        isNew = true;

        //
        this.tenantId = tenantId;
        this.unitId = -1L; // assigned first when stored
        this.corrId = UUID.randomUUID().toString();
        this.name = null != name ? name.trim() : null;
        this.status = Status.EFFECTIVE;

        // Do not set any *Time since these are automatically
        // set when writing unit to database.

        log.debug("Creating new unit: {}({})", id2String(tenantId, unitId), this.name);
    }

    /**
     * Fetches an <I>existing</I> unit.
     * <p>
     * Observe that no new unit is created. We will use the
     * information provided in order to find this unit in
     * the database and inflate an object of it.
     */
    /* package accessible only */
    Unit(
            Context ctx,
            int tenantId,
            long unitId
    ) throws DatabaseConnectionException, DatabaseWriteException, DatabaseReadException, UnitNotFoundException, ConfigurationException {
        this(ctx);

        // Adjusting initial values
        isNew = false;

        //
        if (log.isDebugEnabled())
            log.debug("Fetching unit {}", Unit.id2String(tenantId, unitId));

        Database.useReadonlyPreparedStatement(ctx.getDataSource(), ctx.getStatements().unitGet(), pStmt -> {
            int i = 0;
            pStmt.setInt(++i, tenantId);
            pStmt.setLong(++i, unitId);
            try (ResultSet rs = Database.executeQuery(pStmt)) {
                if (rs.next()) {
                    readEntry(rs);
                } else {
                    String info = "Could not find unit " + Unit.id2String(tenantId, unitId);
                    throw new UnitNotFoundException(info);
                }
            }
        });
    }

    /**
     * Fetches an <I>existing</I> unit from a row in the resultset.
     */
    /* Should be package accessible only */
    public Unit(
            Context ctx,
            ResultSet rs
    ) throws DatabaseReadException {
        this(ctx);

        // Adjusting initial values reflecting reading an existing unit from database
        isNew = false;

        readEntry(rs);

        if (log.isTraceEnabled())
            log.trace("Inflating unit from resultset: {}", Unit.id2String(tenantId, unitId));
    }

    /*
     * Returns a standardised ID string, if the Unit has been persisted
    */
    public static String id2String(int tenantId, long unitId) {
        if (unitId < 0L) {
            return "unit{new for tenant:" + tenantId + "}";
        }
        return tenantId + "." + unitId;
    }


    /**
     * Returns references to external resources associated with this unit.
     */
    public Collection<String> getAssociations(
            Association.Type assocType
    ) throws DatabaseConnectionException, DatabaseReadException, InvalidParameterException {

        Collection<Association> rightAssocs = TimedExecution.run(ctx.getTimingData(), "get right assocs", () ->
                AssociationManager.getRightAssociations(
                        ctx, tenantId, unitId, assocType
                )
        );

        Collection<String> v = new LinkedList<>();
        for (Association assoc : rightAssocs) {
            if (assoc.isAssociation()) {
                ExternalAssociation eassoc = (ExternalAssociation) assoc;
                v.add(eassoc.getAssocString());
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("Unexpected {}", assoc);
                }
            }
        }
        return v;
    }

    /**
     * Returns references to external resources associated with this unit.
     */
    public Collection<Unit> getRelations(
            Association.Type assocType
    ) throws DatabaseConnectionException, DatabaseReadException, InvalidParameterException {

        Collection<Association> rightAssocs = TimedExecution.run(ctx.getTimingData(), "get right relations", () ->
                AssociationManager.getRightAssociations(
                        ctx, tenantId, unitId, assocType
                )
        );

        Collection<Unit> v = new LinkedList<>();
        for (Association assoc : rightAssocs) {
            if (assoc.isRelational()) {
                InternalRelation relation = (InternalRelation) assoc;
                Optional<Unit> unit = UnitFactory.resurrectUnit(ctx, relation.getRelationTenantId(), relation.getRelationUnitId());
                unit.ifPresent(v::add);
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("Unexpected {}", assoc);
                }
            }
        }
        return v;
    }

    /**
     * Stores unit to database.
     */
    /* package accessible only */
    void store(
    ) throws DatabaseConnectionException, AttributeTypeException, AttributeValueException, DatabaseReadException, DatabaseWriteException, ConfigurationException, SystemInconsistencyException {
        if (!isNew) {
            return;
        }

        Database.useConnection(ctx.getDataSource(), conn -> {
            conn.setAutoCommit(false);

            try {
                try {
                    try {
                        store(conn);

                    } catch (DatabaseWriteException dbwe) {
                        SQLException sqle = dbwe.getSQLException();
                        log.error("Transaction rollback due to: {}", Database.squeeze(sqle));

                        conn.rollback();
                        throw dbwe;
                    }
                } catch (SQLException sqle) {
                    log.error("Failed to rollback due to: {}", Database.squeeze(sqle));
                    throw new DatabaseWriteException(sqle);
                }

                try {
                    conn.commit();
                }
                catch (SQLException sqle) {
                    throw new DatabaseWriteException(sqle);
                }

                isNew = false;

            } catch (DatabaseWriteException dbwe) {
                SQLException sqle = dbwe.getSQLException();

                // unitId may not have been assigned yet
                String info = "Failure to store unit " + tenantId + "." + unitId + ": " + sqle.getMessage();
                log.warn(info, dbwe);
                throw dbwe;
            }
        });
    }

    /**
     *
     */
    private void store(
            Connection conn
    ) throws DatabaseConnectionException, AttributeTypeException, AttributeValueException, DatabaseReadException, DatabaseWriteException, ConfigurationException, SystemInconsistencyException {
        // Store unit
        String[] generatedColumns = { "unitid" };
        try (PreparedStatement pStmt = conn.prepareStatement(ctx.getStatements().unitInsertNew(), generatedColumns)) {

            int i = 0;
            pStmt.setInt(++i, tenantId);
            pStmt.setString(++i, corrId);
            pStmt.setInt(++i, status.getStatus());
            if (null != name) {
                pStmt.setString(++i, name);
            } else {
                pStmt.setNull(++i, Types.VARCHAR);
            }

            int rowCount = Database.executeUpdate(pStmt);
            if (rowCount != 1) {
                String info = "Failed to write unit: " + rowCount + " rows inserted != 1";
                log.error(info);
                throw new ConfigurationException(info);
            }

            try (ResultSet rs = pStmt.getGeneratedKeys()) {
                if (rs.next()) {
                    unitId = rs.getLong(1);

                } else {
                    String info = "Failed to determine auto-generated unit ID";
                    log.error(info); // This is nothing we can recover from
                    throw new ConfigurationException(info);
                }
            }
        } catch (SQLException sqle) {
            String info = "Failed to store new unit: " + Database.squeeze(sqle);
            log.error(info, sqle);
            throw new DatabaseWriteException(info, sqle);
        }

        // Store all attributes of unit
        if (null != attributes) {
            for (Attribute<?> attribute : attributes.values()) {
                attribute.store(ctx, this, conn);
            }
        }
    }

    /**
     * Delete unit and all related information
     */
    /* package accessible only */
    void delete() throws DatabaseConnectionException, DatabaseWriteException {
        try (Connection conn = ctx.getDataSource().getConnection()) {
            conn.setReadOnly(false);
            conn.setAutoCommit(false);

            try {
                // Remove unit from repo_unit and fall back on cascaded delete for
                // deleting other relevant entries. repo_log is not touched though.
                Database.usePreparedStatement(conn, ctx.getStatements().unitDelete(), pStmt -> {
                    int i = 0;
                    pStmt.setInt(++i, tenantId);
                    pStmt.setLong(++i, unitId);
                    Database.executeUpdate(pStmt);
                });

                conn.commit();

            } catch (SQLException sqle) {
                conn.rollback();

                log.error("Transaction rollback due to: {}", Database.squeeze(sqle));
                throw new DatabaseWriteException(sqle);
            }
        } catch (SQLException sqle) {
            String info = "Meta-failure when deleting unit: " + Database.squeeze(sqle);
            log.error(info, sqle);
            throw new DatabaseConnectionException(info, sqle);
        }
    }

    private void readEntry(ResultSet rs) throws DatabaseReadException {
        try {
            // Read kernel information
            tenantId = rs.getInt("tenantid");
            unitId = rs.getLong("unitid");
            corrId = rs.getString("corrid");
            name = rs.getString("name");
            if (rs.wasNull()) {
                name = null; // to ensure we don't end up with 'NULL' names
            }
            createdTime = rs.getTimestamp("created");

        } catch (SQLException sqle) {
            throw new DatabaseReadException(sqle);
        }
    }

    /**
     * Adds attribute to unit
     */
    public Attribute<?> addAttribute(
            Attribute<?> attr
    ) throws DatabaseConnectionException, DatabaseReadException, ConfigurationException, IllegalRequestException {

        if (null == attributes) {
            if (isNew)
                attributes = new HashMap<>();
            else
                attributes = fetchAttributes();
        }

        if (attributes.containsKey(attr.getName())) {
            String info = "Unit " + this + " already has attribute " + attr.getAttrId();
            log.info(info);
            throw new IllegalRequestException(info);
        }

        Attribute<?> copy = new Attribute<>(attr);
        log.debug("Adding attribute {}({}) to unit {}", copy.getAttrId(), copy.getName(), getReference());
        attributes.put(copy.getName(), copy);
        return copy;
    }

    public void removeAttribute(
            String attrName
    ) throws IllegalNameException, DatabaseConnectionException, DatabaseReadException, ConfigurationException {

        if (null == attrName || attrName.isEmpty()) {
            String info = "No attribute name was provided";
            throw new IllegalNameException(info);
        }

        // How likely is it that we have no attributes here?  We are actually
        // demanding that fetchAttributes() was called beforehand and therefore
        // should ponder whether calling it here is appropriate...
        if (null == attributes) {
            attributes = fetchAttributes();
        }
    }

    /**
     * Fetch attributes from database if they are not fetched already.
     */
    private Map<String, Attribute<?>> fetchAttributes() throws DatabaseConnectionException, DatabaseReadException, ConfigurationException {

        // Ignore request if we have already loaded our attributes
        if (attributes == null) {
            log.trace("Fetching attributes for unit {}", getReference());
            TimedExecution.run(ctx.getTimingData(), "fetch attributes", () -> Database.useReadonlyConnection(ctx.getDataSource(), this::fetchAttributes));
        }
        return attributes;
    }

    /**
     * Fetch attributes from database if they are not fetched
     * already.
     * <p>
     * Use this method if you already have a connection.
     * <p>
     * Call this method in order to access the attributes map.
     * <p>
     * Will only fetch attributes if they are not fetched already.
     */
    private Map<String, Attribute<?>> fetchAttributes(
            Connection conn
    ) throws DatabaseReadException, AttributeTypeException {

        // Ignore request if we have already loaded our attributes
        if (attributes == null) {
            attributes = new HashMap<>();

            try (PreparedStatement pStmt = conn.prepareStatement(ctx.getStatements().unitGetAttributes(),
                    ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY
            )) {
                int i = 0;
                pStmt.setInt(++i, tenantId);
                pStmt.setLong(++i, unitId);
                try (ResultSet rs = Database.executeQuery(pStmt)) {

                    // ------------------------------------------------
                    // The following construction is used to allow
                    // peeking at the next row in the resultset. If,
                    // after peeking, we want to continue with the
                    // current row we must not call 'rs.next()'.
                    // ------------------------------------------------
                    boolean continueWithCurrentRow = rs.next();
                    while (continueWithCurrentRow || rs.next()) {
                        long currentValueId = rs.getLong("valueid");

                        Attribute<?> attribute = new Attribute<>(rs);

                        if (log.isTraceEnabled()) {
                            log.trace("Fetched {}", attribute);
                        }

                        // Associate attribute with name in hashtable
                        attributes.put(attribute.getName(), attribute);

                        continueWithCurrentRow =
                                !rs.isAfterLast() && rs.getLong("valueid") != currentValueId;
                    }
                }
            } catch (SQLException sqle) {
                log.error(Database.squeeze(sqle));
                throw new DatabaseReadException(sqle);
            }
        }
        return Collections.unmodifiableMap(attributes);
    }

    /**
     * Get attribute associated with unit.
     *
     * @return Attribute if attribute exists, null if it does not
     */
    public Optional<Attribute<?>> getAttribute(
            String name
    ) throws DatabaseConnectionException, DatabaseReadException, ConfigurationException {
        return Optional.ofNullable(fetchAttributes().get(name));
    }

    public Optional<Attribute<?>> getAttribute(
            int attributeId
    ) throws DatabaseConnectionException, SecurityException, AttributeTypeException, DatabaseReadException, SystemInconsistencyException, ConfigurationException {
        return getAttribute(attributeId, false);
    }

    public Optional<Attribute<String>> getStringAttribute(
            String attributeName
    ) throws DatabaseConnectionException, SecurityException, AttributeTypeException, DatabaseReadException, SystemInconsistencyException, ConfigurationException {
        return getStringAttribute(attributeName, false);
    }
    public Optional<Attribute<String>> getStringAttribute(
            int attributeId
    ) throws DatabaseConnectionException, SecurityException, AttributeTypeException, DatabaseReadException, SystemInconsistencyException, ConfigurationException {
        return getStringAttribute(attributeId, false);
    }

    public Optional<Attribute<Integer>> getIntegerAttribute(
            String attributeName
    ) throws DatabaseConnectionException, SecurityException, AttributeTypeException, DatabaseReadException, SystemInconsistencyException, ConfigurationException {
        return getIntegerAttribute(attributeName, false);
    }

    public Optional<Attribute<Integer>> getIntegerAttribute(
            int attributeId
    ) throws DatabaseConnectionException, SecurityException, AttributeTypeException, DatabaseReadException, SystemInconsistencyException, ConfigurationException {
        return getIntegerAttribute(attributeId, false);
    }

    public Optional<Attribute<Long>> getLongAttribute(
            String attributeName
    ) throws DatabaseConnectionException, SecurityException, AttributeTypeException, DatabaseReadException, SystemInconsistencyException, ConfigurationException {
        return getLongAttribute(attributeName, false);
    }

    public Optional<Attribute<Long>> getLongAttribute(
            int attributeId
    ) throws DatabaseConnectionException, SecurityException, AttributeTypeException, DatabaseReadException, SystemInconsistencyException, ConfigurationException {
        return getLongAttribute(attributeId, false);
    }

    public Optional<Attribute<Double>> getDoubleAttribute(
            String attributeName
    ) throws DatabaseConnectionException, SecurityException, AttributeTypeException, DatabaseReadException, SystemInconsistencyException, ConfigurationException {
        return getDoubleAttribute(attributeName, false);
    }

    public Optional<Attribute<Double>> getDoubleAttribute(
            int attributeId
    ) throws DatabaseConnectionException, SecurityException, AttributeTypeException, DatabaseReadException, SystemInconsistencyException, ConfigurationException {
        return getDoubleAttribute(attributeId, false);
    }

    public Optional<Attribute<Boolean>> getBooleanAttribute(
            String attributeName
    ) throws DatabaseConnectionException, SecurityException, AttributeTypeException, DatabaseReadException, SystemInconsistencyException, ConfigurationException {
        return getBooleanAttribute(attributeName, false);
    }

    public Optional<Attribute<Boolean>> getBooleanAttribute(
            int attributeId
    ) throws DatabaseConnectionException, SecurityException, AttributeTypeException, DatabaseReadException, SystemInconsistencyException, ConfigurationException {
        return getBooleanAttribute(attributeId, false);
    }

    public Optional<Attribute<Timestamp>> getTimeAttribute(
            String attributeName
    ) throws DatabaseConnectionException, SecurityException, AttributeTypeException, DatabaseReadException, SystemInconsistencyException, ConfigurationException {
        return getTimeAttribute(attributeName, false);
    }

    public Optional<Attribute<Timestamp>> getTimeAttribute(
            int attributeId
    ) throws DatabaseConnectionException, SecurityException, AttributeTypeException, DatabaseReadException, SystemInconsistencyException, ConfigurationException {
        return getTimeAttribute(attributeId, false);
    }

    public Optional<Attribute<Object>> getDataAttribute(
            String attributeName
    ) throws DatabaseConnectionException, SecurityException, AttributeTypeException, DatabaseReadException, SystemInconsistencyException, ConfigurationException {
        return getDataAttribute(attributeName, false);
    }

    public Optional<Attribute<Object>> getDataAttribute(
            int attributeId
    ) throws DatabaseConnectionException, SecurityException, AttributeTypeException, DatabaseReadException, SystemInconsistencyException, ConfigurationException {
        return getDataAttribute(attributeId, false);
    }

    public Optional<Attribute<?>> getAttribute(
            String attrName, boolean createIfMissing
    ) throws DatabaseConnectionException, SecurityException, AttributeTypeException, DatabaseReadException, SystemInconsistencyException, ConfigurationException, IllegalRequestException {
        Map<String, Attribute<?>> attributes = fetchAttributes();
        Attribute<?> attribute = attributes.get(attrName);
        if (null != attribute) {
            return Optional.of(attribute);
        }

        // Not found among unit's attributes
        if (createIfMissing) {
            //
            Optional<KnownAttributes.AttributeInfo> attributeInfo = KnownAttributes.getAttribute(ctx, attrName);
            if (attributeInfo.isEmpty()) {
                // Attribute does not exist among know attributes!
                String info = String.format("Failed to automatically add attribute %s to unit %s: This attribute does not exist among known attributes and is unknown to the system", attrName, getReference());
                log.error(info);
                throw new SystemInconsistencyException(info);
            }

            return Optional.of(addAttribute(new Attribute<>(attributeInfo.get())));
        }
        return Optional.empty();
    }

    public Optional<Attribute<?>> getAttribute(
            int attributeId, boolean createIfMissing
    ) throws DatabaseConnectionException, SecurityException, AttributeTypeException, DatabaseReadException, SystemInconsistencyException, ConfigurationException, IllegalRequestException {
        Map<String, Attribute<?>> attributes = fetchAttributes();

        Collection<Attribute<?>> myAttributes = attributes.values();
        for (Attribute<?> attribute : myAttributes) {
            if (attribute.getAttrId() == attributeId) {
                return Optional.of(attribute);
            }
        }

        // Not found among unit's attributes
        if (createIfMissing) {
            //
            Optional<KnownAttributes.AttributeInfo> attributeInfo = KnownAttributes.getAttribute(ctx, attributeId);
            if (attributeInfo.isEmpty()) {
                // Attribute does not exist among know attributes!
                String info = String.format("Failed to automatically add attribute with id %d to unit %s: This attribute does not exist among known attributes and is unknown to the system", attributeId, getReference());
                log.error(info);
                throw new SystemInconsistencyException(info);
            }

            return Optional.of(addAttribute(new Attribute<>(attributeInfo.get())));
        }
        return Optional.empty();
    }

    public Optional<Attribute<String>> getStringAttribute(
            String attributeName, boolean createIfMissing
    ) throws DatabaseConnectionException, SecurityException, AttributeTypeException, DatabaseReadException, SystemInconsistencyException, ConfigurationException, IllegalRequestException {
        Optional<Attribute<?>> attr = getAttribute(attributeName, createIfMissing);
        if (attr.isPresent() && attr.get().getType() == Type.STRING) {
            @SuppressWarnings("unchecked")
            Attribute<String> sAttr = (Attribute<String>) attr.get();
            return Optional.of(sAttr);
        }
        return Optional.empty();
    }

    public Optional<Attribute<String>> getStringAttribute(
            int attributeId, boolean createIfMissing
    ) throws DatabaseConnectionException, SecurityException, AttributeTypeException, DatabaseReadException, SystemInconsistencyException, ConfigurationException, IllegalRequestException {
        Optional<Attribute<?>> attr = getAttribute(attributeId, createIfMissing);
        if (attr.isPresent() && attr.get().getType() == Type.STRING) {
            @SuppressWarnings("unchecked")
            Attribute<String> sAttr = (Attribute<String>) attr.get();
            return Optional.of(sAttr);
        }
        return Optional.empty();
    }

    public Optional<Attribute<Integer>> getIntegerAttribute(
            String attributeName, boolean createIfMissing
    ) throws DatabaseConnectionException, SecurityException, AttributeTypeException, DatabaseReadException, SystemInconsistencyException, ConfigurationException, IllegalRequestException {
        Optional<Attribute<?>> attr = getAttribute(attributeName, createIfMissing);
        if (attr.isPresent() && attr.get().getType() == Type.INTEGER) {
            @SuppressWarnings("unchecked")
            Attribute<Integer> sAttr = (Attribute<Integer>) attr.get();
            return Optional.of(sAttr);
        }
        return Optional.empty();
    }

    public Optional<Attribute<Integer>> getIntegerAttribute(
            int attributeId, boolean createIfMissing
    ) throws DatabaseConnectionException, SecurityException, AttributeTypeException, DatabaseReadException, SystemInconsistencyException, ConfigurationException, IllegalRequestException {
        Optional<Attribute<?>> attr = getAttribute(attributeId, createIfMissing);
        if (attr.isPresent() && attr.get().getType() == Type.INTEGER) {
            @SuppressWarnings("unchecked")
            Attribute<Integer> sAttr = (Attribute<Integer>) attr.get();
            return Optional.of(sAttr);
        }
        return Optional.empty();
    }

    public Optional<Attribute<Long>> getLongAttribute(
            String attributeName, boolean createIfMissing
    ) throws DatabaseConnectionException, SecurityException, AttributeTypeException, DatabaseReadException, SystemInconsistencyException, ConfigurationException, IllegalRequestException {
        Optional<Attribute<?>> attr = getAttribute(attributeName, createIfMissing);
        if (attr.isPresent() && attr.get().getType() == Type.LONG) {
            @SuppressWarnings("unchecked")
            Attribute<Long> sAttr = (Attribute<Long>) attr.get();
            return Optional.of(sAttr);
        }
        return Optional.empty();
    }

    public Optional<Attribute<Long>> getLongAttribute(
            int attributeId, boolean createIfMissing
    ) throws DatabaseConnectionException, SecurityException, AttributeTypeException, DatabaseReadException, SystemInconsistencyException, ConfigurationException, IllegalRequestException {
        Optional<Attribute<?>> attr = getAttribute(attributeId, createIfMissing);
        if (attr.isPresent() && attr.get().getType() == Type.LONG) {
            @SuppressWarnings("unchecked")
            Attribute<Long> sAttr = (Attribute<Long>) attr.get();
            return Optional.of(sAttr);
        }
        return Optional.empty();
    }

    public Optional<Attribute<Double>> getDoubleAttribute(
            String attributeName, boolean createIfMissing
    ) throws DatabaseConnectionException, SecurityException, AttributeTypeException, DatabaseReadException, SystemInconsistencyException, ConfigurationException, IllegalRequestException {
        Optional<Attribute<?>> attr = getAttribute(attributeName, createIfMissing);
        if (attr.isPresent() && attr.get().getType() == Type.DOUBLE) {
            @SuppressWarnings("unchecked")
            Attribute<Double> sAttr = (Attribute<Double>) attr.get();
            return Optional.of(sAttr);
        }
        return Optional.empty();
    }

    public Optional<Attribute<Double>> getDoubleAttribute(
            int attributeId, boolean createIfMissing
    ) throws DatabaseConnectionException, SecurityException, AttributeTypeException, DatabaseReadException, SystemInconsistencyException, ConfigurationException, IllegalRequestException {
        Optional<Attribute<?>> attr = getAttribute(attributeId, createIfMissing);
        if (attr.isPresent() && attr.get().getType() == Type.DOUBLE) {
            @SuppressWarnings("unchecked")
            Attribute<Double> sAttr = (Attribute<Double>) attr.get();
            return Optional.of(sAttr);
        }
        return Optional.empty();
    }

    public Optional<Attribute<Boolean>> getBooleanAttribute(
            String attributeName, boolean createIfMissing
    ) throws DatabaseConnectionException, SecurityException, AttributeTypeException, DatabaseReadException, SystemInconsistencyException, ConfigurationException, IllegalRequestException {
        Optional<Attribute<?>> attr = getAttribute(attributeName, createIfMissing);
        if (attr.isPresent() && attr.get().getType() == Type.BOOLEAN) {
            @SuppressWarnings("unchecked")
            Attribute<Boolean> sAttr = (Attribute<Boolean>) attr.get();
            return Optional.of(sAttr);
        }
        return Optional.empty();
    }

    public Optional<Attribute<Boolean>> getBooleanAttribute(
            int attributeId, boolean createIfMissing
    ) throws DatabaseConnectionException, SecurityException, AttributeTypeException, DatabaseReadException, SystemInconsistencyException, ConfigurationException, IllegalRequestException {
        Optional<Attribute<?>> attr = getAttribute(attributeId, createIfMissing);
        if (attr.isPresent() && attr.get().getType() == Type.BOOLEAN) {
            @SuppressWarnings("unchecked")
            Attribute<Boolean> sAttr = (Attribute<Boolean>) attr.get();
            return Optional.of(sAttr);
        }
        return Optional.empty();
    }

    public Optional<Attribute<Timestamp>> getTimeAttribute(
            String attributeName, boolean createIfMissing
    ) throws DatabaseConnectionException, SecurityException, AttributeTypeException, DatabaseReadException, SystemInconsistencyException, ConfigurationException, IllegalRequestException {
        Optional<Attribute<?>> attr = getAttribute(attributeName, createIfMissing);
        if (attr.isPresent() && attr.get().getType() == Type.TIME) {
            @SuppressWarnings("unchecked")
            Attribute<Timestamp> sAttr = (Attribute<Timestamp>) attr.get();
            return Optional.of(sAttr);
        }
        return Optional.empty();
    }

    public Optional<Attribute<Timestamp>> getTimeAttribute(
            int attributeId, boolean createIfMissing
    ) throws DatabaseConnectionException, SecurityException, AttributeTypeException, DatabaseReadException, SystemInconsistencyException, ConfigurationException, IllegalRequestException {
        Optional<Attribute<?>> attr = getAttribute(attributeId, createIfMissing);
        if (attr.isPresent() && attr.get().getType() == Type.TIME) {
            @SuppressWarnings("unchecked")
            Attribute<Timestamp> sAttr = (Attribute<Timestamp>) attr.get();
            return Optional.of(sAttr);
        }
        return Optional.empty();
    }

    public Optional<Attribute<Object>> getDataAttribute(
            String attributeName, boolean createIfMissing
    ) throws DatabaseConnectionException, SecurityException, AttributeTypeException, DatabaseReadException, SystemInconsistencyException, ConfigurationException, IllegalRequestException {
        Optional<Attribute<?>> attr = getAttribute(attributeName, createIfMissing);
        if (attr.isPresent() && attr.get().getType() == Type.DATA) {
            @SuppressWarnings("unchecked")
            Attribute<Object> sAttr = (Attribute<Object>) attr.get();
            return Optional.of(sAttr);
        }
        return Optional.empty();
    }

    public Optional<Attribute<Object>> getDataAttribute(
            int attributeId, boolean createIfMissing
    ) throws DatabaseConnectionException, SecurityException, AttributeTypeException, DatabaseReadException, SystemInconsistencyException, ConfigurationException, IllegalRequestException {
        Optional<Attribute<?>> attr = getAttribute(attributeId, createIfMissing);
        if (attr.isPresent() && attr.get().getType() == Type.DATA) {
            @SuppressWarnings("unchecked")
            Attribute<Object> sAttr = (Attribute<Object>) attr.get();
            return Optional.of(sAttr);
        }
        return Optional.empty();
    }

    /**
     * Get all attributes associated with unit.
     */
    public Collection<Attribute<?>> getAttributes() throws DatabaseConnectionException, DatabaseReadException, ConfigurationException {
        Map<String, Attribute<?>> myAttributes = fetchAttributes();

        // Sort attribute names
        LinkedList<String> keys = myAttributes.keySet().stream().sorted().collect(Collectors.toCollection(LinkedList::new));

        // Prepare feedback
        return keys.stream().map(myAttributes::get).collect(Collectors.toUnmodifiableList()); // immutable
    }

    /**
     * Returns name of unit, if unit has a name.
     * <p>
     * No guarantee is left on uniqueness.
     * The name of (and thus the path of) a unit is not considered
     * to be unique and does not identify a unit. If you need a
     * unique reference, use
     * {@link #getReference}.
     *
     * @return String name of unit
     */
    public Optional<String> getName() {
        return Optional.ofNullable(name);
    }

    /**
     * Set name of unit.
     * <p>
     * No guarantee is left on uniqueness.
     * The name of (and thus the path of) a unit is not considered
     * to be unique and does not identify a unit. If you need a
     * unique reference, use
     * {@link #getReference}.
     *
     * @param name name of unit
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Gets string reference to unit.
     */
    public String getReference() {
        return id2String(tenantId, unitId);
    }

    /**
     * Get tenant id.
     *
     * @return int
     */
    public int getTenantId() {
        return tenantId;
    }

    /**
     * Get unit id.
     *
     * @return long ID of unit
      */
    public long getUnitId() {
        return unitId;
    }

    /**
     * Gets the correlation id of this unit
     */
    public String getCorrId() {
        return corrId;
    }

    /**
     * Get unit created time.
     * <p/>
     * Creation time is assigned upon write to database and is not automatically
     * read back to the Unit (due to the extra round trip). In order to know
     * creation time, we need to load the parent unit from database
     *
     * @return Optional&lt;java.sql.Timestamp&gt; When unit was created
     */
    public Optional<Timestamp> getCreationTime() {
        return Optional.of(createdTime);
    }

    /**
     * Checks if this unit is new and if it has not been stored.
     */
    public boolean isNew() {
        return isNew;
    }

    /**
     * Is unit locked?
     */
    public boolean isLocked() throws DatabaseConnectionException, DatabaseReadException {
        return Lock.isLocked(ctx, tenantId, unitId);
    }

    /**
     * Lock unit.
     *
     * @param purpose purpose of lock
     * @return true if lock was successfully placed on unit, false otherwise
     */
    public boolean lock(
            Lock.Type type,
            String purpose
    ) throws DatabaseConnectionException, DatabaseWriteException, DatabaseReadException, IllegalRequestException, ConfigurationException {

        if (isNew) {
            String info = "Can not lock new unit that has never been saved";
            throw new IllegalRequestException(info);
        }

        return Lock.lock(ctx, tenantId, unitId, type, purpose);
    }

    /**
     * Gets information on locks.
     *
     * @return LockInfo containing the information
     * @see Lock
     */
    public Collection<Lock> getLocks() throws DatabaseConnectionException, DatabaseReadException {
        return Lock.getLocks(ctx, tenantId, unitId);
    }

    /**
     * Unlock unit.
     */
    public void unlock() throws DatabaseConnectionException, DatabaseReadException, DatabaseWriteException {
        Lock.unlock(ctx, tenantId, unitId);
    }

    /**
     * Requests a status transition of a <I>unit</I>.
     *
     * @return new internal status -or- old if request was rejected
     */
    public Status requestStatusTransition(
            Status requestedStatus
    ) throws DatabaseConnectionException, InvalidParameterException, DatabaseReadException, DatabaseWriteException, IllegalRequestException {

        Status currentStatus = getStatus();

        //----------------------------------------------------------
        // Validate request, enforcing transitions according to rules.
        //----------------------------------------------------------
        switch (currentStatus) {
            case ARCHIVED:
                if (log.isInfoEnabled()) {
                    log.info("Rejected: {} -> {} for {}",
                            currentStatus.name(), requestedStatus.name(), Unit.id2String(tenantId, unitId));
                }
                return currentStatus;

            case EFFECTIVE:
                switch (requestedStatus) {
                    case PENDING_DELETION:
                    case PENDING_DISPOSITION:
                        // OK
                        if (log.isDebugEnabled()) {
                            log.debug("Transition: {} -> {} for {}",
                                    currentStatus.name(), requestedStatus.name(), Unit.id2String(tenantId, unitId));
                        }
                        break;

                    default:
                        // As will be the case if you are requesting a
                        // transition to same state. Ignore request.
                        return currentStatus;
                }
                break;

            case PENDING_DELETION:
                if (requestedStatus == Status.PENDING_DISPOSITION) {// OK
                    if (log.isDebugEnabled()) {
                        log.debug("Transition: {} -> {} for {}",
                                currentStatus.name(), requestedStatus.name(), Unit.id2String(tenantId, unitId));
                    }
                } else {// As will be the case if you are requesting a
                    // transition to same state. Ignore request.
                    return currentStatus;
                }
                break;

            case OBLITERATED:
                if (requestedStatus == Status.PENDING_DISPOSITION) {// OK
                    if (log.isDebugEnabled()) {
                        log.debug("Transition: {} -> {} for {}",
                                currentStatus.name(), requestedStatus.name(), Unit.id2String(tenantId, unitId));
                    }
                } else {// As will be the case if you are requesting a
                    // transition to same state. Ignore request.
                    return currentStatus;
                }

            case PENDING_DISPOSITION:
                // Never
                if (log.isInfoEnabled()) {
                    log.info("Rejected: {} -> {} for {}",
                            currentStatus.name(), requestedStatus.name(), Unit.id2String(tenantId, unitId));
                }
                return currentStatus;

            default:
                // Ignore request.
                return currentStatus;
        }

        setStatus(requestedStatus);
        return requestedStatus;
    }

    /**
     * Gets the status of the <I>unit</I>.
     * <p>
     * The unit status determines the visibility during searching,
     * browsing etc and is used to handle creation and disposal of
     * units.
     * <p>
     * Do not confuse this with the <I>record status</I>, that identifies
     * natural steps in the lifecycle of a <I>records</I>.
     */
    public Status getStatus() throws DatabaseConnectionException, DatabaseReadException {
        // Since we may not throw an exception from this method,
        // we need a reasonable safe default. Returning internal
        // status EFFECTIVE will (at least) block an erroneous deletion
        // of the unit.
        Status[] internalStatus = {Status.EFFECTIVE}; // A strict default

        TimedExecution.run(ctx.getTimingData(), "get status", () -> Database.useReadonlyPreparedStatement(ctx.getDataSource(), ctx.getStatements().unitGetStatus(), pStmt -> {
            int i = 0;
            pStmt.setInt(++i, tenantId);
            pStmt.setLong(++i, unitId);
            try (ResultSet rs = Database.executeQuery(pStmt)) {
                if (rs.next()) {
                    internalStatus[0] = Status.of(rs.getInt("status"));
                }
            }
        }));
        return internalStatus[0];
    }

    /**
     * Sets internal status of an <I>unit</I>.
     * <p>
     * Called internally by requestInternalStatus()
     */
    private void setStatus(
            Status requestedStatus
    ) throws DatabaseConnectionException, DatabaseWriteException, IllegalRequestException {

        if (isNew) {
            status = requestedStatus;
        } else {
            TimedExecution.run(ctx.getTimingData(), "set status", () -> Database.usePreparedStatement(ctx.getDataSource(), ctx.getStatements().unitSetStatus(), pStmt -> {
                int i = 0;
                pStmt.setInt(++i, requestedStatus.getStatus());
                pStmt.setInt(++i, tenantId);
                pStmt.setLong(++i, unitId);
                Database.executeUpdate(pStmt);
            }));
        }
    }

    /**
     * Activates a unit.
     * <p>
     * The behaviour is depending on the internal status.
     */
    public void activate() throws DatabaseConnectionException, DatabaseReadException, DatabaseWriteException, IllegalRequestException {
        try {
            Status currentStatus = getStatus();
            switch (currentStatus) {
                case PENDING_DELETION, OBLITERATED -> requestStatusTransition(Status.EFFECTIVE);
                // case PENDING_DISPOSITION, ARCHIVED, default -> {
                default -> {}
            }
        } catch (InvalidParameterException ipe) {
            /* ignore */
        }
    }

    /**
     * Inactivates a unit.
     * <p>
     * The behaviour is depending on the internal status.
     */
    public void inactivate() throws DatabaseConnectionException, DatabaseReadException, DatabaseWriteException, IllegalRequestException {
        try {
            Status currentStatus = getStatus();
            if (currentStatus == Status.EFFECTIVE) {
                requestStatusTransition(Status.PENDING_DELETION);
                // case PENDING_DELETION, PENDING_DISPOSITION, OBLITERATED, ARCHIVED, default -> {}
            }
        } catch (InvalidParameterException ipe) {
            /* ignore */
        }
    }

    /**
     * Return String representation of object.
     *
     * @return Returns created String
     */
    public String toString() {
        return "Unit{" + getReference() + "(" + (null != name ? name : "") + ")" + (isNew ? "*" : "") + "}";
    }

    public Object clone() throws CloneNotSupportedException {
        // Currently, attributes are not cloned!
        // Thus, they have to be fetched separately (from database), which will be handled
        // automatically since 'attributes' is unassigned.
        return super.clone();
    }
}
