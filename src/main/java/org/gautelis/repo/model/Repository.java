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
import org.gautelis.repo.listeners.ActionListener;
import org.gautelis.repo.model.associations.Association;
import org.gautelis.repo.model.associations.ExternalAssociation;
import org.gautelis.repo.model.associations.InternalRelation;
import org.gautelis.repo.model.cache.UnitFactory;
import org.gautelis.repo.model.locks.Lock;
import org.gautelis.repo.model.utils.TimedExecution;
import org.gautelis.repo.model.utils.TimingData;
import org.gautelis.repo.search.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;


public class Repository {
    private static final Logger log = LoggerFactory.getLogger(Repository.class);
    private final Map<String, ActionListener> actionListeners = new HashMap<>();

    private final Context context;
    private final int eventThreshold;

    public Repository(Context context, int eventThreshold, Map<String, ActionListener> actionListeners) {
        this.context = context;
        this.eventThreshold = eventThreshold;
        this.actionListeners.putAll(actionListeners);
    }

    public DatabaseAdapter getDatabaseAdapter() {
        return context.getDatabaseAdapter();
    }

    public Unit createUnit(
            int tenantId,
            String name
    ) throws DatabaseConnectionException, DatabaseReadException, DatabaseWriteException, ConfigurationException {
        return new Unit(context, tenantId, name);
    }


    /**
     * Searches for <B>all</B> units with status 'pending disposal',
     * <I>optionally</I> for a specific type.
     * <p>
     * Provide 'null' values for optional constraints if not applicable.
     */
    private SearchResult getDisposedUnits(
            int tenantId,
            int low, int high, int max
    ) throws InvalidParameterException {
        return getUnits(Unit.Status.PENDING_DISPOSITION, tenantId, low, high, max);
    }

    /**
     * Disposes specified units (specified by type) in <I>chunks</I>
     * (1000 at a time to avoid memory exhaustion).
     * <p>
     * The method returns when <B>all</B> objects are removed.
     * <p>
     * Observe that this method will not return until all objects are removed
     * and thus, if some units may not be removed, it will not return :-)
     */
    private void disposeUnits(
            int tenantId,
            PrintWriter writer
    ) throws InvalidParameterException {

        final int MAX_HITS = 1000;
        final boolean printProgress = (null != writer);

        SearchResult result = getDisposedUnits(tenantId, /* low */ 0, /* high */ MAX_HITS, MAX_HITS);

        while (result.totalNumberOfHits() > 0) {
            Collection<Unit> units = result.results();
            int count = 0;
            for (Unit unit : units) {
                try {
                    disposeUnit(unit);
                    ++count;
                } catch (Exception e) {
                    // Log problem, but do continue...
                    String info = "Failed to dispose unit " + unit.getReference();
                    info += ": " + e.getMessage();
                    log.warn(info);
                }
            }

            if (printProgress) {
                writer.println(" * chunk of " + count + " were removed");
                writer.flush();
            }
            result = getDisposedUnits(tenantId, /* low */ 0, /* high */ MAX_HITS, MAX_HITS);
        }
    }


    /**
     * Dispose all objects marked as 'pending disposition'.
     * <p>
     * User must be root.
     * <p>
     * Only objects with status of <B>INTERNAL_STATUS_PENDING_DISPOSITION</B>
     * are disposed.
     *
     * @param writer an (optional) writer onto which progress information is printed
     */
    public void dispose(
            int tenantId,
            PrintWriter writer
    ) throws InvalidParameterException {

        final boolean printProgress = (null != writer);

        if (printProgress) {
            writer.println("Disposing units for tenant " + tenantId);
            writer.flush();
        }
        disposeUnits(tenantId, writer);
    }


    /**
     * Searches for <B>all</B> units with specified status,
     * <I>optionally</I> for a specific type.
     */
    private SearchResult getUnits(
            Unit.Status status,
            int tenantId,
            int low, int high, int max
    ) throws InvalidParameterException {

        SearchItem<?> item = SearchItem.constrainToSpecificStatus(status);
        SearchExpression expr = new SearchExpression(item);

        // -- of specific tenant (if applicable) --
        if (tenantId > 0) {
            item = SearchItem.constrainToSpecificTenant(tenantId);
            expr = SearchExpression.assembleAnd(expr, item);
        }

        // Search order (pick any)
        SearchOrder order = SearchOrder.getDefaultOrder();

        // Perform search
        SearchExpression _expr = expr;
        return TimedExecution.run(context.getTimingData(), "search", () -> searchUnit(low, high, max, _expr, order));
    }

    /**
     * Fetch an existing unit of latest version.
     */
    public Optional<Unit> getUnit(
            int tenantId,
            long unitId
    ) throws DatabaseConnectionException, DatabaseReadException {

        Optional<Unit> unit = TimedExecution.run(context.getTimingData(), "resurrect unit", () -> UnitFactory.resurrectUnit(context, tenantId, unitId));
        unit.ifPresent(_unit -> generateActionEvent(
                _unit,
                ActionEvent.Type.ACCESSED,
                "Unit accessed"
        ));
        return unit;
    }


    /**
     * Fetches an existing unit from resultset.
     */
    private Optional<Unit> getUnit(
            ResultSet rs
    ) throws DatabaseReadException {

        Optional<Unit> unit = TimedExecution.run(context.getTimingData(), "resurrect unit", () -> UnitFactory.resurrectUnit(context, rs));
        unit.ifPresent(_unit -> generateActionEvent(
                _unit,
                ActionEvent.Type.ACCESSED,
                "Unit accessed"
        ));
        return unit;
    }

    /**
     * Checks existence of unit.
     *
     * @param tenantId type id
     * @param unitId unit id requested
     * @return boolean true if unit exists, false otherwise
     */
    public boolean unitExists(
            int tenantId,
            long unitId
    ) throws DatabaseConnectionException, DatabaseReadException {
        return TimedExecution.run(context.getTimingData(), "unit exists", () -> UnitFactory.unitExists(context, tenantId, unitId));
    }

    /**
     * Store a unit to persistent storage.
     */
    public void storeUnit(
            Unit unit
    ) throws DatabaseConnectionException, DatabaseReadException, DatabaseWriteException, AttributeTypeException, AttributeValueException, UnitReadOnlyException, UnitLockedException, InvalidParameterException, ConfigurationException, SystemInconsistencyException {

        if (null == unit) {
            throw new InvalidParameterException("no unit");
        }

        // --- Applicability control ---
        if (!unit.isNew()) {
            throw new UnitReadOnlyException(
                    "Unit " + unit.getReference() + " is read only");
        }

        if (unit.isLocked()) {
            for (Lock lock : unit.getLocks()) {
                throw new UnitLockedException(
                        "Unit " + unit.getReference() + " has lock: " + lock);
            }
        }

        log.debug("Storing unit {}", unit.getReference());
        TimedExecution.run(context.getTimingData(), "store unit", unit::store);

        generateActionEvent(
                unit,
                ActionEvent.Type.UPDATED,
                "Unit stored"
        );
    }

    /**
     * Associates a unit with an external resource (reference).
     * <p>
     * <I>This method has side effects: If, as a result of calling
     * this method, the internal status of the unit should be
     * updated, a request for this status transition is issued.
     * This information is not persisted to database though. You
     * should be aware of the fact that you may have a <B>modified</B>
     * unit after this call.</I>
     * <p>
     * Currently only accepts the Association.CASE_ASSOCIATION
     * association.
     */
    public void addRelation(
            Unit unit, Association.Type assocType, Unit otherUnit
    ) throws DatabaseConnectionException, DatabaseWriteException, InvalidParameterException, ConfigurationException {

        if (null == unit) {
            throw new InvalidParameterException("no unit");
        }
        if (null == otherUnit) {
            throw new InvalidParameterException("no other unit");
        }

        TimedExecution.run(context.getTimingData(), "create relation", () -> InternalRelation.create(
                context, unit.getTenantId(), unit.getUnitId(), assocType, otherUnit.getTenantId(), otherUnit.getUnitId()
        ));

        generateActionEvent(
                unit,
                ActionEvent.Type.ASSOCIATION_ADDED,
                assocType + " to " + otherUnit.getReference() + " created"
        );
    }

    /**
     * Removes an external association (reference) from a unit.
     * <p>
     * <I>This method has side effects: If, as a result of calling
     * this method, the internal status of the unit should be
     * updated, a request for this status transition is issued.
     * This information is not persisted to database though. You
     * should be aware of the fact that you may have a <B>modified</B>
     * unit after this call.</I>
     * <p>
     * Currently only accepts the Association.CASE_ASSOCIATION
     * association.
     */
    public void removeRelation(
            Unit unit, Association.Type assocType, Unit otherUnit
    ) throws DatabaseConnectionException, DatabaseWriteException, InvalidParameterException {

        if (null == unit) {
            throw new InvalidParameterException("no unit");
        }
        if (null == otherUnit) {
            throw new InvalidParameterException("no other unit");
        }

        TimedExecution.run(context.getTimingData(), "remove relation", () -> InternalRelation.remove(
                context, unit.getTenantId(), unit.getUnitId(), assocType, otherUnit.getTenantId(), otherUnit.getUnitId()
        ));

        generateActionEvent(
                unit,
                ActionEvent.Type.ASSOCIATION_REMOVED,
                assocType + " to " + otherUnit.getReference() + " removed"
        );
    }


    /**
     * Associates a unit with an external resource (reference).
     * <p>
     * <I>This method has side effects: If, as a result of calling
     * this method, the internal status of the unit should be
     * updated, a request for this status transition is issued.
     * This information is not persisted to database though. You
     * should be aware of the fact that you may have a <B>modified</B>
     * unit after this call.</I>
     * <p>
     * Currently only accepts the Association.CASE_ASSOCIATION
     * association.
     */
    public void addAssociation(
            Unit unit, Association.Type assocType, String reference
    ) throws DatabaseConnectionException, DatabaseWriteException, InvalidParameterException, ConfigurationException {

        if (null == unit) {
            throw new InvalidParameterException("no unit");
        }
        if (null == reference || reference.isEmpty()) {
            throw new InvalidParameterException("no (external) reference");
        }

        TimedExecution.run(context.getTimingData(), "create assoc", () -> ExternalAssociation.create(
                context, unit.getTenantId(), unit.getUnitId(), assocType, reference
        ));

        generateActionEvent(
                unit,
                ActionEvent.Type.ASSOCIATION_ADDED,
                assocType + " to " + reference + " created"
        );
    }

    /**
     * Removes an external association (reference) from a unit.
     * <p>
     * <I>This method has side effects: If, as a result of calling
     * this method, the internal status of the unit should be
     * updated, a request for this status transition is issued.
     * This information is not persisted to database though. You
     * should be aware of the fact that you may have a <B>modified</B>
     * unit after this call.</I>
     * <p>
     * Currently only accepts the Association.CASE_ASSOCIATION
     * association.
     */
    public void removeAssociation(
            Unit unit, Association.Type assocType, String reference
    ) throws DatabaseConnectionException, DatabaseWriteException, InvalidParameterException {

        if (null == unit) {
            throw new InvalidParameterException("no unit");
        }
        if (null == reference || reference.isEmpty()) {
            throw new InvalidParameterException("no (external) reference");
        }

        TimedExecution.run(context.getTimingData(), "remove assoc", () -> ExternalAssociation.remove(
                context, unit.getTenantId(), unit.getUnitId(), assocType, reference
        ));

        generateActionEvent(
                unit,
                ActionEvent.Type.ASSOCIATION_REMOVED,
                assocType + " to " + reference + " removed"
        );
    }


    /**
     * Lock unit.
     */
    public boolean lockUnit(
            Unit unit,
            Lock.Type type,
            String purpose
    ) throws DatabaseConnectionException, DatabaseReadException, DatabaseWriteException, InvalidParameterException, IllegalRequestException, ConfigurationException {

        if (null == unit) {
            throw new InvalidParameterException("no unit");
        }
        if (null == purpose || purpose.isEmpty()) {
            purpose = "unspecified";
        }

        String _purpose = purpose;
        boolean success = TimedExecution.run(context.getTimingData(), "lock unit", () -> unit.lock(type, _purpose));
        if (success) {
            generateActionEvent(
                    unit,
                    ActionEvent.Type.LOCKED,
                    "Unit locked"
            );
        }
        return success;
    }

    /**
     * Unlock unit.
     */
    public void unlockUnit(
            Unit unit
    ) throws DatabaseConnectionException, DatabaseReadException, DatabaseWriteException, InvalidParameterException {

        if (null == unit) {
            throw new InvalidParameterException("no unit");
        }

        if (unit.isLocked()) {
            for (Lock lock : unit.getLocks()) {
                TimedExecution.run(context.getTimingData(), "unlock unit", unit::unlock);
            }

            generateActionEvent(
                    unit,
                    ActionEvent.Type.UNLOCKED,
                    "Unit unlocked"
            );
        }
    }

    /**
     * Dispose unit if not locked
     */
    private boolean disposeUnit(
            Unit unit
    ) throws DatabaseConnectionException, DatabaseWriteException, DatabaseReadException, InvalidParameterException {

        if (null == unit) {
            throw new InvalidParameterException("no unit");
        }

        // Delete unit (all versions) if not locked by another user.
        boolean isUndisposable = false;
        {
            for (Lock lock : unit.getLocks()) {
                isUndisposable = true;
                break;
            }
        }
        if (isUndisposable) {
            return false;
        }

        TimedExecution.run(context.getTimingData(), "delete unit", unit::delete);

        generateActionEvent(
                unit,
                ActionEvent.Type.DELETED,
                "Unit deleted"
        );
        return true;
    }

    private void deleteUnit(
            Unit unit
    ) throws DatabaseConnectionException, DatabaseReadException, DatabaseWriteException, AttributeTypeException, AttributeValueException, InvalidParameterException, IllegalRequestException, ConfigurationException, SystemInconsistencyException {

        if (null == unit) {
            throw new InvalidParameterException("no unit");
        }
        // We want to set state to pending disposition.
        TimedExecution.run(context.getTimingData(), "status transition", () -> unit.requestStatusTransition(Unit.Status.PENDING_DISPOSITION));

        generateActionEvent(
                unit,
                ActionEvent.Type.DELETED,
                "Unit deleted"
        );
    }


    /**
     * Activates unit.
     */
    public void activateUnit(
            Unit unit
    ) throws DatabaseConnectionException, DatabaseReadException, DatabaseWriteException, AttributeTypeException, AttributeValueException, InvalidParameterException, IllegalRequestException, ConfigurationException, SystemInconsistencyException {

        if (null == unit) {
            throw new InvalidParameterException("no unit");
        }

        TimedExecution.run(context.getTimingData(), "activate unit", unit::activate);

        generateActionEvent(
                unit,
                ActionEvent.Type.UPDATED,
                "Unit activated"
        );
    }

    /**
     * Inactivates unit.
     */
    public void inactivateUnit(
            Unit unit
    ) throws DatabaseConnectionException, DatabaseReadException, DatabaseWriteException, AttributeTypeException, AttributeValueException, UnitLockedException, InvalidParameterException, IllegalRequestException, ConfigurationException, SystemInconsistencyException {

        if (null == unit) {
            throw new InvalidParameterException("no unit");
        }

        // Check if unit is locked by some other user
        if (unit.isLocked()) {
            for (Lock lock : unit.getLocks()) {
                throw new UnitLockedException("Unit " + unit.getReference() + " has lock: " + lock);
            }
        }

        TimedExecution.run(context.getTimingData(), "inactivate unit", unit::inactivate);

        generateActionEvent(
                unit,
                ActionEvent.Type.UPDATED,
                "Unit inactivated"
        );
    }

    /**
     * Attribute search.
     *
     * @param reqlow  Lower number of units to retrieve
     * @param reqhigh Upper number of units to retrieve
     * @param maxhits Max number of units to search for
     * @param expr    Search expression
     * @param order   Sort order
     */
    public SearchResult searchUnit(
            int reqlow, int reqhigh, int maxhits,
            SearchExpression expr,
            SearchOrder order
    ) throws InvalidParameterException {

        if (null == expr) {
            throw new InvalidParameterException("no search expression");
        }
        if (null == order) {
            throw new InvalidParameterException("no search order");
        }

        Collection<Unit> result = new LinkedList<>();
        int totalNumberOfHits = 0;

        try {
            UnitSearchData sd;
            if (maxhits > 0) {
                sd = new UnitSearchData(expr, order, maxhits);
            } else {
                sd = new UnitSearchData(expr, order, reqlow, (reqhigh - reqlow));
            }
            StringBuilder buf = context.getDatabaseAdapter().generateStatement(sd);

            if (log.isTraceEnabled()) {
                log.trace("Search: expression={}", buf);
            }

            try (Connection conn = context.getDataSource().getConnection()) {
                conn.setReadOnly(true);

                try (Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {
                    //stmt.setFetchSize(maxhits);
                    stmt.setMaxRows(maxhits);

                    int pagedPosition = 1;
                    if (reqlow <= 0) {
                        reqlow = 1;
                    }

                    long before = System.currentTimeMillis();
                    try (ResultSet rs = stmt.executeQuery(buf.toString())) {
                        long after = System.currentTimeMillis();
                        context.getTimingData().update("search",after - before);

                        if (log.isTraceEnabled()) {
                            log.trace("Search: time={}ms", Long.valueOf(after - before));
                        }

                        while (rs.next()) {
                            totalNumberOfHits++;

                            int tenantId = rs.getInt("tenantid");
                            long unitId = rs.getLong("unitid");

                            try {
                                // Skip entries that does not fit the specified "page".
                                if (pagedPosition >= reqlow && pagedPosition <= reqhigh) {
                                    Optional<Unit> unit = UnitFactory.resurrectUnit(context, tenantId, unitId);
                                    unit.ifPresent(result::add);
                                }
                                pagedPosition++;

                            } catch (Exception e) {
                                if (log.isDebugEnabled()) {
                                    String info = "Failed to resurrect unit from search result: " + e.getMessage();
                                    log.debug(info);
                                }
                            }
                        }
                    }
                }
            }
        } catch (SQLException sqle) {
            log.error("Failure in search: {}", Database.squeeze(sqle));

        } catch (Exception e) {
            log.error("Failure in search", e);
        }
        return new SearchResult(result, totalNumberOfHits);
    }

    public Optional<KnownAttributes.AttributeInfo> getAttributeInfo(String attributeName) throws DatabaseConnectionException, DatabaseReadException {
        return KnownAttributes.getAttribute(context, attributeName);
    }

    public Optional<KnownAttributes.AttributeInfo> getAttributeInfo(int attributeId) throws DatabaseConnectionException, DatabaseReadException {
        return KnownAttributes.getAttribute(context, attributeId);
    }

    public Optional<Integer> attributeNameToId(String attributeName) {
        Optional<KnownAttributes.AttributeInfo> attributeInfo = getAttributeInfo(attributeName);
        Integer[] attributeId = { null };
        attributeInfo.ifPresent(attr -> {
            attributeId[0] = (Integer) attr.id;
        });
        return Optional.ofNullable(attributeId[0]);
    }

    public Optional<String> attributeIdToName(int attributeId) {
        Optional<KnownAttributes.AttributeInfo> attributeInfo = getAttributeInfo(attributeId);
        String[] attributeName = { null };
        attributeInfo.ifPresent(attr -> {
            attributeName[0] = attr.name;
        });
        return Optional.ofNullable(attributeName[0]);
    }

    public Optional<Tenant.TenantInfo> getTenantInfo(String tenantName) throws DatabaseConnectionException, DatabaseReadException {
        return Tenant.getTenant(context, tenantName);
    }

    public Optional<Tenant.TenantInfo> getTenantInfo(int tenantId) throws DatabaseConnectionException, DatabaseReadException {
        return Tenant.getTenant(context, tenantId);
    }

    public Optional<Integer> tenantNameToId(String tenantName) {
        Optional<Tenant.TenantInfo> tenantInfo = getTenantInfo(tenantName);
        Integer[] tenantId = { null };
        tenantInfo.ifPresent(info -> {
            tenantId[0] = info.id;
        });
        return Optional.ofNullable(tenantId[0]);
    }

    public Optional<String> tenantIdToName(int tenantId) {
        Optional<Tenant.TenantInfo> tenantInfo = getTenantInfo(tenantId);
        String[] tenantName = { null };
        tenantInfo.ifPresent(info -> {
            tenantName[0] = info.name;
        });
        return Optional.ofNullable(tenantName[0]);
    }

    /**
     * Convenience function for generating an Action event
     *
     * @param source      source object action applies to
     * @param actionType  action type
     * @param description text explaining what has happened
     */
    private void generateActionEvent(
            Object source,
            ActionEvent.Type actionType,
            String description
    ) {
        if (actionListeners.isEmpty())
            return;

        if (actionType.getLevel() < eventThreshold)
            return;

        ActionEvent event = new ActionEvent(source, actionType, description);
        for (ActionListener l : actionListeners.values()) {
            l.actionPerformed(event);
        }
    }

    public TimingData getTimingData() {
        return context.getTimingData();
    }

    public interface InternalDataSourceRunnable {
        void run(javax.sql.DataSource dataSource);
    }

    public void useDataSource(InternalDataSourceRunnable runnable) {
        runnable.run(context.getDataSource());
    }

    public interface InternalConnectionRunnable {
        void run(java.sql.Connection connection);
    }

    public void useConnection(InternalConnectionRunnable runnable) throws java.sql.SQLException {
        runnable.run(context.getDataSource().getConnection());
    }
}

