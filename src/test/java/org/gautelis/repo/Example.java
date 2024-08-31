package org.gautelis.repo;

import org.gautelis.repo.db.Database;
import org.gautelis.repo.model.Repository;
import org.gautelis.repo.model.Unit;
import org.gautelis.repo.model.attributes.Attribute;
import org.gautelis.repo.search.*;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Optional;
import java.util.UUID;
import java.util.Collection;

public class Example {

    private int getTenantId(String tenantName, Repository repo) {
        Optional<Integer> tenantId = repo.tenantNameToId(tenantName);
        if (tenantId.isEmpty()) {
            throw new RuntimeException("Unknown tenant: " + tenantName);
        }
        return tenantId.get();
    }

    private int getAttributeId(String attributeName, Repository repo) {
        Optional<Integer> attributeId = repo.attributeNameToId(attributeName);
        if (attributeId.isEmpty()) {
             throw new RuntimeException("Unknown attribute: " + attributeName);
        }
        return attributeId.get();
    }



    public void createAUnitAndAssignAttributes() {
        Repository repo = RepositoryFactory.getRepository();
        int tenantId = getTenantId("SCRATCH", repo); // SCRATCH is the default space

        // Create a unit, with some random name. Names does not have to be unique
        // as they are not normally used to identify units.
        Unit unit = repo.createUnit(tenantId, "unit-" + UUID.randomUUID());

        // Associate this unit with a string attribute "dc:title" (already known
        // to the system).
        Optional<Attribute<String>> _dctitle = unit.getStringAttribute("dc:title", true);
        _dctitle.ifPresent(dctitle -> {
            ArrayList<String> value = dctitle.getValue(); // no value at present
            value.add("Title-" + UUID.randomUUID());
        });

        // Associate this unit with a time-related attribute "dc:date" (already known
        // to the system).
        Optional<Attribute<Timestamp>> _dcdate = unit.getTimeAttribute("dc:date", true);
        _dcdate.ifPresent(dcdate -> {
            ArrayList<Timestamp> value = dcdate.getValue(); // no value at present
            value.add(new Timestamp(System.currentTimeMillis()));
        });

        // Store this new unit to database
        repo.storeUnit(unit);
    }

    public void searchForAUnitBasedOnAttributes() {
        Repository repo = RepositoryFactory.getRepository();
        int tenantId = getTenantId("SCRATCH", repo); // SCRATCH is the default space

        // Constraints specified for the unit itself (such as being 'EFFECTIVE')
        SearchExpression expr = new SearchExpression(SearchItem.constrainToSpecificTenant(tenantId));
        expr = SearchExpression.assembleAnd(expr, SearchItem.constrainToSpecificStatus(Unit.Status.EFFECTIVE));

        // Constrain to time-related attribute
        int attributeId = getAttributeId("dc:date", repo);

        SearchItem<Timestamp> timestampSearchItem = new TimeAttributeSearchItem(attributeId, Operator.LEQ, new Timestamp(System.currentTimeMillis()));
        expr = SearchExpression.assembleAnd(expr, timestampSearchItem);

        // Constrain to string attribute
        attributeId = getAttributeId("dc:title", repo);
        SearchItem<String> stringSearchItem = new StringAttributeSearchItem(attributeId, Operator.EQ, "Ajj som bara den");
        expr = SearchExpression.assembleAnd(expr, stringSearchItem);

        // Result set constraints (paging)
        SearchOrder order = SearchOrder.getDefaultOrder(); // descending on creation time
        UnitSearchData usd = new UnitSearchData(expr, order, /* selectionSize */ 5);

        // Now we can either use canned search (that produces units) or
        // search "manually" where we retrieve individual fields of units
        // without actually creating Unit objects.
        if (/* canned search? */ true) {
            SearchResult result = repo.searchUnit(
                    /* paging stuff */ 0, 5, 100,
                    expr, order
            );

            Collection<Unit> units = result.results();
            for (Unit unit : units) {
                System.out.println("Found: " + unit);
            }
        } else {
            // Search "manually", in which case no Unit:s are instantiated
            DatabaseAdapter searchAdapter = repo.getDatabaseAdapter();
            StringBuilder buf = searchAdapter.generateStatement(usd);

            try {
                repo.useConnection(conn -> Database.useReadonlyStatement(conn, buf.toString(),
                        stmt -> stmt.setMaxRows(/* selectionSize */ 5),
                        rs -> {
                            while (rs.next()) {
                                int i = 0;
                                int _tenantId = rs.getInt(++i);
                                long _unitId = rs.getLong(++i);
                                Timestamp _created = rs.getTimestamp(++i);

                                System.out.println("Found: tenantId=" + _tenantId + " unitId=" + _unitId + " created=" + _created);
                            }
                        }
                ));
            } catch (SQLException sqle) {
                throw new RuntimeException("Could not search: " + Database.squeeze(sqle), sqle);
            }
        }
    }
}