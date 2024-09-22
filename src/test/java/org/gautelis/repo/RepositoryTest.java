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
package org.gautelis.repo;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.gautelis.repo.db.Database;
import org.gautelis.repo.exceptions.BaseException;
import org.gautelis.repo.model.Repository;
import org.gautelis.repo.model.Unit;
import org.gautelis.repo.model.associations.Association;
import org.gautelis.repo.model.attributes.Attribute;
import org.gautelis.repo.model.locks.Lock;
import org.gautelis.repo.model.utils.TimedExecution;
import org.gautelis.repo.search.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.Optional;

import static org.gautelis.repo.Statistics.dumpStatistics;

/**
 *
 */
public class RepositoryTest extends TestCase {
    private static final Logger log = LoggerFactory.getLogger(RepositoryTest.class);

    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public RepositoryTest(String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( RepositoryTest.class );
    }


    public void testRepository() {
        Repository repo = RepositoryFactory.getRepository();

        final int tenantId = 1; // For the sake of exercising, this is the tenant of units we will create
        final String stringAttribute = "dc:title";
        final String timeAttribute = "dc:date";

        final int numberOfParents = 50; //
        final int numberOfChildren = 50; //

        try {
            Timestamp firstParentCreated = null;
            Timestamp someTimestamp = null;
            String someSpecificString = "Specific string";
            int numberOfUnitsToHaveSpecificString = 1;

            // With resultset paging, we want to skip the first 'pageOffset' results and
            // acquire the 'pageSize' next results. 'pageOffset' and 'pageSize' has precedence over
            // 'selectionSize' (which counts from the first result).
            int pageOffset = 5;  // skip 'pageOffset' first results
            int pageSize = 5;    // pick next 'pageSize' results
            int selectionSize = 0; // pick 'selectionSize' first results

            System.out.println("Generating " + (numberOfParents * numberOfChildren) + " units...");
            System.out.println("(This can take some time)");
            System.out.flush();

            for (int j = 1; j < numberOfParents + 1; j++) {
                Unit parentUnit = repo.createUnit(tenantId, "parent-" + j);
                {
                    Optional<Attribute<String>> attribute = parentUnit.getStringAttribute(stringAttribute, /* create if missing? */ true);
                    attribute.ifPresent(attr -> {
                        ArrayList<String> value = attr.getValue();
                        value.add("First value");
                        value.add("Second value");
                        value.add("Third value");
                    });
                }
                repo.storeUnit(parentUnit);

                // In order to know creation time, we need to load the parent unit from database since
                // these characteristics are assigned upon write. This is not typical use of the API,
                // but we want this information for testing purposes down under.
                Optional<Unit> _parentUnit = repo.getUnit(parentUnit.getTenantId(), parentUnit.getUnitId());
                if (null == firstParentCreated && _parentUnit.isPresent()) {
                    firstParentCreated = _parentUnit.get().getCreationTime().get();
                }

                repo.lockUnit(parentUnit, Lock.Type.EXISTENCE, "test");

                for (int i = 1; i < numberOfChildren + 1; i++) {
                    Unit childUnit = repo.createUnit(tenantId, "child-" + j + "-" + i);

                    // Initial version
                    Optional<Attribute<String>> _stringAttribute = parentUnit.getStringAttribute(stringAttribute);
                    _stringAttribute.ifPresent(attr -> {
                        try {
                            childUnit.addAttribute(attr);

                        } catch (BaseException be) {
                            System.err.println("Failed to add attribute: " + be.getMessage());
                        }
                    });

                    Optional<Attribute<Timestamp>> _timeAttribute = childUnit.getTimeAttribute(timeAttribute, /* create if missing? */ true);
                    Timestamp ts = new Timestamp(System.currentTimeMillis());
                    _timeAttribute.ifPresent(attr -> {
                        ArrayList<Timestamp> value = attr.getValue();
                        value.add(ts);
                    });

                    // Some unique unit will get unique attribute
                    if (/* i within page, that we will search for later down under */
                            i > pageOffset && i == pageOffset + pageSize && numberOfUnitsToHaveSpecificString-- > 0
                    ) {
                        _stringAttribute = childUnit.getStringAttribute(stringAttribute);
                        _stringAttribute.ifPresent(attr -> {
                            ArrayList<String> value = attr.getValue();
                            value.add(someSpecificString);
                        });

                        someTimestamp = ts; // For testing purposes
                    }

                    //
                    repo.storeUnit(childUnit);

                    // Add a relation to parent unit
                    repo.addRelation(parentUnit, Association.Type.PARENT_CHILD_RELATION, childUnit);
                }
                System.out.flush();

                if (false) {
                    System.out.println("Children of " + parentUnit.getName().orElse("parent") + " (" + parentUnit.getReference() + "):");
                    parentUnit.getRelations(Association.Type.PARENT_CHILD_RELATION).forEach(
                            relatedUnit -> System.out.println("  " + relatedUnit.getName().orElse("child") + " (" + relatedUnit.getReference() + ")")
                    );
                }
            }

            // In order to search here in test, we will need some "internal" objects,
            // such as Context, DataSource, etc.
            {
                // Unit constraints
                SearchExpression expr = new SearchExpression(SearchItem.constrainToSpecificTenant(tenantId));
                expr = SearchExpression.assembleAnd(expr, SearchItem.constrainToSpecificStatus(Unit.Status.EFFECTIVE));
                expr = SearchExpression.assembleAnd(expr, SearchItem.constrainToCreatedAfter(firstParentCreated));

                // First attribute constraint
                Optional<Integer> _timeAttributeId = repo.attributeNameToId(timeAttribute);
                int[] timeAttributeId = { 0 };
                _timeAttributeId.ifPresent(attrId -> timeAttributeId[0] = attrId);

                SearchItem<Timestamp> timestampSearchItem = new TimeAttributeSearchItem(timeAttributeId[0], Operator.LEQ, someTimestamp);
                expr = SearchExpression.assembleAnd(expr, timestampSearchItem);

                // Second attribute constraint
                Optional<Integer> _stringAttributeId = repo.attributeNameToId(stringAttribute);
                int[] stringAttributeId = { 0 };
                _stringAttributeId.ifPresent(attrId -> stringAttributeId[0] = attrId);

                SearchItem<String> stringSearchItem = new StringAttributeSearchItem(stringAttributeId[0], Operator.EQ, someSpecificString);
                expr = SearchExpression.assembleAnd(expr, stringSearchItem);

                // Result set constraints (paging)
                SearchOrder order = SearchOrder.getDefaultOrder(); // descending on creation time
                UnitSearchData usd = new UnitSearchData(expr, order, /* selectionSize */ 5);

                // Build SQL statement for search
                DatabaseAdapter searchAdapter = repo.getDatabaseAdapter();
                StringBuilder buf = searchAdapter.generateStatement(usd);
                log.debug("Search statement: {}", buf.toString());

                // Actually searching
                TimedExecution.run(repo.getTimingData(), "custom search", () -> {
                    try {
                        repo.useConnection(conn -> Database.useReadonlyStatement(conn, buf.toString(),
                            stmt -> stmt.setMaxRows(selectionSize),
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
                        log.error("Could not perform custom search: {}", Database.squeeze(sqle), sqle);                    }
                });
            }

            dumpStatistics(repo);

        } catch (Throwable t) {
            String info = t.getMessage();
            log.error(info, t);

            fail(info);
        }
    }
}
