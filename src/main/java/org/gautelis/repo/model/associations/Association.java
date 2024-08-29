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
package org.gautelis.repo.model.associations;


import org.gautelis.repo.exceptions.AssociationTypeException;
import org.gautelis.repo.model.Unit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is any kind of association between a unit and another
 * unit (relation) or between a unit and some external entity.
 * <p>
 * The single/multiple association integrity is maintained
 * internally in AssociationManager.
 * <p>
 * It is always safe to create an association of a specific
 * type for a unit since existing associations are removed
 * if the specified association type does not support
 * multiple associations.
 */
public abstract class Association {
    static final Logger log = LoggerFactory.getLogger(Association.class);

    public enum Type {
        INVALID(0, /* is relational? */ false, /* allows multiples? */ false),
        PARENT_CHILD_RELATION(1, /* is relational? */ true, /* allows multiples? */ true),
        CASE_ASSOCIATION(2, /* is relational? */ false, /* allows multiples? */ false),
        REPLACEMENT_RELATION(3, /* is relational? */ true, /* allows multiples? */ false);

        private final int type;
        // Is this association an internal association (relation) or an external association to an external entity?
        private final boolean isRelational;
        // Does this association accept multiple associations/relations?
        private final boolean allowsMultiples;

        Type(int type, boolean isRelational, boolean allowsMultiples) {
            this.type = type;
            this.isRelational = isRelational;
            this.allowsMultiples = allowsMultiples;
        }

        static Type of(int type) throws AssociationTypeException {
            for (Type t : Type.values()) {
                if (t.type == type) {
                    return t;
                }
            }
            throw new AssociationTypeException("Unknown relation/association type: " + type);
        }

        public int getType() {
            return type;
        }

        boolean isRelational() {
            return isRelational;
        }

        boolean allowsMultiples() {
            return allowsMultiples;
        }
    }

    private int tenantId = -1; // invalid
    private long unitId = -1L; // invalid
    private Type type = Type.INVALID;

    // Used when resurrecting association
    protected Association() {
    }

    // Used when resurrecting association
    protected void inject(int tenantId, long unitId, Type type) {
        this.tenantId = tenantId;
        this.unitId = unitId;
        this.type = type;
    }

    public boolean isRelational() {  // OVERRIDE
        return type.isRelational();
    }

    public boolean isAssociation() {  // OVERRIDE
        return !type.isRelational();
    }

    public boolean allowsMultiples() {
        return type.allowsMultiples();
    }

    public Type getType() {
        return type;
    }

    public int getTenantId() {
        return tenantId;
    }

    public long getUnitId() {
        return unitId;
    }

    public String toString() {
        String info = type.isRelational() ? "Internal relation " : "External association ";
        info += "of type " + type.name() + " (" + type.type + ") from " + Unit.id2String(tenantId, unitId);
        return info;
    }
}
