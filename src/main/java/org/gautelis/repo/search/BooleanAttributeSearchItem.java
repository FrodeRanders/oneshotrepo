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
package org.gautelis.repo.search;

import org.gautelis.repo.model.attributes.Type;
public class BooleanAttributeSearchItem extends AttributeSearchItem<Boolean> {

    private final boolean value;

    public BooleanAttributeSearchItem(int attrId, Operator operator, boolean value) {
        super(Type.BOOLEAN, operator, attrId);
        this.value = value;
    }

    public Boolean getValue() {
        return value;
    }

    /**
     * Generates constraint "boolean attribute == value" for
     * specified attribute id.
     */
    public static BooleanAttributeSearchItem constrainOnEQ(int attrId, boolean value) {
        return new BooleanAttributeSearchItem(attrId, Operator.EQ, value);
    }

    /**
     * Generates constraint "boolean attribute == value" for
     * specified attribute id.
     */
    public static BooleanAttributeSearchItem constrainOnEQ(int attrId, String value) {
        return new BooleanAttributeSearchItem(attrId, Operator.EQ, Boolean.parseBoolean(value));
    }
}
