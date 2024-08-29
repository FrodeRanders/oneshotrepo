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

public class IntegerAttributeSearchItem extends AttributeSearchItem<Integer> {

    private final int value;

    public IntegerAttributeSearchItem(int attrId, Operator operator, int value) {
        super(Type.INTEGER, operator, attrId);
        this.value = value;
    }

    public Integer getValue() {
        return value;
    }

    /**
     * Generates constraint "integer attribute == value" for
     * specified attribute id.
     */
    public static IntegerAttributeSearchItem constrainOnEQ(int attrId, int value) {
        return new IntegerAttributeSearchItem(attrId, Operator.EQ, value);
    }

    /**
     * Generates constraint "integer attribute == value" for
     * specified attribute id.
     */
    public static IntegerAttributeSearchItem constrainOnEQ(int attrId, String value) {
        return new IntegerAttributeSearchItem(attrId, Operator.EQ, Integer.parseInt(value));
    }
}
