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

import org.gautelis.repo.exceptions.InvalidParameterException;
import org.gautelis.repo.model.attributes.Attribute;
import org.gautelis.repo.model.attributes.Type;

import java.text.ParseException;
import java.util.Locale;


public abstract class AttributeSearchItem<T> extends SearchItem<T> {

    private final int attrId;

    protected AttributeSearchItem(Type type, Operator operator, int attrId) {
        super(Variant.ATTRIBUTE, type, operator);
        this.attrId = attrId;
    }

    public int getAttrId() {
        return attrId;
    }

    /**
     * Generates constraint "attribute == value" for specified attribute.
     * <p>
     * This method will handle the various attribute types.
     */
    public static AttributeSearchItem<?> constrainOnValueEQ(
            Attribute<?> attribute, String value, Locale locale
    ) throws NumberFormatException, InvalidParameterException, ParseException {

        int attrId = attribute.getAttrId();
        Type type = attribute.getType();

        return switch (type) {
            case STRING -> StringAttributeSearchItem.constrainOnEQ(attrId, value);
            case TIME -> TimeAttributeSearchItem.constrainOnEQ(attrId, value, locale);
            case INTEGER -> IntegerAttributeSearchItem.constrainOnEQ(attrId, value);
            case LONG -> LongAttributeSearchItem.constrainOnEQ(attrId, value);
            case DOUBLE -> DoubleAttributeSearchItem.constrainOnEQ(attrId, value);
            case BOOLEAN -> BooleanAttributeSearchItem.constrainOnEQ(attrId, value);
            default -> throw new InvalidParameterException("Attribute type " + type + " is not searchable: " + attribute);
        };
    }
}
