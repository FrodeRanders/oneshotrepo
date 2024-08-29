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

import java.sql.Timestamp;
import java.text.ParseException;
import java.util.Locale;

public class TimeAttributeSearchItem extends AttributeSearchItem<Timestamp> {

    private final Timestamp value;

    public TimeAttributeSearchItem(int attrId, Operator operator, Timestamp value) {
        super(Type.TIME, operator, attrId);
        this.value = value;
    }

    public Timestamp getValue() {
        return value;
    }

    /**
     * Generates constraint "date attribute == value" for
     * specified attribute id.
     */
    public static TimeAttributeSearchItem constrainOnEQ(int attrId, Timestamp value) {
        return new TimeAttributeSearchItem(attrId, Operator.EQ, value);
    }

    /**
     * Generates constraint "date attribute == value" for
     * specified attribute id.
     */
    public static TimeAttributeSearchItem constrainOnEQ(int attrId, String value, Locale locale) throws ParseException {
        return new TimeAttributeSearchItem(attrId, Operator.EQ, new Timestamp(SearchItem.string2Date(value, locale).getTime()));
    }

}
