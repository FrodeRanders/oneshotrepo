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

import org.gautelis.repo.db.Column;
import org.gautelis.repo.model.attributes.Type;

/**
 *
 */
public class LongUnitSearchItem extends UnitSearchItem<Long> {

    private final long value;

    public LongUnitSearchItem(Column column, Operator operator, long value) {
        super(Type.LONG, column, operator);
        this.value = value;
    }

    public Long getValue() {
        return value;
    }
}
