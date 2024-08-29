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
package org.gautelis.repo.model.attributes;

import org.gautelis.repo.exceptions.AttributeTypeException;

public enum Type {
    STRING(1),
    TIME(2),
    INTEGER(3),
    LONG(4),
    DOUBLE(5),
    BOOLEAN(6),
    DATA(7); // Not searchable

    private final int type;

    Type(int type) {
        this.type = type;
    }

    public static Type of(int type) throws AttributeTypeException {
        for (Type t : Type.values()) {
            if (t.type == type) {
                return t;
            }
        }
        throw new AttributeTypeException("Unknown attribute type: " + type);
    }

    public int getType() {
        return type;
    }
}
