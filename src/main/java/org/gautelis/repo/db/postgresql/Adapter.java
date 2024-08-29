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
package org.gautelis.repo.db.postgresql;


import org.gautelis.repo.search.CommonDbmsAdapter;
import org.gautelis.repo.search.UnitSearchData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class Adapter extends CommonDbmsAdapter {
    protected static final Logger log = LoggerFactory.getLogger(Adapter.class);

    public Adapter() {
    }

    @Override
    public String asTime(String timeStr) {
        //--------------------------------------------------
        // Format should match result from
        //    org.gautelis.repo.search.CommonAdapter()
        // which for the time being returns
        //   yyyy-MM-dd HH:mm:ss.SSS
        //--------------------------------------------------
        return "TO_TIMESTAMP('" + timeStr.replace('\'', ' ') + "', 'YYYY-MM-DD HH24:MI:SS.MS')";
    }

    public StringBuilder generateStatement(
            UnitSearchData sd
    ) throws IllegalArgumentException {
        StringBuilder buf = super.generateStatement(sd);

        // Paging and/or limiting search results
        int pageOffset = sd.getPageOffset();
        if (pageOffset > 0) {
            buf.append("OFFSET ").append(pageOffset).append(" ROWS ");

            int pageSize = sd.getPageSize();
            if (pageSize > 0) {
                buf.append("FETCH NEXT ").append(pageSize).append(" ROWS ONLY ");
            }
        } else {
            int selectionSize = sd.getSelectionSize();
            if (selectionSize > 0) {
                buf.append("FETCH FIRST ").append(selectionSize).append(" ROWS ONLY ");
            }
        }

        return buf;
    }
}
