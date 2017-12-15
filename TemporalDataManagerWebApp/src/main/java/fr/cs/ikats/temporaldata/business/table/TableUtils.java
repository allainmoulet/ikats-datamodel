/**
 * LICENSE:
 * --------
 * Copyright 2017 CS SYSTEMES D'INFORMATION
 * 
 * Licensed to CS SYSTEMES D'INFORMATION under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. CS licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 * @author Fabien Toral <fabien.toral@c-s.fr>
 */

package fr.cs.ikats.temporaldata.business.table;

import java.util.ArrayList;

import fr.cs.ikats.temporaldata.business.table.TableInfo.Header;
import fr.cs.ikats.temporaldata.business.table.TableInfo.TableContent;
import fr.cs.ikats.temporaldata.business.table.TableInfo.TableDesc;
import fr.cs.ikats.temporaldata.business.table.TableInfo.TableHeaders;

/**
 * Utility class for Table artefacts
 */
public class TableUtils {

    /**
     * Creates and initializes the structure of an empty Table,
     * <ul>
     * <li>with columns header enabled when parameter withColumnsHeader is true ,</li>
     * <li>with rows header enabled when parameter withColumnsHeader is true ,</li>
     * </ul>
     * This Table is initialized without links managed: see how to configure links management with enablesLinks()
     * method.
     *
     * @return created Table, ready to be completed.
     */
    public static Table initEmptyTable(boolean withColumnsHeader, boolean withRowsHeader) {
        TableInfo tableInfo = new TableInfo();
    
        tableInfo.table_desc = new TableDesc();
        tableInfo.headers = new TableHeaders();
    
        if (withRowsHeader) {
            tableInfo.headers.row = new Header();
            tableInfo.headers.row.data = new ArrayList<>();
        }
        if (withColumnsHeader) {
            tableInfo.headers.col = new Header();
            tableInfo.headers.col.data = new ArrayList<>();
        }
    
        tableInfo.content = new TableContent();
        tableInfo.content.cells = new ArrayList<>();
    
        return new Table(tableInfo);
    }
}
