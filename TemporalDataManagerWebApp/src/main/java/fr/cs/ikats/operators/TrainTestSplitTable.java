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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * @author ftoral
 */

package fr.cs.ikats.operators;

import java.util.List;

import fr.cs.ikats.common.dao.exception.IkatsDaoConflictException;
import fr.cs.ikats.common.dao.exception.IkatsDaoMissingRessource;
import fr.cs.ikats.temporaldata.business.table.Table;
import fr.cs.ikats.temporaldata.business.table.TableInfo;
import fr.cs.ikats.temporaldata.business.table.TableManager;
import fr.cs.ikats.temporaldata.business.table.TableUtils;
import fr.cs.ikats.temporaldata.exception.IkatsException;
import fr.cs.ikats.temporaldata.exception.InvalidValueException;
import fr.cs.ikats.temporaldata.exception.ResourceNotFoundException;

/**
 * Class TrainTestSplitTable
 */
public class TrainTestSplitTable {

    /**
     * Information to be provided to the {@link TablesMerge} operator
     */
    public static class Request {

        public String tableName;
        public String targetColumnName;
        public double repartitionRate;
        public String outputTableName;

        public Request() {
            ; // default constructor
        }
    }

    private Request request;
    private TableManager tableManager;

    /**
     * TrainTestSplit Table operator initialization
     *
     * @param request the input data provided to the operator
     */
    public TrainTestSplitTable(Request request) {

        this.request = request;
        this.tableManager = new TableManager();
    }
    
    /**
     * Apply the operator to the {@link Request}, save the result.
     * @throws ResourceNotFoundException 
     * @throws IkatsException 
     * @throws IkatsDaoMissingRessource 
     * @throws InvalidValueException 
     * @throws IkatsDaoConflictException 
     *
     * @throws IkatsOperatorException
     */
    public void apply() throws IkatsDaoMissingRessource, IkatsException, ResourceNotFoundException, IkatsDaoConflictException, InvalidValueException {
        
        // do the job
        List<Table> tabListResult = doCompute();
        
        // Store tables in database
        tabListResult.get(0).setName(request.outputTableName + "_Train");
        tabListResult.get(1).setName(request.outputTableName + "_Test");
        tableManager.createInDatabase(tabListResult.get(0).getTableInfo());
        tableManager.createInDatabase(tabListResult.get(1).getTableInfo());
    }
    
    /**
     * @param tableName
     * @param targetColumnName
     * @param repartitionRate
     * @param outputTableName
     * @throws IkatsException 
     * @throws IkatsDaoMissingRessource 
     * @throws ResourceNotFoundException 
     */
    public List<Table> doCompute() throws IkatsDaoMissingRessource, IkatsException, ResourceNotFoundException {
        
        // retrieve table tableName from db
        TableInfo tableInfo = tableManager.readFromDatabase(request.tableName);
        Table table = tableManager.initTable(tableInfo, false);
    
        List<Table> tabListResult;
        if ("".equals(request.targetColumnName)) {
            tabListResult = TableUtils.randomSplitTable(table, request.repartitionRate);
        } else {
            tabListResult = TableUtils.trainTestSplitTable(table, request.targetColumnName, request.repartitionRate);
        }
    
        return tabListResult;
    }

    
}
