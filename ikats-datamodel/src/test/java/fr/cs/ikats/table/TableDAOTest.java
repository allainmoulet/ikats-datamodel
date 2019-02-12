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
 * @author Fabien TORAL <fabien.toral@c-s.fr>
 * 
 */

package fr.cs.ikats.table;

import static org.junit.Assert.*;

import java.util.List;

import org.hibernate.exception.ConstraintViolationException;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import fr.cs.ikats.common.dao.exception.IkatsDaoException;
import fr.cs.ikats.common.dao.exception.IkatsDaoMissingRessource;


public class TableDAOTest extends TableDAO {
    
    static TableDAO dao;

    @BeforeClass
    public static void setUpClass() throws Exception {
        dao = new TableDAO();
    }

    @After
    public void tearDown() throws Exception {
        List<TableEntity> listAll = dao.listAll();
        for (TableEntity tableEntity : listAll) {
            dao.removeById(tableEntity.getId());
        }
    }

    @Test
    public final void testListAll() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public final void testFindByName() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public final void testGetByName() throws IkatsDaoMissingRessource, IkatsDaoException {
        
        // Create a table
        TableEntity tableEntity = new TableEntity();
        tableEntity.setName("TEST TABLE");
        
        String rawValuesStr = new String("TestRawValuesStr");
        String rawDataLinksStr = new String("TestDataLinksStr");
        
        tableEntity.setRawValues(rawValuesStr.getBytes());
        tableEntity.setRawDataLinks(rawDataLinksStr.getBytes());
        
        // Save it
        dao.persist(tableEntity);
        
        // Get the saved table in another entity
        String name = tableEntity.getName();
        TableEntity tableEntity2 = dao.getByName(name);
        
        // compare the raw values
        byte[] actualRawValues = tableEntity2.getRawValues();
        byte[] actualRawDataLinks = tableEntity2.getRawDataLinks();
        
        assertArrayEquals(rawValuesStr.getBytes(), actualRawValues);
        assertArrayEquals(rawDataLinksStr.getBytes(), actualRawDataLinks);
    }

    @Test(expected = ConstraintViolationException.class)
    public final void testPersist() {

        // Create a table
        TableEntity tableEntity = new TableEntity();
        tableEntity.setName("TEST TABLE");
        
        String rawValuesStr = new String("TestRawValuesStr");
        String rawDataLinksStr = new String("TestDataLinksStr");
        
        tableEntity.setRawValues(rawValuesStr.getBytes());
        tableEntity.setRawDataLinks(rawDataLinksStr.getBytes());
        
        int idBefore = tableEntity.getId();
        
        // Save it
        dao.persist(tableEntity);
        int idAfter = tableEntity.getId();
        
        // Test that we got a new id
        assertTrue(idBefore != idAfter);
        
        // Could not be realized due to unique constraint on name the work around is dao.update();
        dao.persist(tableEntity);
    }

    @Test
    public final void testUpdate() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    @Ignore // Done with the tearDown method
    public final void testRemoveById() {
        fail("Test not needed due to usage by the teardown method");
    }

}
