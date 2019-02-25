/**
 * Copyright 2018-2019 CS Systèmes d'Information
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

package fr.cs.ikats.table;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import fr.cs.ikats.common.dao.exception.IkatsDaoConflictException;
import fr.cs.ikats.common.dao.exception.IkatsDaoMissingResource;

public class TableDAOTest extends TableDAO {

    static TableDAO dao;

    @BeforeClass
    public static void setUpClass() throws Exception {
        dao = new TableDAO();
    }

    @After
    public void tearDown() throws Exception {
        List<TableEntitySummary> listAll = dao.listAll();
        for (TableEntitySummary tableEntity : listAll) {
            dao.removeById(tableEntity.getId());
        }
    }

    @Test
    @Ignore // Not yet implemented
    public final void testListAll() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    @Ignore // Not yet implemented
    public final void testFindByName() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    public final void testGetByName() throws IkatsDaoConflictException, IkatsDaoMissingResource {
        
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

    @Test(expected = IkatsDaoConflictException.class)
    public final void testPersist() throws IkatsDaoConflictException {

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
    @Ignore // Not yet implemented
    public final void testUpdate() {
        fail("Not yet implemented"); // TODO
    }

    @Test
    @Ignore // Done with the tearDown method
    public final void testRemoveById() {
        assertTrue("Let the teardown method cover that method", true);
    }

}
