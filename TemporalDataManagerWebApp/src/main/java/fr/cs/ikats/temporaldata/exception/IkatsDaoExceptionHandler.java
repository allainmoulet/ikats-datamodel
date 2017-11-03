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
 * @author Fabien TORTORA <fabien.tortora@c-s.fr>
 * 
 */

package fr.cs.ikats.temporaldata.exception;


import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import org.apache.log4j.Logger;

import fr.cs.ikats.common.dao.exception.IkatsDaoConflictException;
import fr.cs.ikats.common.dao.exception.IkatsDaoException;
import fr.cs.ikats.common.dao.exception.IkatsDaoMissingRessource;

/**
 * Handler of IkatsDaoExceptions
 */
@Provider
public class IkatsDaoExceptionHandler implements ExceptionMapper<IkatsDaoException> {

    private static Logger logger = Logger.getLogger(IkatsDaoExceptionHandler.class);
     
    /**
     * 
     * {@inheritDoc}
     * @since [#142998] handle IkatsDaoException, with different status Status.CONFLICT, Status.NOT_FOUND ...
     */
    @Override
    public Response toResponse(IkatsDaoException exception) {
        
        if(IkatsDaoMissingRessource.class.isAssignableFrom(exception.getClass())) {
            logger.error("Error processing the request: resource not found on server: ");
            logger.error( exception );
            return Response.status(Status.NOT_FOUND).entity(exception.getMessage()).build(); 
            
        } else if(IkatsDaoConflictException.class.isAssignableFrom(exception.getClass())) {
            logger.error("Error processing the request: resource conflict on server: ");
            logger.error( exception );
            return Response.status(Status.CONFLICT).entity(exception.getMessage()).build();
            
        } else {
            logger.error("Error handled while processing request ",exception);
            return Response.status(Status.BAD_REQUEST).entity(exception.getMessage()).build();
        }
    }
}

