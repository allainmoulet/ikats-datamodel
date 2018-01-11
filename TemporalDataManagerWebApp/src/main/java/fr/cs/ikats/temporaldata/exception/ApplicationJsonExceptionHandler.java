/**
 * LICENSE:
 * --------
 * Copyright 2017-2018 CS SYSTEMES D'INFORMATION
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
 * @author Maxime PERELMUTER <maxime.perelmuter@c-s.fr>
 *
 */

package fr.cs.ikats.temporaldata.exception;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import org.apache.log4j.Logger;

import fr.cs.ikats.temporaldata.utils.ServiceStatus;

/**
 * This handler is adapted to http requests producing json.
 * It is using  IkatsJsonException and its getHtppStatus() method, which may be overridden 
 * (see {@link ResourceNotFoundJsonException}).
 */
@Provider
public class ApplicationJsonExceptionHandler implements ExceptionMapper<IkatsJsonException> {

    private static Logger logger = Logger.getLogger(ApplicationJsonExceptionHandler.class);
     
    /**
     * Encode an error in HTTP Response:
     * <ul>
     * <li>Set status defined in IkatsJsonException (sub)class, using  exception.getHtppStatus()</li>
     * <li>Write json content from thrown instances of IkatsJsonException (sub)class.</li>
     * </ul>
     * {@inheritDoc}
     */
    @Override
    public Response toResponse(IkatsJsonException exception) {

            Status status =  exception.getHtppStatus();
            if(status.equals(Status.NOT_FOUND)) {
                logger.error("Error handled while processing request :" + status.getStatusCode() + " " + status.getReasonPhrase() + " " + exception.getMessage() );
            } else {
                logger.error("Error handled while processing request :" + status.getStatusCode() + " " + status.getReasonPhrase(), 
                        exception );
            }
            
            ServiceStatus lWrappedError = new ServiceStatus( exception );
            return Response.status( status ).type( MediaType.APPLICATION_JSON_TYPE ).entity( lWrappedError ).build();   
    }
}
