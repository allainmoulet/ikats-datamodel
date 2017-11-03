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

import javax.ws.rs.core.Response.Status;

/**
 * Exception indicating no resource is found.
 * Adapted to http requests producing json.
 * 
 */
public class ResourceNotFoundJsonException extends IkatsJsonException {
 
    /**
     * 
     */
    private static final long serialVersionUID = -6942435746728380602L;
    
    /**
     * @param message the message
     * @param cause the cause
     */
    public ResourceNotFoundJsonException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * @param message the message
     */
    public ResourceNotFoundJsonException(String message) {
        super(message);
    }

    /**
     * 
     * @return Status.NOT_FOUND
     */
    public Status getHtppStatus()
    {
        return Status.NOT_FOUND;
    }
   

}

