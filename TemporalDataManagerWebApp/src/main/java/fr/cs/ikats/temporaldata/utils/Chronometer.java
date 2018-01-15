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
 * @author Mathieu BERAUD <mathieu.beraud@c-s.fr>
 * @author Maxime PERELMUTER <maxime.perelmuter@c-s.fr>
 *
 */

package fr.cs.ikats.temporaldata.utils;

import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.log4j.Logger;

/**
 * Class used to start, stop chronometer and log result into a specified LOGGER.
 *
 *
 */
public class Chronometer {

    /**
     * name of the Chronometer
     */
    private String name;

    /**
     * startTime
     */
    private long startTime;

    /**
     * stopTime
     */
    private long stopTime;

    /**
     * time elpased between start and stop time
     */
    private long timeElapsed;

    /**
     * init the Chronometer and start if asked
     *
     * @param name name of the chronometer
     * @param start if chrono must be start
     */
    public Chronometer(String name, boolean start) {
        this.name = name;
        if (start) {
            start();
        }
    }

    /**
     * start the chronometer. Will set the start time with the current
     * millisecond time of the system.
     */
    public void start() {
        startTime = System.currentTimeMillis();
    }

    /**
     * update this name, then (re)start the chronometer, using start(). 
     * Justified use when one same Chronometer mesures durations of several steps. 
     * @param updatedName the updated name
     */
    public void start(String updatedName) {
        name = updatedName;
        start();
    }

    /**
     * stop the chronometer. if LOGGER is not null, use it to log the result
     *
     * @param logger the logger to use to log ellapsed time
     */
    public void stop(Logger logger) {
        stopTime = System.currentTimeMillis();
        timeElapsed = stopTime - startTime;
        if (logger != null) {
            logger.info(this);
        }
    }

    /**
     * print elapsed time in human readable format
     *
     * @param unit the timeUnit to use when logging
     * @return a time printed with correct timeunit
     */
    public String printTimeElapsed(TimeUnit unit) {
        if (TimeUnit.MILLISECONDS.equals(unit)) {
            return "[Duration : " + timeElapsed + " ms]";
        } else {
            return "[Duration : " + timeElapsed + " " + unit.name() + "]";
        }
    }

    /**
     * to string
     */
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[name= ").append(name).append(",");
        sb.append("startTime=").append(DateFormatUtils.ISO_DATETIME_FORMAT.format(startTime)).append(",");
        sb.append("stopTime=").append(DateFormatUtils.ISO_DATETIME_FORMAT.format(stopTime)).append(",");
        sb.append("timeElapsed=").append(timeElapsed).append("]");
        return sb.toString();
    }

}
