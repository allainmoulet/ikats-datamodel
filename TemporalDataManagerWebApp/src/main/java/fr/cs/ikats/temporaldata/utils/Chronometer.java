package fr.cs.ikats.temporaldata.utils;

import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.log4j.Logger;

/**
 * Class used to start, stop chronometer and log result into a specified LOGGER.
 * 
 * @author ikats
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
        }
        else {
        	// FIXME FTO : provide conversio or remove that code and the unit parameter.
            return "[Duration : " + timeElapsed + " ms]";
        }
    }

    /**
     * to string
     */
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[name= ").append(name).append(",");
        sb.append("startTime=").append(DateFormatUtils.ISO_DATETIME_FORMAT.format(startTime)).append(",");
        sb.append("stopTime=").append(DateFormatUtils.ISO_DATETIME_FORMAT.format(startTime)).append(",");
        sb.append("timeElapsed=").append(timeElapsed).append("]");
        return sb.toString();
    }

}
