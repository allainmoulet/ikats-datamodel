package fr.cs.ikats.temporaldata.utils;

import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.log4j.Logger;

/**
 * Class used to start, stop chronometer and log result into a specified logger.
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
	 * init the Chronometer and start if if asked
	 * @param name name of the chronometer
	 * @param start indicates to start the chronometer
	 */
	public Chronometer(String name,boolean start) {
		this.name = name;
		if(start) {
			start();
		}
	}
	
	/**
	 * start the chronometer. Will set the start time with the current millisecond time of the system.
	 */
	public void start() {
		startTime = System.currentTimeMillis();
	}
	
	/**
	 * stop the chronometer. if logger is not null, use it to log the result
	 * @param logger the logger to where to log the ellapsed time
	 */
	public void stop(Logger logger) {
		stopTime = System.currentTimeMillis();
		timeElapsed = stopTime-startTime;
		if(logger!=null) {
			logger.info(this);
		}
	}
	
	/**
	 * print elapsed time in human readable format
	 * @param unit the time Unit to print
	 * @return a printable string
	 */
	public String printTimeElapsed(TimeUnit unit) {
		if(TimeUnit.MILLISECONDS.equals(unit)) {
			return "[Duration : "+timeElapsed + " ms]";
		} else {
			return "[Duration : "+timeElapsed + " ms]";
		}
	}
	
	/**
	 * to string
	 */
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("\n[ Chronometer : "+name).append("\n");
		sb.append("  start at "+DateFormatUtils.ISO_DATETIME_FORMAT.format(startTime)).append("\n");
		sb.append("  end at "+DateFormatUtils.ISO_DATETIME_FORMAT.format(startTime)).append("\n");
		sb.append(printTimeElapsed(TimeUnit.MILLISECONDS));
		sb.append("]");
		return sb.toString();
	}

}
