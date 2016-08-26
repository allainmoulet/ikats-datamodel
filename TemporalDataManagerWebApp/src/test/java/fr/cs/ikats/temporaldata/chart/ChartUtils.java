package fr.cs.ikats.temporaldata.chart;

import fr.cs.ikats.datamanager.client.opentsdb.QueryResult;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.jfree.data.time.FixedMillisecond;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;

public class ChartUtils {

	public ChartUtils() {
		// TODO Auto-generated constructor stub
	}

	
	public TimeSeriesCollection createTimeSerieForChart(String startDate, QueryResult resultat) {
		TimeSeriesCollection dataset = new TimeSeriesCollection();
		Date date = null;
		try {
			// 2015/03/11-08:00:00
			date = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss").parse(startDate);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		addResultatRequeteToTSCollection(resultat, dataset, date);
		return dataset;
	}


	/**
	 * @param resultat
	 * @param dataset
	 * @param date
	 */
	public void addResultatRequeteToTSCollection(QueryResult resultat,
			TimeSeriesCollection dataset, Date date) {
		for (Entry<Integer, Map<Object, Object>> serie : resultat.getSeries().entrySet()) {
			
			TimeSeries s1 = new TimeSeries("Serie : "+serie.getKey());	
			
			Map<Object, Object> vals = serie.getValue();
			for (Object timestamp : vals.keySet()) {
				Double value = (Double) vals.get(timestamp);
				long realtimestamp = date.getTime()+Long.parseLong((String) timestamp);
				s1.addOrUpdate(new FixedMillisecond(new Date(realtimestamp)),value);
			}
			dataset.addSeries(s1);
		
		}
	}
	
	public TimeSeriesCollection createTimeSerieForChart(String startDate, List<QueryResult> resultat) {
		TimeSeriesCollection dataset = new TimeSeriesCollection();
		Date date = null;
		try {
			// 2015/03/11-08:00:00
			date = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss").parse(startDate);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		for(QueryResult piceOfResult : resultat) {
			addResultatRequeteToTSCollection(piceOfResult, dataset, date);
		}
		return dataset;
	}
	
}
