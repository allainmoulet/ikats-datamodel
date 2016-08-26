/**
 * 
 */
package fr.cs.ikats.temporaldata;

import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.core.Response;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import fr.cs.ikats.datamanager.client.RequestSender;
import fr.cs.ikats.datamanager.client.opentsdb.IkatsWebClientException;
import fr.cs.ikats.datamanager.client.opentsdb.QueryResult;
import fr.cs.ikats.datamanager.client.opentsdb.ResponseParser;
import fr.cs.ikats.temporaldata.utils.Chronometer;

/**
 * @author ikats
 *
 */
public class QueryRequestTest extends AbstractRequestTest {

	private ExecutorService executor;

	@BeforeClass
	public static void setUpBeforClass() {
		AbstractRequestTest.setUpBeforClass(QueryRequestTest.class.getSimpleName());
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		AbstractRequestTest.tearDownAfterClass(QueryRequestTest.class.getSimpleName());
	}

	@Test
	public void testNewAPI() {

		String testCaseName = "testNewAPI";
		boolean isNominal = true;
		try {
			start(testCaseName, isNominal);
			// String metrique = utils.getMetric(getHost(), 0);

			String metrique = "WS3";

			String startDate = "2012/07/15-08:00:00";
			String endDate1 = "2012/07/15-09:00:00";
			String endDate2 = "2012/07/30-00:00:00";
			String endDate3 = "2012/08/15-00:00:00";
			String endDate4 = "2012/08/30-00:00:00";
			String endDateFull = "2012/12/30-00:00:00";

			getLogger().info("Request with aggregator sum");
			int i = 0;
			Chronometer chrono = new Chronometer("requete " + (i++), true);
			launchNewAPISearchRequest(metrique, startDate, null, null, "sum", null, null, "show_tsuids");
			chrono.stop(getLogger());
			chrono = new Chronometer("requete " + (i++), true);
			launchNewAPISearchRequest(metrique, startDate, endDateFull, "%7Bnumero=00001%7D", "sum", null, null,
					"show_tsuids");
			chrono.stop(getLogger());
			getLogger().info("3 Requests with aggregators sum, avg and dev");
			chrono = new Chronometer("requete " + (i++), true);
			launchNewAPISearchRequest(metrique, startDate, endDate1, null, "sum", null, null, "show_tsuids");
			chrono.stop(getLogger());
			chrono = new Chronometer("requete " + (i++), true);
			launchNewAPISearchRequest(metrique, startDate, endDate1, null, "avg", null, null, "show_tsuids");
			chrono.stop(getLogger());
			chrono = new Chronometer("requete " + (i++), true);
			launchNewAPISearchRequest(metrique, startDate, endDate1, null, "dev", null, null, "show_tsuids");

			getLogger().info("Request with various end date");

			chrono.stop(getLogger());
			chrono = new Chronometer("requete " + (i++), true);
			launchNewAPISearchRequest(metrique, startDate, endDate1, null, "sum", null, null, "show_tsuids");
			chrono.stop(getLogger());
			chrono = new Chronometer("requete " + (i++), true);
			launchNewAPISearchRequest(metrique, startDate, endDate2, null, "sum", null, null, "show_tsuids");
			chrono.stop(getLogger());
			chrono = new Chronometer("requete " + (i++), true);
			launchNewAPISearchRequest(metrique, startDate, endDate3, null, "sum", null, null, "show_tsuids");
			chrono.stop(getLogger());
			chrono = new Chronometer("requete " + (i++), true);
			launchNewAPISearchRequest(metrique, startDate, endDate4, null, "sum", null, null, "show_tsuids");
			chrono.stop(getLogger());
			chrono = new Chronometer("requete " + (i++), true);
			launchNewAPISearchRequest(metrique, startDate, endDateFull, null, "sum", null, null, "show_tsuids");

			getLogger().info("Request with downsampling");

			chrono.stop(getLogger());
			chrono = new Chronometer("requete " + (i++), true);
			launchNewAPISearchRequest(metrique, startDate, endDate1, "%7Bnumero=00001%7D", "sum", "avg", "10s",
					"show_tsuids");
			chrono.stop(getLogger());
			chrono = new Chronometer("requete " + (i++), true);
			launchNewAPISearchRequest(metrique, startDate, endDate1, "%7Bnumero=00001%7D", "sum", "avg", "5s",
					"show_tsuids");
			chrono.stop(getLogger());
			chrono = new Chronometer("requete " + (i++), true);
			launchNewAPISearchRequest(metrique, startDate, endDate1, "%7Bnumero=00001%7D", "sum", "avg", "1s",
					"show_tsuids");
			chrono.stop(getLogger());
			chrono = new Chronometer("requete " + (i++), true);
			utils.launchNewAPISearchRequest(metrique, startDate, endDate1, "%7Bnumero=00001%7D", "sum", "avg", "1m",
					"show_tsuids", true);

			getLogger().info("Request with aggragation and downsampling");

			chrono.stop(getLogger());
			chrono = new Chronometer("requete " + (i++), true);
			launchNewAPISearchRequest(metrique, startDate, null, null, "sum", "avg", "1m", "show_tsuids");
			chrono.stop(getLogger());
			chrono = new Chronometer("requete " + (i++), true);
			launchNewAPISearchRequest(metrique, startDate, null, null, "sum", "avg", "10s", "show_tsuids");
			chrono.stop(getLogger());

			endNominal(testCaseName);
		} catch (Throwable e) {
			endWithFailure(testCaseName, e);
		}

	}

	@Test
	public void testNewAPITSUID() {

		String testCaseName = "testNewAPITSUID";
		boolean isNominal = true;
		try {
			start(testCaseName, isNominal);

			String startDate = "2012/07/15-08:00:00";
			String endDateFull = "2012/12/30-00:00:00";

			int i = 0;

			Chronometer chrono = new Chronometer("requete TSUID" + (i++), true);
			List<String> tsuid = Arrays.asList("000001000001000001000002000003", "000001000001000001000002000004");
			List<String> bigtsuid = Arrays.asList("00001600000300077D0000040003F1", "00001600000300077F0000040003F1");
			utils.launchNewAPISearchTSUIDRequest(tsuid, startDate, endDateFull, null, null);
			chrono.stop(getLogger());
			chrono = new Chronometer("requete " + (i++), true);
			utils.launchNewAPISearchTSUIDRequest(tsuid, startDate, endDateFull, "sum", null);
			chrono.stop(getLogger());
			chrono = new Chronometer("requete " + (i++), true);
			utils.launchNewAPISearchTSUIDRequest(bigtsuid, startDate, null, null, null);
			chrono.stop(getLogger());
			chrono = new Chronometer("requete " + (i++), true);
			utils.launchNewAPISearchTSUIDRequest(bigtsuid, startDate, null, "avg", null);
			chrono.stop(getLogger());

			endNominal(testCaseName);

		} catch (Throwable e) {
			endWithFailure(testCaseName, e);
		}

	}

	// @Test
	public void testPERFNewAPISearchParallel_withDownsampling() {

		final String testCaseName = "testPERFNewAPISearchParallel_withDownsampling";
		final boolean isNominal = true;
		try {
			start(testCaseName, isNominal);

			executor = Executors.newFixedThreadPool(10);
			for (int i = 0; i < 10; i++) {
				executor.execute(new Runnable() {
					@Override
					public void run() {
						String metrique = utils.getRandomMetric(getHost());
						try {
							launchNewAPISearchRequest(metrique);
						} catch (IkatsWebClientException e) {
							// avoid: fail() here !
							// TODO multithread logs ...
							System.out.println("Thread failed in " + testCaseName + ": execution of Runnable for metric=" + metrique );
							e.printStackTrace();
						}
					}
				});
			}
			waitForExecutor();

			endNominal(testCaseName);
		} catch (Throwable e) {
			endWithFailure(testCaseName, e);
		}
	}

	// @Test
	public void testPERFNewAPISearch() {
		
		String testCaseName = "testPERFNewAPISearch";
		boolean isNominal = true;
		try {
			start(testCaseName, isNominal);
			
			String metrique = utils.getRandomMetric(getHost());
			for (int i = 0; i < 10; i++) {
				launchNewAPISearchRequest(metrique);
			}

			endNominal(testCaseName);
		} catch (Throwable e) {
			endWithFailure(testCaseName, e);
		} 
	}

	// @Test
	public void testPERFNewAPISearchParallel() {
		
		final String testCaseName = "testPERFNewAPISearchParallel";
		final boolean isNominal = true;
		try {
			start(testCaseName, isNominal);
		 
			executor = Executors.newFixedThreadPool(10);
			for (int i = 0; i < 10; i++) {
				executor.execute(new Runnable() {
					@Override
					public void run() {
						String metrique = utils.getRandomMetric(getHost());
						try {
							launchNewAPISearchRequest(metrique);
						} catch (Exception e) {
							// avoid: fail() here !
							// TODO multithread logs ...
							System.out.println("Thread failed in " + testCaseName + ": execution of Runnable for metric=" + metrique );
							e.printStackTrace();
						}
					}
				});
			}
			waitForExecutor();

			endNominal(testCaseName);
		} catch (Throwable e) {
			endWithFailure(testCaseName, e);
		}
	}

	/**
	 * 
	 */
	protected void waitForExecutor() {
		try {
			executor.shutdown();
			boolean timeout = !executor.awaitTermination(100, TimeUnit.SECONDS);
			getLogger().info("Termination with timeout ? " + timeout);
		} catch (InterruptedException e) {
			getLogger().error("", e);
		}
	}

	// @Test
	public void testPERFNativeAPI() {
		
		String testCaseName = "testPERFNativeAPI";
		boolean isNominal = true;
		try {
			start(testCaseName, isNominal);
			
			for (int i = 0; i < 10; i++) {
				String metrique = utils.getRandomMetric(getHost());
				utils.launchNativeAPISearchRequest(metrique);
			}

			endNominal(testCaseName);
		} catch (Throwable e) {
			endWithFailure(testCaseName, e);
		}
	}

	// @Test
	public void testPERFNativeAPIParallel() {
		
		String testCaseName = "testPERFNativeAPIParallel";
		boolean isNominal = true;
		try {
			start(testCaseName, isNominal);
			
			executor = Executors.newFixedThreadPool(10);
			for (int i = 0; i < 10; i++) {
				executor.execute(new Runnable() {
					@Override
					public void run() {
						String metrique = utils.getRandomMetric(getHost());
						utils.launchNativeAPISearchRequest(metrique);
						getLogger().info("Requete traitee");
					}
				});
			}
			waitForExecutor();

			endNominal(testCaseName);
		} catch (Throwable e) {
			endWithFailure(testCaseName, e);
		}

		

	}

	public QueryResult launchNewAPISearchRequest(String metrique) throws IkatsWebClientException {
		
		String startDate = "2015/03/01-00:00:00";
		String endDate = "2015/04/01-00:00:00";
		return launchNewAPISearchRequest(metrique, startDate, endDate, null, "sum", null, null, "show_tsuids");
	}

	public QueryResult launchNewAPISearchRequest(String metrique, String startDate, String endDate, String tags,
			String aggregator, String downsampler, String downsamplerperiod, String options)
			throws IkatsWebClientException {
		return utils.launchNewAPISearchRequest(metrique, startDate, endDate, tags, aggregator, downsampler,
				downsamplerperiod, options, false);
	}

}
