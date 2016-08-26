package fr.cs.ikats.temporaldata.chart;


import java.awt.Color;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.swing.JPanel;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.SystemConfiguration;
import org.apache.log4j.Logger;
import org.glassfish.grizzly.http.server.HttpServer;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.StandardChartTheme;
import org.jfree.chart.axis.DateAxis;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYItemRenderer;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.time.TimeSeriesCollection;
import org.jfree.data.xy.XYDataset;
import org.jfree.ui.ApplicationFrame;
import org.jfree.ui.RectangleInsets;
import org.jfree.ui.RefineryUtilities;

import fr.cs.ikats.datamanager.client.opentsdb.QueryResult;
import fr.cs.ikats.temporaldata.AbstractRequestTest;
import fr.cs.ikats.temporaldata.ServerMain;
import fr.cs.ikats.temporaldata.TestUtils;
import fr.cs.ikats.temporaldata.application.ApplicationConfiguration;

/**
 * An example of a time series chart.  For the most part, default settings are
 * used, except that the renderer is modified to show filled shapes (as well as
 * lines) at each data point.
 */
public class JFreeChartMain extends ApplicationFrame {

    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(JFreeChartMain.class);


    static {
        // set a theme using the new shadow generator feature available in
        // 1.0.14 - for backwards compatibility it is not enabled by default
        ChartFactory.setChartTheme(new StandardChartTheme("JFree/Shadow"));
    }

    /**
     * A demonstration application showing how to create a simple time series
     * chart.  This example uses monthly data.
     *
     * @param title  the frame title.
     */
    public JFreeChartMain(String title) {
        super(title);
        ChartPanel chartPanel = (ChartPanel) createDemoPanel();
        chartPanel.setPreferredSize(new java.awt.Dimension(500, 270));
        setContentPane(chartPanel);
    }

    /**
     * Creates a chart.
     *
     * @param dataset  a dataset.
     *
     * @return A chart.
     */
    private static JFreeChart createChart(XYDataset dataset) {

        JFreeChart chart = ChartFactory.createTimeSeriesChart(
            "IKATS Time Series",  // title
            "Date",             // x-axis label
            "Sensor value",   // y-axis label
            dataset,            // data
            true,               // create legend?
            true,               // generate tooltips?
            false               // generate URLs?
        );


        chart.setBackgroundPaint(Color.white);
        
        XYPlot plot = (XYPlot) chart.getPlot();
        plot.setBackgroundPaint(Color.lightGray);
        plot.setDomainGridlinePaint(Color.white);
        plot.setRangeGridlinePaint(Color.white);
        plot.setAxisOffset(new RectangleInsets(5.0, 5.0, 5.0, 5.0));
        plot.setDomainCrosshairVisible(true);
        plot.setRangeCrosshairVisible(true);

        XYItemRenderer r = plot.getRenderer();
        if (r instanceof XYLineAndShapeRenderer) {
            XYLineAndShapeRenderer renderer = (XYLineAndShapeRenderer) r;
            renderer.setBaseShapesVisible(false);
            renderer.setBaseShapesFilled(false);
            renderer.setDrawSeriesLineAsPath(false);
        }

        DateAxis axis = (DateAxis) plot.getDomainAxis();
        axis.setDateFormatOverride(new SimpleDateFormat("dd-MMM-yyyy'T'HH:mm:ss"));

        return chart;

    }

    /**
     * Creates a dataset, consisting of two series of monthly data.
     *
     * @return The dataset.
     * @throws org.apache.commons.configuration.ConfigurationException 
     */
    private static XYDataset createDataset() {
        // init test configuration
        AbstractRequestTest.setUpBeforClass( JFreeChartMain.class.getSimpleName() );
        String propertiesFile = "test.properties";
        CompositeConfiguration testConfig = new CompositeConfiguration();
        testConfig.addConfiguration(new SystemConfiguration());
        try {
            testConfig.addConfiguration(new PropertiesConfiguration(propertiesFile));
        }
        catch (ConfigurationException e) {
            LOGGER.error("Error loading properties file " + propertiesFile);
        }        
        TestUtils utils = new TestUtils();
        HttpServer server = null;
        if(testConfig.getBoolean("useGrizzlyServer")) {
            server = ServerMain.startServer(testConfig.getString("testAPIURL"));
        }
        TimeSeriesCollection dataset = new TimeSeriesCollection();
        try {
            System.out.println(String.format(
                    "Jersey app started with WADL available at "
                            + "%sapplication.wadl\nHit enter to stop it...",
                    testConfig.getString("testAPIURL")));
            ApplicationConfiguration config = new ApplicationConfiguration();
            String host =   config.getStringValue(ApplicationConfiguration.HOST_DB_API);
            
            String metrique = utils.getMetric(host, 0);
            String startDate = "2015/03/11-08:00:00";
            String endDate1 = "2015/03/11-09:00:00";
            String endDate2 = "2015/03/11-10:00:00";
            String endDate3 = "2015/03/11-11:00:00";
            String endDate4 = "2015/03/11-15:00:00";
            String endDateFull = "2015/04/14-04:00:00";
            
            String startDateAirbus = "2012/07/01-00:00:00";
            String stopDateAirbus = "2013/04/01-00:00:00";          
            
            dataset = new TimeSeriesCollection();
            Date date = null;
            try {
                // 2015/03/11-08:00:00
                date = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss").parse(startDateAirbus);
            } catch (ParseException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            
            ChartUtils chartUtils = new ChartUtils();
            
//          QueryResult resultat = utils.launchNewAPISearchRequest(metrique,startDate,endDate_full,"%7Bnumero=00001%7D","sum","avg","1m","show_tsuids&ms=true",true);
//          
//          chartUtils.addResultatRequeteToTSCollection(resultat, dataset, date);
            
            //QueryResult resultat2 = utils.launchNewAPISearchRequest(metrique,startDate,endDate_full,"%7Bnumero=00001%7D","sum",null,null,"show_tsuids&ms=true",false);
            //QueryResult resultat2 = utils.launchNewAPISearchRequest(metrique,startDate,endDate_full,null,"sum",null,null,"show_tsuids&ms=true",false);
//          QueryResult resultat2 = utils.launchNewAPISearchRequest("WS6",startDateAirbus,stopDateAirbus,null,"sum",null,null,"show_tsuids&ms=true",false);
//          
//          chartUtils.addResultatRequeteToTSCollection(resultat2, dataset, date);
            
            QueryResult resultat3 = utils.launchNewAPISearchRequest("WS7",startDateAirbus,stopDateAirbus,null,"sum","avg","1h","show_tsuids&ms=true",true);
            
            chartUtils.addResultatRequeteToTSCollection(resultat3, dataset, date);
            
        } catch(Exception e) {
            e.printStackTrace();
        } finally {
            if(testConfig.getBoolean("useGrizzlyServer")) {
                server.shutdownNow();
            }
        }
        return dataset;

    }
    

    /**
     * Creates a panel for the demo (used by SuperDemo.java).
     *
     * @return A panel.
     */
    public static JPanel createDemoPanel() {
        JFreeChart chart = createChart(createDataset());
        ChartPanel panel = new ChartPanel(chart);
        panel.setFillZoomRectangle(true);
        panel.setMouseWheelEnabled(true);
       return panel;
    }

    /**
     * Starting point for the demonstration application.
     *
     * @param args  ignored.
     */
    public static void main(String[] args) {

        JFreeChartMain demo = new JFreeChartMain(
                "Time Series Chart Demo 1");
        demo.pack();
        RefineryUtilities.centerFrameOnScreen(demo);
        demo.setVisible(true);

    }

}
