/**
 * 
 */
package fr.cs.ikats.datamanager.client.opentsdb.importer;

import java.text.DateFormat;
import java.text.FieldPosition;
import java.text.ParseException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import fr.cs.ikats.datamanager.client.importer.IImportSerializer;
import fr.cs.ikats.datamanager.client.opentsdb.generator.ReaderConfiguration;
import fr.cs.ikats.datamanager.client.opentsdb.generator.SplittedLineReader;

/**
 * @author ikats
 *
 */
@Component
@Qualifier("EDF")
public class EDFDataJsonIzer extends AbstractDataJsonIzer {

    private static Logger logger = Logger.getLogger(EDFDataJsonIzer.class);
	
	private static SplittedLineReader edfReader = createEDFReader();

	
	public SplittedLineReader getReader() {
		return edfReader;
	}
	
	
	/* (non-Javadoc)
	 * @see java.lang.Object#clone()
	 */
	@Override
	public IImportSerializer clone()  {
		return new EDFDataJsonIzer();
	}
	/**
	 * timestamp format : 01-MAY-13 11:30:34.8
	 * dd-MMM-yy hh:mm:ss.S
	 * @return
	 */
	private static SplittedLineReader createEDFReader() {
		ReaderConfiguration configuration = new ReaderConfiguration();
		configuration.addColumnConfiguration(null, null, getDateFormat(), false, true);
		configuration.addColumnConfiguration(null, "Long", null, true, false);
		configuration.addNonReadColumnConfiguration();
		configuration.addColumnConfiguration("quality", null, null, false, false);
		SplittedLineReader reader = new SplittedLineReader(configuration);
		return reader;
	}

	
	
	private static DateFormat getDateFormat() {
        return new DateFormat() {
            
            DateFormat acceptedFormat = new SimpleDateFormat("dd-MM-yy hh:mm:ss.S");
            
            /**
             * 
             */
            private static final long serialVersionUID = -373121725779963294L;

            @Override
            public Date parse(String source, ParsePosition pos) {
                String source2 = replaceMonth(source);
                logger.info("parsing date "+source2);
                return acceptedFormat.parse(source2, pos);
            }

            /**
             * @param source the input Source
             * @return the source with month in "JAN", "FEB", "MAR","APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV","DEC"
             * replaced with its number in two digit format. 
             */
            protected String replaceMonth(String source) {
                int index = 1;
                String source2 = source;
                for(String month : Arrays.asList("JAN", "FEB", "MAR","APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV","DEC")) {
                    if(source.contains(month)) {
                        if(index<10) {
                            source2 = source2.replaceAll(month,"0"+index);
                        } else {
                            source2 = source2.replaceAll(month,Integer.toString(index));
                        }
                    }
                    index++;
                }
                return source2;
            }
            
            @Override
            public Date parse(String source) throws ParseException {
                String source2 = replaceMonth(source);
                logger.info("parsing date "+source2);
                return acceptedFormat.parse(source2);
            }
            
            @Override
            public StringBuffer format(Date date, StringBuffer toAppendTo, FieldPosition fieldPosition) {
                return acceptedFormat.format(date,toAppendTo,fieldPosition);
            }
        };
    }

}
