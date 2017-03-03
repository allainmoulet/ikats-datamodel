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

// Review#147170 globalement un peu plus de javadoc ... classe + methodes publiques
/**
 * @author ikats
 *
 */

// Review#147170 commentaire explicatif sur le @Qualifier et  @Component ? ingestion spring toujours utilisée ? 
// Review#147170 il me semble que tu utilises le classloader and AbstractImportTaskFactory ... peut on les oter ?

// Review#147170 comment added

// Keep spring annotations @Component @Qualifier until TemporalDataManager 'import' services require them:
// TODO annotations to be removed once these services are suppressed from TemporalDataManager
@Component
@Qualifier("EDF")
public class EDFDataJsonIzer extends AbstractDataJsonIzer {

    private static Logger logger = Logger.getLogger(EDFDataJsonIzer.class);
	
	private static SplittedLineReader edfReader = createEDFReader();

	
	public SplittedLineReader getReader() {
		return edfReader;
	}
	
	// Review#147170 pourquoi non-Javadoc ? => javadoc
	/** 
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
                // Review#147170 info -> debug ?
                logger.info("parsing date "+source2);
                return acceptedFormat.parse(source2, pos);
            }

            // Review#147170
            /**
             * From the source content, replaces the literal months 
             * (resp. "JAN", "FEB", "MAR","APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV","DEC") 
             * by their number in two digit format ( resp. "01", ..., "12").
             * @param source the input Source
             * @return the computed string with month literal replaced by the 2-digit month number. 
             */
            protected String replaceMonth(String source) {
                int index = 1;
                
                // Review#147170 OPTIM: une optim sur ce code serait intéressante car on va très fréquemment l'utiliser ... 
                // Review#147170  - utiliser un tableau constant -final static- (ou mieux enum? ou regexp Pattern ... voir plus bas) plutot que
                // Review#147170 Arrays.asList("JAN", "FEB", "MAR","APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV","DEC") dans la boucle!
                // Review#147170  - sortir du for des que la substitution a eu lieu
                // Review#147170  - http://stackoverflow.com/questions/6262397/string-replaceall-is-considerably-slower-than-doing-the-job-yourself
                // Review#147170 (je sais ... optim peut etre negligeble par rapport au services opentsdb ... )
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
