package fr.cs.ikats.datamanager.client.opentsdb.importer;

import java.text.DateFormat;
import java.text.FieldPosition;
import java.text.ParsePosition;
import java.util.Date;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import fr.cs.ikats.datamanager.client.importer.IImportSerializer;
import fr.cs.ikats.datamanager.client.opentsdb.generator.ReaderConfiguration;
import fr.cs.ikats.datamanager.client.opentsdb.generator.SplittedLineReader;

@Component
@Qualifier("Airbus")
public class AirbusDataJsonIzer extends AbstractDataJsonIzer {
	
	private static SplittedLineReader airbusReader = createAirbusReader();
	
	public SplittedLineReader getReader() {
		return airbusReader;
	}
	

	private static SplittedLineReader createAirbusReader() {
		ReaderConfiguration configuration = new ReaderConfiguration();
		configuration.addColumnConfiguration(null, null, getDateFormat(), false, true);
		configuration.addColumnConfiguration(null, "Long", null, true, false);
		SplittedLineReader reader = new SplittedLineReader(configuration);
		return reader;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#clone()
	 */
	@Override
	public IImportSerializer clone()  {
		return new AirbusDataJsonIzer();
	}
	
	private static DateFormat getDateFormat() {
	    return new DateFormat() {
            
            /**
             * 
             */
            private static final long serialVersionUID = -373121725779963294L;

            @Override
            public Date parse(String source, ParsePosition pos) {
                return parse(source);
            }
            
            @Override
            public Date parse(String source) {
                Double doubleValue = new Double(source)/1000d;
                return new Date(doubleValue.longValue());
            }
            
            @Override
            public StringBuffer format(Date date, StringBuffer toAppendTo, FieldPosition fieldPosition) {
                toAppendTo.append(Long.toString(date.getTime()));
                return toAppendTo;
            }
        };
	}

}
