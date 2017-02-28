package fr.cs.ikats.datamanager.client.opentsdb.importer;

import static org.junit.Assert.fail;

import java.text.DateFormat;
import java.text.ParseException;
import java.util.Date;

import org.junit.Test;

/**
 * test the internal DateFormat of the commonDatajsonizer.
 */
public class DateFormatTest {

    /**
     * test several input format to parse.
     */
    @Test
    public void testFormatIso() {
        DateFormat format = CommonDataJsonIzer.getDateFormat();
        try {
            Date date = null;

            date = format.parse("2012-07-04T12:00:00-0700");
            System.out.println(format.format(date));
            date = format.parse("2012-07-04T12:00:00+0200");
            System.out.println(format.format(date));
            date = format.parse("2012-07-04T12:00:00.500+0200");
            System.out.println(format.format(date));
            date = format.parse("2012-07-04T12:00:00.500+02:00");
            System.out.println(format.format(date));
            date = format.parse("2012-07-04T12:00+0200");
            System.out.println(format.format(date));
            // WITHOUT TIMEZONE
            date = format.parse("2012-07-04T12:00:00-0000");
            System.out.println(format.format(date));
            date = format.parse("2012-07-04T12:00:00.100");
            System.out.println(format.format(date));
            date = format.parse("2012-07-04T12:00:00.1");
            System.out.println(format.format(date));
            date = format.parse("2012-07-04T12:00:00");
            System.out.println(format.format(date));
        }
        catch (ParseException e) {
            e.printStackTrace();
            fail();
        }

    }

   
}
