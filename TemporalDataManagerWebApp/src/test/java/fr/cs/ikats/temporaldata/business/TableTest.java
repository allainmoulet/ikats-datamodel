package fr.cs.ikats.temporaldata.business;

import java.util.List;

import org.junit.Test;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import fr.cs.ikats.temporaldata.business.Table.DataLink;
import junit.framework.TestCase;

/**
 * JUnit tests on Table business resource
 */
public class TableTest extends TestCase {

    public final static String JSON_CONTENT_SAMPLE_1 = "{\"table_desc\":{\"title\":\"Discretized matrix\",\"desc\":\"This is a ...\"},\"headers\":{\"col\":{\"data\":[\"funcId\",\"metric\",\"min_B1\",\"max_B1\",\"min_B2\",\"max_B2\"],\"links\":null,\"default_links\":null},\"row\":{\"data\":[null,\"Flid1_VIB2\",\"Flid1_VIB3\",\"Flid1_VIB4\",\"Flid1_VIB5\"],\"default_links\":{\"type\":\"ts_bucket\",\"context\":\"processdata\"},\"links\":[null,{\"val\":\"1\"},{\"val\":\"2\"},{\"val\":\"3\"},{\"val\":\"4\"}]}},\"content\":{\"cells\":[[\"VIB2\",-50.0,12.1,1.0,3.4],[\"VIB3\",-5.0,2.1,1.0,3.4],[\"VIB4\",0.0,2.1,12.0,3.4],[\"VIB5\",0.0,2.1,1.0,3.4]]}}";
    
    @Test
    public void testLoadJSON()
    { 
        try { 
            // V3: table first version (before 15/06/17)
            String jsonContent= TableTest.JSON_CONTENT_SAMPLE_1;
            
            // Prepare expected config of ObjectMapper.
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false);
            objectMapper.configure(DeserializationFeature.ACCEPT_EMPTY_ARRAY_AS_NULL_OBJECT, false);
            objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            objectMapper.configure(SerializationFeature.WRITE_NULL_MAP_VALUES, false);
            objectMapper.configure(SerializationFeature.WRITE_EMPTY_JSON_ARRAYS,  false);
            objectMapper.setSerializationInclusion(Include.NON_NULL);
            Table testedTable = objectMapper.readValue( jsonContent, Table.class);
            System.out.println( testedTable );
            
            
             String written1 = new ObjectMapper().writeValueAsString(testedTable);
             System.out.println( "avec null: " + written1);
             String written2 = objectMapper.writeValueAsString(testedTable);
             System.out.println( "sans null: " + written2);
             System.out.println( "ref input: " + jsonContent);
            
            // non-exhaustive test
            //
            // tests TableDesc init
            assertEquals( testedTable.table_desc.title, "Discretized matrix");
            assertEquals( testedTable.table_desc.desc, "This is a ...");
            // tests TableHeaders : initialized columns
            assertEquals( testedTable.headers.col.data.size(), 6);
            assertEquals( testedTable.headers.col.data.get(1), "metric");
            assertEquals( testedTable.headers.col.links, null);
            assertEquals( testedTable.headers.col.default_links, null);
            // tests TableHeaders : initialized rows
            assertEquals( testedTable.headers.row.data.size(), 5);
            assertEquals( testedTable.headers.row.data.get(0), null);
            assertEquals( testedTable.headers.row.data.get(1), "Flid1_VIB2");
            assertEquals( testedTable.headers.row.default_links.type, "ts_bucket");
            assertEquals( testedTable.headers.row.default_links.context, "processdata");
            assertEquals( testedTable.headers.row.links.size(), 5);
            assertEquals( testedTable.headers.row.links.get(0), null);
            assertTrue( testedTable.headers.row.links.get(1) instanceof DataLink);
            assertEquals( testedTable.headers.row.links.get(1).val, "1");
            
            // tests TableContent (... just adding a few asserts)
            assertEquals( testedTable.content.cells.size(), 4 );
            for (List rowCells : testedTable.content.cells) {
                assertEquals( rowCells.size(), 5 );               
            }
            assertEquals( testedTable.content.cells.get(1).get(0), "VIB3" );
            assertEquals( testedTable.content.cells.get(1).get(1), -5.0 ); 
            assertEquals( testedTable.content.links, null);
            assertEquals( testedTable.content.default_links, null);
        }
        catch (Exception e) {
            fail("Failed test");
            e.printStackTrace();
        }
    }
    
    @Test
    public void testWriteJSON()
    { 
        try { 
            // TODO
        }
        catch (Exception e) {
            fail("Failed test");
            System.err.println(e);
            e.printStackTrace();
        }
   
    }
    //-----------------------------------------------------------------------------------------------------
    // TODO 158227 suppress comments: kept to code the sort
    //             this will be modified: for unederlying type T, for the column at sortingColumnLabel:
    //             sortByColumn( String sortingColumnLabel ) based upon a class RowComparatorByColumn<T>
    //-----------------------------------------------------------------------------------------------------
    // ... to be refactored into specific service and associated test ... :
    //
    //    static public class CompareCells implements Comparator<Object>
    //    {
    //        private static int compareNumbers(Number x, Number y) {
    //            if(isSpecial(x) || isSpecial(y))
    //                return Double.compare(x.doubleValue(), y.doubleValue());
    //            else
    //                return toBigDecimal(x).compareTo(toBigDecimal(y));
    //        }
    //        
    //        private static boolean isSpecial(Number x) {
    //            boolean specialDouble = x instanceof Double
    //                    && (Double.isNaN((Double) x) || Double.isInfinite((Double) x));
    //            boolean specialFloat = x instanceof Float
    //                    && (Float.isNaN((Float) x) || Float.isInfinite((Float) x));
    //            return specialDouble || specialFloat;
    //        }
    //
    //        private static BigDecimal toBigDecimal(Number number) {
    //            if(number instanceof BigDecimal)
    //                return (BigDecimal) number;
    //            if(number instanceof BigInteger)
    //                return new BigDecimal((BigInteger) number);
    //            if(number instanceof Byte || number instanceof Short
    //                    || number instanceof Integer || number instanceof Long)
    //                return new BigDecimal(number.longValue());
    //            if(number instanceof Float || number instanceof Double)
    //                return new BigDecimal(number.doubleValue());
    //
    //            try {
    //                return new BigDecimal(number.toString());
    //            } catch(final NumberFormatException e) {
    //                throw new RuntimeException("The given number (\"" + number + "\" of class " + number.getClass().getName() + ") does not have a parsable string representation", e);
    //            }
    //        }
    //        
    //        
    //        /**
    //         * {@inheritDoc}
    //         */
    //        @Override
    //        public int compare(Object o1, Object o2) {
    //            // TODO Auto-generated method stub            
    //            if ( ( o1 == null ) && (o2 == null ) )
    //            {
    //                return 0;
    //            }
    //            else if ((o1 == null) || (o2 == null) )
    //            {
    //                return ( o1 == null) ? -1 : 1;
    //            }
    //            // now no more null values ...
    //            
    //            if ( (o1 instanceof Number) && (o2 instanceof Number))
    //            {   
    //                BigDecimal fo1 = new BigDecimal( o1.toString() );
    //                BigDecimal fo2 = new BigDecimal( o2.toString() );
    //                return fo1.compareTo( fo2);
    //                // taken from https://stackoverflow.com/questions/2683202/comparing-the-values-of-two-generic-numbers
    //                // return compareNumbers((Number) o1, (Number) o2);
    //            }
    //            else if ((o1 instanceof String) && (o2 instanceof String))
    //            {
    //                return ((String) o1).compareTo( (String) o2);
    //            }
    //            else if ((o1 instanceof Boolean) && (o2 instanceof Boolean))
    //            {
    //                return ((Boolean) o1).compareTo( (Boolean) o2);
    //            }
    //            else
    //            {
    //                if (o1 instanceof Number)
    //                String so1=  o1.getClass().getSimpleName() + o1.toString();
    //                String so2=  o2.getClass().getSimpleName() + o2.toString();
    //                return so1.compareTo( so2 );
    //            }
    //        }
    //        
    //    }
    //    
    //    
    //    
    //    
    //    // TODO remove testSort
    //    public void testSort()
    //    {
    //        List<Object> listO = new ArrayList<Object>();
    //        listO.add("TOTO");
    //        listO.add( Boolean.TRUE );
    //
    //        listO.add( new Integer(100) );
    //        listO.add( new Float(99.99) );
    //        listO.add( new Integer(101) );
    //        listO.add( new Long(10000000) );
    //        listO.add( new Long(-10000000) );
    //        listO.add( new BigDecimal(-1000.00005) );
    //        listO.add( "101" );
    //        listO.add( Boolean.TRUE );
    //
    //        
    //        for (Object object : listO) {
    //            System.out.println( "avant : " + object);
    //        }
    //        Collections.sort( listO, new CompareCells() );
    //        
    //        for (Object object : listO) {
    //            System.out.println( "APPRES: " + object + " " + object.getClass().getSimpleName() );
    //        }
    //    }
}
