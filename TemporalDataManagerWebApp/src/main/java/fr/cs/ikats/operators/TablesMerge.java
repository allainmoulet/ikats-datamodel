package fr.cs.ikats.operators;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import fr.cs.ikats.temporaldata.business.Table;
import fr.cs.ikats.temporaldata.business.TableElement;
import fr.cs.ikats.temporaldata.business.TableInfo;
import fr.cs.ikats.temporaldata.business.TableInfo.DataLink;
import fr.cs.ikats.temporaldata.business.TableInfo.Header;
import fr.cs.ikats.temporaldata.business.TableManager;
import fr.cs.ikats.temporaldata.exception.IkatsException;
import fr.cs.ikats.temporaldata.exception.ResourceNotFoundException;

/**
 * IKATS Operator Tables Merge<br>
 * 
 * Provides
 * 
 */
public class TablesMerge {

    static private final Logger logger = Logger.getLogger(TablesMerge.class);

    /**
     * The JSON Request
     * 
     * @author ftoral
     *
     */
    public static class Request {

        public TableInfo[] tables;
        public String      joinOn;
        public String      outputTableName;

        public Request() {
            ; // default constructor
        }
    }

    private Request      request;
    private TableManager tableManager;

    /**
     * Table Merge operator initialization
     * 
     * @param request the input data provided to the operator
     * @throws IkatsOperatorException
     */
    public TablesMerge(Request request) throws IkatsOperatorException {

        // check the inputs
        if (request.tables.length < 2) {
            throw new IkatsOperatorException("There should be 2 tables for a merge");
        }

        this.request = request;
        this.tableManager = new TableManager();
    }

    /**
     * Operator processing for the merge
     * 
     * @return the merged table
     * @throws IkatsOperatorException
     */
    public Table doMerge() throws IkatsOperatorException {

        // Normalize the request data
        Table firstTable = tableManager.initTable(this.request.tables[0], false);
        Table secondTable = tableManager.initTable(this.request.tables[1], false);
        String joinKey = (request.joinOn != null && !request.joinOn.isEmpty()) ? request.joinOn : null;

        // -- Get the left join column header name and index for the first table
        int joinIndexOnFirstTable = -1;
        if (joinKey == null) {
            joinIndexOnFirstTable = 0;
            // case of the non provided join key => get the first column in the
            // first table
            try {
                if (firstTable.getColumnsHeader() != null) {
                    joinKey = firstTable.getColumnsHeader().getItems().get(0);
                }
            }
            catch (IkatsException e) {
                throw new IkatsOperatorException("The table '" + firstTable.getName() + "' has no column", e);
            }
        }
        else {
            try {
                joinIndexOnFirstTable = firstTable.getIndexColumnHeader(joinKey);
            }
            catch (IkatsException e) {
                throw new IkatsOperatorException("Join column '" + joinKey + "' not found in table '" + firstTable.getName() + "'. Additional info: " + e.getMessage(), e);
            }
        }

        // -- Verify that the join column is in the second, if found :
        // - store the index
        // - point the to the column values
        boolean joinFound = false;
        int joinIndexInSecondTable = 0; // default: use the first column for
                                        // the join
        List<String> columnValues = null;

        // -- find join in the table and get the index of the row to merge
        if (joinKey != null) {
            // case where we have a join key -> (try to) find the corresponding
            // column in the current table
            try {
                joinIndexInSecondTable = secondTable.getIndexColumnHeader(joinKey);
                joinFound = true;
            }
            catch (IkatsException e) {
                // Exception is synomim of nof found
                // FULL INNER JOIN could not be realized
                throw new IkatsOperatorException("Join column not found in the second table");
            }
        }

        try {
            columnValues = secondTable.getColumn(joinIndexInSecondTable, String.class);
        }
        catch (IkatsException | ResourceNotFoundException e) {
            logger.error("Can't get the column data at index " + joinIndexInSecondTable + " for table " + secondTable.getName());
            joinFound = false;
        }

        // -- Initialize the result/merged table
        boolean withColHeaders = firstTable.isHandlingColumnsHeader() | secondTable.isHandlingColumnsHeader();
        boolean withRowHeaders = firstTable.isHandlingRowsHeader() | secondTable.isHandlingRowsHeader();

        Table resultTable = tableManager.initEmptyTable(withColHeaders, withRowHeaders);
        resultTable.setName(request.outputTableName);
        resultTable.enableLinks(withColHeaders, new DataLink(), withRowHeaders, new DataLink(), true, new DataLink());

        // -- Set the result table columns header from the first and second
        // table
        if (withColHeaders) {
            reportTableColumsHeader(firstTable, resultTable, -1);
            reportTableColumsHeader(secondTable, resultTable, joinIndexInSecondTable);
        }

        // -- Loop over the values in the join column of the first table to
        // found matching keys in the second
        int firstRow = firstTable.isHandlingColumnsHeader() ? 1 : 0;
        int rowCount = firstTable.getRowCount(firstTable.isHandlingColumnsHeader());
        for (int i = firstRow; i < rowCount; i++) {

            // -- get the join value of the row in the first table
            List<TableElement> firstTableRowData = null;
            // first : try to get the row with link
            try {
                firstTableRowData = firstTable.getRow(i, TableElement.class);
            }
            catch (IkatsException | ResourceNotFoundException e) {
                logger.error("Can't get the row at index " + i + " for table " + firstTable.getName());
                throw new IkatsOperatorException("Can't get the row at index " + i + " for table " + firstTable.getName(), e);
            }
            String joinValue = firstTableRowData.get(joinIndexOnFirstTable).data.toString();

            // -- find the join value in the join column and get the row index
            // of the second table
            int rowIndexForMerge = -1;
            joinFound = false;
            for (int k = 0; k < columnValues.size(); k++) {
                if (joinValue.equals(columnValues.get(k))) {
                    if (secondTable.isHandlingColumnsHeader()) {
                        rowIndexForMerge = k + 1;
                    }
                    else {
                        rowIndexForMerge = k;
                    }
                    joinFound = true;
                    break;
                }
            }

            if (!joinFound) {
                // FULL INNER JOIN could not be realized -> no matching value
                // for that row
                break;
            }
            // ELSE -> we could merge

            // append the row values to the firstTableRowData
            try {
                // Get the row
                List<TableElement> secondTableRowData = secondTable.getRow(rowIndexForMerge, TableElement.class);
                // loop over the row values to add them to the firstTable
                for (int j = 0; j < secondTableRowData.size(); j++) {
                    if (j == joinIndexInSecondTable) {
                        // skip the join value
                        continue;
                    }
                    firstTableRowData.add(secondTableRowData.get(j));
                }

                // finally append the new row
                resultTable.appendRow(firstTableRowData);
            }
            catch (IkatsException | ResourceNotFoundException e) {
                logger.error("Can't get the row at index " + rowIndexForMerge + " for table " + secondTable.getName());
                throw new IkatsOperatorException("Can't get the row at index " + rowIndexForMerge + " for table " + secondTable.getName(), e);
            }

        }

        return resultTable;
    }

    /**
     * Fill the columns header for the result table
     * 
     * @param fromTable the table from where header should be copied
     * @param resultTable the result table where to fill the header
     * @param skipColumnIndex index of the column to skip (case of the join
     *            column), put -1 for not skipping any column
     * @throws IkatsOperatorException In case the operation could not complete
     */
    private void reportTableColumsHeader(Table fromTable, Table resultTable, int skipColumnIndex) throws IkatsOperatorException {

        Header resultHeader = resultTable.getColumnsHeader();
        int numberOfColumnsHeaders = fromTable.getColumnCount(true);

        if (fromTable.isHandlingColumnsHeader()) {
            // Report columns header
            List<TableElement> colHeaderElements = null;

            try {
                // Try to get header with links
                colHeaderElements = fromTable.getColumnsHeader().getDataWithLink();
            }
            catch (IkatsException e1) {
                // Else manage header with no links
                colHeaderElements = new ArrayList<TableElement>();
                try {
                    // An a new element to the
                    for (String stringHeader : fromTable.getColumnsHeader().getItems()) {
                        colHeaderElements.add(new TableElement(stringHeader, null));
                    }
                }
                catch (IkatsException e) {
                    throw new IkatsOperatorException("Could not get columns header items from table " + fromTable.getName(), e);
                }
            }

            for (int i = 0; i < numberOfColumnsHeaders; i++) {
                if (i == skipColumnIndex) {
                    // skip that column header
                    continue;
                }

                try {
                    TableElement headerElement = colHeaderElements.get(i);
                    resultHeader.addItem(headerElement.data, headerElement.link);
                }
                catch (IkatsException e) {
                    logger.error("Should not be raised", e);
                    throw new IkatsOperatorException("Could not merge", e);
                }
            }
        }
        else {
            // or fill with empty header if the first table do not have headers
            if (skipColumnIndex != -1) {
                numberOfColumnsHeaders -= 1;
            }

            for (int i = 0; i < numberOfColumnsHeaders; i++) {
                try {
                    resultHeader.addItems((Object) null);
                    // cast trick to avoid compiler warning
                }
                catch (IkatsException e) {
                    logger.error("Should not be raised", e);
                    throw new IkatsOperatorException("Could not merge", e);
                }
            }
        }

        // Fianlly, reset the result table columns header with the merged columns headers
        resultTable.setColumnsHeader(resultHeader);
    }

}
