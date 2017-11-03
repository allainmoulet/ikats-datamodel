/**
 * LICENSE:
 * --------
 * Copyright 2017 CS SYSTEMES D'INFORMATION
 * 
 * Licensed to CS SYSTEMES D'INFORMATION under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. CS licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 * @author Fabien TORAL <fabien.toral@c-s.fr>
 * @author Fabien TORTORA <fabien.tortora@c-s.fr>
 * 
 */

package fr.cs.ikats.client.temporaldata.importer;

import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;

/**
 * Main executable Class
 */
public class AirbusMainClient {

    /**
     * private static logger.
     */
    private static Logger logger = Logger.getLogger(AirbusMainClient.class);

    
    /**
     * Main method: import in DB the Airbus data.
     *  
     * Main arguments from args param are:
     * <ul>
     * <li>args[0]: required: the dataset name, which defines
     *    <ul> 
     *    <li>the foldername under the root path (defined by args[1]).</li>
     *    <li>the dataset name created in DB</li>
     *    </ul>
     * </li>
     * <li>args[1]: required: the root path: folder.</li>
     * <li>args[2]: optional: 
     *    <ul>
     *         <li>"true" for the LOG_ONLY mode, deactivating DB writing.</li>
     *         <li>"false" : for the REAL mode: activating the DB writing</li>
     *         <li>missing: the main starts with LOG_ONLY mode, displaying logs, 
     *             and then ask the user to confirm the next step,
     *             the import in REAL mode.
     *         </li>
     *    </ul>
     * </li>
     * </ul>
     *  
     * @param args main arguments
     */
    public static void main(String[] args) {
        //System.out.println("Arguments size : "+args.length);
        int codeExit=0;
        if(args.length<2 || args.length>3) {
            usage();
        } else {
            logger.info( "Starting AirbusMainClient::main ..." );
            for (int i = 0; i < args.length; i++) {
                logger.info( "with arg[" + i + "] == " + args[i] );
            }
            AirbusClientConfiguration config = new AirbusClientConfiguration();
            AirbusClient client = new AirbusClient(config);
            
            boolean logOnly = (args.length==3) ? Boolean.parseBoolean(args[2]) : false;
            boolean explicitLogOnlyToFalse = (args.length==3 && logOnly == false);
            boolean simulateImportFirstly=(args.length==2) || logOnly;
            
            try {
                // import the resulting dataset
                // - if simulateImportFirstly is true: just simulate the import before all ... 
                //   => only log the import entries
                List<String> tsuidList = client.importFullDirectory(args[0], args[1], simulateImportFirstly);
            
                logger.info( ( simulateImportFirstly ? "Logged list" : "Tsuid list" ) + " size: " + tsuidList.size() );
                if( (tsuidList.isEmpty() == false) && (logOnly == false) ) {
                    // there are some TS imported, check if user has  
                    if (explicitLogOnlyToFalse == false) {
                        System.out.println(String.format("if you wan't to continue, Press Enter, Else Ctrl-x"));
                        try {
                            System.in.read();
                            
                        }
                        catch (IOException ioe) {
                            logger.debug("Error while reading user input", ioe);
                        }
                        try {
                            // avoid blocked main
                            System.in.close();
                        }
                        catch (Exception e) {  
                            logger.warn("Failure on System.in.close(): exception not raised: ", e);
                        }
                    }
                    if ( simulateImportFirstly )
                    {
                        // secondly : really import when it was firstly simulated 
                        tsuidList = client.importFullDirectory(args[0], args[1], false);
                        logger.info( "Tsuid list size: " + tsuidList.size() );
                    
	                    // import the resulting dataset
	                    if(config.getStringValue("dataset.creation.mode").equals("create")) {
	                        logger.info( "Create dataSet, with creation mode: create ..." );
	                        client.importDataSet(args[0], tsuidList,false);
	                    } else if(config.getStringValue("dataset.creation.mode").equals("update")) {
	                        logger.info( "Create dataSet, with creation mode: update ..." );
	                        client.importDataSet(args[0], tsuidList,true);
	                    } else if(config.getStringValue("dataset.creation.mode").equals("none")) {
	                        logger.info( "DataSet creation mode is None => skipping dataset creation/update" );
	                    }
                    }
                }
                logger.info("... AirbusMainClient::main ended");
            }
            catch(Exception e) {
                logger.error("... AirbusMainClient::main ended with Exception: ", e); 
                codeExit=1;
            }
            catch(Error err) {
                logger.error("... AirbusMainClient::main ended with Error: ", err);
                codeExit=1;
            }
            finally { 
                client.stop(); 
            }
        }
        System.exit(codeExit);
    }

    /**
     * print usage
     */
    private static void usage() {
        System.out.println("Usage : startup.sh datasetName rootPath [logOnly]");
        System.out.println("--------------");
        System.out.println("  parameters :");
        System.out.println(" ");
        System.out.println("  - [datasetName] : name of the imported dataset.");
        System.out.println("    all imported timeseries found in rootPath will be included into this dataset");
        System.out.println(" ");
        System.out.println("  - [rootPath] : root directory from where to search Time Series csv files.");
        System.out.println("    Relative directory will be scanned to extract metadata and build metrics and tags.");
        System.out.println("    This directory must contains the datasetName subdirectory");
        System.out.println(" ");
        System.out.println("  - [logOnly] (optional) : True or False, use to indicate a fake import,");
        System.out.println("    if True, logging all the file and metadata extracted from the path.");
        System.out.println("    if False, do the import without logging anything before.");
        System.out.println("    if ommited, information is logged and with a confirmation ([Enter]),");
        System.out.println("       import is done or cancel ([Ctrl-x]).");
        System.out.println(" ");
        System.out.println("END ");
    }

}

