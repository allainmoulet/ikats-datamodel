package fr.cs.ikats.temporaldata.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.log4j.Logger;

/**
 * Class to merge files into one.
 */
public class FileMerger {

    private final Logger logger = Logger.getLogger(FileMerger.class);

    /**
     * merge files in directory 
     * @param metric name of the metric
     * @param directory the input directory
     * @param targetDirectory the target directory
     * @throws IOException if files cannot be read or written
     */
    public void mergeFiles(String metric, String directory,String targetDirectory) throws IOException {

        File rootDir = new File(directory);

        if (rootDir.exists()) {
            File targetFile = new File(targetDirectory+"/"+"raw_0.csv");
            if (targetFile.exists()) {
                targetFile.delete();
            }
            BufferedWriter writer = new BufferedWriter(new FileWriter(targetFile));
            int count = 0;
            try {
                writer.write("TIMESTAMP;" + metric);
                writer.newLine();
                for (File file : rootDir.listFiles()) {

                    BufferedReader reader = new BufferedReader(new FileReader(file));
                    // read the first line
                    try {
                        String line = reader.readLine();
                        while ((line = reader.readLine()) != null) {
                            writer.write(line);
                            writer.newLine();
                            count++;
                        }
                    }
                    finally {
                        reader.close();
                    }
                }
            }
            finally {
                writer.close();
            }
            logger.info(targetFile.getAbsolutePath()+" File written with "+count+" lines");
        }
        else {
            logger.warn("Directory " + directory + " not found or not readable");
        }
    }
    
    
    

}
