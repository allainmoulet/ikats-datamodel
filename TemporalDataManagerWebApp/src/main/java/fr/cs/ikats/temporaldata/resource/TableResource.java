package fr.cs.ikats.temporaldata.resource;

import java.io.*;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.UriInfo;

import org.apache.log4j.Logger;
import org.apache.commons.io.IOUtils;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.FormDataParam;

import fr.cs.ikats.common.dao.exception.IkatsDaoException;
import fr.cs.ikats.process.data.model.ProcessData;
import fr.cs.ikats.temporaldata.business.ProcessDataManager;
import fr.cs.ikats.temporaldata.business.ProcessDataManager.ProcessResultTypeEnum;
import fr.cs.ikats.temporaldata.exception.ResourceNotFoundException;
import fr.cs.ikats.temporaldata.utils.Chronometer;

/**
 * resource for Table
 */
@Path("table")
public class TableResource extends AbstractResource {

    private static Logger logger = Logger.getLogger(TableResource.class);

    /**
     * ProcessManager
     */
    private ProcessDataManager processDataManager;

    /**
     * init the processDataManager
     */
    public TableResource() {
        processDataManager = new ProcessDataManager();
    }
    // Review#156259:MBD: javadoc: manque le throws IkatsDaoException
    /**
     * get the result as an attachement file in the response. media type will
     * depends on the dataType of the result.
     *
     * @param tableName the name of the table to retrieve
     * @return a Response with content-type depending on the processData type
     * @throws ResourceNotFoundException if result is null
     * @throws SQLException              if Blob cannot be read
     */
    @GET
    @Path("/{tableName}")
    public Response downloadTable(@PathParam("tableName") String tableName) throws ResourceNotFoundException, SQLException, IkatsDaoException {

        // get id of table in processData db
        // assuming there is only one table by tableName
        
        // Review#156259:MBD: risque de lever une exception sans message clair ni code d'erreur valide
        // si la processdata n'est pas trouvée; ou si hibernate KO
        // 
        //     La javadoc est fausse (ProcessDataDAO et par recopie du @return: ProcessDataFacade; ProcessDataManager) 
        //     Vu ensemble => dans ce cas ca vaut le coup de mettre a jour la javadoc pour getProcessData ...
        //         - requete hibernate ok mais pas de resultat => retourne liste vide (non nulle)
        //         - requete KO (ex: driver KO, BD arretee ...)=> retourne null
        // 
        // Puis ci-dessous etre robuste aux 2 cas: liste vide et liste null => ResourceNotFoundException 
        //    - il faudrait d'abord processDataManager.getProcessData(tableName)
        //    avant d'evaluer le premier element : eviter  NullPointerException et IndexOutOfBoundsException
        
        ProcessData dataTable = processDataManager.getProcessData(tableName).get(0);

        if (dataTable == null) {
            throw new ResourceNotFoundException("No result found for tableName " + tableName);
        }

        try {

            logger.info(dataTable.toString());

            String fileName = dataTable.getName();
            ResponseBuilder responseBuilder;
            
            // Review#156259:MBD: ligne trop longue: refactoring avec des variables intermediaires
            //  souhaitable pour lisibilité et maintenance
            responseBuilder = Response.ok(getOut(dataTable.getData().getBytes(1, (int) dataTable.getData().length()))).header("Content-Disposition",
                    "attachment;filename=" + fileName);
            if (dataTable.getDataType().equals(ProcessResultTypeEnum.CSV.toString())) {
                responseBuilder.header("Content-Type", "application/ms-excel");
            }

            return responseBuilder.build();

        } catch (SQLException sqle) {
            // Review#156259:MBD: rajouter le tableName dans le message
            logger.error("Failed: service downloadProcessData(): caught SQLException:", sqle);
            // Review#156259:MBD: pour info [Exception handlers]:
            // malheureusement les handlers actuels traitent mal les exceptions par defaut:
            // => cette classe etend Exception et non IkatsExceptionDA => redirigée vers ApplicationExceptionHandler
            //    => par defaut retourne une reponse BAD_REQUEST au lieu de SERVER_ERROR
            //       c'est pas un pb introduit par ton code ... mais devrait etre corrigé par FT
            //       FTL propose d'attendre un peu sur la correction des handlers
            throw sqle;
        } catch (Throwable e) {
            // Review#156259:MBD: rajouter le tableName dans le message
            IkatsDaoException ierror = new IkatsDaoException("Failed: service downloadProcessData(): caught unexpected Throwable:", e);
            logger.error(ierror);
            
            // Review#156259:MBD: pour info: idem remarque precedente [Exception handlers]
            // (non répété dans le reste du code)
            throw ierror;
        }

    }

    // Review#156259:MBD: la javadoc a corriger pour coller au code: param rowName, et exception
    /**
     * Review#156259:MBD: in english
     * lance l'import d'une table de type fichier csv.
     *
     * @param tableName       name of the table
     * @param fileis          the file input stream
     * @param fileDisposition information about the Multipart with file
     * @param formData        the form data
     * @param uriInfo         all info on URI
     * @return the internal id
     */
    @POST
    @Path("/{tableName}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response importTable(@PathParam("tableName") String tableName, @FormDataParam("file") InputStream fileis,
                                @FormDataParam("file") FormDataContentDisposition fileDisposition, @FormDataParam("rowName") String rowName,
                                FormDataMultiPart formData, @Context UriInfo uriInfo) throws IOException {
        
       
        Chronometer chrono = new Chronometer(uriInfo.getPath(), true);
        String fileName = fileDisposition.getFileName();
        logger.info("Import csv file : " + fileName);
        logger.info("Table Name : " + tableName);
        Map<String, List<FormDataBodyPart>> params = formData.getFields();
        Long fileSize = fileDisposition.getSize();

        // duplicate inputstream for both parsing and storing in db
        byte[] byteArray = IOUtils.toByteArray(fileis);
        InputStream fileis1 = new ByteArrayInputStream(byteArray);
        // Review#156259:MBD: rapprocher la var fileis2 de son utilisation
        InputStream fileis2 = new ByteArrayInputStream(byteArray);

        // default value to file
        String fileType = "file";
        for (String key : params.keySet()) {
            switch (key) {
                case "fileType":
                    fileType = params.get(key).get(0).getValue();
                    break;
                case "fileSize":
                    fileSize = Long.parseLong((params.get(key).get(0).getValue()));
            }
        }

        BufferedReader reader = new BufferedReader(new InputStreamReader(fileis1));
        // Review#156259:MBD: le separateur est a priori une virgule: est ce fixé par MAM ?
        //                    Si oui cela simplifie. Sinon en faire un parametre
        //
        String separator = ",";
        Integer rowIndexId = -1;
        String[] columnHeaders;
        try {
            // consume header to retrieve column index of unique identifier in the table
            columnHeaders = reader.readLine().split(separator);
            for (int i = 0; i < columnHeaders.length; i++) {
                if (!columnHeaders[i].isEmpty() && columnHeaders[i].equals(rowName)) {
                    rowIndexId = i;
                    break;
                }
            }

            if (rowIndexId == -1){
                String context = "Row name not found in csv file header : "+ rowName;
                logger.error(context);
                return Response.status(Response.Status.BAD_REQUEST).entity(context).build();
            }

            HashMap<String, String> keyTableMap = new HashMap<>();

            // parse csv content to fix duplicates : we stop at first duplicate and send back 409 http code
            String line = reader.readLine();
            while (line != null) {
                String[] items = line.split(separator);
                String idRef = items[rowIndexId];
                if (!keyTableMap.containsKey(idRef)) {
                    keyTableMap.put(idRef, "NA");
                } else {
                    String context = "Duplicate found in csv file: " + idRef;
                    logger.error(context);
                    return Response.status(Response.Status.BAD_REQUEST).entity(context).build();
                }
                line = reader.readLine();
            }
        } catch (IOException e) {
            String contextError = "Unexpected interruption while parsing CSV file";
            logger.error(contextError);
            throw new IOException(contextError, e);
        }

        // Review#156259:MBD: afin d'etre robuste aux erreurs DAO: encadrer le code final par try ... catch(Throwable) ... 
        // permettrait de retourner une reponse INTERNAL_SERVER_ERROR plus parlante
        //
        //     - cas 1/ sur une erreur serveur/bd/hibernate: getProcessData pourrait retourner null
        //       => intercepter NullPointerException avec getProcessData(tableName).size()
        //     - cas 2/ en cas d'erreur d'import de PreocessDataDAO:  processDataManager.importProcessData 
        //              va lever un NullPointerException
        //       => intercepter NullPointerException et encoder le retour
        //       try {
        //            ...
        //        }
        //        catch (Throwable e) {
        //            String context = "Server error handling Table " + tableName + "(detailed in the server logs)";
        //            logger.error(context, e);
        //            chrono.stop(logger);
        //            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(context).build();
        //        }
        
        // check that tableName does not already exist
        if (processDataManager.getProcessData(tableName).size() == 0) {
            // Review#156259:MBD: pour info 
            //    
            //    => cas a gerer
            processDataManager.importProcessData(fileis2, fileSize, tableName, fileType, fileName);
            logger.info("Table stored Ok in db : " + tableName);
            chrono.stop(logger);
            return Response.status(Response.Status.OK).build();
        } else {
            String context = "Table name already exists : " + tableName;
            logger.error(context);
            chrono.stop(logger);
            return Response.status(Response.Status.CONFLICT).entity(context).build();
        }
        
    }

    /**
     * return a new Instance of OutputStreaming, used for streaming out the csv
     * file
     *
     * @param bs the bytes to write
     * @return
     */
    private StreamingOutput getOut(final byte[] bs) {
        return new StreamingOutput() {
            @Override
            public void write(OutputStream out) throws IOException, WebApplicationException {
                out.write(bs);
            }
        };
    }

}
