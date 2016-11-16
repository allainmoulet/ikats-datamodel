package fr.cs.ikats.datamanager.client.opentsdb;

/**
 * Provide an enumeration of the OpenTSDB HTTP return status codes<br>
 * See : http://opentsdb.net/docs/build/html/api_http/index.html#response-codes 
 */
public enum ApiStatus {
	// Successful codes returned from the API
	/** The request completed successfully */
	CODE_200(200), 
	/** The server has completed the request successfully but is not returning content in the body. This is primarily used for storing data points as it is not necessary to return data to caller */
	CODE_204(204), 
	/** This may be used in the event that an API call has migrated or should be forwarded to another server */
	CODE_301(301), 

	// Common error response codes
	/** Information provided by the API user, via a query string or content data, was in error or missing. This will usually include information in the error body about what parameter caused the issue. Correct the data and try again. */
	CODE_400(400), 
	/** The requested endpoint or file was not found. This is usually related to the static file endpoint. */
	CODE_404(404), 
	/** The requested verb or method was not allowed. Please see the documentation for the endpoint you are attempting to access */
	CODE_405(405), 
	/** The request could not generate a response in the format specified. For example, if you ask for a PNG file of the logs endpoing, you will get a 406 response since log entries cannot be converted to a PNG image (easily) */
	CODE_406(406), 
	/** The request has timed out. This may be due to a timeout fetching data from the underlying storage system or other issues */
	CODE_408(408), 
	/** The results returned from a query may be too large for the server's buffers to handle. This can happen if you request a lot of raw data from OpenTSDB. In such cases break your query up into smaller queries and run each individually */
	CODE_413(413), 
	/** An internal error occured within OpenTSDB. Make sure all of the systems OpenTSDB depends on are accessible and check the bug list for issues */
	CODE_500(500), 
	/** The requested feature has not been implemented yet. This may appear with formatters or when calling methods that depend on plugins */
	CODE_501(501), 
	/** A temporary overload has occurred. Check with other users/applications that are interacting with OpenTSDB and determine if you need to reduce requests or scale your system. */
	CODE_503(503);
	
	int status;

	private ApiStatus(int status) {
		this.status = status;
	}
	
	public int value() {
		return status;
	}
	
	public static boolean isError(int statusCode) {
		return isError(valueOf(statusCode));
	}
	
	public static boolean isError(ApiStatus statusCode) {
		switch (statusCode) {
			case CODE_200: 
			case CODE_204: 
			case CODE_301:
				return false;
			case CODE_400:
			case CODE_404:
			case CODE_405:
			case CODE_406:
			case CODE_408:
			case CODE_413:
			case CODE_500:
			case CODE_501:
			case CODE_503:
				return true;
		default:
		    throw new IllegalArgumentException("OpenTSDB return status '" + statusCode.status + "' unknown.");
		}
	}

	public static ApiStatus valueOf(int status) throws IllegalArgumentException {
	    for (ApiStatus value : values()) {
	         if (status == value.status) {
	        	 
	         }
	    }
	    throw new IllegalArgumentException("OpenTSDB return status '" + status + "' unknown.");
	}
}
