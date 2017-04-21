package fr.cs.ikats.datamanager.client.opentsdb;

// Review#147170 javadoc expliquant l'objectif et utilisation de la classe
public class ApiResponse {

	private String summary;

	private int statusCode;
	
	private Error error;
	
	/**
	 * Getter
	 * @return the summary
	 */
	public String getSummary() {
	    return summary;
	}

	/**
	 * Setter
	 * @param summary
	 *            the summary to set
	 */
	public void setSummary(String summary) {
	    this.summary = summary;
	}

	/**
	 * @return the statusCode
	 */
	public int getStatusCode() {
		return statusCode;
	}

	/**
	 * @param statusCode the statusCode to set
	 */
	protected void setStatusCode(int reponseCode) {
		this.statusCode = reponseCode;
	}
	
	/**
	 * Return an {@link Error} details for that API response
	 * @return the current error for that response
	 */
	public Error getError() {
		return error;
	}

	/**
	 * Provides an {@link Error} details for that API response
	 * @param error current error for that response
	 */
	protected void setError(Error error) {
		this.error = error;
	}

	/**
	 * Error elements returned in case of API code error response. 
	 */
	class Error {
		int code;
		String message;
		String trace;
	}

}
