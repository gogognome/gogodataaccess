package nl.gogognome.dataaccess.dao;

import java.sql.SQLException;

public class NoRecordFoundException extends SQLException {

	private static final long serialVersionUID = 1L;

	public NoRecordFoundException() {
	}

	public NoRecordFoundException(String reason, String sqlState, int vendorCode, Throwable cause) {
		super(reason, sqlState, vendorCode, cause);
	}

	public NoRecordFoundException(String reason, String SQLState, int vendorCode) {
		super(reason, SQLState, vendorCode);
	}

	public NoRecordFoundException(String reason, String sqlState, Throwable cause) {
		super(reason, sqlState, cause);
	}

	public NoRecordFoundException(String reason, String SQLState) {
		super(reason, SQLState);
	}

	public NoRecordFoundException(String reason, Throwable cause) {
		super(reason, cause);
	}

	public NoRecordFoundException(String reason) {
		super(reason);
	}

	public NoRecordFoundException(Throwable cause) {
		super(cause);
	}


}
