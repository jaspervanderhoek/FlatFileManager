package replication.implementation;

import com.mendix.core.CoreRuntimeException;

public class NotImplementedException extends CoreRuntimeException {
	private static final long serialVersionUID = 201400707L;

	public NotImplementedException( String message ) {
		super( message );
	}
	
	public NotImplementedException( String message, Throwable t ) {
		super( message, t );
	}		
}
