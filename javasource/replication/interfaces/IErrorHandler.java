package replication.interfaces;



public interface IErrorHandler {

	public abstract boolean valueException(Exception e, String message);
	
	public abstract boolean connectionException(Exception e, String message);

	public abstract boolean queryException(Exception e, String message);
	
	public abstract boolean generalException(Exception e, String message);
	
	public abstract boolean invalidSettingsException( Exception e, String message );
	
}
