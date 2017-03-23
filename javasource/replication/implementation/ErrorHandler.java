package replication.implementation;

import replication.interfaces.IErrorHandler;


public class ErrorHandler implements IErrorHandler {

	@Override
	public boolean connectionException(Exception e, String message) {
		return false;
	}

	@Override
	public boolean generalException(Exception e, String message) {
		return false;
	}

	@Override
	public boolean queryException(Exception e, String message) {
		return false;
	}

	@Override
	public boolean valueException(Exception e, String message) {
		return false;
	}
	
	@Override
	public boolean invalidSettingsException(Exception e, String message) {
		return false;
	}
}
