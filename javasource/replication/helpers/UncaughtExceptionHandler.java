package replication.helpers;

public class UncaughtExceptionHandler implements java.lang.Thread.UncaughtExceptionHandler {
	private Throwable e;
	private boolean exceptionCaught = false;
	@Override
	public void uncaughtException(Thread t, Throwable e) {
		this.e  = e;
		this.exceptionCaught = true;
	}
	
	public Throwable getException() {
		return this.e;
	}
	
	public boolean hasException() {
		return this.exceptionCaught;
	}
}