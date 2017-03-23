package replication.interfaces;

import com.mendix.core.Core;

import replication.helpers.MessageOptions;
import replication.helpers.MessageOptions.Language;

public abstract class IInfoHandler {

	protected String name;
	protected StatisticsLevel statisticLogLevel;
	public enum StatisticsLevel {
		AllStatistics,
		OnlyFinalStatistics,
		NoStatistics
	}
	
	public IInfoHandler( String name ) {
		this.name = name;
	}

	public void setStatisticLogLevel( StatisticsLevel statisticLogLevel ) {
		this.statisticLogLevel = statisticLogLevel;
	}

	public void connectionMessage( Language lang, String connectionString ) {
		this.connectionMessage( lang, connectionString, false );
	}
	public abstract void connectionMessage( Language lang, String connectionString, boolean isDebugMessage  );


	public void queryMessage( Language lang, String queryString ) {
		this.queryMessage( lang, queryString, false );
	}
	public abstract void queryMessage( Language lang, String queryString, boolean isDebugMessage  );


	public void printRuntimeStatistics( Language lang, String objectType, Integer amountObjectsCreated, Integer amountObjectsSynced, Integer amountNotFound, Integer amountSkipped  ) {
		this.printRuntimeStatistics(lang, objectType, amountObjectsCreated, amountObjectsSynced, amountNotFound, amountSkipped, false);
	}
	public abstract void printRuntimeStatistics( Language lang, String objectType, Integer amountObjectsCreated, Integer amountObjectsSynced, Integer amountNotFound, Integer amountSkipped , boolean isDebugMessage );

	public abstract String printFinalStatistics( Language lang, String objectType, Integer amountObjectsCreated, Integer amountObjectsSynced, Integer amountNotFound, Integer amountRemoved, Integer amountSkipped );

	public abstract String printFinalStatisticsAssociation( Language lang, String objectType, String associationName, Integer amountObjectsCreated, Integer amountObjectsSynced, Integer amountNotFound, Integer amountSkipped );


	public abstract String printRuntimeUnknownAssociation( Language lang, String associationName, String objectKey );



	public abstract String printGeneralInfoMessage( Language lang, MessageOptions message, Object ...strings );
	public abstract String printDebugMessage( Language lang, MessageOptions message, Object ...strings );
	public abstract String printTraceMessage( Language lang, MessageOptions message, Object ...strings );



	public void printInfo( String message ) {
			Core.getLogger(this.name).info(message);
	}
	
	public void printTrace( String message ) {
		Core.getLogger(this.name).trace(message);
	}

	public void printDebug( String message ) {
		Core.getLogger(this.name).debug(message);
	}

}
