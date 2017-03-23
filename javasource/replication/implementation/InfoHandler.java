package replication.implementation;

import replication.helpers.MessageOptions;
import replication.helpers.MessageOptions.Language;
import replication.interfaces.IInfoHandler;


public class InfoHandler extends IInfoHandler {

	public InfoHandler( String name ) {
		super(name);
	}

	@Override
	public void connectionMessage(Language lang, String connectionString, boolean isDebugMessage ) {
		if( isDebugMessage )
			this.printDebug( MessageOptions.CONNECTING_TO.getMessage(lang, connectionString ) );
		else
			this.printInfo( MessageOptions.CONNECTING_TO.getMessage(lang, connectionString ) );
	}
	@Override
	public void queryMessage(Language lang, String queryString, boolean isDebugMessage ) {
		if( isDebugMessage )
			this.printDebug( MessageOptions.EXECUTING_QUERY_EXTERNAL.getMessage(lang, queryString ) );
		else
			this.printInfo( MessageOptions.EXECUTING_QUERY_EXTERNAL.getMessage(lang, queryString ) );
	}

	@Override
	public void printRuntimeStatistics(Language lang, String objectType, Integer amountObjectsCreated, Integer amountObjectsSynced, Integer amountNotFound, Integer amountSkipped, boolean isDebugMessage ) {
		if( amountObjectsCreated == null ) amountObjectsCreated = 0;
		if( amountObjectsSynced == null ) amountObjectsSynced = 0;
		if( amountNotFound == null ) amountNotFound = 0;
		if( amountSkipped == null ) amountSkipped = 0;

		if( !isDebugMessage && this.statisticLogLevel == StatisticsLevel.AllStatistics ) 
			this.printInfo( MessageOptions.RUNTIME_STATISTICS.getMessage(lang, objectType, amountObjectsCreated, amountObjectsSynced, amountNotFound, amountSkipped ) );
		else 
			this.printDebug( MessageOptions.RUNTIME_STATISTICS.getMessage(lang, objectType, amountObjectsCreated, amountObjectsSynced, amountNotFound , amountSkipped ) );
	}

	@Override
	public String printFinalStatistics(Language lang, String objectType, Integer amountObjectsCreated, Integer amountObjectsSynced, Integer amountNotFound, Integer amountRemoved, Integer amountSkipped ) {
		
		if( amountObjectsCreated == null ) amountObjectsCreated = 0;
		if( amountObjectsSynced == null ) amountObjectsSynced = 0;
		if( amountNotFound == null ) amountNotFound = 0;
		if( amountSkipped == null ) amountSkipped = 0;

		String message = MessageOptions.STATISTICS.getMessage(lang) + "   -   ";
		if( amountRemoved == null )
			message += MessageOptions.FINAL_STATISTICS.getMessage( lang, objectType, amountObjectsCreated, amountObjectsSynced, amountNotFound, amountSkipped);
		else
			message += MessageOptions.FINAL_STATISTICS_WITH_REMOVE.getMessage( lang, objectType, amountObjectsCreated, amountObjectsSynced, amountNotFound, amountSkipped, amountRemoved);

		if( this.statisticLogLevel == StatisticsLevel.AllStatistics || this.statisticLogLevel == StatisticsLevel.OnlyFinalStatistics )
			this.printInfo( message );
		else 
			this.printDebug( message );

		return message;
	}

	@Override
	public String printFinalStatisticsAssociation(Language lang, String objectType, String associationName, Integer amountObjectsCreated, Integer amountObjectsSynced, Integer amountNotFound, Integer amountSkipped ) {
		if( amountObjectsCreated == null ) amountObjectsCreated = 0;
		if( amountObjectsSynced == null ) amountObjectsSynced = 0;
		if( amountNotFound == null ) amountNotFound = 0;
		if( amountSkipped == null ) amountSkipped = 0;

		String message = MessageOptions.STATISTICS.getMessage(lang) + "   -   ";
		message += MessageOptions.FINAL_STATISTICS_ASSOCIATION.getMessage(lang, associationName, objectType, amountObjectsCreated, amountObjectsSynced, amountNotFound, amountSkipped );

		if( this.statisticLogLevel == StatisticsLevel.AllStatistics || this.statisticLogLevel == StatisticsLevel.OnlyFinalStatistics )
			this.printInfo( message );
		else 
			this.printDebug( message );

		return message;
	}

	@Override
	public String printRuntimeUnknownAssociation(Language lang, String associationName, String objectKey) {
		this.printInfo( "TODO RUNTIME UNKNOWN ASSOCIATION, ASSOCIATION: " + associationName + " OBJECT KEY: " + objectKey);
		return "TODO RUNTIME UNKNOWN ASSOCIATION, ASSOCIATION: " + associationName + " OBJECT KEY: " + objectKey;
		//TODO
	}

	@Override
	public String printDebugMessage(Language lang, MessageOptions message, Object... strings) {
		super.printDebug( message.getMessage(lang, strings) );

		return message.getMessage(lang, strings);
	}
	
	@Override
	public String printTraceMessage(Language lang, MessageOptions message, Object... strings) {
		super.printTrace( message.getMessage(lang, strings) );

		return message.getMessage(lang, strings);
	}

	@Override
	public String printGeneralInfoMessage(Language lang, MessageOptions message, Object... strings) {
		super.printInfo( message.getMessage(lang, strings) );

		return message.getMessage(lang, strings);
	}
}
