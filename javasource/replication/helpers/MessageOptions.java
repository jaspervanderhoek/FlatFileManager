package replication.helpers;



public enum MessageOptions {
	START_SYNCHRONIZING("Start met synchronizeren","Start synchronizing"),

	COULD_NOT_CONNECT_CLOUD_SECURITY("Er kon geen verbinding gemaakt worden met de database, controleer of de cloud security is uitgeschakeld", "Could not connect with the database, check if emulate cloud security is turned off."),
	COULD_NOT_CONNECT_WITH_DB("Er kon geen verbinding gemaakt worden met de database op locatie: ", "Could not connect with the database at location: "),
	UNKNOWN_DATABASE_TYPE("Het opgegeven database type kon niet worden gevonden, het type is: %s", "Unkown database type: %s" ),
	COULD_NOT_LOCATE_DB_DRIVER("De driver voor de database kon niet gevonden worden, de database type is: ", "Could not locatie the driver for the current database type, the type is: "),
	CONNECTING_TO("Verbinding aan het maken met: ","Connecting to: %s"),

	COULD_NOT_EXECUTE_QUERY( "De query kon niet uitgevoerd word op de externe database, de foutmelding is: %s, de uitgevoerde query is: %s", "Could not execute query on the external database, the message is: %s, the executed query is: %s" ),

	ERROR_READING_RESULTSET( "Er is iets mis gegaan tijdens het lezen van het resultaat van de query","Could not get the result from the query"),
	UNKNOWN_COLUMN("De kolom: %s kon niet gevonden worden in de query ook al was deze wel gedefinieerd in de ReplicationSettings.", "The column: %s could not be located in the query even though it was defined."),
	UNKNOWN_COLUMN_IN_OBJECT("Er is geen attribuut: %s gevonden in meta object: %s", "No column found with the name: %s in meta object of type: %s" ),
	EXECUTING_QUERY_EXTERNAL("De query op de externe database wordt uitgevoerd, de query is: %s", "Executing query on external database, query: %s"),

	FINAL_STATISTICS("Definitieve statistieken, Objecten van het type '%s' \n\tAangemaakt: %d \n\tGesynchronizeerd: %d \n\tNiet gevonden: %d \n\tOvergeslagen: %d ",
	"FINAL statistics, Objects of type '%s' \n\tCreated: %d \n\tSynchronized: %d \n\tNot found: %d \n\tSkipped: %d " ),
	FINAL_STATISTICS_ASSOCIATION("Definitieve statistieken voor associatie: %s, Objecten van het type '%s' \n\tAangemaakt: %d \n\tGesynchronizeerd: %d  \n\tNiet gevonden: %d \n\tOvergeslagen: %d ",
	"FINAL statistics for association: %s,objects of type '%s' \n\tCreated: %d \n\tSynchronized: %d \n\tNot found: %d \n\tSkipped: %d " ),

	FINAL_STATISTICS_WITH_REMOVE("Definitieve statistieken, Objecten van het type '%s' \n\nAangemaakt: %d \n\tGesynchronizeerd: %d \n\tniet gevonden: %d \n\tOvergeslagen: %d \n\tVerwijderd: %d ",
	"FINAL statistics, Objects of type '%s' \n\tCreated: %d \n\tSynchronized: %d \n\tNot found: %d \n\tSkipped: %d \n\tRemoved: %d " ),
	
	RUNTIME_STATISTICS("Statistieken, Objecten van het type '%s' \n\tAangemaakt: %d \n\tGesynchronizeerd: %d \n\tNiet gevonden: %d \n\tOvergeslagen: %d ",
	"Statistics, Objects of type '%s'\n\tCreated: %d \n\tSynchronized: %d \n\tNot found: %d \n\tSkipped: %d " ),


//	ERROR_WHILE_COMMITTING("De objecten konden niet gecommit worden","Could not commit the objects"),
	ERROR_REMOVING_UNCHANGED_OBJS("De ongebruikten objecten konden niet worden verwijderd", "The unchanged objects could not be removed"),
	COULD_NOT_CREATE_EDIT_OBJECT("Het object van het type: %s kon niet aangemaakt of gewijzigd worden.","The object of type: %s could not be created or changed."),

	IMPORT_FINISHED("De import is afgerond...","The import has finished..."),


	START_PREPARING_METAINFO("Er wordt gestart met het voorbereiden van de meta informatie", "Start preparing the meta information"),
	EXECUTING_QUERY_LOCAL("De query op de mendix database, de query is: %s", "Executing query on mendix database, query: %s"),
	EXECUTING_QUERY_ASSOCIATION("De query wordt uitgevoerd voor associate: %s, de OQL query is: %s" , "Executing oql query for association: %s, the OQL query is: %s"),
	INVALID_REMOVE_INDICATORS("De verwijder indicator moet dezelfde waarde hebben in alle objecten, maar de volgende waardes zijn gevonden: %s, %s" , "The remove indicator should have the same value in all objects, found values: %d, %d"),

	STATISTICS_METAINFO("Statistieken, Sync met de MendixDB \t\tRijen verwerkt : %d van totaal %d", "Statistics, Sync from MendixDB \t\tRows handled : %d of %d"),
	NO_CONSTRAINTS("Geen constraints gevonden voor associatie: %s" , "No constraints found for association: %s"),
	MISSING_COMBINEDKEY("Geen gecombineerde sleutel gevonden voor associatie: %s de associatie sleutel is: %s" , "No combined key found for association: %s the key from the association is: %s"),

	STATISTICS("Statistieken", "Statistics"),
	COULD_NOT_REMOVE_OBJECTS("De niet gesynchroniseerde objecten konden niet verwijderd worden door de volgende fout melding: %s", "The not synchronised objects could not be removed because of the following error: %s"),
	INVALID_COLUMN_CONTENT("Er is een ongeldige inhoud gevonden op sheet: %d kolom: %d en rij: %d. De waarde is van het type: %s, dit type is niet ondersteund.", "Invalid contenttype was found on sheet: %d column: %d and row: %d. The cell has a content of type: %s, this type is not supported."), 
	RETRIEVED_COLUMN_IN_DATASET( "Kolom %d in ResultSet, met naam: %s het type: %s ", "Column %d in ResultSet with name: %s of type: %s " )
	;

	private String messageNL;
	private String messageENG;
	private MessageOptions(String msgNL, String msgENG) {
		this.messageENG = msgENG;
		this.messageNL = msgNL;
	}

	public String getMessage( Language lang, Object ...texts ) {
		if( texts.length > 0 ) {
			if( lang == Language.NL )
				return String.format(this.messageNL, texts);

			return String.format(this.messageENG, texts);
		}

		if( lang == Language.NL )
			return this.messageNL;

		return this.messageENG;
	}


	public enum Language {
		NL,ENG
	}
}
