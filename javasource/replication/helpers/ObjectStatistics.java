package replication.helpers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import replication.MetaInfo;
import replication.MetaInfo.MetaInfoObjectState;
import replication.ReplicationSettings;
import replication.interfaces.IInfoHandler;

public class ObjectStatistics {
	public enum Stat {
		Created(0),
		Synchronized(1),
		NotFound(2),
		Removed(3),
		ObjectsSkipped(4);

		int intpos;
		private Stat( int intpos ) {
			this.intpos = intpos;
		}

		public int intValue() {
			return this.intpos;
		}
	}


	/**
	 * Prepare the statistics array to count the type of objects changes. The statistics position is specified by the Stat enum
	 */
	public Integer[] objectStats = new Integer[5];

	/**
	 * Prepare the map that can count the statistics for associations. The key of the Map is the association name, the value is a array of integers.
	 * The statistic position in the array is specified by the Stat enum.
	 */
	private Map<String, Integer[]> associationStats = new HashMap<String, Integer[]>();

	/**
	 * Prepare the map that keeps track of all the associated objects that can not be found. The key of the Map is the association name, the value is a List of all the object keys.
	 */
	private HashMap<String, List<String>> unknownObjectSet = new HashMap<String, List<String>>();

	private IInfoHandler infoHandler;
	private ReplicationSettings settings;
	public ObjectStatistics( ReplicationSettings settings )  {
		this.infoHandler = settings.getInfoHandler();
		this.settings = settings;
	}
	

	public synchronized void addStat(String associationName, Stat statEnum) {
		if( !this.associationStats.containsKey(associationName) )
			this.associationStats.put(associationName, new Integer[5] );

		Integer[] intValues = this.associationStats.get(associationName);
		if( intValues[statEnum.intValue()] == null )
			intValues[statEnum.intValue()] = 1;
		else
			intValues[statEnum.intValue()]++;
	}

	public int getTotalProcessedRecords( ) {
		int total = 0;
		for( Integer value : this.objectStats ) {
			if( value != null )
				total += value;
		}

		return total;
	}

	public Object getObjectStats(Stat statEnum) {
		return this.objectStats[statEnum.intValue()];
	}


	public void addObjectStat(MetaInfoObjectState state) {
		switch (state) {
		case New:
			this.addObjectStat(Stat.Created);
			break;
		case Changed:
			this.addObjectStat(Stat.Synchronized);
			break;
		case Unchanged:
			this.addObjectStat(Stat.ObjectsSkipped);
			break;
		case Undetermined:
			this.addObjectStat(Stat.NotFound);
			MetaInfo.MILogNode.Replication_MetaInfo_AssociatedObjects.getLogger().warn("Found an undetermined state while updating the statistics. This should not be possible!!");
			break;
		case Reset:
			break;
		}
	}


	public void addAssociationStat(String associationName, MetaInfoObjectState state) {
		switch (state) {
		case New:
			this.addStat(associationName, Stat.Created);
			break;
		case Changed:
			this.addStat(associationName, Stat.Synchronized);
			break;
		case Unchanged:
			this.addStat(associationName, Stat.ObjectsSkipped);
			break;
		case Undetermined:
			this.addStat(associationName, Stat.NotFound);
			MetaInfo.MILogNode.Replication_MetaInfo_AssociatedObjects.getLogger().warn("Found an undetermined state while updating the statistics. This should not be possible!!");
			break;
		case Reset:
			break;
		}
	}

	public void addObjectStat(Stat statEnum) {
		addObjectStat(statEnum, 1);
	}
	public synchronized void addObjectStat(Stat statEnum, int amount) {
		if( this.objectStats[statEnum.intValue()] == null )
			this.objectStats[statEnum.intValue()] = amount;
		else
			this.objectStats[statEnum.intValue()] += amount;
	}
	
	public synchronized void recordUnknownAssociatedObject( String associationName, String objectKey ) {
		if( this.settings.getAssociationConfig(associationName).printNotFoundMessages() ) { 
			if( !this.unknownObjectSet.containsKey(associationName) )
				this.unknownObjectSet.put(associationName, new ArrayList<String>());
			
			this.unknownObjectSet.get(associationName).add(objectKey);
		}
		
		this.addStat(associationName, Stat.NotFound);
	}	
	public synchronized void recordUnknownObject( String objectKey ) {
		if( this.settings.getMainObjectConfig().printNotFoundMessages() ) { 
			String typeName = this.settings.getMainObjectConfig().getObjectType();
			if( !this.unknownObjectSet.containsKey(typeName) )
				this.unknownObjectSet.put(typeName, new ArrayList<String>());
			
			this.unknownObjectSet.get(typeName).add(objectKey);
		}
		
		this.addObjectStat(Stat.NotFound);
	}
	
	
	public void printFinalStatistics() {
		this.infoHandler.printFinalStatistics( this.settings.getLanguage(), this.settings.getMainObjectConfig().getObjectType(), this.objectStats[Stat.Created.intValue()], this.objectStats[Stat.Synchronized.intValue()], this.objectStats[Stat.NotFound.intValue()], this.objectStats[Stat.Removed.intValue()], this.objectStats[Stat.ObjectsSkipped.intValue()] );
		for( Entry<String,Integer[]> entry : this.associationStats.entrySet() ) {
			Integer[] intArr = entry.getValue();
			String objectType = this.settings.getAssociationConfig(entry.getKey()).getObjectType();
			this.infoHandler.printFinalStatisticsAssociation( this.settings.getLanguage(), objectType, entry.getKey(), intArr[Stat.Created.intValue()], intArr[Stat.Synchronized.intValue()], intArr[Stat.NotFound.intValue()], intArr[Stat.ObjectsSkipped.intValue()] );
		}
	}
	
	public void printNotFoundMessages( ) {
		if( this.settings.printAnyNotFoundMessage() && this.unknownObjectSet.size() > 0 ) {
			StringBuilder logMessage = new StringBuilder().append( "Statistics   -   NotFound Objects are listed below: " );
			
			for( Entry<String,List<String>> entry : this.unknownObjectSet.entrySet() ) {
				String prevValue = null;
				long occurences = 0;
				
				logMessage.append( "\n Type: " ).append( entry.getKey() );
				List<String> valueList = entry.getValue();
				Collections.sort(valueList, String.CASE_INSENSITIVE_ORDER);
				for( String value :  valueList ) {
					if( !value.equals(prevValue) ) {
						if( prevValue != null)
							logMessage.append( "\n\t- " ).append( prevValue ).append( "   Occurences: " ).append( occurences );
						prevValue = value;
						occurences = 0L;
					}
					occurences++;
				}
				logMessage.append( "\n\t- " ).append( prevValue ).append( "   Occurences: " ).append( occurences );
			}
			
			this.infoHandler.printInfo(logMessage.toString());
		}
	}
	
	public void printRuntimeStatistics() {
		//Print an info message for every 1000 objects
		if( (this.getTotalProcessedRecords() % 1000) == 0 ) {
			this.infoHandler.printRuntimeStatistics( this.settings.getLanguage(), this.settings.getMainObjectConfig().getObjectType(), this.objectStats[Stat.Created.intValue()], this.objectStats[Stat.Synchronized.intValue()], this.objectStats[Stat.NotFound.intValue()], this.objectStats[Stat.ObjectsSkipped.intValue()] );
			for( Entry<String,Integer[]> entry : this.associationStats.entrySet() ) {
				Integer[] intArr = entry.getValue();
				String objectType = this.settings.getAssociationConfig(entry.getKey()).getObjectType();
				this.infoHandler.printRuntimeStatistics( this.settings.getLanguage(), entry.getKey() + " / " + objectType, intArr[Stat.Created.intValue()], intArr[Stat.Synchronized.intValue()], intArr[Stat.NotFound.intValue()], intArr[Stat.ObjectsSkipped.intValue()] );
			}
		}

		//Print a debug message for every 100 objects
		else if( (this.getTotalProcessedRecords() % 100) == 0 )
			this.infoHandler.printRuntimeStatistics( this.settings.getLanguage(), this.settings.getMainObjectConfig().getObjectType(), this.objectStats[Stat.Created.intValue()], this.objectStats[Stat.Synchronized.intValue()], this.objectStats[Stat.NotFound.intValue()], this.objectStats[Stat.ObjectsSkipped.intValue()], true );
	}

	
	public void clear() {
		this.associationStats.clear();
		this.unknownObjectSet.clear();
		this.objectStats = new Integer[5];
	}
}