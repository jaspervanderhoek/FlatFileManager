package replication;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.UUID;

import replication.ReplicationSettings.ChangeTracking;
import replication.ReplicationSettings.MendixReplicationException;
import replication.ReplicationSettings.ObjectSearchAction;
import replication.ValueParser.ParseException;
import replication.helpers.ObjectStatistics;
import replication.helpers.ObjectStatistics.Stat;
import replication.helpers.TimeMeasurement;

import com.mendix.core.Core;
import com.mendix.logging.ILogNode;
import com.mendix.systemwideinterfaces.connectionbus.data.IDataRow;
import com.mendix.systemwideinterfaces.connectionbus.data.IDataTable;
import com.mendix.systemwideinterfaces.core.IMendixIdentifier;
import com.mendix.systemwideinterfaces.core.IMendixObject;

/**
 * This class processes all supplied values and makes sure all values are stored in the correct
 * MetaInfoObject Based on all these values this class can create the oql query and can retrieve all
 * IMendixObjects by the object values and association values
 * 
 * @author Mendix - Jasper van der Hoek
 */
public class MetaInfo {

	public static final String _version = "Mx5_20161013";

	public enum MetaInfoObjectState {
		New, Changed, Unchanged, Undetermined, Reset
	}

	private Long miCounter = 0L;

	private synchronized Long getNewMINr() {
		return this.miCounter++;
	}

	/**
	 * The MetaInfoObject is a simple version of an IMendixObject. It contains the key of the object
	 * (based on the ReplicationSettings) and all the associated MetaInfoObject's and their values
	 * 
	 * @author Mendix - Jasper van der Hoek
	 */
	public class MetaInfoObject {

		private Long id;
		private String objectID;
		private MetaInfoObjectState state = MetaInfoObjectState.Undetermined;
		private String objectKey = null;
		private IMendixObject object = null;
		private HashMap<String, Object> attributes; // Map<AttributeName, Value>
		private Map<String, MetaInfoObject> meta_References; // Map<AssociationName, MetaInfoObject>
		private Map<String, List<MetaInfoObject>> meta_ReferenceSets; // Map<AssociationName,
																		// List<MetaInfoObject>>
		private boolean isAnyReferenceSet = false;

		private TreeMap<String, Boolean> keys;

		/**
		 * Create a new Instance, specify the names of all members that should be used as key member
		 * 
		 * @param keys
		 */
		private MetaInfoObject( String objectID, String objectKey, TreeMap<String, Boolean> keys ) {
			this.id = getNewMINr();
			this.keys = keys;
			this.objectKey = objectKey;
			this.attributes = new LinkedHashMap<String, Object>(MetaInfo.this.settings.getNrOfColumns());
			this.objectID = objectID;

			ObjectConfig config = MetaInfo.this.settings.getObjectConfiguration(objectID);
			if ( config.useRemoveIndicatorAttribute() ) {
				this.attributes.put(config.getRemoveIndicator(), config.getNewRemoveIndicatorValue());
			}

			this.meta_References = new LinkedHashMap<String, MetaInfoObject>();
			this.meta_ReferenceSets = new LinkedHashMap<String, List<MetaInfoObject>>();

		}

		/**
		 * @return An unique MetaInfoObject nr, this nr is unique per import run
		 */
		public Long getId() {
			return this.id;
		}

		public String getObjectID() {
			return this.objectID;
		}

		/**
		 * @return a map containing the values for all primitive attributes, this map can be empty
		 *         but never null
		 */
		public Map<String, Object> getAttributeValues() {
			return this.attributes;
		}

		/**
		 * This function returns a map with all referenced MetaInfoObjects. The key of the map is
		 * association name (moduleName.AssociationName) and the value is the MetaInfoObject
		 * 
		 * @return Map<AssociationName, MetaInfoObject>
		 */
		public Map<String, MetaInfoObject> getReferenceValues() {
			return this.meta_References;
		}

		/**
		 * This function returns a map with all MetaInfoObjects for the reference sets. The key of
		 * the map is association name (moduleName.AssociationName) and the value is a list with all
		 * MetaInfoObject's for the association.
		 * 
		 * When no referenceSets are configured this map will be empty. When no values can be found
		 * for the referenSet the map will always contain an entry but with a list without any
		 * MetaInfoObject's
		 * 
		 * @return Map<AssociationName, List<MetaInfoObject>>
		 */
		public Map<String, List<MetaInfoObject>> getReferenceSetValues() {
			return this.meta_ReferenceSets;
		}

		/**
		 * @return the objectkey based on all supplied attribute values
		 * @throws ParseException
		 */
		public String getObjectKey() throws ParseException {
			if ( ValueParser.isValueEmpty(this.objectKey) )
				this.objectKey = MetaInfo.this.valueParser.buildObjectKey(this.keys, this.attributes);

			return this.objectKey;
		}

		public Object getAttributeValue( String memberName ) {
			return this.attributes.get(memberName);
		}

		public void setAttribute( String alias, String memberName, Object value ) {
			this.attributes.put(memberName, value);

			if ( alias != null && this.keys.containsKey(alias) )
				this.objectKey = null;
		}

		public MetaInfoObject setAssociationValue( String alias, String associationName, String attributeName, Object value ) throws MendixReplicationException {
			this.isAnyReferenceSet = true;
			MetaInfoObject miObject = null;

			if ( this.keys.containsKey(alias) )
				this.objectKey = null;

			boolean addInfo = false;
			if ( !MetaInfo.this.settings.getMainObjectConfig().ignoreEmptyKeys() || (value != null && !value.toString().isEmpty()) ) {
				if ( !this.meta_References.containsKey(associationName) ) {
					miObject = MetaInfo.this.createMetaInfoObject(associationName,
							new TreeMap<String, Boolean>(MetaInfo.this.settings.getAssociationKeys(associationName)));
					addInfo = true;

					miObject.setAttribute(alias, attributeName, value);
				}
				else {
					miObject = this.meta_References.get(associationName);

					String key = miObject.getObjectKey();
					miObject.setAttribute(alias, attributeName, value);
					if ( (key == null && miObject.getObjectKey()!=null) || (key != miObject.getObjectKey() && !key.equals(miObject.getObjectKey())) )
						addInfo = true;
				}
			}

			if ( addInfo ) {
				miObject = addMetaInfoObject(associationName, miObject.getObjectKey(), miObject);
				this.meta_References.put(associationName, miObject);
			}

			return miObject;
		}

		public MetaInfoObject addAssociationValue( String alias, String associationName, String attributeName, Object value ) throws MendixReplicationException {
			this.isAnyReferenceSet = true;
			if ( !this.meta_ReferenceSets.containsKey(associationName) )
				this.meta_ReferenceSets.put(associationName, new ArrayList<MetaInfoObject>());

			if ( this.keys.containsKey(alias) )
				this.objectKey = null;

			MetaInfoObject lastMIObj = null;
			int nrOfRefSetEntries = this.meta_ReferenceSets.get(associationName).size();
			if ( nrOfRefSetEntries > 0 )
				lastMIObj = this.meta_ReferenceSets.get(associationName).get(nrOfRefSetEntries - 1);

			AssociationConfig config = MetaInfo.this.settings.getAssociationConfig(associationName);
			if ( lastMIObj == null ||
					(lastMIObj.getAttributeValue(attributeName) != null && config.containsKeyMember(attributeName)) ||		// For
					// reference sets, in case the attribute is found for the second time, we want to create a new
					// object
					(config.ignoreEmptyKeys() && lastMIObj.getAttributeValue(attributeName) != null && config.getObjectSearchAction() == ObjectSearchAction.CreateEverything) ||
					(!config.ignoreEmptyKeys() && lastMIObj.getAttributeValues().containsKey(attributeName) && config.getObjectSearchAction() == ObjectSearchAction.CreateEverything) )
			{

				lastMIObj = MetaInfo.this.createMetaInfoObject(associationName,
						new TreeMap<String, Boolean>(MetaInfo.this.settings.getAssociationKeys(associationName)));

				lastMIObj.setAttribute(alias, attributeName, value);
				lastMIObj = addMetaInfoObject(associationName, lastMIObj.getObjectKey(), lastMIObj);

				this.meta_ReferenceSets.get(associationName).add(lastMIObj);
			}
			else {
				lastMIObj.setAttribute(alias, attributeName, value);
				MetaInfoObject miObj = addMetaInfoObject(associationName, lastMIObj.getObjectKey(), lastMIObj);
				if ( miObj != lastMIObj ) {
					this.meta_ReferenceSets.get(associationName).remove(lastMIObj);
					this.meta_ReferenceSets.get(associationName).add(miObj);
				}
			}

			return lastMIObj;
		}

		public boolean hasReferenceValues() {
			return this.meta_References.size() > 0;
		}

		/**
		 * Reset all maps, in the current MetaInfoObject
		 */
		public void clear() {
			this.attributes.clear();
			this.meta_References.clear();
			this.meta_ReferenceSets.clear();

			this.state = MetaInfoObjectState.Reset;
		}

		public boolean hasReferenceSetValues() {
			return this.meta_ReferenceSets.size() > 0;
		}

		/**
		 * @return true if either the reference or the reference set contains at least one entry
		 */
		public boolean isAnyReferenceSet() {
			return this.isAnyReferenceSet;
		}

		protected void setObject( IMendixObject object, boolean isNew ) {
			if ( this.state == MetaInfoObjectState.Reset )
				throw new ReplicationSettings.MendixReplicationRuntimeException("Invalid object state : Reset , for object " + this.getObjectID() + " / " + this.getId(), MetaInfo._version);

			this.object = object;
			if ( isNew )
				this.state = MetaInfoObjectState.New;
			else
				this.state = MetaInfoObjectState.Unchanged;
		}

		/**
		 * If there was an object retrieved earlier, this function could return the object. When
		 * there hasn't been any search action this function returns a null or when the object could
		 * not be found this returns a null as well
		 * 
		 * @return (OPTIONAL) the object found by the keys.
		 */
		public IMendixObject getObject() {
			return this.object;
		}

		public MetaInfoObjectState getState() {
			return this.state;
		}

		public void markAsChanged() {
			if ( this.state == MetaInfoObjectState.Reset )
				throw new ReplicationSettings.MendixReplicationRuntimeException("Invalid object state : Reset , for object " + this.getObjectID() + " / " + this.getId(), MetaInfo._version);
			this.state = MetaInfoObjectState.Changed;
		}

		@Override
		public boolean equals( Object obj ) {
			if ( obj instanceof MetaInfoObject ) {
				try {
					if ( this.getObjectKey() != null )
						return this.getObjectKey().equals(((MetaInfoObject) obj).getObjectKey());
				}
				catch( ParseException e ) {
					Core.getLogger("MetaInfoObject").error("Could not compare the two metainfoobjects, error: " + e.getMessage(), e);
					return false;
				}
			}
			else if ( obj instanceof String ) {
				try {
					if ( this.getObjectKey() != null )
						return this.getObjectKey().equals((String) obj);
				}
				catch( ParseException e ) {
					Core.getLogger("MetaInfoObject").error("Could not compare the two metainfoobjects, error: " + e.getMessage(), e);
					return false;
				}
			}

			return false;
		}

		@Override
		public int hashCode() {
			int hash = 7;
			hash = 17 * hash + (this.id != null ? this.id.hashCode() : 0);
			return hash;
		}

		public String getValueString() {
			StringBuilder b = new StringBuilder(100);
			for( Entry<String, Object> value : this.attributes.entrySet() ) {
				b.append(value.getKey()).append(" : ").append(value.getValue()).append("  |  ");

				if ( b.length() > 100 )
					return b.toString();
			}

			return b.toString();
		}

		public TreeMap<String, Boolean> getKeys() {
			return this.keys;
		}
	}

	/**
	 * Create a new instance of an MetaInfoObject and store the keys for that object as well
	 * 
	 * @param Set
	 *        containing all the members that are specified as key member
	 * @return new Instance of MetaInfoObject
	 */
	public MetaInfoObject createMetaInfoObject( String associationName, TreeMap<String, Boolean> keySet ) {
		AssociationConfig config = MetaInfo.this.settings.getAssociationConfig(associationName);
		if ( config.getObjectSearchAction() == ObjectSearchAction.CreateEverything && !config.hasKeyMembers() ) {
			return new MetaInfoObject(associationName, UUID.randomUUID().toString(), keySet);
		}

		return new MetaInfoObject(associationName, null, keySet);
	}

	/**
	 * This map contains all objects with their values. The key is the ObjectKey of the
	 * MetaInfoObject, value is the corresponding MetaInfoObject
	 */
	private LinkedHashMap<String, MetaInfoObject> meta_Objects;
	private LinkedHashMap<String, LinkedHashMap<String, MetaInfoObject>> totalSet_meta_Objects;

	/**
	 * The Objectkey from the object that is put in the variable lastObject
	 */
	private String lastKey = null;
	/**
	 * The MetaInfoObject that has the same objectKey as in the variable lastKey. This object is put
	 * in the map meta_Objects as well
	 */
	private MetaInfoObject lastObject = null;

	private ReplicationSettings settings;
	private ValueParser valueParser;

	// public Integer currentRemoveIndicatorValue = null;
	// public Integer newRemoveIndicatorValue = null;

	private ObjectStatistics objectStats;

	public TimeMeasurement TimeMeasurement;
	public String replicationName;

	public MetaInfo( ReplicationSettings settings, ValueParser valueParser, String replicationName ) throws MendixReplicationException {
		this.settings = settings;
		this.settings.validateSettings();
		this.objectStats = new ObjectStatistics(settings);
		this.replicationName = replicationName;
		this.TimeMeasurement = new TimeMeasurement(replicationName);

		this.valueParser = valueParser;

		this.meta_Objects = new LinkedHashMap<String, MetaInfoObject>();
		this.totalSet_meta_Objects = new LinkedHashMap<String, LinkedHashMap<String, MetaInfoObject>>();
		this.prepareRemoveIndicator(this.settings.getMainObjectConfig());
	}

	/**
	 * Retrieve all unique remove indicator values and set the values that are used as
	 * removeIndicator Value
	 * 
	 * This function will only make some changes if the ReplicationSettings specify that the objects
	 * should be synchronized and a remove indicator is specified After this function is executed
	 * the current remove indicator value is set and the expected new value is defined.
	 * 
	 * @throws MendixReplicationException
	 */
	private void prepareRemoveIndicator( ObjectConfig objectConfig ) throws MendixReplicationException {
		try {
			ObjectSearchAction searchAction = objectConfig.getObjectSearchAction();
			if ( searchAction != ObjectSearchAction.CreateEverything &&
					searchAction != ObjectSearchAction.OnlyCreateNewObjects &&
					(objectConfig.getChangeTracking() == ChangeTracking.RemoveUnchangedObjects ||
					objectConfig.getChangeTracking() == ChangeTracking.TrackChanges) )
			{
				IDataTable table =
						Core.retrieveOQLDataTable(
								this.settings.getContext(),
								"SELECT DISTINCT " + objectConfig.getRemoveIndicator() +
										" FROM " + objectConfig.getObjectType() +
										" WHERE " + objectConfig.getRemoveIndicator() + " IS NOT NULL ORDER BY " + objectConfig.getRemoveIndicator() + " ASC ");

				// Only when the table contains any rows the content must be checked
				if ( table.getRowCount() > 0 ) {
					for( IDataRow row : table.getRows() ) {
						Integer riValue = row.getValue(this.settings.getContext(), objectConfig.getRemoveIndicator());

						// Set the current and new value if the values are never set before
						if ( objectConfig.getCurrentRemoveIndicatorValue() == null && riValue != null ) {
							objectConfig.setCurrentRemoveIndicatorValue(riValue);
							if ( riValue != Integer.MAX_VALUE )
								objectConfig.setNewRemoveIndicatorValue(riValue + 1);
							else
								objectConfig.setNewRemoveIndicatorValue(0);
						}
						// if the remove indicator is already set but the retrieved value is
						// different from the remove indicator
						else if ( objectConfig.getCurrentRemoveIndicatorValue() != riValue && riValue != null ) {
							if ( objectConfig.getNewRemoveIndicatorValue() <= riValue )
								objectConfig.setNewRemoveIndicatorValue(riValue + 1);

							// Print a log message
							MILogNode.Replication_MetaInfo.getLogger().debug(
									"The remove indicator should have the same value in all objects, found values: " + riValue + ", " + objectConfig
											.getCurrentRemoveIndicatorValue());
						}
					}
				}

				// When there are no remove indicator values just set the new value to 0
				if ( objectConfig.getNewRemoveIndicatorValue() == null )
					objectConfig.setNewRemoveIndicatorValue(1);

				MILogNode.Replication_MetaInfo.getLogger().debug(
						"The remove indicator is set to the value: " + objectConfig.getCurrentRemoveIndicatorValue());
			}
		}
		catch( Exception e ) {
			throw new MendixReplicationException(e.getMessage(), MetaInfo._version, e);
		}
	}

	public void addMetaInfoObject( MetaInfoObject object ) throws MendixReplicationException {
		String objectKey = object.getObjectKey();
		if ( objectKey == null ) {
			objectKey = this.valueParser.buildObjectKey(object.getKeys(), object.getAttributeValues());
			object.objectKey = objectKey;
		}

		if ( objectKey != null ) {
			if ( this.meta_Objects.containsKey(objectKey) ) {
				mergeMetaInfoObjects(this.meta_Objects.get(objectKey), object);
			}
			else
				this.meta_Objects.put(objectKey, object);
		}
		else
			MetaInfo.MILogNode.Replication_MetaInfo.getLogger().trace("No object key, not storing object: " + object.getObjectID());
	}

	public MetaInfoObject addMetaInfoObject( String associationName, String objectKey, MetaInfoObject object ) throws MendixReplicationException {
		ObjectConfig config;

		if ( objectKey == null )
			return object;

		if ( associationName == null ) {
			config = this.settings.getMainObjectConfig();
		}

		else {
			config = this.settings.getAssociationConfig(associationName);
		}

		String ObjectType = config.getObjectType();
		if ( !this.totalSet_meta_Objects.containsKey(ObjectType) )
			this.totalSet_meta_Objects.put(ObjectType, new LinkedHashMap<String, MetaInfoObject>());


		if ( this.totalSet_meta_Objects.get(ObjectType).containsKey(objectKey) ) {
			object = addToListOrMerge(objectKey, object, ObjectType);
		}
		else {
			this.totalSet_meta_Objects.get(ObjectType).put(objectKey, object);

		}

		if ( this.lastKey != null && this.lastKey.equals(objectKey) && associationName == null ) 
			this.lastObject = object;

		if ( associationName == null )
			this.meta_Objects.put(objectKey, object);

		return object;
	}

	protected MetaInfoObject addToListOrMerge( String objectKey, MetaInfoObject object, String ObjectType ) throws ParseException, MendixReplicationException {
		MetaInfoObject miObj = this.totalSet_meta_Objects.get(ObjectType).get(objectKey);
		
		String otherKey = miObj.getObjectKey();
		if( miObj.getId() == object.getId() ) {
			object = miObj;
		}
		else if( miObj.equals(object) ) { 
			mergeMetaInfoObjects(miObj, object);
			object = miObj;
		}
		else {
			this.totalSet_meta_Objects.get(ObjectType).put(objectKey, object);
			
			//The other object is placed in the list under the wrong key, so lets place it correctly
			this.totalSet_meta_Objects.get(ObjectType).remove(objectKey);
			addToListOrMerge(otherKey, miObj, ObjectType);
		}
		return object;
	}

	/**
	 * Add the value to the MetaInfoObject with a corresponding objectKey. Adds a new MetaInfoObject
	 * or adds the value to the existing MetaInfoObject.
	 * 
	 * The alias must be the same value as specified in the ReplicationSettings. The value can be
	 * any of any type but when calling this function the value must be of a type supported by the
	 * XAS for the member type that has to be changed (i.e. Long or Date for a MendixDateTime, int
	 * for MendixInteger, etc...)
	 * 
	 * @param objectKey
	 * @param alias
	 * @param value
	 * @return
	 * @throws MendixReplicationException
	 */
	public MetaInfoObject addValue( String objectKey, String alias, Object value ) throws MendixReplicationException {
		if ( this.prepareObject(objectKey) ) {
			this.lastObject.setAttribute(alias, this.settings.getMemberNameByAlias(alias), value);

			return this.lastObject;
		}
		else
			MILogNode.Replication_MetaInfo_AssociatedObjects.getLogger().info(
					"Unable set the value from the alias: " + alias + " for object with key: " + objectKey);

		return null;
	}

	/**
	 * Add the value to the association from the MetaInfoObject with a corresponding objectKey.
	 * 
	 * The alias must be the same value as specified in the ReplicationSettings. The value can be
	 * any of any type but when calling this function the value must be of a type supported by the
	 * XAS for the member type that has to be changed (i.e. Long or Date for a MendixDateTime, int
	 * for MendixInteger, etc...)
	 * 
	 * @param objectKey
	 * @param alias
	 * @param value
	 * @throws MendixReplicationException
	 */
	public MetaInfoObject setAssociationValue( String objectKey, String alias, Object value ) throws MendixReplicationException {
		if ( this.prepareObject(objectKey) ) {
			String associationName = this.settings.getAssociationNameByAlias(alias);
			return this.lastObject.setAssociationValue(alias, associationName, this.settings.getAssociationColumnByAlias(alias), value);
		}
		else
			MILogNode.Replication_MetaInfo_AssociatedObjects.getLogger().info(
					"Unable to instantiate and set the associated object for alias: " + alias + " for object with key: " + objectKey);

		return null;
	}

	public MetaInfoObject addAssociationValue( String objectKey, String alias, Object value ) throws MendixReplicationException {
		if ( this.settings.treatFieldAsReference(alias) )
			throw new MendixReplicationException("The column: " + alias + " isn't a reference Set, this function can only be called when the association (" + this.settings.getAssociationNameByAlias(alias) + ") is a reference set.", MetaInfo._version);

		if ( this.prepareObject(objectKey) ) {
			String associationName = this.settings.getAssociationNameByAlias(alias);

			return this.lastObject.addAssociationValue(alias, associationName, this.settings.getAssociationColumnByAlias(alias), value);
		}
		else
			MILogNode.Replication_MetaInfo_AssociatedObjects.getLogger().info(
					"Unable to instantiate and add the associated object for alias: " + alias + " for object with key: " + objectKey);

		return null;
	}

	/**
	 * This function checks the meta_Objets map if an object with the provided objectKey already
	 * exists. If it does that MetaInfoObject will be stored in the lastObject variable. When it
	 * doesn't exist a new MetaInfoObject will be added to the map and the new object will be stored
	 * in the lastObject variable as well
	 * 
	 * @param objectKey
	 * @return true when a MetaInfoObject can retrieved or created. false will be returned when the
	 *         objectKey is invalid.
	 * @throws MendixReplicationException
	 */
	private boolean prepareObject( String objectKey ) throws MendixReplicationException {
		if ( objectKey != null && objectKey.length() > 0 ) {
			if ( !objectKey.equals(this.lastKey) ) {

				if ( this.meta_Objects.size() >= this.settings.Configuration.MetaInfoProcessingBatchThreshold ) {
					this.startProcessingObjects();
				}

				if ( !this.meta_Objects.containsKey(objectKey) ) {
					ObjectConfig mainConfig = this.settings.getMainObjectConfig();
					addMetaInfoObject(null, objectKey, new MetaInfoObject(mainConfig.getObjectType(), objectKey, mainConfig.getKeys()));

					if ( MILogNode.Replication_MetaInfo_MainObject.getLogger().isTraceEnabled() )
						MILogNode.Replication_MetaInfo_MainObject.getLogger().trace(
								"Different objectKey(" + objectKey + ") than the previous object, start initializing a new MetaInfoObject");
				}
				else if ( MILogNode.Replication_MetaInfo_MainObject.getLogger().isTraceEnabled() )
					MILogNode.Replication_MetaInfo_MainObject.getLogger().trace(
							"Different objectKey(" + objectKey + ") than the previous object, found an MetaInfoObject with the same ObjectKey");

				this.lastObject = this.meta_Objects.get(objectKey);

				this.lastKey = objectKey;
			}

			return true;
		}
		else
			MILogNode.Replication_MetaInfo.getLogger().trace(
					"Unable to prepare object with type: " + this.settings.getMainObjectConfig().getObjectType() + " because of an empty objectkey");

		return false;
	}

	/**
	 * Copy all the values from two MetaInfoObjects to each other (For now this is just a simpel
	 * override action)
	 * 
	 * @param metaInfoObject_To
	 * @param metaInfoObject_From
	 * @throws MendixReplicationException
	 */
	private static void mergeMetaInfoObjects( MetaInfoObject metaInfoObject_To, MetaInfoObject metaInfoObject_From ) throws MendixReplicationException {
		if ( metaInfoObject_From.getObjectKey() != null && metaInfoObject_To.getObjectKey() != null && !metaInfoObject_From.getObjectKey().equals(
				metaInfoObject_To.getObjectKey()) )
			MetaInfo.MILogNode.Replication_MetaInfo
					.getLogger()
					.debug("Merging different objects: [" + metaInfoObject_From.getId() + " / " + metaInfoObject_From.getObjectKey() + "] into [" + metaInfoObject_To
							.getId() + " / " + metaInfoObject_To.getObjectKey() + "] object: " + metaInfoObject_To.getObjectID());
		else
			MetaInfo.MILogNode.Replication_MetaInfo.getLogger()
					.debug("Merging [" + metaInfoObject_From.getId() + " / " + metaInfoObject_From.getObjectKey() + "] into [" + metaInfoObject_To
							.getId() + " / " + metaInfoObject_To.getObjectKey() + "] object: " + metaInfoObject_To.getObjectID());

		for( Entry<String, Object> attribute : metaInfoObject_From.getAttributeValues().entrySet() ) {
			metaInfoObject_To.setAttribute(null, attribute.getKey(), attribute.getValue());
		}

		for( Entry<String, List<MetaInfoObject>> refSetEntry : metaInfoObject_From.getReferenceSetValues().entrySet() ) {
			String associationName = refSetEntry.getKey();
			for( MetaInfoObject obj : refSetEntry.getValue() ) {

				for( Entry<String, Object> attribute : obj.getAttributeValues().entrySet() ) {
					metaInfoObject_To.addAssociationValue(null, associationName, attribute.getKey(), attribute.getValue());
				}
			}
		}

		for( Entry<String, MetaInfoObject> attribute : metaInfoObject_From.getReferenceValues().entrySet() ) {
			metaInfoObject_To.setAttribute(null, attribute.getKey(), attribute.getValue());
		}
		metaInfoObject_To.objectKey = null;
	}

	private void removeUnchangedObjects( ObjectConfig objectConfig ) throws MendixReplicationException {
		if ( objectConfig.getChangeTracking() == ChangeTracking.RemoveUnchangedObjects ) {
			String xPath = "//" + objectConfig.getObjectType() + "[" + objectConfig.getRemoveIndicator() + "!= " + objectConfig
					.getNewRemoveIndicatorValue() + " or " + objectConfig.getRemoveIndicator() + "=NULL]";
			this.removeUnchangedObjectsByQuery(xPath);
		}
		else
			MILogNode.Replication_MetaInfo.getLogger().debug("Not removing any unchanged objects, configuration is disabled");
	}

	public void removeUnchangedObjectsByQuery( String xPath ) throws MendixReplicationException {
		this.TimeMeasurement.startPerformanceTest("Removing unchanged objects by xPath");
		ILogNode logNode = MILogNode.Replication_MetaInfo.getLogger();
		int limit = this.settings.Configuration.RetrieveToBeRemovedObjectsXPath_Limit;

		List<IMendixIdentifier> idList = new ArrayList<IMendixIdentifier>(limit);
		int size = limit;
		long nrOfRemoved = 0;
		HashMap<String, String> sortMap = new HashMap<String, String>();

		logNode.debug("Start removing unchanged objects, with query " + xPath);

		while( size == limit ) {
			// Replace the offset value with the new expected offset
			logNode.debug("Start retrieving next batch, total removed: " + nrOfRemoved);

			List<IMendixObject> result;
			try {
				result = Core.retrieveXPathQuery(this.settings.getContext(), xPath, size, 0, sortMap);
			}
			catch( Throwable t ) {
				throw new MendixReplicationException("Could not execute the following OQL query: " + xPath, MetaInfo._version, t);
			}
			size = result.size();
			if ( size > 0 ) {
				this.objectStats.addObjectStat(Stat.Removed, size);
				Core.delete(this.settings.getContext(), result);
				idList.clear();
			}
		}
		this.TimeMeasurement.endPerformanceTest("Removing unchanged objects by xPath");
	}

	private void startProcessingObjects() throws MendixReplicationException {

		MILogNode.Replication_MetaInfo.getLogger().debug("Initializing new MetaInfoProcessor");
		MetaInfoProcessor miProcessor = new MetaInfoProcessor(this, this.meta_Objects);
		miProcessor.startProcessing();

		/*
		 * Resetting all cached objects
		 */
		this.meta_Objects.clear();
		
		//Only iterate over the total set, because this also includes all content from the meta_objects maps
		for( Entry<String, LinkedHashMap<String,MetaInfoObject>> meoL : this.totalSet_meta_Objects.entrySet() ) {
			for( Entry<String, MetaInfoObject> meo : meoL.getValue().entrySet() ) 
				meo.getValue().clear();
		}
		this.totalSet_meta_Objects.clear();
	}

	/**
	 * Process all remaining object values When unchanged objects should be removed that will be
	 * done here too
	 * 
	 * Finally print the statistics and reset all list, statistics and temp values
	 * 
	 * @throws MendixReplicationException
	 */
	public void finished() throws MendixReplicationException {
		try {
			this.startProcessingObjects();
			this.removeUnchangedObjects(this.settings.getMainObjectConfig());
			this.objectStats.printFinalStatistics();
			this.objectStats.printNotFoundMessages();
			this.clear();
		}
		catch( Exception e ) {
			throw new MendixReplicationException(e.getMessage(), MetaInfo._version, e);
		}
	}

	/**
	 * Process all remaining object values When unchanged objects should be removed that will be
	 * done here too
	 * 
	 * Finally print the statistics and reset all list, statistics and temp values
	 * 
	 * @throws MendixReplicationException
	 */
	public void finish() throws MendixReplicationException {
		try {
			this.startProcessingObjects();
			this.removeUnchangedObjects(this.settings.getMainObjectConfig());
			this.objectStats.printFinalStatistics();
			this.objectStats.printNotFoundMessages();
		}
		catch( Exception e ) {
			throw new MendixReplicationException(e.getMessage(), MetaInfo._version, e);
		}
	}

	public void clear() {
		if ( this.meta_Objects.size() > 0 )
			MILogNode.Replication_MetaInfo
					.getLogger()
					.error("Illegal State, something went wrong with committing the meta infor objects, there are still " + this.meta_Objects.size() + " objects left to commit.");

		this.lastKey = null;
		this.lastObject = null;

		this.objectStats.clear();
	}

	public enum MILogNode {
		Replication_MetaInfo,
		Replication_MetaInfo_AssociatedObjects,
		Replication_MetaInfo_MainObject;

		private ILogNode logNode;

		private MILogNode() {
			this.logNode = Core.getLogger(this.toString());
		}

		public ILogNode getLogger() {
			return this.logNode;
		}
	}

	public Object getObjectStats( Stat statEnum ) {
		return this.objectStats.getObjectStats(statEnum);
	}

	protected ReplicationSettings getSettings() {
		return this.settings;
	}

	protected ObjectStatistics getObjectStats() {
		return this.objectStats;
	}

	protected ValueParser getValueParser() {
		return this.valueParser;
	}

}
