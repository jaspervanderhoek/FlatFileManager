package replication;

import java.util.TreeMap;

import replication.ReplicationSettings.ChangeTracking;
import replication.ReplicationSettings.MendixReplicationException;
import replication.ReplicationSettings.ObjectSearchAction;

import com.mendix.core.Core;
import com.mendix.core.CoreException;
import com.mendix.core.CoreRuntimeException;
import com.mendix.systemwideinterfaces.core.meta.IMetaObject;
import com.mendix.systemwideinterfaces.core.meta.IMetaPrimitive;
import com.mendix.systemwideinterfaces.core.meta.IMetaPrimitive.PrimitiveType;

public class ObjectConfig {
	
	private ObjectSearchAction handling = ObjectSearchAction.FindIgnore;

	/**
	 * All MemberNames that are used as a keys
	 */
	private TreeMap<String, Boolean> keys;
	private TreeMap<String, String> members;
	private String objectType = null;

	private boolean commitUnchangedObjects = false;
	private boolean ignoreEmptyKeys = false;
	private boolean printNotFoundMessages = false;
	
	protected IMetaObject metaObject;
	
	/**
	 * The attribute that indicates whether or not the object should be removed
	 */
	private String removeIndicator  = null;
	
	public Integer currentRemoveIndicatorValue = null;
	public Integer newRemoveIndicatorValue = null;
	/**
	 * Should unused (unsynced) objects be removed after the replication is completed, this is check with the removeIndicator
	 */
	private ChangeTracking trackOption = ChangeTracking.Nothing;
	private Boolean includeRemoveIndicatorAttribute = null;
	
	
	public ObjectConfig(String objectType) throws MendixReplicationException {
		this.objectType = objectType;
		this.members = new TreeMap<String, String>();
		this.keys = new TreeMap<String, Boolean>();

		this.metaObject = Core.getMetaObject(objectType);

		if( this.metaObject == null )
			throw new MendixReplicationException("The objectType: " + objectType + " does not exist.", MetaInfo._version);
	}


	/**
	 * @return The type of the object that has to be created
	 */
	public String getObjectType() {
		return this.objectType;
	}

	/**
	 * Retrieve how the association should be processed (create, find, ignore)
	 * 
	 * @DEFAULT: FindIgnore
	 */
	public ObjectSearchAction getObjectSearchAction() {
		return this.handling;
	}

	/**
	 * Set how the association should be processed (create, find, ignore)
	 * 
	 * @DEFAULT: FindIgnore
	 * @param handling
	 */
	public ObjectConfig setObjectSearchAction(ObjectSearchAction handling) {
		if (handling == null)
			throw new CoreRuntimeException("The association handling may not be set to empty, this happened for object: " + this.objectType);

		this.handling = handling;
		
		return this;
	}

	public String getMemberByAlias(String columnAlias) {
		return this.members.get(columnAlias);
	}

	/**
	 * Is the specified alias a key column for the 'Main' object
	 * @param alias
	 * @return true if the member is a key column
	 */
	public boolean isKey(String alias) {
		return this.keys.containsKey(alias);
	}

	public TreeMap<String, Boolean> getKeys() {
		return this.keys;
	}

	public boolean hasNonKeyMembers() {
		return this.keys.size() < this.members.size();
	}
	public boolean hasKeyMembers() {
		return this.keys.size() > 0;
	}

	public boolean isCaseSensitive(String alias) {
		Boolean caseSensitiveBool = this.keys.get(alias);
		if (caseSensitiveBool != null)
			return caseSensitiveBool;

		return false;
	}

	public void addMember(String columnAlias, String memberName, boolean isKey, Boolean isCaseSensitive) {
		this.members.put(columnAlias, memberName);

		if (isKey)
			this.keys.put(columnAlias, isCaseSensitive);
	}

	public boolean containsKeyMember(String attributeName) {
		for (String key : this.keys.keySet()) {
			if (attributeName.equals(this.members.get(key)))
				return true;
		}

		return false;
	}


	public ObjectConfig setCommitUnchangedObjects(Boolean commitUnchangedObjects) throws MendixReplicationException {
		this.commitUnchangedObjects = commitUnchangedObjects;
		
		if( this.trackOption != ChangeTracking.Nothing && !this.commitUnchangedObjects() && this.getObjectSearchAction() == ObjectSearchAction.FindCreate ) {
			throw new MendixReplicationException("The combination ImportAction: Synchronize / RemoveAction: " + this.trackOption + " without committing unchanged objects is not allowed.");
		}
		
		return this;
	}

	public boolean commitUnchangedObjects() {
		return this.commitUnchangedObjects;
	}

	public ObjectConfig setIgnoreEmptyKeys(boolean ignoreEmptyKeys) {
		this.ignoreEmptyKeys = ignoreEmptyKeys;
		
		return this;
	}

	/**
	 * If any of the keys is null, should the object be ignore.
	 * @return default TRUE
	 */
	public boolean ignoreEmptyKeys() {
		return this.ignoreEmptyKeys;
	}
	public ObjectConfig setPrintNotFoundMessages(boolean printNotFound) {
		this.printNotFoundMessages = printNotFound;
		
		return this;
	}
	public boolean printNotFoundMessages() {
		return this.printNotFoundMessages;
	}


	public ChangeTracking getChangeTracking() {
		return this.trackOption;
	}


	public String getRemoveIndicator() {
		return this.removeIndicator;
	}
	
	/**
	 * Remove any unused objects after replicating.
	 * The removeIndicator should be a integer with a empty default value
	 * 
	 * @param removeIndicator
	 * @throws CoreException
	 */
	public ObjectConfig removeUnusedObjects( ChangeTracking trackOption, String removeIndicator ) throws MendixReplicationException {
		try {
			this.trackOption = trackOption;
			if( this.trackOption != ChangeTracking.Nothing ) {
				if( removeIndicator != null ){
					IMetaPrimitive prim = this.metaObject.getMetaPrimitive(removeIndicator);
					if( prim.getType() == PrimitiveType.Integer ) {
						this.removeIndicator = removeIndicator;
					}
					else {
						throw new MendixReplicationException("The removeIndicator should be of type: " + PrimitiveType.Integer + " but is of type: " + prim.getType(), MetaInfo._version);
					}
				}
				else {
					throw new MendixReplicationException("The removeIndicator does is not specified, there should be an attribute of the type: " + PrimitiveType.Integer + " in metaobject: " + this.getObjectType(), MetaInfo._version );
				}
				
				if( !this.commitUnchangedObjects() && this.getObjectSearchAction() == ObjectSearchAction.FindCreate ) {
					throw new MendixReplicationException("The combination ImportAction: Synchronize / RemoveAction: " + this.trackOption + " without committing unchanged objects is not allowed.");
				}
			}

		}catch (IllegalArgumentException e) {
			throw new MendixReplicationException( "The removeIndicator does not exists, there should be an attribute: " + removeIndicator + " of the type: " + PrimitiveType.Integer + " in metaobject: " + this.getObjectType(), MetaInfo._version );
		}
		
		return this;
	}
	
	public boolean useRemoveIndicatorAttribute() {
		if( this.includeRemoveIndicatorAttribute == null  )  {
			this.includeRemoveIndicatorAttribute = 
					(this.getObjectSearchAction() != ObjectSearchAction.CreateEverything &&
					 this.getObjectSearchAction() != ObjectSearchAction.OnlyCreateNewObjects &&
					(this.trackOption == ChangeTracking.RemoveUnchangedObjects || this.trackOption == ChangeTracking.TrackChanges));
		}
		
		if( !(this instanceof AssociationConfig) ) {
			return this.includeRemoveIndicatorAttribute ;
		}
		
		return false;	
	}


	public Integer getNewRemoveIndicatorValue() {
		return this.newRemoveIndicatorValue;
	}
	public Integer getCurrentRemoveIndicatorValue() {
		return this.currentRemoveIndicatorValue;
	}
	public void setNewRemoveIndicatorValue( Integer newRemoveIndicatorValue ) {
		this.newRemoveIndicatorValue = newRemoveIndicatorValue;
	}
	public void setCurrentRemoveIndicatorValue( Integer currentRemoveIndicatorValue ) {
		this.currentRemoveIndicatorValue = currentRemoveIndicatorValue;
	}
}
