package replication;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.TreeSet;

import replication.MetaInfo.MILogNode;
import replication.helpers.MessageOptions.Language;
import replication.implementation.ErrorHandler;
import replication.implementation.InfoHandler;
import replication.interfaces.IErrorHandler;
import replication.interfaces.IInfoHandler;
import replication.interfaces.IInfoHandler.StatisticsLevel;
import replication.interfaces.IUnknownObjectHandler;
import replication.interfaces.IValueParser;

import com.mendix.core.Core;
import com.mendix.core.CoreException;
import com.mendix.core.CoreRuntimeException;
import com.mendix.systemwideinterfaces.core.IContext;
import com.mendix.systemwideinterfaces.core.IMendixIdentifier;
import com.mendix.systemwideinterfaces.core.IMendixObject;
import com.mendix.systemwideinterfaces.core.meta.IMetaObject;
import com.mendix.systemwideinterfaces.core.meta.IMetaPrimitive;
import com.mendix.systemwideinterfaces.core.meta.IMetaPrimitive.PrimitiveType;


/**
 * This class contains all specific settings that can be used in the replication DataManager
 * When this object is created, database settings must be predefined, the object type must be set and the current action
 * context must be set
 * The complete column mapping must be set and
 * 
 * @author Mendix - Jasper van der Hoek
 * @version $Id: ReplicationSettings.java 9272 2009-05-11 09:19:47Z Jasper van der Hoek $
 */
public abstract class ReplicationSettings {

	public class Configuration {

		/**
		 * VALUE: 200 Nr of objects which are to be retrieved in one RetrieveById action, This occurs after an OQL query
		 * has been executed
		 */
		public int RetrieveById_Limit = 200;
		public int RetrieveOQL_Limit = 1000;
		/** VALUE: 200 Nr of objects which are to be retrieved in a single query and removed directly after the retrieve */
		public int RetrieveToBeRemovedObjectsXPath_Limit = 200;
		/** VALUE: 1000 Threshold after which a new Thread is started to process the imported objects */
		public int MetaInfoProcessingBatchThreshold = 1000;

		public boolean RetrieveObjectsAsync = false;

		public void calculateOQLRetrieveLimit( int keySize ) {

			int result = Long.valueOf(Math.round(Math.floor((2100 / keySize) * 0.8))).intValue();
			if ( result < this.RetrieveOQL_Limit ) {
				this.RetrieveOQL_Limit = result;

				if ( MILogNode.Replication_MetaInfo.getLogger().isDebugEnabled() )
					MILogNode.Replication_MetaInfo.getLogger().debug("Changing the retrieve limit to: " + result);
			}
		}
	}

	protected ObjectConfig mainObjectConfig;

	protected ReplicationSettings( IContext context, String objectType, IErrorHandler errorHandler ) throws MendixReplicationException {
		this.mainObjectConfig = new ObjectConfig(objectType);
		this.mainObjectConfig.setIgnoreEmptyKeys(true);

		this.resetEmptyAssociations = false;

		this.context = context;
		this.metaObject = Core.getMetaObject(objectType);
		this.errorHandler = errorHandler;
		if ( this.errorHandler == null )
			this.errorHandler = new ErrorHandler();
		this.infoMessageHandler = new InfoHandler("DataProcessor");

		if ( this.metaObject == null )
			throw new MendixReplicationException("The objectType: " + objectType + " does not exist.", MetaInfo._version);

	}

	public final Configuration Configuration = new Configuration();
	private boolean resetEmptyAssociations;
	private Language lang = Language.ENG;

	/**
	 * Whenever an exception is thrown this class will process that exception
	 */
	private IErrorHandler errorHandler;

	private boolean importInNewContext = false;
	private boolean useTransactions = true;

	private Boolean printAnyNotFoundMessage = null;
	private StatisticsLevel printStatistics = StatisticsLevel.AllStatistics;

	private IContext context;
	protected TreeMap<String, IValueParser> valueParsers = new TreeMap<String, IValueParser>();

	// This map holds the display masks, this is currently only used for the excel importer, but can also be used for Db
	// rep
	protected TreeMap<String, String> displayMasks = new TreeMap<String, String>();

	private IMendixIdentifier parentObjectId = null;
	private String parentObjectAssociation = null;
	protected IMetaObject metaObject;

	protected List<String> aliasList = new ArrayList<String>();
	protected Map<String, IMetaPrimitive> memberInfo = new TreeMap<String, IMetaPrimitive>();	// Alias,
																								// memberName

	private Map<String, AssociationConfig> associationConfigs = new TreeMap<String, AssociationConfig>(); // AssociationName,
																											// Config
	private Map<String, String> associationNames = new TreeMap<String, String>();			// ColumnAlias,
	// AssociationName
	private IInfoHandler infoMessageHandler;
	private TreeMap<String, String> defaultInputMasks = new TreeMap<String, String>();


	public enum ObjectSearchAction {
		FindCreate,
		FindIgnore,
		CreateEverything,
		OnlyCreateNewObjects
	}

	public enum AssociationDataHandling {
		Overwrite,
		Append
	}

	public enum KeyType {
		NoKey,
		AssociationKey,
		ObjectKey,
		AssociationAndObjectKey
	}

	public enum ChangeTracking {
		Nothing,
		TrackChanges,
		RemoveUnchangedObjects
	}

	public static class MendixReplicationException extends CoreException {

		private static final long serialVersionUID = 1123455468734555L;

		public MendixReplicationException() {
			super();
		}

		public MendixReplicationException( Throwable e ) {
			super(e);
		}

		public MendixReplicationException( String msg, String version ) {
			super((msg != null && msg.contains("| VERSION") ? msg : msg + " | VERSION: " + version));
		}

		public MendixReplicationException( String msg ) {
			super(msg);
		}

		public MendixReplicationException( String msg, String version, Throwable e ) {
			super((msg != null && msg.contains("| VERSION") ? msg : msg + " | VERSION: " + version), e);
		}

		public MendixReplicationException( String msg, Throwable e ) {
			super(msg, e);
		}
	}

	public static class MendixReplicationRuntimeException extends CoreRuntimeException {

		private static final long serialVersionUID = 1123455468734555L;

		public MendixReplicationRuntimeException( Throwable e ) {
			super(e);
		}

		public MendixReplicationRuntimeException( String msg, String version ) {
			super((msg != null && msg.contains("| VERSION") ? msg : msg + " | VERSION: " + version));
		}

		public MendixReplicationRuntimeException( String msg ) {
			super(msg);
		}

		public MendixReplicationRuntimeException( String msg, String version, Throwable e ) {
			super((msg != null && msg.contains("| VERSION") ? msg : msg + " | VERSION: " + version), e);
		}

		public MendixReplicationRuntimeException( String msg, Throwable e ) {
			super(msg, e);
		}
	}

	public boolean treatFieldAsReference( String alias ) {
		if ( this.associationNames.containsKey(alias) ) {

			String associationName = this.associationNames.get(alias);
			if ( this.associationConfigs.containsKey(associationName) ) {
				return !this.associationConfigs.get(associationName).actAsReferenceSet();
			}
		}

		return false;
	}

	public boolean treatFieldAsReferenceSet( String alias ) {
		if ( this.associationNames.containsKey(alias) ) {

			String associationName = this.associationNames.get(alias);
			if ( this.associationConfigs.containsKey(associationName) ) {
				return this.associationConfigs.get(associationName).actAsReferenceSet();
			}
		}

		return false;
	}

	public boolean objectIdIsAssociation( String objectIdentifier ) {
		if ( this.mainObjectConfig.getObjectType().equals(objectIdentifier) )
			return false;

		return true;
	}

	public String getMemberNameByAlias( String alias ) throws MendixReplicationException {
		if ( !this.memberInfo.containsKey(alias) )
			throw new MendixReplicationException("The given alias isn't a valid member for object: " + this.mainObjectConfig.getObjectType() + ", the given alias was: " + alias, MetaInfo._version);

		return this.memberInfo.get(alias).getName();
	}

	/**
	 * Associations use a different alias, try to get the name of the association by this alias
	 * If the given alias isn't a association an Exception will be thrown.
	 * 
	 * @param columnAlias
	 * @return the name of the association
	 * @throws CoreException
	 */
	public String getAssociationNameByAlias( String columnAlias ) throws MendixReplicationException {
		if ( !this.associationNames.containsKey(columnAlias) )
			throw new MendixReplicationException("The given alias isn't an association, the given columname was: " + columnAlias, MetaInfo._version);

		return this.associationNames.get(columnAlias);
	}

	public String getAssociationObjectTypeByAlias( String columnAlias ) throws MendixReplicationException {
		if ( !this.associationNames.containsKey(columnAlias) )
			throw new MendixReplicationException("The given alias isn't an association, the given columname was: " + columnAlias, MetaInfo._version);
		String associationName = this.associationNames.get(columnAlias);

		return this.associationConfigs.get(associationName).getObjectType();
	}

	/**
	 * Associations use a different alias, try to get the name of the member in the associated MetaObject
	 * If the given alias isn't a association an Exception will be thrown.
	 * 
	 * @param columnAlias
	 * @return the name of a member in the associated MetaObject
	 * @throws CoreException
	 */
	public String getAssociationColumnByAlias( String columnAlias ) throws MendixReplicationException {
		if ( !this.associationNames.containsKey(columnAlias) )
			throw new MendixReplicationException("The given alias isn't an association, the given columname was: " + columnAlias, MetaInfo._version);
		String associationName = this.associationNames.get(columnAlias);

		return this.associationConfigs.get(associationName).getMemberByAlias(columnAlias);
	}


	public boolean isAssociationKey( String associationName, String alias ) {
		if ( this.associationConfigs.containsKey(associationName) )
			return this.associationConfigs.get(associationName).isKey(alias);

		return false;
	}

	public TreeMap<String, Boolean> getObjectKeys( String objectIdentifier ) {
		if ( this.objectIdIsAssociation(objectIdentifier) )
			return this.getAssociationKeys(objectIdentifier);
		else
			return this.mainObjectConfig.getKeys();
	}

	public TreeMap<String, Boolean> getAssociationKeys( String associationName ) {
		if ( this.associationConfigs.containsKey(associationName) )
			return this.associationConfigs.get(associationName).getKeys();

		return null;
	}

	public ObjectConfig getObjectConfiguration( String objectID ) {
		if ( this.mainObjectConfig.getObjectType().equals(objectID) )
			return this.mainObjectConfig;
		else
			return this.getAssociationConfig(objectID);
	}

	public AssociationConfig getAssociationConfig( String associationName ) {
		if ( !this.associationConfigs.containsKey(associationName) )
			throw new MendixReplicationRuntimeException("The association: " + associationName + " has not been specified in the configuration. Config Details: (" + this.associationConfigs.keySet() + ")", MetaInfo._version);

		return this.associationConfigs.get(associationName);
	}

	public Map<String, AssociationConfig> getAssociationConfigMap() {
		return this.associationConfigs;
	}

	/**
	 * Set the handler class for processing unknown associated objects
	 * 
	 * @param associationName
	 * @param handler
	 * @throws MendixReplicationException
	 */
	public void setUnkownAssociationHandler( String objectType, String associationName, IUnknownObjectHandler handler ) throws MendixReplicationException {
		if ( !this.associationConfigs.containsKey(associationName) )
			this.associationConfigs.put(associationName, new AssociationConfig(objectType, associationName));

		this.associationConfigs.get(associationName).setUnkownAssociationHandler(handler);
	}

	public IUnknownObjectHandler getUnkownAssociationHandler( String associationName ) {
		if ( this.associationConfigs.containsKey(associationName) )
			return this.associationConfigs.get(associationName).getUnkownAssociationHandler();

		return null;
	}

	public boolean isCaseSensitive( String alias ) {
		boolean isCaseSensitive = true;
		if ( this.treatFieldAsReference(alias) || this.treatFieldAsReferenceSet(alias) ) {
			isCaseSensitive = this.associationConfigs.get(this.associationNames.get(alias)).isCaseSensitive(alias);
		}
		else {
			Boolean caseSensitiveBool = this.mainObjectConfig.getKeys().get(alias);
			if ( caseSensitiveBool != null ) {
				isCaseSensitive = caseSensitiveBool;
			}
		}

		return isCaseSensitive;
	}

	public IContext getContext() {
		return this.context;
	}

	public void setContext( IContext newContext ) {
		this.context = newContext;
	}

	/**
	 * Default the DataManager uses a new context for each run
	 * 
	 * @param useNewContext
	 */
	public void importInNewContext( boolean useNewContext ) {
		this.importInNewContext = useNewContext;
	}

	/**
	 * Should the DataManager create a new Context when importing.
	 * 
	 * @return, default TRUE
	 */
	public boolean importInNewContext() {
		return this.importInNewContext;
	}

	public void setErrorHandler( IErrorHandler errorHandler ) {
		this.errorHandler = errorHandler;
	}

	public IErrorHandler getErrorHandler() {
		return this.errorHandler;
	}

	public PrimitiveType getMemberType( String columnName ) {
		IMetaPrimitive prim = this.memberInfo.get(columnName);
		if ( prim == null )
			throw new MendixReplicationRuntimeException("Invalid configuration, column: " + columnName + " cannot be found in the configuration. Config Details: (" + this.memberInfo.keySet() + ")", MetaInfo._version);

		return prim.getType();
	}

	public TimeZone getTimeZoneForMember( String columnName ) {

		if ( this.memberInfo.get(columnName).shouldLocalizeDate() ) {
			TimeZone tz = this.getContext().getSession().getTimeZone();
			if ( tz != null )
				return tz;
		}

		return TimeZone.getTimeZone("UTC");
	}

	public void setInfoHandler( IInfoHandler handler ) {
		this.infoMessageHandler = handler;

		this.getInfoHandler().setStatisticLogLevel(this.printStatistics);
	}

	public IInfoHandler getInfoHandler() {
		return this.infoMessageHandler;
	}

	public void ignoreEmptyKeys( boolean ignore ) {
		this.mainObjectConfig.setIgnoreEmptyKeys(ignore);
		for( AssociationConfig config : this.associationConfigs.values() )
			config.setIgnoreEmptyKeys(ignore);
	}


	public boolean resetEmptyAssociations() {
		return this.resetEmptyAssociations;
	}

	public void resetEmptyAssociations( boolean reset ) {
		this.resetEmptyAssociations = reset;
	}

	/**
	 * Return all the member names that are specified as keys, (not the alias)
	 * For all associated key columns the alias will be put in the map instead of the member name.
	 * 
	 * @return All member names that are specified as Key
	 * @throws MendixReplicationException
	 */
	public TreeSet<String> getKeyMembers() throws MendixReplicationException {
		TreeSet<String> keyMemberSet = new TreeSet<String>();
		for( String keyAlias : this.mainObjectConfig.getKeys().keySet() ) {
			if ( this.treatFieldAsReference(keyAlias) || this.treatFieldAsReferenceSet(keyAlias) )
				keyMemberSet.add(keyAlias);
			else
				keyMemberSet.add(this.getMemberNameByAlias(keyAlias));
		}

		return keyMemberSet;
	}

	public void setLanguage( Language lang ) {
		this.lang = lang;
	}

	public Language getLanguage() {
		return this.lang;
	}

	public String getParentAssociation() {
		return this.parentObjectAssociation;
	}

	public void setParentAssociation( String associationName ) {
		this.parentObjectAssociation = associationName;
	}

	public IMendixIdentifier getParentObjectId() {
		return this.parentObjectId;
	}

	public void setParentObjectId( IMendixObject parentObject ) throws MendixReplicationException {
		if ( this.parentObjectAssociation != null && !"".equals(this.parentObjectAssociation) ) {
			if ( parentObject != null )
				this.parentObjectId = parentObject.getId();
			else {
				this.parentObjectId = null;
				MILogNode.Replication_MetaInfo.getLogger().warn(
						"There is a parent association configured (" + this.parentObjectAssociation + ") but no parent object provided.");
			}
		}
		else
			throw new MendixReplicationException("There is no parent association configured so no parent object can be added", MetaInfo._version);
	}

	public int getMaxRetrieveSizeOQL() {
		return 1000;
	}

	/**
	 * @return the total number of specified columns
	 */
	public int getNrOfColumns() {
		return this.aliasList.size();
	}

	/**
	 * Should transactions be used when importing
	 * Default is TRUE
	 * 
	 * @param useTransactions
	 */
	public void useTransactions( boolean useTransactions ) {
		this.useTransactions = useTransactions;
	}

	/**
	 * Should transactions be used when importing
	 * 
	 * @return Default is TRUE
	 */
	public boolean useTransactions() {
		return this.useTransactions;
	}

	public Map<String, IValueParser> getValueParsers() {
		return this.valueParsers;
	}

	public boolean hasValueParser( String column ) {
		return this.valueParsers.containsKey(column);
	}

	/**
	 * Should the statistics at the end of the import be printed in the log file.
	 * 
	 * @param printStatistics
	 */
	public void printImportStatistics( StatisticsLevel printStatistics ) {
		this.printStatistics = printStatistics;
		this.getInfoHandler().setStatisticLogLevel(this.printStatistics);
	}

	public boolean printAnyNotFoundMessage() {
		if ( this.printAnyNotFoundMessage == null ) {
			this.printAnyNotFoundMessage = this.mainObjectConfig.printNotFoundMessages();
			for( AssociationConfig config : this.associationConfigs.values() ) {
				if ( config.printNotFoundMessages() ) {
					this.printAnyNotFoundMessage = true;
					break;
				}
			}
		}
		
		return this.printAnyNotFoundMessage;
	}

	/**
	 * Add a new mapping for a column in the external database to a Member in the MxDatabase.
	 * Just set the table where the member is located in. That table can be de default from table, or it can be any
	 * other joined table.
	 * Set the membername where the column should be mapped to, this memberName must be a member in the MetaObject type
	 * which was specified in the constructor.
	 * Also specify if this column is an key, when true the DataManager will use the value from this column to compare
	 * the MetaObjects with.
	 * 
	 * The Value parser given in this column will be called before the value from the column is stored in the
	 * MetaObject, the result from the ValueParser will be used to store.
	 * If the member does not exists in the specified MetaObject an exception will be thrown. Or when the tableName
	 * wasn't specified earlier, an exception will be thrown as well.
	 * 
	 * @param tableName, The name or alias from the table in the external DB
	 * @param columnAlias, Alias for the column
	 * @param memberName, The name of the member where the value should be stored in
	 * @param isKey, Is this member a key column, i.e. should the DataManager search for any other objects with this
	 *        value
	 * @param parser, The parser that is going to be used to change the value of this member before storing it
	 * @return
	 * @throws CoreException
	 * @throws CoreException
	 */
	protected ObjectConfig addMappingForAttribute( String columnAlias, String memberName, KeyType isKey, Boolean isCaseSensitive, IValueParser parser ) throws MendixReplicationException {
		if ( this.aliasList.contains(columnAlias) )
			throw new MendixReplicationRuntimeException("This column alias: " + columnAlias + " already exists in this configuration", MetaInfo._version);

		this.aliasList.add(columnAlias);

		IMetaPrimitive prim = this.metaObject.getMetaPrimitive(memberName);
		if ( prim == null )
			throw new MendixReplicationRuntimeException("There is not attribute: " + memberName + " found in metaobject " + this.mainObjectConfig.getObjectType());
		
		this.memberInfo.put(columnAlias, prim);
		
		// Make sure case sensitive is not null
		isCaseSensitive = (isCaseSensitive == null ? false : isCaseSensitive);
		if ( isKey != KeyType.NoKey )
			this.mainObjectConfig.getKeys().put(columnAlias, isCaseSensitive);
		else {
			if( "changedDate".equals(memberName) || "createdDate".equals(memberName) || prim.isVirtual() )
				throw new MendixReplicationRuntimeException("The column with alias: " + columnAlias + " is setup to write in attribute: " + memberName + " which is a readonly " + (prim.isVirtual() ? "Calculated" : "System" ) + " attribute.", MetaInfo._version);				
		}
			

		if ( parser != null )
			this.valueParsers.put(columnAlias, parser);

		return this.mainObjectConfig;
	}

	protected AssociationConfig addMappingForAssociation( String columnAlias, String associationName, String associatedObjectType, String memberName, IValueParser parser, KeyType isKey, Boolean isCaseSensitive ) throws MendixReplicationException {
		if ( this.aliasList.contains(columnAlias) )
			throw new MendixReplicationRuntimeException("This column alias: " + columnAlias + " is already mapped ", MetaInfo._version);

		this.aliasList.add(columnAlias);

		if ( parser != null )
			this.valueParsers.put(columnAlias, parser);

		if ( !this.associationConfigs.containsKey(associationName) )
			this.associationConfigs.put(associationName, new AssociationConfig(associatedObjectType, associationName));

		this.associationNames.put(columnAlias, associationName);
		AssociationConfig config = this.associationConfigs.get(associationName);

		// Make sure case sensitive is not null
		isCaseSensitive = (isCaseSensitive == null ? false : isCaseSensitive);
		switch (isKey) {
		case AssociationAndObjectKey:
			config.addMember(columnAlias, memberName, true, isCaseSensitive);
			this.mainObjectConfig.getKeys().put(columnAlias, isCaseSensitive);
			break;
		case AssociationKey:
			config.addMember(columnAlias, memberName, true, isCaseSensitive);
			break;
		case ObjectKey:
			this.mainObjectConfig.getKeys().put(columnAlias, isCaseSensitive);
			config.addMember(columnAlias, memberName, false, false);
			break;
		default:
			config.addMember(columnAlias, memberName, false, false);
			break;
		}
		config.validateObjectType(associatedObjectType);


		IMetaObject metaObject = Core.getMetaObject(associatedObjectType);
		if ( metaObject == null )
			throw new MendixReplicationRuntimeException("The metaobject could not be found " + associatedObjectType);

		IMetaPrimitive prim = metaObject.getMetaPrimitive(memberName);
		if ( prim == null )
			throw new MendixReplicationRuntimeException("There is not attribute: " + memberName + " found in metaobject " + metaObject.getName());

		this.memberInfo.put(columnAlias, prim);

		return config;
	}

	public void addDisplayMask( String alias, String mask ) {
		this.displayMasks.put(alias, mask);
	}

	public String getDisplayMask( String alias ) {
		return this.displayMasks.get(alias);
	}

	public Object hasDefaultInputMask( String column ) {
		return this.defaultInputMasks.containsKey(column);
	}

	public String getDefaultInputMask( String column ) {
		return this.defaultInputMasks.get(column);
	}

	public void addDefaultInputMask( String column, String mask ) {
		if ( mask != null )
			this.defaultInputMasks.put(column, mask);
	}

	public boolean aliasIsMapped( String alias ) {
		if ( this.aliasList.contains(alias) )
			return true;

		return false;
	}

	public void validateSettings() throws MendixReplicationException {
		/*
		 * Check all the reference configuration objects and validate if there is at least one association key
		 * configured
		 * unless the association has the object handling CreateEverything
		 */
		if ( this.associationConfigs.size() > 0 ) {
			String errorMessage = "";

			for( Entry<String, AssociationConfig> configEntry : this.associationConfigs.entrySet() ) {
				String associationName = configEntry.getKey();
				switch (configEntry.getValue().getObjectSearchAction()) {
				case CreateEverything:
					break;
				case FindCreate:
				case FindIgnore:
				case OnlyCreateNewObjects:
					int nrOfAssociationKeys = this.getAssociationKeys(associationName).size();
					if ( nrOfAssociationKeys == 0 ) {
						errorMessage += associationName + ", ";
					}
					break;
				}

			}

			if ( !"".equals(errorMessage) ) {
				throw new MendixReplicationException("There are no keys defined for association: " + errorMessage.substring(0,
						errorMessage.length() - 2), MetaInfo._version);
			}
		}

		for( String key : this.mainObjectConfig.getKeys().keySet() ) {
			if ( this.treatFieldAsReferenceSet(key) )
				throw new MendixReplicationException("Column " + key + " is configured as object key, this is configured to use association: " + this.getAssociationNameByAlias(key) + " and this association acts as reference set. Therefore it cannot be an set as object key.", MetaInfo._version);
		}
		
		if( this.mainObjectConfig.getObjectSearchAction() == ObjectSearchAction.OnlyCreateNewObjects && this.mainObjectConfig.commitUnchangedObjects() ) {
			MILogNode.Replication_MetaInfo.getLogger().warn("Invalid configuration, the import action 'Only create new objects' conflicts with the instruction to 'Commit unchanged objects', Ignoring this option and only committing new (changed) objects." );
		}
		
		

		if ( this.mainObjectConfig.getKeys().size() > 0 ) {
			this.Configuration.calculateOQLRetrieveLimit(this.mainObjectConfig.getKeys().size());
		}
		MILogNode.Replication_MetaInfo.getLogger().debug( "Replication settings initialized with [ ProcessBatch: " + this.Configuration.MetaInfoProcessingBatchThreshold + " | RetrieveId: " + this.Configuration.RetrieveById_Limit + " | RetrieveOQL: " + this.Configuration.RetrieveOQL_Limit + " | RemoveBatch: " + this.Configuration.RetrieveToBeRemovedObjectsXPath_Limit + " ] " );
		
	}


	public void clear() {
		this.associationConfigs.clear();
		this.associationNames.clear();
		this.aliasList.clear();
		this.mainObjectConfig.getKeys().clear();
		this.memberInfo.clear();
		this.valueParsers.clear();
		this.displayMasks.clear();
	}

	public ObjectConfig getMainObjectConfig() {
		return this.mainObjectConfig;
	}

}