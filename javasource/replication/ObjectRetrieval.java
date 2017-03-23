package replication;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import replication.MetaInfo.MILogNode;
import replication.MetaInfo.MetaInfoObject;
import replication.ReplicationSettings.ObjectSearchAction;
import replication.ReplicationSettings.MendixReplicationException;
import replication.ReplicationSettings.MendixReplicationRuntimeException;
import replication.helpers.OQLBuilder;

import com.mendix.core.Core;
import com.mendix.systemwideinterfaces.connectionbus.data.IDataRow;
import com.mendix.systemwideinterfaces.connectionbus.data.IDataTable;
import com.mendix.systemwideinterfaces.core.IMendixIdentifier;
import com.mendix.systemwideinterfaces.core.IMendixObject;

public class ObjectRetrieval extends Thread {

	public enum Type {
		Main,
		Association
	}

	private HashMap<String, MetaInfoObject> meta_Objects;
	private ReplicationSettings settings;
	private replication.helpers.UncaughtExceptionHandler ueHandler;
	private ValueParser valueParser;
	private Type type;
	private String objectIdentifier;
	private Collection<MetaInfoObject> associationValues;
	private HashMap<String, Map<String, MetaInfoObject>> associationResultList;

	public ObjectRetrieval( MetaInfo mInfo ) {
		this.settings = mInfo.getSettings();
		this.valueParser = mInfo.getValueParser();

		this.ueHandler = new replication.helpers.UncaughtExceptionHandler();
		this.setUncaughtExceptionHandler(this.ueHandler);
	}

	public replication.helpers.UncaughtExceptionHandler getUeHandler() {
		return this.ueHandler;
	}

	public void startMainObjectSyncDetails( HashMap<String, MetaInfoObject> meta_Objects, String objectType ) {
		this.type = Type.Main;
		this.meta_Objects = meta_Objects;
		this.objectIdentifier = objectType;

		this.start();
	}

	public void startAssociatedObjectSyncDetails( Collection<MetaInfoObject> values, HashMap<String, Map<String, MetaInfoObject>> resultList ) {
		this.type = Type.Association;
		this.associationValues = values;
		this.associationResultList = resultList;

		this.start();
	}

	@Override
	public void run() {
		switch (this.type) {
		case Association:
			this.populateAssociatedObjects(this.associationValues, this.associationResultList);
			break;

		case Main:
			this.retrieveMxObjectsByMetaInfoObject(this.meta_Objects, this.objectIdentifier);
			break;
		}
	}


	/**
	 * Analyse the MetaInfoObject create an OQL query based on this MetaData
	 * The result from the OQL query will be stored directly into their respective MetaInfoObject
	 * 
	 * @param meta_Objects
	 * @param objectIdentifier
	 */
	private void retrieveMxObjectsByMetaInfoObject( Map<String, MetaInfoObject> meta_Objects, String objectIdentifier ) {
		if ( meta_Objects != null && meta_Objects.size() > 0 ) {
			try {
				int limit = this.settings.Configuration.RetrieveOQL_Limit, totalSize = meta_Objects.size(), offset = 0;
				boolean isAssociation = this.settings.objectIdIsAssociation(objectIdentifier);

				while( offset < totalSize ) {
					StringBuilder builder = OQLBuilder.buildOQLQuery(this.settings, meta_Objects, objectIdentifier, isAssociation, limit, offset, this.valueParser);
					offset += limit;
					if ( builder.length() > 0 ) {
						IDataTable table;
						try {
							table = Core.retrieveOQLDataTable(this.settings.getContext(), builder.toString());
						}
						catch( Exception e ) {
							throw new MendixReplicationRuntimeException("Unable to execute the OQL query for objects: " + objectIdentifier + " range: " + offset + "/" + limit + " because of error: " + e.getMessage(), e);
						}

						builder.delete(0, builder.length());
						builder = null;

						// When there is any information in the DataSet do some stuff to analyse the objects
						int nrOfRows = table.getRowCount();
						if ( nrOfRows > 0 ) {

							if ( MILogNode.Replication_MetaInfo.getLogger().isDebugEnabled() )
								MILogNode.Replication_MetaInfo.getLogger()
										.debug("MetaInfo - Retrieved id's by the oql query: " + table.getRowCount());

							/*
							 * There is a possiblity the user configured a template with a key that isn't unique
							 * The module will pick one of the duplicates at random.
							 * 
							 * If the module receives more than double then nr of expected objects there is definitely
							 * something wrong and start printing all dubplicate entries
							 */
							boolean toManyObjectsRetrieved = false;
							if ( nrOfRows > limit * 2 ) {
								MILogNode.Replication_MetaInfo
										.getLogger()
										.warn("Found more than the expected nr of associated objects, limit: " + limit + " retrieved objects: " + nrOfRows + " for the main object.  Please make sure the key for this association identifies a unique object.");
								toManyObjectsRetrieved = true;
							}

							Iterator<? extends IDataRow> rowIter = table.getRows().iterator();

							List<IMendixIdentifier> idList = new ArrayList<IMendixIdentifier>();
							IDataRow row;
							while( rowIter.hasNext() ) {

								// Update the idlist so we can retrieve the specific objects in batches
								for( int i = 0; i < this.settings.Configuration.RetrieveById_Limit && rowIter.hasNext(); i++ ) {
									row = rowIter.next();
									IMendixIdentifier id = (IMendixIdentifier) row.getValue(this.settings.getContext(), 0);
									idList.add(id);
								}

								// If there still are id's to retrieve send the id's to the Core
								if ( idList.size() > 0 ) {
									TreeMap<String, Boolean> keys = this.settings.getObjectKeys(objectIdentifier);
									List<IMendixObject> result;
									try {
										result = Core.retrieveIdList(this.settings.getContext(), idList);
									}
									catch( Exception e ) {
										throw new MendixReplicationRuntimeException("Unable to retrieve the id's from the database after executing the OQL query for object: " + objectIdentifier + " range: " + offset + "/" + limit + " because of error: " + e.getMessage(), e);
									}

									String key;
									MetaInfoObject miObj;
									for( IMendixObject mxObject : result ) {
										key = this.valueParser.buildObjectKey(keys, mxObject, isAssociation);
										miObj = meta_Objects.get(key);

										if ( toManyObjectsRetrieved )
											MILogNode.Replication_MetaInfo.getLogger().debug("Retrieved object for main object with key: " + key);

										if ( miObj != null )
											miObj.setObject(mxObject, false);
										else
											throw new MendixReplicationRuntimeException("Invalid object key located, retrieved all objects for object: " + objectIdentifier + " but found an object with an unexpected key: " + key, MetaInfo._version);
									}
									result.clear();
								}
								idList.clear();
							}
						}
						else if ( MILogNode.Replication_MetaInfo.getLogger().isDebugEnabled() )
							MILogNode.Replication_MetaInfo.getLogger().debug("MetaInfo - No objects were found by the oql query");
					}
				}
			}
			catch( MendixReplicationRuntimeException e ) {
				throw e;
			}
			catch( Exception e ) {
				throw new MendixReplicationRuntimeException(e.getMessage(), MetaInfo._version, e);
			}
		}
	}


	/**
	 * Try to find all associated objects by the supplied Collection of all MetaInfoObjects.
	 * 
	 * First the return map will be prepared, and empty records for all possible associations will be added to the map
	 * After the map is prepared the funcion fillAssociationEntry will be executed which provides the returnmap with all
	 * relevant MxObjects
	 * 
	 * @return a map containing all association and with a map in the value sorted by the object keys
	 * @throws MendixReplicationException
	 */
	private void populateAssociatedObjects( Collection<MetaInfoObject> metaInfoObjectsList, HashMap<String, Map<String, MetaInfoObject>> associationResultList ) {
		try {
			// Create a map that contains all unique associated metainfoObjects
			for( MetaInfoObject obj : metaInfoObjectsList ) {
				// Only evaluate all the reference values if it contains at least one entry
				if ( obj.isAnyReferenceSet() ) {
					for( Entry<String, List<MetaInfoObject>> entry : obj.getReferenceSetValues().entrySet() ) {
						for( MetaInfoObject curMIObject : entry.getValue() ) {
							Map<String, MetaInfoObject> values;

							if ( !associationResultList.containsKey(entry.getKey()) ) {
								values = new LinkedHashMap<String, MetaInfoObject>();
								associationResultList.put(entry.getKey(), values);
							}
							else
								values = associationResultList.get(entry.getKey());

							if ( !values.containsKey(curMIObject.getObjectKey()) )
								values.put(curMIObject.getObjectKey(), curMIObject);
						}
					}


					for( Entry<String, MetaInfoObject> entry : obj.getReferenceValues().entrySet() ) {
						Map<String, MetaInfoObject> values;
						if ( !associationResultList.containsKey(entry.getKey()) ) {
							values = new LinkedHashMap<String, MetaInfoObject>();
							associationResultList.put(entry.getKey(), values);
						}
						else
							values = associationResultList.get(entry.getKey());

						MetaInfoObject curMIObject = entry.getValue();
						if ( !values.containsKey(curMIObject.getObjectKey()) )
							values.put(curMIObject.getObjectKey(), curMIObject);
					}
				}
			}
			metaInfoObjectsList.clear();

			for( Entry<String, AssociationConfig> entry : this.settings.getAssociationConfigMap().entrySet() ) {
				if ( entry.getValue().getObjectSearchAction() != ObjectSearchAction.CreateEverything ) {
					this.retrieveMxObjectsByMetaInfoObject(associationResultList.get(entry.getKey()), entry.getKey());
				}
			}
		}
		catch( MendixReplicationException e ) {
			throw new MendixReplicationRuntimeException(e);
		}
	}

}
