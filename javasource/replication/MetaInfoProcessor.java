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
import replication.MetaInfo.MetaInfoObjectState;
import replication.ReplicationSettings.AssociationDataHandling;
import replication.ReplicationSettings.MendixReplicationException;
import replication.ReplicationSettings.MendixReplicationRuntimeException;
import replication.ReplicationSettings.ObjectSearchAction;
import replication.helpers.MessageOptions;
import replication.helpers.OQLBuilder;
import replication.helpers.ObjectStatistics;
import replication.helpers.ObjectStatistics.Stat;
import replication.interfaces.IUnknownObjectHandler;

import com.mendix.core.Core;
import com.mendix.core.CoreException;
import com.mendix.core.CoreRuntimeException;
import com.mendix.core.objectmanagement.member.MendixObjectReferenceSet;
import com.mendix.systemwideinterfaces.connectionbus.data.IDataRow;
import com.mendix.systemwideinterfaces.connectionbus.data.IDataTable;
import com.mendix.systemwideinterfaces.core.IContext;
import com.mendix.systemwideinterfaces.core.IMendixIdentifier;
import com.mendix.systemwideinterfaces.core.IMendixObject;
import com.mendix.systemwideinterfaces.core.IMendixObjectMember;

public class MetaInfoProcessor {

	private MetaInfo mInfo;
	private HashMap<String, MetaInfoObject> meta_Objects;
	private ReplicationSettings settings;

	private ObjectStatistics objectStats;
	private replication.helpers.UncaughtExceptionHandler ueHandler;
	private ObjectRetrieval[] objectRetrievalArray = new ObjectRetrieval[2];
	private ValueParser valueParser;

	public MetaInfoProcessor( MetaInfo mInfo, HashMap<String, MetaInfoObject> meta_Objects ) {
		this.meta_Objects = meta_Objects;
		this.settings = mInfo.getSettings();
		this.objectStats = mInfo.getObjectStats();
		this.mInfo = mInfo;
		this.valueParser = mInfo.getValueParser();

		this.ueHandler = new replication.helpers.UncaughtExceptionHandler();
	}

	public void startProcessing() throws MendixReplicationException {
		try {
			int numRecs = 0;
			if ( MILogNode.Replication_MetaInfo.getLogger().isTraceEnabled() ) {
				numRecs = this.meta_Objects.size();
				MILogNode.Replication_MetaInfo.getLogger().trace(
						"Starting MetaInfoProcessor, processing " + numRecs + " objects / " + this.toString());
			}
			storeChangedObjects();
			resetChangedObjects();

			if ( MILogNode.Replication_MetaInfo.getLogger().isTraceEnabled() )
				MILogNode.Replication_MetaInfo.getLogger()
						.trace("Finished MetaInfoProcessor, processed " + numRecs + " objects / " + this.toString());
		}
		catch( MendixReplicationException e ) {
			MILogNode.Replication_MetaInfo.getLogger().error(e);
			throw e;
		}
		// only create a new exception if we are not processing the MXRepRuntime exception (try to keep a small and
		// clear StackTrace)
		catch( Exception e ) {
			MILogNode.Replication_MetaInfo.getLogger().error(e);
			throw new MendixReplicationException(e);
		}
	}

	/**
	 * Validate all MetaInfoObjects, both the main object and the associated object. First tries to
	 * retrieve all MxObject and populates all MetaInfo objects with the retrieve objects. It
	 * validates which action to take with the MxObject and processes the object according to that
	 * action.
	 * 
	 * @throws MendixReplicationException
	 */
	private void storeChangedObjects() throws MendixReplicationException {

		this.mInfo.TimeMeasurement.startPerformanceTest("Storing Objects");
		HashMap<String, Map<String, MetaInfoObject>> associatedObjects = new LinkedHashMap<String, Map<String, MetaInfoObject>>();
		ObjectConfig objectConfig = this.settings.getMainObjectConfig();

		if ( this.settings.Configuration.RetrieveObjectsAsync ) {
			if ( objectConfig.getObjectSearchAction() != ObjectSearchAction.CreateEverything ) {
				this.objectRetrievalArray[0] = new ObjectRetrieval(this.mInfo);
				this.objectRetrievalArray[0].startMainObjectSyncDetails(this.meta_Objects, objectConfig.getObjectType());
			}

			/**
			 * Map with the association name as primary key, the submap contains all meta info
			 * objects, with the objectkey as key and the metainfo object as value
			 */
			this.objectRetrievalArray[1] = new ObjectRetrieval(this.mInfo);
			this.objectRetrievalArray[1].startAssociatedObjectSyncDetails(this.meta_Objects.values(), associatedObjects);

			checkRunningProcesses();
		}
		else {
			if ( objectConfig.getObjectSearchAction() != ObjectSearchAction.CreateEverything ) {
				this.retrieveMxObjectsByMetaInfoObject(this.meta_Objects, objectConfig.getObjectType());
			}

			/**
			 * Map with the association name as primary key, the submap contains all meta info
			 * objects, with the objectkey as key and the metainfo object as value
			 */
			this.populateAssociatedObjects(this.meta_Objects.values(), associatedObjects);
		}

		this.processAssociatedObjects(associatedObjects);

		boolean commitUnchangedObjects = objectConfig.commitUnchangedObjects();
		ObjectSearchAction syncAction = objectConfig.getObjectSearchAction();

		this.mInfo.TimeMeasurement.startPerformanceTest("Storing -> Determine Object Handling");
		for( Entry<String, MetaInfoObject> entry : this.meta_Objects.entrySet() ) {
			String objectKey = entry.getKey();
			MetaInfoObject mInfoObject = entry.getValue();

			if ( !objectConfig.ignoreEmptyKeys() || !isObjectKeyEmpty(objectKey) ) {
				try {
					switch (syncAction) {
					case CreateEverything:
						if ( mInfoObject.getObject() == null ) {
							mInfoObject.setObject(Core.instantiate(this.settings.getContext(), objectConfig.getObjectType()), true);
							// Don't update statistics here, do that after changing all the members

							if ( MILogNode.Replication_MetaInfo_MainObject.getLogger().isTraceEnabled() )
								MILogNode.Replication_MetaInfo_MainObject.getLogger().trace(
										"Creating new object, the key was: " + mInfoObject.getValueString() + " - " + mInfoObject.getObject().getId() );

						}
						else if ( MILogNode.Replication_MetaInfo_MainObject.getLogger().isTraceEnabled() )
							MILogNode.Replication_MetaInfo_MainObject.getLogger().trace(
									"Duplicate object , ran in to the object a second time keeping the previously created object, the key was: " + objectKey);

						break;
					case FindCreate:
						if ( mInfoObject.getObject() == null ) {
							mInfoObject.setObject(Core.instantiate(this.settings.getContext(), objectConfig.getObjectType()), true);
							// Don't update statistics here, do that after changing all the members

							if ( MILogNode.Replication_MetaInfo_MainObject.getLogger().isTraceEnabled() )
								MILogNode.Replication_MetaInfo_MainObject.getLogger().trace("Creating new object, the key was: " + objectKey);
						}
						else if ( MILogNode.Replication_MetaInfo_MainObject.getLogger().isTraceEnabled() )
							MILogNode.Replication_MetaInfo_MainObject.getLogger().trace("Changing existing object, the key was: " + objectKey);

						break;
					case OnlyCreateNewObjects:
						if ( mInfoObject.getObject() != null ) {
							mInfoObject.setObject(null, false);
							
							//Only set statistics when we skip an MetaInfoObject. The create/synced/skipped stats are counted later
							this.objectStats.addObjectStat(Stat.ObjectsSkipped);

							if ( MILogNode.Replication_MetaInfo_MainObject.getLogger().isTraceEnabled() )
								MILogNode.Replication_MetaInfo_MainObject.getLogger().trace(
										"Skipping object, because only new objects should be created. The key was: " + objectKey);
						}
						else {
							mInfoObject.setObject(Core.instantiate(this.settings.getContext(), objectConfig.getObjectType()), true);

							if ( MILogNode.Replication_MetaInfo_MainObject.getLogger().isTraceEnabled() )
								MILogNode.Replication_MetaInfo_MainObject.getLogger().trace("Creating new object, the key was: " + objectKey);
						}

						break;
					case FindIgnore:
						/*
						 * Do nothing, when there is an object, the synchronisation should go on
						 * when there is no object found, we don't need to create one
						 */
						if ( mInfoObject.getObject() == null ) {
							
							//Only set statistics when we skip an MetaInfoObject. The create/synced/skipped stats are counted later
							this.objectStats.recordUnknownObject( objectKey );
							if ( MILogNode.Replication_MetaInfo_MainObject.getLogger().isDebugEnabled() )
								MILogNode.Replication_MetaInfo_MainObject.getLogger().debug("Object not found, the key was: " + objectKey);
						}
						// If there is an object found, don't update statistics here, do that after
						// changing all the members
						break;
					}

				}
				catch( CoreRuntimeException e ) {
					if ( !this.settings.getErrorHandler().valueException(e,
							MessageOptions.COULD_NOT_CREATE_EDIT_OBJECT.getMessage(this.settings.getLanguage(), objectConfig.getObjectType())) )
						throw new MendixReplicationRuntimeException(e);
				}

				/*
				 * Depending of the earlier validated rules there could be an MxObject (otherwise we
				 * should ignore the data) First validate the Parent association and set that value
				 * here. Next evaluate all the contents in the MetaInfoObject and set the values in
				 * the MxObject
				 */
				if ( mInfoObject.getObject() != null ) {
					if ( this.settings.getParentAssociation() != null ) {
						try {
							if ( MILogNode.Replication_MetaInfo.getLogger().isDebugEnabled() )
								MILogNode.Replication_MetaInfo
										.getLogger()
										.trace("Adding parent association: " + this.settings.getParentAssociation() + " to object: " + objectKey + ", adding value: " + this.settings
												.getParentObjectId());

							mInfoObject.getObject().setValue(this.settings.getContext(), this.settings.getParentAssociation(),
									this.settings.getParentObjectId());
						}
						catch( CoreRuntimeException e ) {
							if ( !this.settings.getErrorHandler().valueException(e, e.getMessage()) )
								throw new MendixReplicationRuntimeException(e);
						}
					}

					// Update the changed members and directly place the result of the function into
					// the statistics class
					this.objectStats.addObjectStat(this.changeMembersForBatch(mInfoObject, associatedObjects, commitUnchangedObjects));
				}

				this.objectStats.printRuntimeStatistics();
			}
			else {
				this.objectStats.addObjectStat(Stat.ObjectsSkipped);

				if ( MILogNode.Replication_MetaInfo_MainObject.getLogger().isTraceEnabled() )
					MILogNode.Replication_MetaInfo_MainObject.getLogger().trace(
							"Skipping object, because of empty object key");
			}
		}
		this.mInfo.TimeMeasurement.endPerformanceTest("Storing -> Determine Object Handling");

		/*
		 * First commit all the objects on the child side of the associations in order to prevent
		 * the Runtime to commit them twice. We need this specific order to prevent the commit
		 * actions from the runtime
		 */
		this.mInfo.TimeMeasurement.startPerformanceTest("Storing -> Commit child objects");
		MetaInfoObject obj;
		List<IMendixObject> objectList = new ArrayList<IMendixObject>();
		List<String> keyList = new ArrayList<String>();
		for( Entry<String, Map<String, MetaInfoObject>> entry : associatedObjects.entrySet() ) {
			AssociationConfig associationConfig = this.settings.getAssociationConfig(entry.getKey());
			if ( associationConfig.isReferencedObjectAssociationOwner() ) {
				for( Entry<String, MetaInfoObject> subEntry : entry.getValue().entrySet() ) {
					obj = subEntry.getValue();
					MetaInfoObjectState state = obj.getState();

					if ( state == MetaInfoObjectState.Reset )
						throw new ReplicationSettings.MendixReplicationRuntimeException("Invalid object state : Reset , for object " + obj.getObjectID() + " / " + obj.getId(), MetaInfo._version);

					if ( obj.getObject() != null && (state == MetaInfoObjectState.Changed || state == MetaInfoObjectState.New) )
						objectList.add(obj.getObject());
					else if ( MetaInfo.MILogNode.Replication_MetaInfo_AssociatedObjects.getLogger().isTraceEnabled() )
						MetaInfo.MILogNode.Replication_MetaInfo_AssociatedObjects.getLogger().trace(
								"Not committing object: " + entry.getKey() + "/(" + subEntry.getKey() + ") because it has state: " + state);
				}

				if ( objectList.size() > 0 )
					Core.commit(this.settings.getContext(), objectList);

				objectList.clear();

				// Add the key to a list so we can remove the item later in the process (to preven
				// concurrent mod exception)
				keyList.add(entry.getKey());
			}
		}
		this.mInfo.TimeMeasurement.endPerformanceTest("Storing -> Commit child objects");

		for( String key : keyList )
			associatedObjects.remove(key);

		/*
		 * Now commit the main objects which should only refer to the child entities
		 */
		this.mInfo.TimeMeasurement.startPerformanceTest("Storing -> Commit parent objects");
		for( Entry<String, MetaInfoObject> entry : this.meta_Objects.entrySet() ) {
			obj = entry.getValue();
			MetaInfoObjectState state = obj.getState();
			if ( state == MetaInfoObjectState.Reset )
				throw new ReplicationSettings.MendixReplicationRuntimeException("Invalid object state : Reset , for object " + obj.getObjectID() + " / " + obj.getId(), MetaInfo._version);

			if ( obj.getObject() != null && (state == MetaInfoObjectState.Changed || state == MetaInfoObjectState.New) )
				objectList.add(obj.getObject());
			else if ( MetaInfo.MILogNode.Replication_MetaInfo_MainObject.getLogger().isTraceEnabled() )
				MetaInfo.MILogNode.Replication_MetaInfo_MainObject.getLogger().trace(
						"Not committing object: " + objectConfig.getObjectType() + "/(" + entry.getKey() + ") because it has state: " + state);
		}
		if ( objectList.size() > 0 )
			Core.commit(this.settings.getContext(), objectList);
		objectList.clear();
		this.mInfo.TimeMeasurement.endPerformanceTest("Storing -> Commit parent objects");

		/*
		 * Now as final action, commit all the objects which are the owner of the association and
		 * point towards the main object
		 */
		this.mInfo.TimeMeasurement.startPerformanceTest("Storing -> Commit remaining associated objects");
		for( Entry<String, Map<String, MetaInfoObject>> entry : associatedObjects.entrySet() ) {
			for( Entry<String, MetaInfoObject> subEntry : entry.getValue().entrySet() ) {
				obj = subEntry.getValue();
				MetaInfoObjectState state = obj.getState();
				if ( state == MetaInfoObjectState.Reset )
					throw new ReplicationSettings.MendixReplicationRuntimeException("Invalid object state : Reset , for object " + obj.getObjectID() + " / " + obj.getId(), MetaInfo._version);

				if ( obj.getObject() != null && (state == MetaInfoObjectState.Changed || state == MetaInfoObjectState.New) )
					objectList.add(obj.getObject());
				else if ( MetaInfo.MILogNode.Replication_MetaInfo_AssociatedObjects.getLogger().isTraceEnabled() )
					MetaInfo.MILogNode.Replication_MetaInfo_AssociatedObjects.getLogger().trace(
							"Not committing object: " + entry.getKey() + "/(" + subEntry.getKey() + ") because it has state: " + state);
			}

			if ( objectList.size() > 0 )
				Core.commit(this.settings.getContext(), objectList);

			objectList.clear();
		}
		associatedObjects.clear();
		this.mInfo.TimeMeasurement.endPerformanceTest("Storing -> Commit remaining associated objects");


		this.mInfo.TimeMeasurement.endPerformanceTest("Storing Objects");

	}

	/**
	 * Prepare all associated objects, check if any objects should be created. Create them when
	 * necessary, and update the values by the supplied MetaInfo When any objects where created, the
	 * function fillAssociationEntry will be called for a second time to retrieve the latest values
	 * of the created objects. This function won't return any values all changes are directly made
	 * in the associatedObjects map
	 * 
	 * @param associatedObjects
	 *        , all entries in this map will be updated with the latest values
	 * @throws MendixReplicationException
	 */
	private void processAssociatedObjects( HashMap<String, Map<String, MetaInfoObject>> associatedObjects ) throws MendixReplicationException {

		this.mInfo.TimeMeasurement.startPerformanceTest("Storing -> Determine Object Handling For all Associated Objects");
		for( Entry<String, Map<String, MetaInfoObject>> entry : associatedObjects.entrySet() ) {
			String associationName = entry.getKey();

			AssociationConfig associationConfig = this.settings.getAssociationConfig(associationName);

			boolean hasAdditionalAssociationMembers = associationConfig.hasNonKeyMembers();
			ObjectSearchAction handling = associationConfig.getObjectSearchAction();
			boolean commitUnchangedObjects = associationConfig.commitUnchangedObjects();

			try {

				String objectType = associationConfig.getObjectType();
				for( Entry<String, MetaInfoObject> associationEntry : entry.getValue().entrySet() ) {
					MetaInfoObject miObject = associationEntry.getValue();
					IMendixObject mxObject = miObject.getObject();
					switch (handling) {
					case FindIgnore:
						if ( mxObject == null )
							this.objectStats.recordUnknownAssociatedObject(associationName, associationEntry.getKey());
						break;
					case CreateEverything:
					case FindCreate:
					case OnlyCreateNewObjects:
						if ( mxObject != null && handling != ObjectSearchAction.CreateEverything && handling != ObjectSearchAction.OnlyCreateNewObjects ) {

							if ( hasAdditionalAssociationMembers || commitUnchangedObjects ) {

								// Update the changed members and directly place the result of the
								// function into the statistics class
								this.objectStats.addAssociationStat(associationName,
										this.changeMembersForBatch(miObject, null, commitUnchangedObjects));

								if ( MILogNode.Replication_MetaInfo_AssociatedObjects.getLogger().isTraceEnabled() )
									MILogNode.Replication_MetaInfo_AssociatedObjects.getLogger().trace(
											"Changing existing object for association: " + associationName + ". The key was: " + miObject.getObjectKey());
							}
							else {
								this.objectStats.addStat(associationName, Stat.ObjectsSkipped);

								if ( MILogNode.Replication_MetaInfo_AssociatedObjects.getLogger().isTraceEnabled() )
									MILogNode.Replication_MetaInfo_AssociatedObjects.getLogger().trace(
											"Found existing object for association: " + associationName + ", no changes. The key was: " + miObject.getObjectKey());
							}

						}
						else if ( (mxObject == null && (handling == ObjectSearchAction.FindCreate || handling == ObjectSearchAction.OnlyCreateNewObjects)) || handling == ObjectSearchAction.CreateEverything ) {

							if ( !isObjectKeyEmpty(miObject.getObjectKey()) || associationConfig.ignoreEmptyKeys() == false ) {
								
								miObject.setObject(Core.instantiate(this.settings.getContext(), objectType), true);
								
								if ( MILogNode.Replication_MetaInfo_AssociatedObjects.getLogger().isTraceEnabled() )
									MILogNode.Replication_MetaInfo_AssociatedObjects
											.getLogger()
											.trace("Creating new object for association: " + associationName + " the key was: " + (handling == ObjectSearchAction.CreateEverything ? miObject
													.getValueString() : miObject.getObjectKey()) + " - " + miObject.getObject().getId() );


								// Update the changed members and directly place the result of the
								// function into the statistics class
								this.objectStats.addAssociationStat(associationName,
										this.changeMembersForBatch(miObject, null, commitUnchangedObjects));
							}
							else if ( MILogNode.Replication_MetaInfo_AssociatedObjects.getLogger().isDebugEnabled() || true ) // TODO
																																// replace
																																// with
																																// debug
								MILogNode.Replication_MetaInfo_AssociatedObjects
										.getLogger()
										.warn("Not creating new object for association: " + associationName + " the key was: " + (handling == ObjectSearchAction.CreateEverything ? miObject
												.getValueString() : miObject.getObjectKey()));

						}
						else if ( mxObject != null && handling == ObjectSearchAction.OnlyCreateNewObjects ) {
							if ( MILogNode.Replication_MetaInfo_AssociatedObjects.getLogger().isDebugEnabled() )
								MILogNode.Replication_MetaInfo_AssociatedObjects.getLogger().debug(
										"Skipping object for association: " + associationName + " the key was: " + miObject.getObjectKey());

							this.objectStats.addStat(associationName, Stat.ObjectsSkipped);
						}
						else {
							if ( MILogNode.Replication_MetaInfo_AssociatedObjects.getLogger().isDebugEnabled() )
								MILogNode.Replication_MetaInfo_AssociatedObjects.getLogger().debug(
										"Object not found for association: " + associationName + " the key was: " + miObject.getObjectKey());

							this.objectStats.recordUnknownObject( miObject.getObjectKey() );
						}
						break;
					}
				}
			}
			catch( Exception e ) {
				throw new MendixReplicationRuntimeException(e.getMessage(), MetaInfo._version, e);
			}
		}
		this.mInfo.TimeMeasurement.endPerformanceTest("Storing -> Determine Object Handling For all Associated Objects");
	}

	private void checkRunningProcesses() {
		try {
			for( ObjectRetrieval orEntry : this.objectRetrievalArray ) {
				if ( orEntry != null ) {
					orEntry.join();

					replication.helpers.UncaughtExceptionHandler exceptionHandler = orEntry.getUeHandler();
					if ( exceptionHandler.hasException() ) {
						Throwable t = exceptionHandler.getException();
						throw new MendixReplicationRuntimeException(t);
					}
				}
			}
		}
		catch( InterruptedException e1 ) {
			throw new MendixReplicationRuntimeException(e1);
		}
	}

	/**
	 * Process the MetaInfoObject and update the currentBatch with all attribute and reference
	 * values from the MetaInfoObject
	 * 
	 * @param curBatch
	 * @param MetaInfoObject
	 * @param associatedObjects
	 * @throws MendixReplicationException
	 */
	protected MetaInfoObjectState changeMembersForBatch( MetaInfoObject mInfoObject, HashMap<String, Map<String, MetaInfoObject>> associatedObjects, boolean commitUnchangedObject ) {
		IMendixObject mainMxObject = mInfoObject.getObject();
		for( Entry<String, Object> memberValue : mInfoObject.getAttributeValues().entrySet() ) {
			updateMemberAndSetState(this.settings.getContext(), mInfoObject, mainMxObject, memberValue.getKey(), memberValue.getValue(),
					commitUnchangedObject, AssociationDataHandling.Overwrite);
		}

		if ( associatedObjects != null ) {
			IMendixObject referencedMxObject;
			IMendixIdentifier referencedMxObjectId;
			IUnknownObjectHandler unknownAssociationHandler;

			/*
			 * Evaluate all References, this only includes all references where there can be just 1 associated object
			 */
			for( Entry<String, MetaInfoObject> referenceEntry : mInfoObject.getReferenceValues().entrySet() ) {
				String associationName = referenceEntry.getKey();
				if ( associatedObjects.containsKey(associationName) ) {
					MetaInfoObject curReferencedObject = referenceEntry.getValue();

					try {
						String refKey = curReferencedObject.getObjectKey();
						//TODO test this, should we check for null values only, or should we use the function: isObjectKeyEmpty(refKey)
						referencedMxObject = (refKey != null ? associatedObjects.get(associationName).get(refKey).getObject() : null);
						referencedMxObjectId = null;

						AssociationConfig associationConfig = this.settings.getAssociationConfig(associationName);
						unknownAssociationHandler = associationConfig.getUnkownAssociationHandler();

						/*
						 * If there is no object found in the Mendix database check if the
						 * user defined a custom association handler. Use that handler for
						 * dealing with the custom values and the association id.
						 */
						if ( referencedMxObject != null )
							referencedMxObjectId = referencedMxObject.getId();
						else {
							MILogNode.Replication_MetaInfo_AssociatedObjects.getLogger().trace(
									"No associated object found for association: " + associationName + " within object: " + mInfoObject
											.getObjectKey());

							if ( referencedMxObject == null && unknownAssociationHandler != null )
								referencedMxObject = unknownAssociationHandler.processUnknownObject(curReferencedObject, mainMxObject);
						}

						if ( associationConfig.isReferencedObjectAssociationOwner() ) {
							if ( referencedMxObject != null )
								updateMemberAndSetState(this.settings.getContext(), curReferencedObject, referencedMxObject, associationName,
										mainMxObject.getId(), commitUnchangedObject, associationConfig.getAssociationDataHandling());
						}
						else
							updateMemberAndSetState(this.settings.getContext(), mInfoObject, mainMxObject, associationName, referencedMxObjectId,
									commitUnchangedObject, associationConfig.getAssociationDataHandling());
					}
					catch( CoreException e ) {
						if ( !this.settings.getErrorHandler().valueException(e, e.getMessage()) )
							throw new MendixReplicationRuntimeException(e);
					}
				}
			}


			/*
			 * Evaluate all *-to-Many References, this includes both reference sets, but also default references owned
			 * by the associate object
			 */
			List<IMendixIdentifier> refObjectIdList = new ArrayList<IMendixIdentifier>();
			for( Entry<String, List<MetaInfoObject>> referenceEntry : mInfoObject.getReferenceSetValues().entrySet() ) {
				String associationName = referenceEntry.getKey();
				if ( associatedObjects.containsKey(associationName) ) {
					try {
						// Just retrieve the association handler now instead of in the following
						// loop to increase the performance
						AssociationConfig associationConfig = this.settings.getAssociationConfig(associationName);
						unknownAssociationHandler = associationConfig.getUnkownAssociationHandler();


						for( MetaInfoObject curReferenceObject : referenceEntry.getValue() ) {
							String refKey = curReferenceObject.getObjectKey();
							MetaInfoObject assMinfoObject = associatedObjects.get(associationName).get(refKey);
							referencedMxObject = (refKey != null ? assMinfoObject.getObject() : null);
							if ( referencedMxObject != null )
								refObjectIdList.add(referencedMxObject.getId());

							else {
								MILogNode.Replication_MetaInfo_AssociatedObjects.getLogger().warn(
										"No associated object found for association: " + associationName + " within object: " + mInfoObject
												.getObjectKey());
								/*
								 * If there is no object found in the Mendix database check if the
								 * user defined a custom association handler. Use that handler for
								 * dealing with the custom values and the association id.
								 */
								if ( unknownAssociationHandler != null ) {
									IMendixIdentifier id = unknownAssociationHandler.processUnknownObjectGetId(assMinfoObject, mainMxObject);
									if ( id != null )
										refObjectIdList.add(id);
								}
							}
						}
						AssociationDataHandling dataHandling = associationConfig.getAssociationDataHandling();

						/*
						 * A *-something reference can be used by either having a reference set
						 * or if the associated object has a regular reference to the object
						 */
						if ( associationConfig.isReferencedObjectAssociationOwner() ) {
							for( MetaInfoObject curReferencedObject : referenceEntry.getValue() ) {
								if ( curReferencedObject.getObject() != null )
									updateMemberAndSetState(this.settings.getContext(), curReferencedObject, curReferencedObject.getObject(),
											associationName, mainMxObject.getId(), commitUnchangedObject, dataHandling);
							}
						}
						else {
							updateMemberAndSetState(this.settings.getContext(), mInfoObject, mainMxObject, associationName, refObjectIdList,
									commitUnchangedObject, dataHandling);
						}
					}
					catch( CoreException e ) {
						if ( !this.settings.getErrorHandler().valueException(e, e.getMessage()) )
							throw new MendixReplicationRuntimeException(e);
					}
				}


				// Reset the list for the next iteration
				refObjectIdList.clear();
			}
		}

		return mInfoObject.getState();
	}

	/**
	 * Checks the state of the MetaInfo object, depending on the Meta object state the member state
	 * will be validated which might result in a changed MetaInfoObject state
	 * 
	 * @param mInfoObject
	 * @param member
	 * @return
	 * @throws MendixReplicationException
	 */
	private static MetaInfoObjectState updateMemberAndSetState( IContext context, MetaInfoObject mInfoObject, IMendixObject mxObject, String memberName, Object newValue, boolean commitUnchangedObject, AssociationDataHandling dataHandling ) {

		MetaInfoObjectState objectState = mInfoObject.getState();
		IMendixObjectMember<?> member = mxObject.getMember(context, memberName);


		Object currentValue = null;
		boolean isPassword = !member.hasReadAccess(context) && member.hasWriteAccess(context);
		if ( !isPassword )
			currentValue = mxObject.getValue(context, memberName);

		if ( commitUnchangedObject || (isPassword && currentValue != null) || currentValue == null && newValue != null || currentValue != null && !currentValue
				.equals(newValue) ) {

			if ( member instanceof MendixObjectReferenceSet ) {
				if ( dataHandling == AssociationDataHandling.Append ) {
					if ( newValue instanceof List<?> ) {
						for( Object id : (List<?>) newValue ) {
							((MendixObjectReferenceSet) member).addValue(context, (IMendixIdentifier) id);
						}
					}
					else
						((MendixObjectReferenceSet) member).addValue(context, (IMendixIdentifier) newValue);
				}
				//The handling is override 
				else {
					if ( newValue instanceof List<?> ) {
						mxObject.setValue(context, memberName, newValue);
					}
					else {
						ArrayList<IMendixIdentifier> idList = new ArrayList<IMendixIdentifier>();
						idList.add((IMendixIdentifier) newValue);
						mxObject.setValue(context, memberName, idList);
					}
				}
			}
			else {
				MILogNode.Replication_MetaInfo.getLogger().trace("Setting value of member: " + memberName + " value: " + newValue);

				mxObject.setValue(context, memberName, newValue);
			}

			if ( objectState == MetaInfoObjectState.Unchanged ) {
				mInfoObject.markAsChanged();
				return MetaInfoObjectState.Changed;
			}
		}

		return objectState;
	}

	/**
	 * Reset all MetaInfoObjects, clear all attribute values and empty some of the local variables.
	 * This makes sure that the next iteration starts with clean/empty values
	 */
	private void resetChangedObjects() {
		this.meta_Objects = null;
		this.objectRetrievalArray = null;
		this.objectStats = null;
		this.settings = null;
		this.valueParser = null;
	}

	public replication.helpers.UncaughtExceptionHandler getUeHandler() {
		return this.ueHandler;
	}

	// ////////////////////////////////------------------------------------------------

	/**
	 * Analyse the MetaInfoObject create an OQL query based on this MetaData The result from the OQL
	 * query will be stored directly into their respective MetaInfoObject
	 * 
	 * @param meta_Objects
	 * @param objectIdentifier
	 */
	private void retrieveMxObjectsByMetaInfoObject( Map<String, MetaInfoObject> meta_Objects, String objectIdentifier ) {
		if ( meta_Objects != null && meta_Objects.size() > 0 ) {
			this.mInfo.TimeMeasurement.startPerformanceTest("Storing -> Retrieving MetaInfo Objects: " + objectIdentifier);
			try {
				int limit = this.settings.Configuration.RetrieveOQL_Limit, totalSize = meta_Objects.size(), offset = 0;
				boolean isAssociation = this.settings.objectIdIsAssociation(objectIdentifier);

				while( offset < totalSize ) {
					StringBuilder builder = OQLBuilder.buildOQLQuery(this.settings, meta_Objects, objectIdentifier, isAssociation, limit, offset,
							this.mInfo.getValueParser());
					offset += limit;
					this.mInfo.TimeMeasurement.startPerformanceTest("Storing -> Processing Query: " + objectIdentifier + " / " + offset);
					if ( builder.length() > 0 ) {
						IDataTable table;
						try {
							table = Core.retrieveOQLDataTable(this.settings.getContext(), builder.toString());
						}
						catch( Exception e ) {
							if ( e.getMessage() != null && e.getMessage().contains("Query result is not valid") )
								throw new MendixReplicationRuntimeException("Unable to execute the OQL query. This exception usually occurs if you don't have entity access to the imported for object: " + objectIdentifier + " range: " + offset + "/" + limit + " because of error: " + e.getMessage(), e);

							throw new MendixReplicationRuntimeException("Unable to execute the OQL query for objects: " + objectIdentifier + " range: " + offset + "/" + limit + " because of error: " + e.getMessage(), e);
						}

						builder.delete(0, builder.length());
						builder = null;

						this.mInfo.TimeMeasurement.endPerformanceTest("Storing -> Processing Query: " + objectIdentifier + " / " + offset);

						// When there is any information in the DataSet do some stuff to analyse the
						// objects
						int nrOfRows = table.getRowCount();
						if ( nrOfRows > 0 ) {
							this.mInfo.TimeMeasurement
									.startPerformanceTest("Storing -> Evaluating Query Result: " + objectIdentifier + " / " + offset + " / " + nrOfRows);

							if ( MILogNode.Replication_MetaInfo.getLogger().isDebugEnabled() )
								MILogNode.Replication_MetaInfo.getLogger()
										.debug("MetaInfo - Retrieved id's by the oql query: " + table.getRowCount());

							/*
							 * There is a possiblity the user configured a template with a key that
							 * isn't unique The module will pick one of the duplicates at random.
							 * 
							 * If the module receives more than double then nr of expected objects
							 * there is definitely something wrong and start printing all dubplicate
							 * entries
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

								// Update the idlist so we can retrieve the specific objects in
								// batches
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
							this.mInfo.TimeMeasurement.endPerformanceTest("Storing -> Evaluating Query Result: " + objectIdentifier + " / " + offset);
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
			finally {
				this.mInfo.TimeMeasurement.endPerformanceTest("Storing -> Retrieving MetaInfo Objects: " + objectIdentifier);
			}
		}
	}

	/**
	 * Try to find all associated objects by the supplied Collection of all MetaInfoObjects.
	 * 
	 * First the return map will be prepared, and empty records for all possible associations will
	 * be added to the map After the map is prepared the funcion fillAssociationEntry will be
	 * executed which provides the returnmap with all relevant MxObjects
	 * 
	 * @return a map containing all association and with a map in the value sorted by the object
	 *         keys
	 * @throws MendixReplicationException
	 */
	private void populateAssociatedObjects( Collection<MetaInfoObject> metaInfoObjectsList, HashMap<String, Map<String, MetaInfoObject>> associationResultList ) {
		try {
			this.mInfo.TimeMeasurement.startPerformanceTest("Storing -> Populated Associated MetaInfo Objects");
			MILogNode.Replication_MetaInfo_AssociatedObjects.getLogger().debug("Start caching and evaluating all association records.");
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


							if ( isObjectKeyEmpty(curMIObject.getObjectKey()) && this.settings.getAssociationConfig(entry.getKey()).ignoreEmptyKeys() )
								continue;
							else if ( !values.containsKey(curMIObject.getObjectKey()) )
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

						if ( isObjectKeyEmpty( curMIObject.getObjectKey() ) && this.settings.getAssociationConfig(entry.getKey()).ignoreEmptyKeys() )
							continue;
						else if ( !values.containsKey(curMIObject.getObjectKey()) )
							values.put(curMIObject.getObjectKey(), curMIObject);
					}
				}
			}

			/*
			 * For each of the associations, try and find all the existing IMendixObjects and 
			 *   store them in the MetaInfoObject for further processing later in this flow 
			 */
			for( Entry<String, AssociationConfig> entry : this.settings.getAssociationConfigMap().entrySet() ) {
				if ( entry.getValue().getObjectSearchAction() != ObjectSearchAction.CreateEverything ) {
					if ( MILogNode.Replication_MetaInfo_AssociatedObjects.getLogger().isTraceEnabled() )
						MILogNode.Replication_MetaInfo_AssociatedObjects.getLogger().trace(
								"Retrieving all existing objecst for association: " + entry.getKey());

					this.retrieveMxObjectsByMetaInfoObject(associationResultList.get(entry.getKey()), entry.getKey());
				}
			}

			MILogNode.Replication_MetaInfo_AssociatedObjects.getLogger().debug("Finished caching and evaluating all association records.");
		}
		catch( MendixReplicationException e ) {
			throw new MendixReplicationRuntimeException(e);
		}
		finally {
			this.mInfo.TimeMeasurement.endPerformanceTest("Storing -> Populated Associated MetaInfo Objects");
		}
	}

	/**
	 * Checks the key for null values, and if the key is only made up out of the separator
	 */
	private static boolean isObjectKeyEmpty( String objectKey ) {
		return objectKey == null || "".equals(objectKey.replace(ValueParser.keySeparator, ""));
	}
}
