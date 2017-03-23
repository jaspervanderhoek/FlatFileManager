package replication;

import replication.ReplicationSettings.AssociationDataHandling;
import replication.ReplicationSettings.MendixReplicationException;
import replication.ReplicationSettings.MendixReplicationRuntimeException;
import replication.ReplicationSettings.ObjectSearchAction;
import replication.interfaces.IUnknownObjectHandler;

import com.mendix.core.Core;
import com.mendix.systemwideinterfaces.core.meta.IMetaAssociation;
import com.mendix.systemwideinterfaces.core.meta.IMetaAssociation.AssociationOwner;
import com.mendix.systemwideinterfaces.core.meta.IMetaAssociation.AssociationType;

public class AssociationConfig extends ObjectConfig {

	private AssociationDataHandling dataHandling = AssociationDataHandling.Overwrite;

	private String associationName;
	private IUnknownObjectHandler unknownAssociationHandler = null;

	private boolean referencedObjectIsOwner = true;
	private ReferenceType referenceType;

	public enum ReferenceType {
		Reference_Default,
		Reference_BothOwner,
		ReferenceSet_Default,
		ReferenceSet_Both
	}

	public AssociationConfig( String associatedObjectType, String associationName ) throws MendixReplicationException {
		super(associatedObjectType);

		this.associationName = associationName;

		validateAssociationInfo();
	}

	private void validateAssociationInfo() throws MendixReplicationException {
		if ( this.referenceType != null )
			return;

		if ( "System.owner".equals(this.associationName) || "System.changedBy".equals(this.associationName) ) {
			this.referenceType = ReferenceType.Reference_Default;
			this.referencedObjectIsOwner = false;
		}
		else if ( this.getObjectType() != null ) {

			IMetaAssociation association = Core.getMetaAssociation(this.associationName);

			String parentObjectName = association.getParent().getName(), childObjectName = association.getChild().getName();

			if ( childObjectName.equals(parentObjectName) || doObjectsInherit(parentObjectName, childObjectName) ) {
				this.referencedObjectIsOwner = false;

			}
			// Associated object owns the relationship
			else if ( association.getOwner() == AssociationOwner.DEFAULT && parentObjectName.equals(this.getObjectType()) ) {
				this.referencedObjectIsOwner = true;
			}
			// the main object owns the relationship
			else if ( association.getOwner() == AssociationOwner.DEFAULT && childObjectName.equals(this.getObjectType()) ) {
				this.referencedObjectIsOwner = false;
			}
			// Associated object owns the relationship?
			else if ( association.getOwner() == AssociationOwner.BOTH ) {
				this.referencedObjectIsOwner = false;
			}
			else
				throw new MendixReplicationException("Unsupported configuration for association: " + this.associationName + " ownership fall through.", MetaInfo._version);


			if ( association.getType() == AssociationType.REFERENCE && association.getOwner() == AssociationOwner.DEFAULT )
				this.referenceType = ReferenceType.Reference_Default;
			else if ( association.getType() == AssociationType.REFERENCE && association.getOwner() == AssociationOwner.BOTH )
				this.referenceType = ReferenceType.Reference_BothOwner;
			else if ( association.getType() == AssociationType.REFERENCESET && association.getOwner() == AssociationOwner.DEFAULT )
				this.referenceType = ReferenceType.ReferenceSet_Default;
			else if ( association.getType() == AssociationType.REFERENCESET && association.getOwner() == AssociationOwner.BOTH )
				this.referenceType = ReferenceType.ReferenceSet_Both;
			else
				throw new MendixReplicationException("Unsupported configuration for association: " + this.associationName + " reference type fall through.", MetaInfo._version);
		}
	}

	private static boolean doObjectsInherit( String parentObjectName, String childObjectName ) {
		return (Core.isSubClassOf(parentObjectName, childObjectName) || Core.isSubClassOf(childObjectName, parentObjectName));
	}

	public boolean actAsReferenceSet() {
		if ( this.referencedObjectIsOwner )
			return true;
		else
			return this.referenceType == ReferenceType.ReferenceSet_Default || this.referenceType == ReferenceType.ReferenceSet_Both;
	}


	public boolean isReferencedObjectAssociationOwner() {
		return this.referencedObjectIsOwner;
	}

	public ReferenceType getReferenceType() {
		return this.referenceType;
	}

	@Override
	public AssociationConfig setObjectSearchAction( ObjectSearchAction handling ) {
		super.setObjectSearchAction(handling);

		return this;
	}

	/**
	 * Retrieve how the association should be processed (overwrite or append in case of a reference
	 * set)
	 * 
	 * @DEFAULT: Overwrite
	 */
	public AssociationDataHandling getAssociationDataHandling() {
		return this.dataHandling;
	}

	/**
	 * Set how the association should be processed (overwrite or append in case of a reference set)
	 * 
	 * @DEFAULT: Overwrite
	 * @param handling
	 */
	public AssociationConfig setAssociationDataHandling( AssociationDataHandling handling ) {
		if ( handling == null )
			throw new MendixReplicationRuntimeException("The association data handling may not be set to empty, this happened for association: " + this.associationName, MetaInfo._version);

		this.dataHandling = handling;

		return this;
	}

	public void validateObjectType( String associatedObjectType ) throws MendixReplicationException {
		if ( this.getObjectType() != null ) {
			if ( !associatedObjectType.equals(this.getObjectType()) )
				throw new MendixReplicationException("The association " + this.associationName + " is already defined but with a different object type, the given type is: " + associatedObjectType + " and the retrieved type is: " + this.getObjectType(), MetaInfo._version);
		}
	}

	public IUnknownObjectHandler getUnkownAssociationHandler() {
		return this.unknownAssociationHandler;
	}

	public void setUnkownAssociationHandler( IUnknownObjectHandler handler ) {
		this.unknownAssociationHandler = handler;
	}

}
