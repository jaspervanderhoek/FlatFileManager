package replication.implementation;

import replication.AssociationConfig;
import replication.ObjectConfig;
import replication.ReplicationSettings;
import replication.interfaces.IErrorHandler;
import replication.interfaces.IValueParser;

import com.mendix.core.CoreException;
import com.mendix.systemwideinterfaces.core.IContext;

public class CustomReplicationSettings extends ReplicationSettings {

	/**
	 * Initialize the settings, provide the starting context. If a new context has to be created the settings will be copied from the provided context.
	 * The last parameter ErrorHandler is optional. This parameter should contain a specfic project errorhandler or when null the default errorhandler will be used. This handler aborts the import for eacht exception
	 * 
	 * @param context
	 * @param dbSettings
	 * @param objectType
	 * @param errorHandler
	 * @throws MendixReplicationException
	 */
	public CustomReplicationSettings(IContext context, String objectType, IErrorHandler errorHandler ) throws MendixReplicationException {
		super(context, objectType, errorHandler);
		this.setInfoHandler( new InfoHandler("CustomReplication") );
	}


	public ObjectConfig addColumnMapping( String alias, String memberName, KeyType isKey, Boolean isCaseSensitive, IValueParser parser ) throws CoreException {
		return this.addMappingForAttribute(alias, memberName, isKey, isCaseSensitive, parser);
	}



	public AssociationConfig addAssociationMapping( String alias, String associationName, String associatedObjectType, String memberName, IValueParser parser, KeyType isKey, Boolean isCaseSensitive ) throws CoreException {
		return this.addMappingForAssociation(alias, associationName, associatedObjectType, memberName, parser, isKey, isCaseSensitive);
	}
}
