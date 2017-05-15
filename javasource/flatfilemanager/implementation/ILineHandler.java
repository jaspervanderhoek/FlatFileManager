package flatfilemanager.implementation;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;
import java.util.List;

import com.mendix.core.Core;
import com.mendix.core.CoreException;
import com.mendix.systemwideinterfaces.core.IContext;
import com.mendix.systemwideinterfaces.core.IMendixIdentifier;
import com.mendix.systemwideinterfaces.core.IMendixObject;

import flatfilemanager.proxies.DataSource;
import flatfilemanager.proxies.Field;
import mxmodelreflection.proxies.MxObjectMember;
import mxmodelreflection.proxies.MxObjectReference;
import mxmodelreflection.proxies.MxObjectType;
import replication.ObjectConfig;
import replication.ValueParser;
import replication.ReplicationSettings.KeyType;
import replication.ReplicationSettings.MendixReplicationException;
import replication.ReplicationSettings.ObjectSearchAction;
import replication.implementation.CustomReplicationSettings;
import replication.implementation.ErrorHandler;
import replication.implementation.MFValueParser;
import replication.interfaces.IValueParser;

public abstract class ILineHandler {
	protected IContext context;
	protected TemplateConfiguration config;


	public abstract void initialize( IContext context, Writer writer, TemplateConfiguration config );
	
	public abstract void initialize( IContext context, TemplateConfiguration config );

	public abstract void writeLine(IMendixObject object) throws CoreException;

	public abstract void importFromFile(BufferedReader reader, IMendixObject importFile) throws CoreException, IOException;
	public abstract void importFromFile(BufferedReader reader, IMendixObject importFile, IMendixObject parameterObject, String referenceName) throws CoreException, IOException;
	
	public CustomReplicationSettings initializeSettings(IMendixObject parameterObjectId, String associationName ) throws CoreException {
		CustomReplicationSettings settings = new CustomReplicationSettings(this.context, this.config.getObjectType(), new ErrorHandler());
		ObjectConfig mainConfig = settings.getMainObjectConfig();
		mainConfig.setObjectSearchAction(ObjectSearchAction.CreateEverything);

		if( associationName != null  && parameterObjectId != null ) { 
			settings.setParentAssociation(associationName);
			settings.setParentObjectId(parameterObjectId);
		}

		HashMap<String, String> sortMap = new HashMap<String, String>();
		sortMap.put(Field.MemberNames.ColNumber.toString(), "ASC");
		List<IMendixObject> result = Core.retrieveXPathQuery(this.context, "//" + Field.getType() + "[" + Field.MemberNames.Field_Template + "='" + this.config.getId() + "']", Integer.MAX_VALUE, 0, sortMap);
		for (IMendixObject column : result) {
			DataSource source = DataSource.valueOf((String) column.getValue(this.context, Field.MemberNames.DataSource.toString()));
			switch (source) {
			case Attribute:
				setMappingFromAttribute(settings, column);
				break;
			case Reference:
				setMappingFromAssociation(settings, column);
				break;
			case Newline:
				break;
			case StaticValue:
				break;
			}
		}
		
		return settings;
	}
	
	private void setMappingFromAttribute(CustomReplicationSettings settings, IMendixObject columnObject) throws CoreException {
		IMendixObject member = Core.retrieveId(this.context, (IMendixIdentifier) columnObject.getValue(this.context, Field.MemberNames.Field_MxObjectMember.toString()));

		IValueParser parser = null;
		IMendixIdentifier mfId = (IMendixIdentifier) columnObject.getValue(this.context, Field.MemberNames.Field_Microflows.toString());
		if (mfId != null) {
			parser = new MFValueParser(this.context, Core.retrieveId(this.context, mfId));
		}
		
		String alias = ValueParser.getTrimmedValue( columnObject.getValue(this.context, Field.MemberNames.ColNumber.toString()), null, null );
		settings.addColumnMapping(alias, (String) member.getValue(this.context, MxObjectMember.MemberNames.AttributeName.toString()), KeyType.NoKey, false, parser);
	}

	private void setMappingFromAssociation(CustomReplicationSettings settings, IMendixObject columnObject) throws CoreException {
		IMendixObject member = Core.retrieveId(this.context, (IMendixIdentifier) columnObject.getValue(this.context, Field.MemberNames.Field_MxObjectMember.toString()));
		IMendixObject objectType = Core.retrieveId(this.context, (IMendixIdentifier) columnObject.getValue(this.context, Field.MemberNames.Field_MxObjectType_Reference.toString()));
		IMendixObject reference = Core.retrieveId(this.context, (IMendixIdentifier) columnObject.getValue(this.context, Field.MemberNames.Field_MxObjectReference.toString()));

		IValueParser parser = null;
		IMendixIdentifier mfId = (IMendixIdentifier) columnObject.getValue(this.context, Field.MemberNames.Field_Microflows.toString());
		if (mfId != null) {
			parser = new MFValueParser(this.context, Core.retrieveId(this.context, mfId));
		}

		String alias = String.valueOf( columnObject.getValue(this.context, Field.MemberNames.ColNumber.toString()) );
		settings.addAssociationMapping(alias, (String) reference.getValue(this.context, MxObjectReference.MemberNames.CompleteName.toString()), (String) objectType.getValue(this.context, MxObjectType.MemberNames.CompleteName.toString()), (String) member.getValue(this.context, MxObjectMember.MemberNames.AttributeName.toString()), parser, KeyType.NoKey, false);
	}

}
