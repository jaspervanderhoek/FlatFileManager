package replication.helpers;

import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import replication.MetaInfo.MILogNode;
import replication.MetaInfo.MetaInfoObject;
import replication.ReplicationSettings;
import replication.ReplicationSettings.MendixReplicationException;
import replication.ValueParser;

import com.mendix.systemwideinterfaces.core.meta.IMetaPrimitive.PrimitiveType;

public class OQLBuilder {

	/**
	 * Create an OQL query which should retrieve all objects that are specified by the keys from the MetaInfoObject
	 * @return the OQL query that should be executed to retrieve all objects
	 * @throws MendixReplicationException
	 */
	public static StringBuilder buildOQLQuery( ReplicationSettings settings, Map<String, MetaInfoObject> meta_Objects, String objectDefinition, boolean isAssociationQuery, int limit, int offset, ValueParser parser ) throws MendixReplicationException {
		StringBuilder builder = new StringBuilder(1000);

		if( meta_Objects.size() > 0 ) {
			String[] objectTypeArr;
			if( isAssociationQuery )
				objectTypeArr = settings.getAssociationConfig(objectDefinition).getObjectType().split("\\.");
			else 
				objectTypeArr = objectDefinition.split("\\.");
			
			builder.append("SELECT specifiedObject/ID FROM \"").append(objectTypeArr[0]).append("\".\"").append(objectTypeArr[1]).append("\" AS specifiedObject ");
			if( !isAssociationQuery ) {
				for( String key : settings.getMainObjectConfig().getKeys().keySet() ) {
					if( settings.treatFieldAsReference(key) || settings.treatFieldAsReferenceSet(key) ) {
						builder.append(" LEFT JOIN specifiedObject").append("/");
	
						objectTypeArr = settings.getAssociationNameByAlias(key).split("\\.");
						builder.append("\"").append(objectTypeArr[0]).append("\".\"").append(objectTypeArr[1]).append("\"/");
	
						objectTypeArr = settings.getAssociationObjectTypeByAlias(key).split("\\.");
						builder.append( "\"" ).append(objectTypeArr[0]).append("\".\"").append(objectTypeArr[1]).append("\" AS tbl_").append(key.replace("-", "")).append(" ");
					}
				}
			}
			builder.append(" WHERE ");

			/*
			 * Build the oql constraint out of all members and associated members
			 */
			Set<String> keySet;
			if( isAssociationQuery )
				keySet = settings.getAssociationKeys(objectDefinition).keySet();
			else 
				keySet = settings.getMainObjectConfig().getKeys().keySet();
			
			int counter = 0;
			limit += offset;
			SortedSet<String> sortedKeys = new TreeSet<String>(meta_Objects.keySet());
			for( String objectKey : sortedKeys ) { //Entry<String, MetaInfoObject> entry : meta_Objects.entrySet() ) {
				MetaInfoObject object = meta_Objects.get(objectKey); //entry.getKey();
				counter++;
				if( counter <= offset ) 
					continue;
				if( counter > limit ) 
					break;
				
				Map<String, Object> memberValues = object.getAttributeValues();
				builder.append( "(" );
				for( String key : keySet ) {
					String value = "", memberName, queryAlias;
					if( isAssociationQuery ) {
						memberName = settings.getAssociationColumnByAlias(key);
						queryAlias = "specifiedObject";
						//Call the get query key value which prepares the value for an oql query
						value = parser.getQueryKeyValue( settings.getMemberType(key), key, memberValues.get(memberName) );
					}
					else if( settings.treatFieldAsReference(key) || settings.treatFieldAsReferenceSet(key) ) {
						memberName = settings.getAssociationColumnByAlias(key);
						queryAlias = "tbl_" + key.replace("-", "");
						//Call the get query key value which prepares the value for an oql query
						value = parser.getQueryKeyValue( settings.getMemberType(key), key, object.getReferenceValues().get(settings.getAssociationNameByAlias(key)).getAttributeValue(memberName) );
					}
					else {
						memberName = settings.getMemberNameByAlias(key);
						queryAlias = "specifiedObject";
						//Call the get query key value which prepares the value for an oql query
						value = parser.getQueryKeyValue( settings.getMemberType(key), key, memberValues.get(memberName) );
					}
					
					builder.append(queryAlias).append("/").append(memberName);

					if( value == null || "null".equals(value) )
						builder.append("=NULL AND ");
					else {
						PrimitiveType type = settings.getMemberType(key);
						switch (type) {
						case AutoNumber: 
						case Integer: 
						case Long:
						case Decimal:
							builder.append("=").append( value );
							break;

						default:
							//Compatibility fix since Currency is no longer part of the latest release 
							if( "Currency".equals(type.toString()) || "Float".equals(type.toString()) )
								builder.append("=").append( value );
							else 
								builder.append("='").append( value ).append("'");
							break;
						}
						builder.append(" AND ");
					}
				}
				builder.delete((builder.length()-4), builder.length()).append( ") OR " );
			}
			builder.delete(builder.length()-3, builder.length());
			
			if( isAssociationQuery ) {
				if( MILogNode.Replication_MetaInfo_AssociatedObjects.getLogger().isDebugEnabled() )
					MILogNode.Replication_MetaInfo_AssociatedObjects.getLogger().debug("Creating an OQL query for association: " + objectDefinition + " containing " + (counter-offset) + " associated MetaInfoObjects. Query: \r\n" + builder.toString() );
			}
			else {
				if( MILogNode.Replication_MetaInfo_MainObject.getLogger().isDebugEnabled() )
					MILogNode.Replication_MetaInfo_MainObject.getLogger().debug("Creating an OQL query for: " + (counter-offset) + " MetaInfoObjects. Query: \r\n" + builder.toString() );
			}
		}

		return builder;
	}
}