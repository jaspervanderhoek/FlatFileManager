package replication.interfaces;

import java.util.List;

import replication.MetaInfo.MetaInfoObject;

import com.mendix.core.CoreException;
import com.mendix.systemwideinterfaces.core.IMendixIdentifier;
import com.mendix.systemwideinterfaces.core.IMendixObject;

public abstract class IUnknownObjectHandler {
	
	/**
	 * 
	 * @param map
	 * @param mendixObject
	 * @return
	 * @throws CoreException
	 */
	public IMendixIdentifier processUnknownObjectGetId( MetaInfoObject referencedObjectValue, IMendixObject mendixObject ) throws CoreException {
		IMendixObject obj = processUnknownObject( referencedObjectValue, mendixObject );
		if( obj == null )
			return null;
				
		else 
			return obj.getId();
	}
	
	public abstract IMendixObject processUnknownObject( MetaInfoObject referencedObjectValue, IMendixObject mendixObject ) throws CoreException;

	/**
	 * 
	 * @param map
	 * @param mendixObject
	 * @return
	 * @throws CoreException
	 */
	public abstract List<IMendixIdentifier> processUnknownObject(List<MetaInfoObject> referencedObjectsValue, IMendixObject mendixObject) throws CoreException;
	
}
