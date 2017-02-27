// This file was generated by Mendix Modeler.
//
// WARNING: Only the following code will be retained when actions are regenerated:
// - the import list
// - the code between BEGIN USER CODE and END USER CODE
// - the code between BEGIN EXTRA CODE and END EXTRA CODE
// Other code you write will be lost the next time you deploy the project.
// Special characters, e.g., é, ö, à, etc. are supported in comments.

package flatfilemanager.actions;

import com.mendix.systemwideinterfaces.core.IMendixObject;
import flatfilemanager.implementation.FileHandler;
import com.mendix.systemwideinterfaces.core.IContext;
import com.mendix.webui.CustomJavaAction;

public class ImportFromFile extends CustomJavaAction<Boolean>
{
	private IMendixObject ImportObject;
	private IMendixObject __ImportConfig;
	private flatfilemanager.proxies.TemplateSet ImportConfig;
	private IMendixObject __ImportFileParam;
	private system.proxies.FileDocument ImportFileParam;

	public ImportFromFile(IContext context, IMendixObject ImportObject, IMendixObject ImportConfig, IMendixObject ImportFileParam)
	{
		super(context);
		this.ImportObject = ImportObject;
		this.__ImportConfig = ImportConfig;
		this.__ImportFileParam = ImportFileParam;
	}

	@Override
	public Boolean executeAction() throws Exception
	{
		this.ImportConfig = __ImportConfig == null ? null : flatfilemanager.proxies.TemplateSet.initialize(getContext(), __ImportConfig);

		this.ImportFileParam = __ImportFileParam == null ? null : system.proxies.FileDocument.initialize(getContext(), __ImportFileParam);

		// BEGIN USER CODE
		FileHandler handler = new FileHandler(getContext(), this.ImportConfig.getMendixObject(), this.ImportObject);
		handler.importFromFile(this.__ImportFileParam);
		
		return true;
		// END USER CODE
	}

	/**
	 * Returns a string representation of this action
	 */
	@Override
	public String toString()
	{
		return "ImportFromFile";
	}

	// BEGIN EXTRA CODE
	// END EXTRA CODE
}
