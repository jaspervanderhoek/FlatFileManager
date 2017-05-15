package flatfilemanager.implementation;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.io.IOUtils;

import com.ibm.icu.text.CharsetDetector;
import com.ibm.icu.text.CharsetMatch;
import com.mendix.core.Core;
import com.mendix.core.CoreException;
import com.mendix.logging.ILogNode;
import com.mendix.systemwideinterfaces.core.IContext;
import com.mendix.systemwideinterfaces.core.IMendixIdentifier;
import com.mendix.systemwideinterfaces.core.IMendixObject;

import flatfilemanager.proxies.ReferenceOrObject;
import flatfilemanager.proxies.TemplateReference;
import mxmodelreflection.proxies.MxObjectMember;
import mxmodelreflection.proxies.MxObjectReference;
import mxmodelreflection.proxies.MxObjectType;

public class FileHandler {

	private HashMap<Long, TemplateConfiguration> config = new HashMap<Long, TemplateConfiguration>();
	private ILogNode logger = Core.getLogger("FlatFileExport");
	private IContext context;
	private IMendixObject parameterObject;
	private IMendixObject templateConfig;

	private TemplateConfiguration getTemplateConfig( IMendixObject template ) throws CoreException {
		Long id = template.getId().toLong();
		if ( !this.config.containsKey(id) ) {
			this.config.put(id, new TemplateConfiguration(this.context, template));
		}

		return this.config.get(id);
	}

	public FileHandler( IContext context, IMendixObject exportConfig, IMendixObject parameterObject ) {
		this.context = context;
		this.templateConfig = exportConfig;
		this.parameterObject = parameterObject;
	}

	public FileInputStream exportToFile() throws CoreException {
		File tmpFile = new File(Core.getConfiguration().getTempPath().getAbsolutePath() + "/" + this.parameterObject.getId().toLong());

		HashMap<String, String> sortmap = new HashMap<String, String>();
		sortmap.put(TemplateReference.MemberNames.OrderNr.toString(), "ASC");
		List<IMendixObject> sortedList = Core.retrieveXPathQuery(this.context, "//" + TemplateReference.getType() + 
				"[" + TemplateReference.MemberNames.TemplateReference_TemplateSet + "=" + this.templateConfig.getId().toLong() + "]",
				Integer.MAX_VALUE, 0, sortmap);
		try {
			FileOutputStream out = new FileOutputStream(tmpFile);
			OutputStreamWriter writer = new OutputStreamWriter(out, "UTF-8");

			for( IMendixObject templateRef : sortedList ) {
				processTemplateReference(writer, templateRef, this.parameterObject);
			}
			writer.flush();
			writer.close();

			FileInputStream result = new FileInputStream(tmpFile);
			return result;
		}
		catch( IOException e ) {
			throw new CoreException(e);
		}
	}

	public void processTemplateReference( OutputStreamWriter writer, IMendixObject templateRef, IMendixObject exportObject ) throws CoreException {
		IMendixObject template = Core.retrieveId(this.context, (IMendixIdentifier) templateRef.getValue(this.context, TemplateReference.MemberNames.TemplateReference_Template.toString()));
		TemplateConfiguration config = getTemplateConfig(template);
		this.logger.debug("Start exporting using template: " + config.getTemplateName());

		ILineHandler lineHandler = LineHandlerFactory.getLineHandler(this.context, config, writer);

		ReferenceOrObject source = ReferenceOrObject.valueOf((String) templateRef.getValue(this.context, TemplateReference.MemberNames.ObjectSource.toString()));
		List<IMendixIdentifier> subTemplateIdList = templateRef.getValue(this.context, TemplateReference.MemberNames.TemplateReference_SubTemplate.toString());
		List<IMendixObject> subTemplates = null;
		if ( subTemplateIdList != null && subTemplateIdList.size() > 0 ) {
			subTemplates = new ArrayList<IMendixObject>();
			for( IMendixIdentifier id : subTemplateIdList )
				subTemplates.add(Core.retrieveId(this.context, id));
		}

		if ( source == ReferenceOrObject.Reference ) {
			this.logger.debug("Creating multiple lines, using template: " + config.getTemplateName());

			TemplateReference tr = TemplateReference.initialize(this.context, templateRef);

			MxObjectReference ref = tr.getTemplateReference_MxObjectReference();
			if ( ref == null )
				throw new CoreException("The Template reference is required");
			MxObjectType objTypeTo = tr.getTemplateReference_MxObjectType_To();
			if ( objTypeTo == null )
				throw new CoreException("The Template Object Type is required");
			MxObjectMember sortOnMember = tr.getTemplateReference_SortOn_MxObjectMember();
			if ( sortOnMember == null )
				throw new CoreException("The attribute to sort on is required");

			HashMap<String, String> sortMap = new HashMap<String, String>();
			sortMap.put(sortOnMember.getAttributeName(), "ASC");
			int totalSize = 0, limit = 1000, offset = 0;
			List<IMendixObject> result;
			do {
				result = Core.retrieveXPathQuery(this.context,
						"//" + objTypeTo.getCompleteName() + "[" + ref.getCompleteName() + "=" + exportObject.getId().toLong() + "]", limit, offset,
						sortMap);
				for( IMendixObject associatedObject : result ) {
					lineHandler.writeLine(associatedObject);

					if ( subTemplates != null ) {
						for( IMendixObject subTemplate : subTemplates )
							processTemplateReference(writer, subTemplate, associatedObject);
					}
				}
				totalSize += result.size();
				offset += limit;
			} while( result.size() > 0 );

			this.logger.trace("Processing association: " + ref
					.getCompleteName() + ", retrieved " + totalSize + " associated objects, using template: " + config.getTemplateName());
		}
		else {
			this.logger.debug("Creating single line, using template: " + config.getTemplateName());
			lineHandler.writeLine(exportObject);
			if ( subTemplates != null ) {
				for( IMendixObject subTemplate : subTemplates )
					processTemplateReference(writer, subTemplate, exportObject);
			}

		}

	}

	public void importFromFile( IMendixObject importFile ) throws CoreException {
		HashMap<String, String> sortmap = new HashMap<String, String>();
		sortmap.put(TemplateReference.MemberNames.OrderNr.toString(), "ASC");
		List<IMendixObject> sortedList = Core.retrieveXPathQuery(this.context, "//" + TemplateReference
				.getType() + "[" + TemplateReference.MemberNames.TemplateReference_TemplateSet + "=" + this.templateConfig.getId().toLong() + "]",
				Integer.MAX_VALUE, 0, sortmap);
		try {
			if ( sortedList.size() > 1 )
				throw new CoreException("Import templates currently only support 1 template. Please us a template set with a single template in it.");

			BufferedReader reader = new BufferedReader(new InputStreamReader(Core.getFileDocumentContent(this.context, importFile), "UTF-8"));

			for( IMendixObject templateReference : sortedList ) {
				IMendixObject template = Core.retrieveId(this.context, (IMendixIdentifier) templateReference.getValue(this.context, TemplateReference.MemberNames.TemplateReference_Template.toString()));
				TemplateConfiguration config = getTemplateConfig(template);

				this.logger.debug("Start importing using template: " + config.getTemplateName());

				// Initialize the line handler
				ILineHandler lineHandler = LineHandlerFactory.getLineHandler(this.context, config);

				/*
				 * Validate the input object and import the file based on the association or direct into the parameter
				 */
				IMendixIdentifier objFromId = templateReference.getValue(this.context, TemplateReference.MemberNames.TemplateReference_MxObjectType_From.toString());
				if ( objFromId == null )
					throw new CoreException("Invalid configuration for template: " + config.getTemplateName() + " no parameter object type specified");
				// TODO validate: IMendixObject objFrom = Core.retrieveId(this.context, objFromId);

				IMendixIdentifier refId = templateReference.getValue(this.context, TemplateReference.MemberNames.TemplateReference_MxObjectReference.toString());
				if ( refId != null ) {
					IMendixIdentifier objToId = templateReference.getValue(this.context, TemplateReference.MemberNames.TemplateReference_MxObjectType_To.toString());
					if ( objToId == null )
						throw new CoreException("Invalid configuration for template: " + config.getTemplateName() + " no target object type specified");

					// TODO validate: IMendixObject objTo = Core.retrieveId(this.context, objToId);
					IMendixObject ref = Core.retrieveId(this.context, refId);

					// TODO validate and compare the object types and association

					this.logger.debug("Importing a parameter over association: " + ref.getValue(this.context, MxObjectReference.MemberNames.CompleteName.toString()) + " , using template: " + config.getTemplateName());
					lineHandler.importFromFile(reader, importFile, this.parameterObject, (String) ref.getValue(this.context, MxObjectReference.MemberNames.CompleteName.toString()));
				}
				else {
					this.logger.debug("Importing without a parameter, using template: " + config.getTemplateName());
					lineHandler.importFromFile(reader, importFile);
				}

			}
		}
		catch( IOException e ) {
			throw new CoreException(e);
		}
	}

	@SuppressWarnings("unused")
	private static File createUTF8FileFromStream( InputStream is ) throws FileNotFoundException, IOException {
		File file = createTempFile(is, null);

		BufferedInputStream fis1 = new BufferedInputStream(new FileInputStream(file));
		byte[] byteData = new byte[fis1.available()];
		fis1.read(byteData);
		fis1.close();

		CharsetDetector detector = new CharsetDetector();
		detector.setText(byteData);
		CharsetMatch match = detector.detect();

		BufferedInputStream fis = new BufferedInputStream(new FileInputStream(file));
		File f2 = createTempFile(fis, match.getName());

		return f2;
	}

	private static File createTempFile( InputStream is, String encoding ) throws FileNotFoundException, IOException {
		File file = File.createTempFile("CSVMx", "CSVMx");
		FileOutputStream ous = new FileOutputStream(file);
		byte buf[] = new byte[1024];
		int len;
		while( (len = is.read(buf)) > 0 ) {
			if ( encoding != null ) {
				byte b[] = IOUtils.toString(buf, encoding).getBytes("UTF-8");
				ous.write(b, 0, len);
			}
			else
				ous.write(buf, 0, len);
		}
		ous.close();
		is.close();
		return file;
	}
}
