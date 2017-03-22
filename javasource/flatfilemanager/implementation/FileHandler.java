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
import java.util.Map;

import org.apache.commons.io.IOUtils;

import com.ibm.icu.text.CharsetDetector;
import com.ibm.icu.text.CharsetMatch;
import com.mendix.core.Core;
import com.mendix.core.CoreException;
import com.mendix.logging.ILogNode;
import com.mendix.systemwideinterfaces.core.IContext;
import com.mendix.systemwideinterfaces.core.IMendixIdentifier;
import com.mendix.systemwideinterfaces.core.IMendixObject;

import flatfilemanager.proxies.Field;
import flatfilemanager.proxies.FormatType;
import flatfilemanager.proxies.LineEndChar;
import flatfilemanager.proxies.ReferenceOrObject;
import flatfilemanager.proxies.Template;
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

	protected class TemplateConfiguration {
		private Integer totalLength;
//		private Object number
		private String templateName;
		private FormatType formatType;
		private Long id;
		private String delimiter;
		private LineEndChar lineEnd;
		private String escapeChar;
		private String quoteChar;
		private boolean headerOnFirstLine;
		private String objectType;

		private Map<Integer, ColumnConfig> columns;
		
		public class ColumnConfig {
			
			private int colNr;
			private int position;
			private String microflow;
			private String mask = null;
			private String staticValue = null;
			
			private IMendixObject columnObj;

			protected ColumnConfig( IMendixObject columnObj, int colNr, int position ) {
				this.colNr = colNr;
				this.position = position;
				this.columnObj = columnObj;
				
				this.staticValue = columnObj.getValue(FileHandler.this.context, Field.MemberNames.StaticValue.toString());
			}
			
			protected void addMask( String mask ) {
				if( mask == null || mask.isEmpty() )
					this.mask = null;
				else 
					this.mask = mask;
			}
			
			public void addMicroflow( String microflow ) {
				this.microflow = microflow;
			}
			
			public IMendixObject getColumnObj() {
				return this.columnObj;
			}
			protected String getMicroflow( ) {
				return this.microflow;
			}
			
			public String getMask() {
				return this.mask;
			}
			
			public String getStaticValue() {
				return this.staticValue;
			}
		}

		private TemplateConfiguration(IMendixObject template) throws CoreException {
			IContext context = FileHandler.this.context;
					
			this.totalLength = (Integer) template.getValue(context, Template.MemberNames.TotalLength.toString());
//			this.number = template.getValue(context, Template.MemberNames.Nr.toString());
			this.templateName = template.getValue(context, Template.MemberNames.Title.toString());
			this.formatType = FormatType.valueOf((String) template.getValue(context, Template.MemberNames.FormatType.toString()));
			this.delimiter = (String) template.getValue(context, Template.MemberNames.Delimiter.toString());
			String lineEnd = (String) template.getValue(context, Template.MemberNames.LineEnd.toString());
			if( lineEnd != null )
				this.lineEnd = LineEndChar.valueOf(lineEnd);
			this.escapeChar = (String) template.getValue(context, Template.MemberNames.EscapeChar.toString());
			this.quoteChar = (String) template.getValue(context, Template.MemberNames.QuoteChar.toString());
			this.headerOnFirstLine = (Boolean) template.getValue(context, Template.MemberNames.HeadersOnFirstLine.toString());

			IMendixIdentifier id = template.getValue(context, Template.MemberNames.Template_MxObjectType.toString());
			IMendixObject obj = Core.retrieveId(context, id);
			
			this.objectType = (String) obj.getValue(context, MxObjectType.MemberNames.CompleteName.toString());
			
			this.id = template.getId().toLong();

			HashMap<String, String> sortMap = new HashMap<String, String>();
			sortMap.put(Field.MemberNames.ColNumber.toString(), "ASC");
			this.columns = new HashMap<Integer, ColumnConfig>();
			List<IMendixObject> result = Core.retrieveXPathQuery(context, "//" + Field.getType() + "[" + Field.MemberNames.Field_Template + "='" + this.id + "']", Integer.MAX_VALUE, 0, sortMap);
			
			for( IMendixObject column : result ) {

				int colNr = column.getValue(context, Field.MemberNames.ColNumber.toString());
				int position = column.getValue(context, Field.MemberNames.FlatFilePosition.toString());
				
				ColumnConfig cc = new ColumnConfig(column, colNr, position);
				
				cc.addMask( column.getValue(context, Field.MemberNames.ValueMask.toString()) );
				
				IMendixIdentifier mfId = column.getValue(context, Field.MemberNames.Field_Microflows.toString());
				if( mfId != null ) {
					IMendixObject mfObj = Core.retrieveId(context, mfId);
					cc.addMicroflow( mfObj.getValue(context, "CompleteName") );
				}
				
				this.columns.put(colNr, cc);
			}
		}

		public String getObjectType() {
			return this.objectType;
		}
		
		public Integer getTotalLength() {
			return this.totalLength;
		}

		public Object getTemplateName() {
			return this.templateName;
		}

		public FormatType getFormatType() {
			return this.formatType;
		}

		public Long getId() {
			return this.id;
		}

		public char getDelimiter() {
			if (this.delimiter == null || this.delimiter.length() == 0)
				return ',';

			return this.delimiter.charAt(0);
		}

		public char getQuoteChar() {
			if (this.quoteChar == null || this.quoteChar.length() == 0)
				return '"';

			return this.quoteChar.charAt(0);
		}

		public char getEscapeChar() {
			if (this.escapeChar == null || this.escapeChar.length() == 0)
				return '\\';

			return this.escapeChar.charAt(0);
		}

		public String getLineEnd() {
			switch (this.lineEnd) {
			case Line_end:
				return "\n";
			case Carriage_return:
			default:
				return "\r\n";
			}
		}

		public boolean headerOnFirstLine() {
			return this.headerOnFirstLine;
		}

		public Map<Integer, ColumnConfig> getColumns() {
			return this.columns;
		}

		public String getMicroflowParser( int colNr ) {
			if( this.columns.containsKey(colNr) ) 
				return this.columns.get(colNr).getMicroflow();
			
			return null;
		}
	}

	private TemplateConfiguration getTemplateConfig(IMendixObject template) throws CoreException {
		Long id = template.getId().toLong();
		if (!this.config.containsKey(id)) {
			this.config.put(id, new TemplateConfiguration(template));
		}

		return this.config.get(id);
	}

	public FileHandler(IContext context, IMendixObject exportConfig, IMendixObject parameterObject) {
		this.context = context;
		this.templateConfig = exportConfig;
		this.parameterObject = parameterObject;
	}

	public FileInputStream exportToFile() throws CoreException {
		File tmpFile = new File(Core.getConfiguration().getTempPath().getAbsolutePath() + "/" + this.parameterObject.getId().toLong());

		HashMap<String, String> sortmap = new HashMap<String, String>();
		sortmap.put(TemplateReference.MemberNames.OrderNr.toString(), "ASC");
		List<IMendixObject> sortedList = Core.retrieveXPathQuery(this.context, "//" + TemplateReference.getType() + "[" + TemplateReference.MemberNames.TemplateReference_TemplateSet + "=" + this.templateConfig.getId().toLong() + "]", Integer.MAX_VALUE, 0, sortmap);
		try {
			FileOutputStream out = new FileOutputStream(tmpFile);
			OutputStreamWriter writer = new OutputStreamWriter(out, "UTF-8");

			for (IMendixObject templateRef : sortedList) {
				processTemplateReference(writer, templateRef, this.parameterObject);
			}
			writer.flush();
			writer.close();

			FileInputStream result = new FileInputStream(tmpFile);
			return result;
		}
		catch (IOException e) {
			throw new CoreException(e);
		}
	}

	public void processTemplateReference( OutputStreamWriter writer, IMendixObject templateRef, IMendixObject exportObject ) throws CoreException {
		IMendixObject template = Core.retrieveId(this.context, (IMendixIdentifier) templateRef.getValue(this.context, TemplateReference.MemberNames.TemplateReference_Template.toString()));
		TemplateConfiguration config = getTemplateConfig(template);
		this.logger.debug("Start exporting using template: " + config.getTemplateName() );
		
		ILineHandler lineHandler = LineHandlerFactory.getLineHandler(this.context, config, writer);

		ReferenceOrObject source = ReferenceOrObject.valueOf((String) templateRef.getValue(this.context, TemplateReference.MemberNames.ObjectSource.toString()));
		List<IMendixIdentifier> subTemplateIdList = templateRef.getValue(this.context, TemplateReference.MemberNames.TemplateReference_SubTemplate.toString());
		List<IMendixObject> subTemplates = null;
		if( subTemplateIdList != null && subTemplateIdList.size() > 0 ) {
			subTemplates = new ArrayList<IMendixObject>();
			for( IMendixIdentifier id : subTemplateIdList )
				subTemplates.add( Core.retrieveId(this.context, id) );
		}
		
		if (source == ReferenceOrObject.Reference) {
			this.logger.debug("Creating multiple lines, using template: " + config.getTemplateName() );
			
			TemplateReference tr = TemplateReference.initialize(this.context, templateRef);

			MxObjectReference ref = tr.getTemplateReference_MxObjectReference();
			if( ref == null )
				throw new CoreException("The Template reference is required");
			MxObjectType objTypeTo = tr.getTemplateReference_MxObjectType_To();
			if( objTypeTo == null )
				throw new CoreException("The Template Object Type is required");
			MxObjectMember sortOnMember = tr.getTemplateReference_SortOn_MxObjectMember();
			if( sortOnMember == null )
				throw new CoreException("The attribute to sort on is required");
			
			HashMap<String, String> sortMap = new HashMap<String, String>();
			sortMap.put(sortOnMember.getAttributeName(), "ASC");
			int totalSize = 0, limit = 1000, offset = 0;
			List<IMendixObject> result;
			do {
				result = Core.retrieveXPathQuery(this.context, "//" + objTypeTo.getCompleteName() + "[" + ref.getCompleteName() + "=" + exportObject.getId().toLong() + "]", limit, offset, sortMap);
				for (IMendixObject associatedObject : result) {
					lineHandler.writeLine(associatedObject);
					
					if( subTemplates != null ) {
						for( IMendixObject subTemplate : subTemplates )
							processTemplateReference(writer, subTemplate, associatedObject);
					}
				}
				totalSize+=result.size();
				offset += limit;
			}
			while( result.size() > 0 );

			this.logger.trace("Processing association: " + ref.getCompleteName() + ", retrieved " + totalSize + " associated objects, using template: " + config.getTemplateName() );
		}
		else {
			this.logger.debug("Creating single line, using template: " + config.getTemplateName() );
			lineHandler.writeLine(exportObject);
			if( subTemplates != null ) {
				for( IMendixObject subTemplate : subTemplates )
					processTemplateReference(writer, subTemplate, exportObject);
			}

		}
		
	}

	public void importFromFile(IMendixObject importFile) throws CoreException {
		HashMap<String, String> sortmap = new HashMap<String, String>();
		sortmap.put(TemplateReference.MemberNames.OrderNr.toString(), "ASC");
		List<IMendixObject> sortedList = Core.retrieveXPathQuery(this.context, "//" + TemplateReference.getType() + "[" + TemplateReference.MemberNames.TemplateReference_TemplateSet + "=" + this.templateConfig.getId().toLong() + "]", Integer.MAX_VALUE, 0, sortmap);
		try {
			if (sortedList.size() > 1)
				throw new CoreException("Import templates currently only support 1 template. Please us a template set with a single template in it.");

			BufferedReader reader = new BufferedReader(new InputStreamReader(Core.getFileDocumentContent(this.context, importFile), "UTF-8"));

			for (IMendixObject sortOrder : sortedList) {
				IMendixObject template = Core.retrieveId(this.context, (IMendixIdentifier) sortOrder.getValue(this.context, TemplateReference.MemberNames.TemplateReference_Template.toString()));
				TemplateConfiguration config = getTemplateConfig(template);
				
				this.logger.debug("Start importing using template: " + config.getTemplateName() );

				//Initialize the line handler
				ILineHandler lineHandler = LineHandlerFactory.getLineHandler(this.context, config);
				
				/* 
				 * Validate the input object and import the file based on the association or direct into the parameter
				 */
				IMendixIdentifier objFromId = sortOrder.getValue(this.context, TemplateReference.MemberNames.TemplateReference_MxObjectType_From.toString() );
				if( objFromId == null ) 
					throw new CoreException("Invalid configuration for template: " + config.getTemplateName() + " no parameter object type specified");
//TODO validate:				IMendixObject objFrom = Core.retrieveId(this.context, objFromId);
				
				IMendixIdentifier refId = sortOrder.getValue(this.context, TemplateReference.MemberNames.TemplateReference_MxObjectReference.toString() );
				if( refId != null ) {
					IMendixIdentifier objToId = sortOrder.getValue(this.context, TemplateReference.MemberNames.TemplateReference_MxObjectType_To.toString() );
					if( objToId == null ) 
						throw new CoreException("Invalid configuration for template: " + config.getTemplateName() + " no target object type specified");
					
//TODO validate:					IMendixObject objTo = Core.retrieveId(this.context, objToId);
					IMendixObject ref = Core.retrieveId(this.context, refId);

					//TODO validate and compare the object types and association
					
					this.logger.debug("Importing a parameter over association: " + ref.getValue(this.context, MxObjectReference.MemberNames.CompleteName.toString()) + " , using template: " + config.getTemplateName() );
					lineHandler.importFromFile(reader, importFile, this.parameterObject, (String) ref.getValue(this.context, MxObjectReference.MemberNames.CompleteName.toString()));
				}
				else {
					this.logger.debug("Importing without a parameter, using template: " + config.getTemplateName() );
					lineHandler.importFromFile(reader, importFile);
				}
				
			}
		}
		catch (IOException e) {
			throw new CoreException(e);
		}
	}

	@SuppressWarnings("unused")
	private static File createUTF8FileFromStream(InputStream is) throws FileNotFoundException, IOException {
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

	private static File createTempFile(InputStream is, String encoding) throws FileNotFoundException, IOException {
		File file = File.createTempFile("CSVMx", "CSVMx");
		FileOutputStream ous = new FileOutputStream(file);
		byte buf[] = new byte[1024];
		int len;
		while ((len = is.read(buf)) > 0) {
			if (encoding != null) {
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
