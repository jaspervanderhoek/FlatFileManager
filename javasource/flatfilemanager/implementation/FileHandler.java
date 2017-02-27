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
import java.util.HashMap;
import java.util.List;

import mxmodelreflection.proxies.MxObjectReference;
import mxmodelreflection.proxies.MxObjectType;

import org.apache.commons.io.IOUtils;

import com.ibm.icu.text.CharsetDetector;
import com.ibm.icu.text.CharsetMatch;
import com.mendix.core.Core;
import com.mendix.core.CoreException;
import com.mendix.logging.ILogNode;
import com.mendix.systemwideinterfaces.core.IContext;
import com.mendix.systemwideinterfaces.core.IMendixIdentifier;
import com.mendix.systemwideinterfaces.core.IMendixObject;

import flatfilemanager.proxies.FormatType;
import flatfilemanager.proxies.LineEndChar;
import flatfilemanager.proxies.ReferenceOrObject;
import flatfilemanager.proxies.SortOrder;
import flatfilemanager.proxies.Template;

public class FileHandler {

	private HashMap<Long, TemplateConfiguration> config = new HashMap<Long, TemplateConfiguration>();
	private ILogNode logger = Core.getLogger("FlatFileExport");
	private IContext context;
	private IMendixObject parameterObject;
	private IMendixObject templateConfig;

	protected class TemplateConfiguration {
		private Integer totalLength;
		private Object number;
		private FormatType formatType;
		private Long id;
		private String delimiter;
		private LineEndChar lineEnd;
		private String escapeChar;
		private String quoteChar;
		private boolean headerOnFirstLine;
		private String objectType;

		private TemplateConfiguration(IMendixObject template) throws CoreException {
			this.totalLength = (Integer) template.getValue(FileHandler.this.context, Template.MemberNames.TotalLength.toString());
			this.number = template.getValue(FileHandler.this.context, Template.MemberNames.Nr.toString());
			this.formatType = FormatType.valueOf((String) template.getValue(FileHandler.this.context, Template.MemberNames.FormatType.toString()));
			this.delimiter = (String) template.getValue(FileHandler.this.context, Template.MemberNames.Delimiter.toString());
			String lineEnd = (String) template.getValue(FileHandler.this.context, Template.MemberNames.LineEnd.toString());
			if( lineEnd != null )
				this.lineEnd = LineEndChar.valueOf(lineEnd);
			this.escapeChar = (String) template.getValue(FileHandler.this.context, Template.MemberNames.EscapeChar.toString());
			this.quoteChar = (String) template.getValue(FileHandler.this.context, Template.MemberNames.QuoteChar.toString());
			this.headerOnFirstLine = (Boolean) template.getValue(FileHandler.this.context, Template.MemberNames.HeadersOnFirstLine.toString());

			IMendixIdentifier id = template.getValue(FileHandler.this.context, Template.MemberNames.Template_MxObjectType.toString());
			IMendixObject obj = Core.retrieveId(FileHandler.this.context, id);
			
			this.objectType = (String) obj.getValue(FileHandler.this.context, MxObjectType.MemberNames.CompleteName.toString());
			
			this.id = template.getId().toLong();
		}

		public String getObjectType() {
			return this.objectType;
		}
		
		public Integer getTotalLength() {
			return this.totalLength;
		}

		public Object getNumber() {
			return this.number;
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
		sortmap.put(SortOrder.MemberNames.OrderNr.toString(), "ASC");
		List<IMendixObject> sortedList = Core.retrieveXPathQuery(this.context, "//" + SortOrder.getType() + "[" + SortOrder.MemberNames.SortOrder_TemplateSet + "=" + this.templateConfig.getId().toLong() + "]", Integer.MAX_VALUE, 0, sortmap);
		try {
			FileOutputStream out = new FileOutputStream(tmpFile);
			OutputStreamWriter writer = new OutputStreamWriter(out, "UTF-8");

			for (IMendixObject sortOrder : sortedList) {
				IMendixObject template = Core.retrieveId(this.context, (IMendixIdentifier) sortOrder.getValue(this.context, SortOrder.MemberNames.SortOrder_Template.toString()));
				TemplateConfiguration config = getTemplateConfig(template);
				this.logger.debug("Start exporting using template: " + config.getNumber() );
				
				ILineHandler lineHandler = LineHandlerFactory.getLineHandler(this.context, config, writer);

				ReferenceOrObject source = ReferenceOrObject.valueOf((String) sortOrder.getValue(this.context, SortOrder.MemberNames.ObjectSource.toString()));

				if (source == ReferenceOrObject.Reference) {
					this.logger.debug("Creating multiple lines, using template: " + config.getNumber() );
					
					SortOrder so = SortOrder.initialize(this.context, sortOrder);
					MxObjectReference ref = so.getSortOrder_MxObjectReference();
					MxObjectType objTypeTo = so.getSortOrder_MxObjectType_To();

					HashMap<String, String> sortMap = new HashMap<String, String>();
					sortMap.put("createdDate", "ASC");
					int totalSize = 0, limit = 1000, offset = 0;
					List<IMendixObject> result;
					do {
						result = Core.retrieveXPathQuery(this.context, "//" + objTypeTo.getCompleteName() + "[" + ref.getCompleteName() + "=" + this.parameterObject.getId().toLong() + "]", limit, offset, sortMap);
						for (IMendixObject associatedObject : result) {
							lineHandler.writeLine(associatedObject);
						}
						totalSize+=result.size();
						offset += limit;
					}
					while( result.size() > 0 );
					
					this.logger.trace("Processing association: " + ref.getCompleteName() + ", retrieved " + totalSize + " associated objects, using template: " + config.getNumber() );
				}
				else {
					this.logger.debug("Creating single line, using template: " + config.getNumber() );
					lineHandler.writeLine(this.parameterObject);
				}
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

	public void importFromFile(IMendixObject importFile) throws CoreException {
		HashMap<String, String> sortmap = new HashMap<String, String>();
		sortmap.put(SortOrder.MemberNames.OrderNr.toString(), "ASC");
		List<IMendixObject> sortedList = Core.retrieveXPathQuery(this.context, "//" + SortOrder.getType() + "[" + SortOrder.MemberNames.SortOrder_TemplateSet + "=" + this.templateConfig.getId().toLong() + "]", Integer.MAX_VALUE, 0, sortmap);
		try {
			if (sortedList.size() > 1)
				throw new CoreException("Import templates currently only support 1 template. Please us a template set with a single template in it.");

			BufferedReader reader = new BufferedReader(new InputStreamReader(Core.getFileDocumentContent(this.context, importFile), "UTF-8"));

			for (IMendixObject sortOrder : sortedList) {
				IMendixObject template = Core.retrieveId(this.context, (IMendixIdentifier) sortOrder.getValue(this.context, SortOrder.MemberNames.SortOrder_Template.toString()));
				TemplateConfiguration config = getTemplateConfig(template);
				
				this.logger.debug("Start importing using template: " + config.getNumber() );

				//Initialize the line handler
				ILineHandler lineHandler = LineHandlerFactory.getLineHandler(this.context, config);
				
				/* 
				 * Validate the input object and import the file based on the association or direct into the parameter
				 */
				IMendixIdentifier objFromId = sortOrder.getValue(this.context, SortOrder.MemberNames.SortOrder_MxObjectType_From.toString() );
				if( objFromId == null ) 
					throw new CoreException("Invalid configuration for template: " + config.getNumber() + " no parameter object type specified");
//TODO validate:				IMendixObject objFrom = Core.retrieveId(this.context, objFromId);
				
				IMendixIdentifier refId = sortOrder.getValue(this.context, SortOrder.MemberNames.SortOrder_MxObjectReference.toString() );
				if( refId != null ) {
					IMendixIdentifier objToId = sortOrder.getValue(this.context, SortOrder.MemberNames.SortOrder_MxObjectType_To.toString() );
					if( objToId == null ) 
						throw new CoreException("Invalid configuration for template: " + config.getNumber() + " no target object type specified");
					
//TODO validate:					IMendixObject objTo = Core.retrieveId(this.context, objToId);
					IMendixObject ref = Core.retrieveId(this.context, refId);

					//TODO validate and compare the object types and association
					
					this.logger.debug("Importing a parameter over association: " + ref.getValue(this.context, MxObjectReference.MemberNames.CompleteName.toString()) + " , using template: " + config.getNumber() );
					lineHandler.importFromFile(reader, importFile, this.parameterObject, (String) ref.getValue(this.context, MxObjectReference.MemberNames.CompleteName.toString()));
				}
				else {
					this.logger.debug("Importing without a parameter, using template: " + config.getNumber() );
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