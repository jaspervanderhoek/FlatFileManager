package flatfilemanager.implementation;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.mendix.core.Core;
import com.mendix.core.CoreException;
import com.mendix.logging.ILogNode;
import com.mendix.systemwideinterfaces.core.IContext;
import com.mendix.systemwideinterfaces.core.IMendixIdentifier;
import com.mendix.systemwideinterfaces.core.IMendixObject;

import au.com.bytecode.opencsv.CSVParser;
import au.com.bytecode.opencsv.CSVWriter;
import flatfilemanager.implementation.TemplateConfiguration.ColumnConfig;
import flatfilemanager.proxies.DataSource;
import flatfilemanager.proxies.Field;
import mxmodelreflection.proxies.MxObjectMember;
import mxmodelreflection.proxies.MxObjectReference;
import mxmodelreflection.proxies.MxObjectType;
import replication.MetaInfo;
import replication.ReplicationSettings.MendixReplicationException;
import replication.ValueParser;
import replication.implementation.CustomReplicationSettings;

public class DelimitedLineHandler extends ILineHandler {

	private ILogNode logger = Core.getLogger("FlatFileExport");
	private Writer writer;

	public DelimitedLineHandler() {
	}

	@Override
	public void initialize(IContext context, Writer writer, TemplateConfiguration config) {
		this.context = context;
		this.writer = writer;
		this.config = config;
	}

	@Override
	public void initialize(IContext context, TemplateConfiguration config) {
		this.context = context;
		this.config = config;
	}

	@Override
	public void writeLine(IMendixObject object) throws CoreException {
		try {
			CSVWriter writer = new CSVWriter(this.writer, this.config.getDelimiter(), this.config.getQuoteChar(), this.config.getEscapeChar(), this.config.getLineEnd());

			String[] entries = this.getLineContent(object);

			writer.writeNext(entries);
			// writer.close();
		}
		catch (IOException e) {
			throw new CoreException(e);
		}
	}

	private String[] getLineContent(IMendixObject object) throws CoreException, IOException {

		

		String[] entries = new String[this.config.getColumns().size()];
		int i = -1;
		for (Entry<Integer, ColumnConfig> entry : this.config.getColumns().entrySet()) {
			ColumnConfig config = entry.getValue();
			IMendixObject column = config.getColumnObj();
			DataSource source = DataSource.valueOf((String) column.getValue(this.context, Field.MemberNames.DataSource.toString()));
//			Integer colNr = entry.getKey();
			
			String value = null;
			switch (source) {
			case Attribute:
				value = (String) this.getValueFromAttribute(column, object);
				break;
			case Reference:
				value = (String) this.getValueFromReference(column, object);
				break;
			case StaticValue:
				value = (String) column.getValue(this.context, Field.MemberNames.StaticValue.toString());
				break;
			case Newline:
				continue;
			}
			entries[++i] = value;
		}

		return entries;
	}

	@Override
	public void importFromFile(BufferedReader reader, IMendixObject importFile) throws CoreException, IOException {
		importFromFile(reader, importFile, null, null);
	}
	@Override
	public void importFromFile(BufferedReader reader, IMendixObject importFile, IMendixObject parameterObjectId, String associationName) throws CoreException, IOException {
	
		CustomReplicationSettings settings = initializeSettings(parameterObjectId, associationName);

		Map<Integer,ColumnConfig> columns = this.config.getColumns();
		
		FFValueParser vparser = new FFValueParser(settings.getValueParsers(), settings);
		MetaInfo info = new MetaInfo(settings, vparser, "DelimitedFileImport");
		CSVParser parser = new CSVParser(this.config.getDelimiter(), this.config.getQuoteChar(), this.config.getEscapeChar(), false, true);

		String nextLine;
		String[] content;
		int lineNumber = 0, colNr = 1;
		while ((nextLine = reader.readLine()) != null) {
			colNr = 1;
			if (lineNumber != 0 || !this.config.headerOnFirstLine() ) {
				content = parser.parseLine(nextLine);
				if (content.length == 1)
					content = parser.parseLine(content[0]);

				for (Entry<Integer, ColumnConfig> entry : columns.entrySet() ) {
					try {
						ColumnConfig cConfig = entry.getValue();
						colNr = entry.getKey();
						
						String alias = cConfig.getDescription();
						if (this.logger.isDebugEnabled())
							this.logger.debug(alias + " - NrOfValues" + content.length);
						
						
						if (colNr <= content.length) {
							switch (cConfig.getValueSource()) {
							case Attribute:
							case Reference:
								String fieldAlias = String.valueOf(colNr);
								
								info.addValue(String.valueOf(lineNumber), fieldAlias, vparser.getValue(settings.getMemberType(fieldAlias), fieldAlias, content[colNr-1]));
								break;
							case Newline:
								break;
							case StaticValue:
								break;
							}
						}
						else {
							break;
						}
					}
					catch (Exception e) {
						throw new CoreException("Error occured while processing line: " + (1 + lineNumber) + ", the error was: " + e.getMessage(), e);
					}
				}
			}
			lineNumber++;
		}

		try {
			info.finished();
		}
		catch (Exception e) {
			if (e instanceof NumberFormatException)
				throw new MendixReplicationException("Error occured while processing the file, the error was: Invalid number " + e.getMessage(), e);
			else
				throw new MendixReplicationException("Error occured while processing the file, the error was: " + e.toString(), e);
		}
	}


//	private PrimitiveType determineRenderType(IMendixObject columnObject) {
//		FieldDataType dataType = FieldDataType.valueOf((String) columnObject.getValue(this.context, Field.MemberNames.FormatAsDataType.toString()));
//		switch (dataType) {
//		case DecimalType:
//			return PrimitiveType.Decimal;
//		case IntegerType:
//			return PrimitiveType.Long;
//		case StringType:
//		default:
//			return PrimitiveType.String;
//		}
//	}

	private Object getValueFromReference(IMendixObject highlightObject, IMendixObject exportObject) throws CoreException {
		List<IMendixObject> referenceResult = this.getResultByHighlight(highlightObject, exportObject, 1);

		return this.getValueFromReference(referenceResult, highlightObject, 0);
	}

	private Object getValueFromReference(List<IMendixObject> referenceResult, IMendixObject columnObject, int listPosition) throws CoreException {
		Object value = "";

		if (referenceResult.size() > listPosition) {
			IMendixObject referencedObject = referenceResult.get(listPosition);
			IMendixObject member = Core.retrieveId(this.context, (IMendixIdentifier) columnObject.getValue(this.context, Field.MemberNames.Field_MxObjectMember.toString()));
			String memberName = (String) member.getValue(this.context, MxObjectMember.MemberNames.AttributeName.toString());

			value = referencedObject.getValue(this.context, memberName);
		}

		if (value == null)
			return "";

		return value;
	}

	public List<IMendixObject> getResultByHighlight(IMendixObject columnObject, IMendixObject exportObject, int limit) throws CoreException {
		IMendixObject reference = Core.retrieveId(this.context, (IMendixIdentifier) columnObject.getValue(this.context, Field.MemberNames.Field_MxObjectReference.toString()));
		IMendixObject objectType = Core.retrieveId(this.context, (IMendixIdentifier) columnObject.getValue(this.context, Field.MemberNames.Field_MxObjectType_Reference.toString()));

		String referenceName = (String) reference.getValue(this.context, MxObjectReference.MemberNames.CompleteName.toString()), objectTypeName = (String) objectType.getValue(this.context, MxObjectType.MemberNames.CompleteName.toString());

		HashMap<String, String> sortMap = new HashMap<String, String>();
		sortMap.put("createdDate", "ASC");
		return Core.retrieveXPathQuery(this.context, "//" + objectTypeName + "[" + referenceName + "='" + exportObject.getId().toLong() + "']", limit, 0, sortMap);
	}

	private Object getValueFromAttribute(IMendixObject columnObject, IMendixObject exportObject) throws CoreException {
		IMendixObject member = Core.retrieveId(this.context, (IMendixIdentifier) columnObject.getValue(this.context, Field.MemberNames.Field_MxObjectMember.toString()));
		Object value = exportObject.getValue(this.context, (String) member.getValue(this.context, MxObjectMember.MemberNames.AttributeName.toString()));

		Object strValue = ValueParser.getTrimmedValue(value, null, null);
		if (strValue == null)
			return "";

		return strValue;
	}
}
