package flatfilemanager.implementation;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.sadun.text.ffp.FFPParseException;
import org.sadun.text.ffp.FlatFileParser;
import org.sadun.text.ffp.LineFormat;
import org.sadun.text.ffp.NotCondition;

import com.mendix.core.Core;
import com.mendix.core.CoreException;
import com.mendix.core.CoreRuntimeException;
import com.mendix.logging.ILogNode;
import com.mendix.systemwideinterfaces.core.IContext;
import com.mendix.systemwideinterfaces.core.IMendixIdentifier;
import com.mendix.systemwideinterfaces.core.IMendixObject;
import com.mendix.systemwideinterfaces.core.meta.IMetaPrimitive.PrimitiveType;

import flatfilemanager.implementation.TemplateConfiguration.ColumnConfig;
import flatfilemanager.proxies.Field;
import flatfilemanager.proxies.FieldDataType;
import flatfilemanager.proxies.SuffixOrPrefix;
import mxmodelreflection.proxies.MxObjectMember;
import mxmodelreflection.proxies.MxObjectReference;
import mxmodelreflection.proxies.MxObjectType;
import replication.MetaInfo;
import replication.ValueParser;
import replication.implementation.CustomReplicationSettings;
import replication.implementation.NotImplementedException;

public class FixedLengthLineHandler extends ILineHandler {

	private static ILogNode logger = Core.getLogger("FlatFileExport");
	private Writer writer;

	public FixedLengthLineHandler() {

	}

	@Override
	public void initialize( IContext context, Writer writer, TemplateConfiguration config ) {
		this.context = context;
		this.writer = writer;
		this.config = config;
	}

	@Override
	public void initialize( IContext context, TemplateConfiguration config ) {
		this.context = context;
		this.config = config;
	}

	@Override
	public void writeLine( IMendixObject object ) throws CoreException {
		try {
			StringBuilder builder = this.getLineContent(object);
			if ( builder.length() > 0 )
				this.writer.append(builder.toString() + this.config.getLineEnd());
		}
		catch( IOException e ) {
			throw new CoreException(e);
		}
	}

	protected StringBuilder getLineContent( IMendixObject object ) throws CoreException {
		StringBuilder builder = new StringBuilder(this.config.getTotalLength());

		for( Entry<Integer, ColumnConfig> entry : this.config.getColumns().entrySet() ) {
			ColumnConfig config = entry.getValue();
			IMendixObject column = config.getColumnObj();
			Integer colNr = entry.getKey();

			String value = null;

			switch (config.getValueSource()) {
			case Attribute:
				value = this.getValueFromAttribute(colNr, config, column, object);
				break;
			case Reference:
				value = this.getValueFromReference(config, column, object);
				break;
			case StaticValue:
				value = config.getStaticValue();
				break;
			case Newline:
				builder.append(this.config.getLineEnd());
				continue;
			}

			builder.append(generateFixedLengthOutput(this.config, value, config));
		}

		return builder;
	}

	@Override
	public void importFromFile( BufferedReader reader, IMendixObject importFile ) throws CoreException, IOException {
		importFromFile(reader, importFile, null, null);
	}

	@Override
	public void importFromFile( BufferedReader reader, IMendixObject importFile, IMendixObject parameterObject, String referenceName ) throws CoreException, IOException {
		// throw new CoreException("This function is not implemented");

		LineFormat format = new LineFormat();
		FlatFileParser ffp = new FlatFileParser();

		for( Entry<Integer, ColumnConfig> entry : this.config.getColumns().entrySet() ) {
			ColumnConfig config = entry.getValue();
			IMendixObject column = config.getColumnObj();

			Integer startPos = column.getValue(this.context, Field.MemberNames.FlatFilePosition.toString());
			Integer length = column.getValue(this.context, Field.MemberNames.Length.toString());
			Integer colNr = column.getValue(this.context, Field.MemberNames.ColNumber.toString());

			format.defineNextField(String.valueOf(colNr), (startPos + length - 1)); // -1 because we are looking for position not length

			// TODO specify code that determines which format to use.
			// ffp.declare( new ConstantFoundInLineCondition(format), format );
		}
		FlatFileLineParser lineParser = new FlatFileLineParser(this, parameterObject, referenceName);
		ffp.addListener(lineParser);

		ffp.declare( new NotCondition( org.sadun.text.ffp.NullCondition.INSTANCE ),format);
		// Finally, do the parsing
		try {
			ffp.parse(reader);
		}
		catch( FFPParseException e ) {
			throw new CoreException("Unable to evaluate the flat file", e);
		}
		lineParser.info.finished();
	}
	
	private static class FlatFileLineParser implements FlatFileParser.Listener {
		private FixedLengthLineHandler _self;
		private Map<Integer, ColumnConfig> columns;
		private MetaInfo info;
		private FFValueParser vparser;
		private CustomReplicationSettings settings;
		
		protected FlatFileLineParser(FixedLengthLineHandler _self, IMendixObject parameterObjectId, String associationName) throws CoreException {
			this._self = _self;
			this.columns = this._self.config.getColumns();

			this.settings = this._self.initializeSettings(parameterObjectId, associationName);
			
			this.vparser = new FFValueParser(this.settings.getValueParsers(), this.settings);
			this.info = new MetaInfo(this.settings, this.vparser, "DelimitedFileImport");

		}
		
		@Override
		public void lineParsed( LineFormat format, int logicalLinecount, int physicalLineCount, String[] values ) {
			if( this.columns.size() > values.length )
				throw new CoreRuntimeException("Incorrect line legth, found: " + values.length + " expecting " + this.columns.size() + ", ll:" + logicalLinecount + ", pl:" + physicalLineCount);
			
			for( int colNr = 1; colNr <= values.length; colNr++ ) {
				System.out.println(values[colNr-1]);
				
				ColumnConfig cConfig = this.columns.get(colNr);
				
				switch (cConfig.getValueSource()) {
				case Attribute:
				case Reference:
					String fieldAlias = String.valueOf(colNr);
					
					try {
						this.info.addValue(String.valueOf(logicalLinecount), fieldAlias, this.vparser.getValue(this.settings.getMemberType(fieldAlias), fieldAlias, values[colNr-1]));
					}catch (Exception e) {
						throw new CoreRuntimeException(e);
					}
					break;
				case Newline:
					break;
				case StaticValue:
					break;
				}
			}
		}
	}

	private static String generateFixedLengthOutput( TemplateConfiguration config, String value, ColumnConfig cConfig) throws CoreException {
		try {
			// String mask = determineMask(field);
			// logger.debug("Using mask: " + mask);
			String outputValue = padIfString(cConfig, value);

			if ( outputValue.length() > cConfig.getLength() ) {
				logger.error("Field: " + config.getTemplateName() + " - " + cConfig.getColNumber() + " value is to long: " + outputValue
						.length() + " instead of: " + cConfig.getLength() + " the value is: " + outputValue);
				outputValue = outputValue.substring(0, cConfig.getLength());
			}

			logger.debug("Appending value: " + outputValue);
			return outputValue;
		}
		catch( Exception e ) {
			throw new CoreException("Unable to export Field: " + config.getTemplateName() + " - " + cConfig
					.getColNumber() + " with value: " + value + ", error: " + e.getMessage(), e);
		}
	}

	private static String padIfString( ColumnConfig cConfig, String value ) {
		// Catch null values since the pad function doesn't work for null values.
		if ( value == null )
			value = "";

		String appendChar = cConfig.getAppendCharacter();
		SuffixOrPrefix location = cConfig.getSuffixOrPrefix();
		if ( location == SuffixOrPrefix.Prefix ) {
			return StringUtils.leftPad(value, cConfig.getLength(), appendChar);
		}
		else {
			return StringUtils.rightPad(value, cConfig.getLength(), appendChar);
		}
	}

	// private static String determineMask(Field field) {
	// int length = field.getLength();
	// FieldDataType datatype = field.getFormatAsDataType();
	// int nrOfDecimals = field.getNrOfDecimals();
	//// String appendChar = (field.getAppendCharacter() == null ? "" : field.getAppendCharacter());
	// String mask = "";
	// switch (datatype) {
	// case DecimalType:
	// mask = length + "." + nrOfDecimals;
	// mask += "f";
	// break;
	// case IntegerType:
	// mask = length + "d";
	// break;
	// case StringType:
	// case DateType:
	// mask += "s";
	//
	// // switch (location) {
	// // case Prefix:
	// // mask = appendChar + mask;
	// // break;
	// // case Suffix:
	// // mask = "-" + appendChar + mask;
	// // break;
	// // }
	//
	// break;
	// }
	//
	// return "%" + mask;
	// }

	private PrimitiveType determineRenderType( IMendixObject columnObject ) {
		FieldDataType dataType = FieldDataType.valueOf((String) columnObject.getValue(this.context, Field.MemberNames.FormatAsDataType.toString()));
		switch (dataType) {
		case DecimalType:
			return PrimitiveType.Decimal;
		case IntegerType:
			return PrimitiveType.Long;
		case DateType:
			return PrimitiveType.DateTime;
		case StringType:
			return PrimitiveType.String;
		}

		return null;
	}

	private String getValueFromReference( ColumnConfig config, IMendixObject highlightObject, IMendixObject exportObject ) throws CoreException {
		List<IMendixObject> referenceResult = this.getResultByHighlight(highlightObject, exportObject, 1);

		return this.getValueFromReference(config, referenceResult, highlightObject, 0);
	}

	private String getValueFromReference( ColumnConfig config, List<IMendixObject> referenceResult, IMendixObject columnObject, int listPosition ) throws CoreException {
		String value = "";

		if ( referenceResult.size() > listPosition ) {
			IMendixObject referencedObject = referenceResult.get(listPosition);
			IMendixObject member = Core.retrieveId(this.context,
					(IMendixIdentifier) columnObject.getValue(this.context, Field.MemberNames.Field_MxObjectMember.toString()));
			String memberName = (String) member.getValue(this.context, MxObjectMember.MemberNames.AttributeName.toString());

			value = getValueByType(config, this.determineRenderType(columnObject), referencedObject.getValue(this.context, memberName),
					config.getMask());
		}

		if ( value == null )
			return "";

		return value;
	}

	public List<IMendixObject> getResultByHighlight( IMendixObject columnObject, IMendixObject exportObject, int limit ) throws CoreException {
		IMendixObject reference = Core.retrieveId(this.context,
				(IMendixIdentifier) columnObject.getValue(this.context, Field.MemberNames.Field_MxObjectReference.toString()));
		IMendixObject objectType = Core.retrieveId(this.context,
				(IMendixIdentifier) columnObject.getValue(this.context, Field.MemberNames.Field_MxObjectType_Reference.toString()));

		String referenceName = (String) reference.getValue(this.context, MxObjectReference.MemberNames.CompleteName.toString()),
				objectTypeName = (String) objectType.getValue(this.context, MxObjectType.MemberNames.CompleteName.toString());

		HashMap<String, String> sortMap = new HashMap<String, String>();
		sortMap.put("createdDate", "ASC");
		return Core.retrieveXPathQuery(this.context, "//" + objectTypeName + "[" + referenceName + "='" + exportObject.getId().toLong() + "']", limit,
				0, sortMap);
	}

	private String getValueFromAttribute( int colNr, ColumnConfig config, IMendixObject columnObject, IMendixObject exportObject ) throws CoreException {
		IMendixIdentifier memberId = columnObject.getValue(this.context, Field.MemberNames.Field_MxObjectMember.toString());
		if ( memberId == null )
			throw new CoreException("No attribute selected for field: " + colNr + "-" + columnObject.getValue(this.context,
					Field.MemberNames.Description.toString()));
		IMendixObject member = Core.retrieveId(this.context, (IMendixIdentifier) memberId);
		Object value = exportObject.getValue(this.context,
				(String) member.getValue(this.context, MxObjectMember.MemberNames.AttributeName.toString()));

		String attrValue = getValueByType(config, this.determineRenderType(columnObject), value, config.getMask());
		// if (attrValue == null)
		// return ;

		return attrValue;
	}


	private String getValueByType( ColumnConfig columnConfig, PrimitiveType type, Object value, String mask ) throws CoreException {
		String returnValue = null;
		switch (type) {
		case Decimal:
			returnValue = ValueParser.getStringValueFromNumber(value, columnConfig.getNrOfDecimals());
			break;
		case Integer:
		case AutoNumber:
		case Long:
		case Enum:
		case String:
		case HashString:
		case DateTime:
			returnValue = ValueParser.getTrimmedValue(value, null, mask);
			break;
		case Binary:
			throw new NotImplementedException("Binary members are currently not supported");
		default:
			// Compatibility fix since Currency is no longer part of the latest release
			if ( "Currency".equals(type.toString()) || "Float".equals(type.toString()) )
				returnValue = ValueParser.getStringValueFromNumber(value, columnConfig.getNrOfDecimals());
			else
				logger.warn("Unknown attribute type: " + type);

			break;
		}

		String microflowParser = columnConfig.getMicroflow();
		if ( microflowParser != null )
			returnValue = Core.execute(this.context, microflowParser, returnValue);

		return returnValue;
	}

}
