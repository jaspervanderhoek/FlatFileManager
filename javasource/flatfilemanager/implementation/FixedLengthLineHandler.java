package flatfilemanager.implementation;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;

import com.mendix.core.Core;
import com.mendix.core.CoreException;
import com.mendix.logging.ILogNode;
import com.mendix.systemwideinterfaces.core.IContext;
import com.mendix.systemwideinterfaces.core.IMendixIdentifier;
import com.mendix.systemwideinterfaces.core.IMendixObject;
import com.mendix.systemwideinterfaces.core.meta.IMetaPrimitive.PrimitiveType;

import flatfilemanager.implementation.FileHandler.TemplateConfiguration;
import flatfilemanager.implementation.FileHandler.TemplateConfiguration.ColumnConfig;
import flatfilemanager.proxies.DataSource;
import flatfilemanager.proxies.Field;
import flatfilemanager.proxies.FieldDataType;
import flatfilemanager.proxies.SuffixOrPrefix;
import mxmodelreflection.proxies.MxObjectMember;
import mxmodelreflection.proxies.MxObjectReference;
import mxmodelreflection.proxies.MxObjectType;
import replication.ValueParser;
import replication.implementation.NotImplementedException;

public class FixedLengthLineHandler extends ILineHandler {

	private static ILogNode logger = Core.getLogger("FlatFileExport");
	private IContext context;
	private Writer writer;
	private TemplateConfiguration config;

	public FixedLengthLineHandler() {
		
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
			StringBuilder builder = this.getLineContent(object);
			if( builder.length() > 0 )
				this.writer.append(builder.toString() + this.config.getLineEnd());
		}
		catch (IOException e) {
			throw new CoreException(e);
		}
	}

	protected StringBuilder getLineContent(IMendixObject object) throws CoreException {
		StringBuilder builder = new StringBuilder(this.config.getTotalLength());

		for (Entry<Integer, ColumnConfig> entry : this.config.getColumns().entrySet()) {
			ColumnConfig config = entry.getValue();
			IMendixObject column = config.getColumnObj();
			DataSource source = DataSource.valueOf((String) column.getValue(this.context, Field.MemberNames.DataSource.toString()));
			Integer colNr = entry.getKey();
			
			Object value = null;
			Field field = Field.initialize(this.context, column);
			switch (source) {
			case Attribute:
				value = this.getValueFromAttribute(colNr, config, column, object);
				break;
			case Reference:
				value = this.getValueFromReference(colNr, config, column, object);
				break;
			case StaticValue:
				value = config.getStaticValue();
				break;
			case Newline:
				builder.append(this.config.getLineEnd());
				continue;
			}

			builder.append(generateFixedLengthOutput(this.config, value, field));
		}

		return builder;
	}
	
	@Override
	public void importFromFile(BufferedReader reader, IMendixObject importFile) throws CoreException, IOException {
		importFromFile(reader, importFile, null, null);
	}

	@Override
	public void importFromFile(BufferedReader reader, IMendixObject importFile, IMendixObject parameterObject, String referenceName) throws CoreException, IOException {
		throw new CoreException("This function is not implemented");
	}

	private static String generateFixedLengthOutput(TemplateConfiguration config, Object value, Field field) throws CoreException {
		try {
			String mask = determineMask(field);
			logger.debug("Using mask: " + mask);
			String outputValue = padIfString(field, mask, value);
	
			if (outputValue.length() > field.getLength()) {
				logger.error("Field: " + config.getTemplateName() + " - " + field.getColNumber() + " value is to long: " + outputValue.length() + " instead of: " + field.getLength() + " the value is: " + outputValue);
				outputValue = outputValue.substring(0, field.getLength());
			}
			
			logger.debug("Appending value: " + outputValue);
			return outputValue;
		}
		catch (Exception e) {
			throw new CoreException("Unable to export Field: " + config.getTemplateName() + " - " + field.getColNumber() + " with value: " + value + ", error: " + e.getMessage(), e);
		}
	}

	private static String padIfString(Field field, String mask, Object value) {
		String outputValue = "";
		if( value != null )
			outputValue = String.format(mask, value);
			
		String appendChar = field.getAppendCharacter();
		SuffixOrPrefix location = field.getAppend();
		if (location == SuffixOrPrefix.Prefix) {
			return StringUtils.leftPad(outputValue, field.getLength(), appendChar);
		}
		else {
			return StringUtils.rightPad(outputValue, field.getLength(), appendChar);
		}	
	}

	private static String determineMask(Field field) {
		int length = field.getLength();
		FieldDataType datatype = field.getFormatAsDataType();
		int nrOfDecimals = field.getNrOfDecimals();
//		String appendChar = (field.getAppendCharacter() == null ? "" : field.getAppendCharacter());
		String mask = "";
		switch (datatype) {
		case DecimalType:
			mask = length + "." + nrOfDecimals;
			mask += "f";
			break;
		case IntegerType:
			mask = length + "d";
			break;
		case StringType:
		case DateType:
			mask += "s";

			// switch (location) {
			// case Prefix:
			// mask = appendChar + mask;
			// break;
			// case Suffix:
			// mask = "-" + appendChar + mask;
			// break;
			// }

			break;
		}

		return "%" + mask;
	}

	private PrimitiveType determineRenderType(IMendixObject columnObject) {
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

	private Object getValueFromReference(int colNr, ColumnConfig config, IMendixObject highlightObject, IMendixObject exportObject) throws CoreException {
		List<IMendixObject> referenceResult = this.getResultByHighlight(highlightObject, exportObject, 1);

		return this.getValueFromReference(colNr, config, referenceResult, highlightObject, 0);
	}

	private Object getValueFromReference(int colNr, ColumnConfig config, List<IMendixObject> referenceResult, IMendixObject columnObject, int listPosition) throws CoreException {
		Object value = "";

		if (referenceResult.size() > listPosition) {
			IMendixObject referencedObject = referenceResult.get(listPosition);
			IMendixObject member = Core.retrieveId(this.context, (IMendixIdentifier) columnObject.getValue(this.context, Field.MemberNames.Field_MxObjectMember.toString()));
			String memberName = (String) member.getValue(this.context, MxObjectMember.MemberNames.AttributeName.toString());

			value = getValueByType(colNr, this.determineRenderType(columnObject), referencedObject.getValue(this.context, memberName), config.getMask());
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

	private Object getValueFromAttribute(int colNr, ColumnConfig config, IMendixObject columnObject, IMendixObject exportObject) throws CoreException {
		IMendixIdentifier memberId = columnObject.getValue(this.context, Field.MemberNames.Field_MxObjectMember.toString());
		if( memberId == null )
			throw new CoreException("No attribute selected for field: " + colNr + "-" + columnObject.getValue(this.context, Field.MemberNames.Description.toString()));
		IMendixObject member = Core.retrieveId(this.context, (IMendixIdentifier) memberId);
		Object value = exportObject.getValue(this.context, (String) member.getValue(this.context, MxObjectMember.MemberNames.AttributeName.toString()));

		Object attrValue = getValueByType(colNr, this.determineRenderType(columnObject), value, config.getMask());
//		if (attrValue == null)
//			return ;

		return attrValue;
	}
	
	
	private Object getValueByType( int colNr, PrimitiveType type, Object value, String mask ) throws CoreException {
		Object returnValue = null;
		switch (type) {
		case Boolean:
			returnValue = ValueParser.getBooleanValue(value);
			break;
		case Decimal:
			returnValue = ValueParser.getBigDecimalValue(value);
			break;
		case Integer:
			returnValue = ValueParser.getIntegerValue(value);
			break;
		case AutoNumber:
		case Long:
			returnValue = ValueParser.getLongValue(value);
			break;
		case Enum:
		case String:
		case HashString:
		case DateTime:
			returnValue = ValueParser.getTrimmedValue(value, null, mask);
			break;
		case Binary:
			throw new NotImplementedException("Binary members are currently not supported");
		default:
			//Compatibility fix since Currency is no longer part of the latest release 
			if( "Currency".equals( type.toString() ) || "Float".equals( type.toString() ) )
				returnValue = ValueParser.getDoubleValue(value);
			else 
				logger.warn("Unknown attribute type: " + type );
			
			break;
		}
		
		String microflowParser = this.config.getMicroflowParser( colNr );  
		if( microflowParser != null )
			returnValue = Core.execute(this.context, microflowParser, returnValue);

		return returnValue;
	}

}
