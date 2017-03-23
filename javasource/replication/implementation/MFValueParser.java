package replication.implementation;

import java.util.HashMap;
import java.util.List;

import mxmodelreflection.proxies.Microflows;
import mxmodelreflection.proxies.Parameter;
import mxmodelreflection.proxies.PrimitiveTypes;
import replication.ValueParser;
import replication.ValueParser.ParseException;
import replication.interfaces.IValueParser;

import com.mendix.core.Core;
import com.mendix.core.CoreException;
import com.mendix.systemwideinterfaces.core.IContext;
import com.mendix.systemwideinterfaces.core.IMendixIdentifier;
import com.mendix.systemwideinterfaces.core.IMendixObject;

public class MFValueParser implements IValueParser {

	private String mfName;
	private String paramNameSimple = null;
	private PrimitiveTypes paramTypeSimple = null;
	private String paramNameObject = null;
	private IContext context;

	public MFValueParser( IContext context, IMendixObject microflowObject ) throws CoreException {
		// TODO, build structured solution for creating microflows.
		this.context = context;
		this.mfName = microflowObject.getValue(this.context, Microflows.MemberNames.CompleteName.toString());
		List<IMendixIdentifier> paramaters = microflowObject.getValue(this.context, Microflows.MemberNames.Microflows_InputParameter.toString());
		if ( paramaters.size() == 0 )
			throw new CoreException("Could not find any parameters for microflow: " + this.mfName);

		if ( paramaters.size() == 1 ) {
			IMendixObject param1 = Core.retrieveId(this.context, paramaters.get(0));
			this.paramNameSimple = (String) param1.getValue(this.context, Parameter.MemberNames.Name.toString());
			if ( param1.getValue(this.context, Parameter.MemberNames.Parameter_ValueType.toString()) != null ) {
				IMendixObject valueType = Core.retrieveId(this.context,
						(IMendixIdentifier) param1.getValue(this.context, Parameter.MemberNames.Parameter_ValueType.toString()));
				this.paramTypeSimple = PrimitiveTypes.valueOf((String) valueType.getValue(this.context,
						mxmodelreflection.proxies.ValueType.MemberNames.TypeEnum.toString()));
			}
		}
		else if ( paramaters.size() == 2 ) {
			IMendixObject param1 = Core.retrieveId(this.context, paramaters.get(0));
			IMendixObject param2 = Core.retrieveId(this.context, paramaters.get(1));
			// Is primitive parameter?
			if ( param1.getValue(this.context, Parameter.MemberNames.Parameter_ValueType.toString()) != null ) {
				if ( param2.getValue(this.context, Parameter.MemberNames.Parameter_ValueType.toString()) != null ) {
					throw new CoreException("Invalid parameter types for microflow: " + this.mfName + " there are two parameters with a primitive type but there should be 1 mxObject parameter");
				}
				this.paramNameSimple = param1.getValue(this.context, Parameter.MemberNames.Name.toString());
				if ( param1.getValue(this.context, Parameter.MemberNames.Parameter_ValueType.toString()) != null ) {
					IMendixObject valueType = Core.retrieveId(this.context,
							(IMendixIdentifier) param1.getValue(this.context, Parameter.MemberNames.Parameter_ValueType.toString()));
					this.paramTypeSimple = PrimitiveTypes.valueOf((String) valueType.getValue(this.context,
							mxmodelreflection.proxies.ValueType.MemberNames.TypeEnum.toString()));
				}

				this.paramNameObject = param2.getValue(this.context, Parameter.MemberNames.Name.toString());
			}
			else {
				if ( param2.getValue(this.context, Parameter.MemberNames.Parameter_ValueType.toString()) != null ) {
					this.paramNameSimple = param2.getValue(this.context, Parameter.MemberNames.Name.toString());
					if ( param2.getValue(this.context, Parameter.MemberNames.Parameter_ValueType.toString()) != null ) {
						IMendixObject valueType = Core.retrieveId(this.context,
								(IMendixIdentifier) param2.getValue(this.context, Parameter.MemberNames.Parameter_ValueType.toString()));
						this.paramTypeSimple = PrimitiveTypes.valueOf((String) valueType.getValue(this.context,
								mxmodelreflection.proxies.ValueType.MemberNames.TypeEnum.toString()));
					}

					this.paramNameObject = param1.getValue(this.context, Parameter.MemberNames.Name.toString());
				}
				else
					throw new CoreException("Invalid parameter types for microflow: " + this.mfName + " there are two parameters with a primitive type but there should be 1 mxObject parameter");
			}

		}
		else
			throw new CoreException("Invalid number of parameters for this microflow, maximal 2 parameters are allowed (value and the object)");
	}

	@Override
	public Object parseValue( Object value ) throws ParseException {
		HashMap<String, Object> paramMap = new HashMap<String, Object>();
		if ( this.paramNameSimple != null ) {

			switch (this.paramTypeSimple) {
			case AutoNumber:
			case LongType:
				value = ValueParser.getLongValue(value);
				break;
			case IntegerType:
				value = ValueParser.getIntegerValue(value);
				break;
			case StringType:
			case HashString:
			case EnumType:
				value = ValueParser.getTrimmedValue(value, null, null);
				break;
			case BooleanType:
				value = ValueParser.getBooleanValue(value);
				break;
			case Decimal:
				value = ValueParser.getBigDecimalValue(value);
				break;
			case Currency:
			case FloatType:
				value = ValueParser.getDoubleValue(value);
				break;
			case DateTime:
				value = ValueParser.getDateValue(null, value, null);
				break;
			case ObjectList:
			case ObjectType:
				ValueParser.logNode.trace("Ignoring parameter: " + this.paramNameSimple);
				break;
			}

			paramMap.put(this.paramNameSimple, value);
		}

		if ( this.paramNameObject != null )
			paramMap.put(this.paramNameObject, null);


		try {
		this.context.startTransaction();
			Object newValue = Core.execute(this.context, this.mfName, paramMap);

			if ( ValueParser.logNode.isTraceEnabled() ) {
				ValueParser.logNode.trace("Executed microflow: " + this.mfName + " processed old value: " + value + " into new value: " + newValue);
			}
			this.context.endTransaction();
			return newValue;
		}
		catch( Exception e ) {
			this.context.rollbackTransAction();
			throw new ParseException("Exception occured while executing microflow: " + this.mfName, e);
		}

	}

}
