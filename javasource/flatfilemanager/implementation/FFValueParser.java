package flatfilemanager.implementation;

import java.util.Map;

import replication.ReplicationSettings;
import replication.ValueParser;
import replication.interfaces.IValueParser;

import com.mendix.systemwideinterfaces.core.meta.IMetaPrimitive.PrimitiveType;

public class FFValueParser extends ValueParser {

	public FFValueParser( Map<String, IValueParser> customValueParsers, ReplicationSettings settings ) {
		super(settings, customValueParsers);
	}

	@Override
	public Object getValueFromDataSet( String column, PrimitiveType type, Object dataSet ) throws ParseException {
		throw new ParseException("Not implemented");
	}
}
