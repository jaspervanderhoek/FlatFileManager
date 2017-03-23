package replication.interfaces;

import replication.ValueParser.ParseException;

public interface IValueParser {

	public Object parseValue(Object value) throws ParseException;
	
}
