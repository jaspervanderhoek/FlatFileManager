package replication;

import java.math.BigDecimal;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;

import replication.ReplicationSettings.MendixReplicationException;
import replication.ReplicationSettings.ObjectSearchAction;
import replication.implementation.NotImplementedException;
import replication.interfaces.IValueParser;

import com.mendix.core.Core;
import com.mendix.core.CoreException;
import com.mendix.logging.ILogNode;
import com.mendix.systemwideinterfaces.core.IMendixIdentifier;
import com.mendix.systemwideinterfaces.core.IMendixObject;
import com.mendix.systemwideinterfaces.core.meta.IMetaPrimitive.PrimitiveType;

/**
 * 
 * 
 * @author Mendix - Jasper van der Hoek
 * @version $Id: ValueParser.java 9272 2009-05-11 09:19:47Z Jasper van der Hoek $
 */
public abstract class ValueParser {

	private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
	public static final String keySeparator = "|KEY|";

	protected ReplicationSettings settings;
	private Map<String, IValueParser> customValueParsers;

	public static ILogNode logNode = Core.getLogger("ValueParser");

	protected ValueParser( ReplicationSettings settings, Map<String, IValueParser> customValueParsers ) {
		this.settings = settings;

		this.customValueParsers = customValueParsers;

		if ( this.customValueParsers == null )
			this.customValueParsers = new HashMap<String, IValueParser>();
	}

	public boolean hasCustomHandling( String alias ) {
		return this.customValueParsers.containsKey(alias);
	}

	public Object getValueFromObject( IMendixObject object, String keyAlias ) throws MendixReplicationException {
		Object value = null;
		if ( this.settings.treatFieldAsReference(keyAlias) ) {
			IMendixIdentifier id = object.getValue(this.settings.getContext(), this.settings.getAssociationNameByAlias(keyAlias));
			if ( id != null ) {
				IMendixObject object2;
				try {
					object2 = Core.retrieveId(this.settings.getContext(), id);
				}
				catch( CoreException e ) {
					throw new MendixReplicationException(e);
				}
				if ( object2 != null )
					value = object2.getValue(this.settings.getContext(), this.settings.getAssociationColumnByAlias(keyAlias));
				object2 = null;
			}
			id = null;
		}
		else if ( this.settings.treatFieldAsReferenceSet(keyAlias) ) {
			List<IMendixIdentifier> id = object.getValue(this.settings.getContext(), this.settings.getAssociationNameByAlias(keyAlias));
			if ( id != null && id.size() >= 1 ) {
				IMendixObject object2;
				try {
					object2 = Core.retrieveId(this.settings.getContext(), id.get(0));
				}
				catch( CoreException e ) {
					throw new MendixReplicationException(e);
				}

				if ( object2 != null )
					value = object2.getValue(this.settings.getContext(), this.settings.getAssociationColumnByAlias(keyAlias));
				object2 = null;

				if ( id.size() > 1 )
					Core.getLogger("ValueParser")
							.warn("The association: " + this.settings.getAssociationNameByAlias(keyAlias) + " contains more than 1 associated object, the first object was used as key. Therefore it could be possible the wrong object was used");
				id.clear();
			}
			id = null;
		}
		else
			value = object.getValue(this.settings.getContext(), this.settings.getMemberNameByAlias(keyAlias));

		return value;
	}


	public String buildObjectKey( TreeMap<String, Boolean> keySet, IMendixObject object, boolean isAssociation ) throws ParseException {
		StringBuilder keyBuilder = new StringBuilder();

		Iterator<Entry<String, Boolean>> keyIter = keySet.entrySet().iterator();

		TreeSet<String> processedMembers = new TreeSet<String>();
		String keyMember = "";
		try {
			while( keyIter.hasNext() ) {
				Entry<String, Boolean> keyEntry = keyIter.next();
				String keyAlias = keyEntry.getKey();
				Boolean keyIsCaseSensitive = keyEntry.getValue();

				if ( isAssociation )
					keyMember = this.settings.getAssociationColumnByAlias(keyAlias);
				else
					keyMember = keyAlias;

				if ( !processedMembers.contains(keyMember) ) {
					processedMembers.add(keyMember);
					Object value;
					if ( isAssociation )
						value = object.getValue(this.settings.getContext(), keyMember);
					else
						value = this.getValueFromObject(object, keyAlias);

					String keyValue = this.processKeyValue(keyIsCaseSensitive, this.getKeyValueByPrimitiveType(this.settings.getMemberType(keyAlias), keyAlias, value));

					if ( keyValue != null ) {
						keyBuilder.append(keyValue).append(keySeparator);
					}
					else
						keyBuilder.append("").append(keySeparator);
				}
			}
		}
		catch( MendixReplicationException e ) {
			throw new ParseException(e);
		}
		processedMembers.clear();

		return keyBuilder.toString().trim();
	}

	public String buildObjectKey( Object recordDataSet, ObjectConfig config ) throws ParseException {
		StringBuilder keyBuilder = new StringBuilder();

		if ( config.getObjectSearchAction() != ObjectSearchAction.CreateEverything || config.hasKeyMembers() ) {
			try {
				for( Entry<String, Boolean> entry : config.getKeys().entrySet() ) {
					String keyAlias = entry.getKey();
					Boolean isCaseSensitive = entry.getValue();

					String keyValue;
					if ( this.customValueParsers.containsKey(keyAlias) ) {
						IValueParser vp = this.customValueParsers.get(keyAlias);
						Object value = vp.parseValue(getValueFromDataSet(keyAlias, PrimitiveType.String, recordDataSet));

						keyValue = getTrimmedValue(value, keyAlias);
					}
					else
						keyValue = getKeyValueByPrimitiveType(this.settings.getMemberType(keyAlias), keyAlias,
								getValueFromDataSet(keyAlias, this.settings.getMemberType(keyAlias), recordDataSet));

					keyBuilder.append(this.processKeyValue(isCaseSensitive, keyValue)).append(keySeparator);
				}
			}
			catch( ParseException e ) {
				throw e;
			}
			catch( Exception e ) {
				throw new ParseException(e);
			}
		}
		else {
			keyBuilder.append(UUID.randomUUID().toString());
		}

		return keyBuilder.toString().trim();
	}

	public abstract Object getValueFromDataSet( String keyAlias, PrimitiveType type, Object dataSet ) throws ParseException;

	/**
	 * Iterate through the keyset and create an combined key using the values from the values map
	 * 
	 * @param keySet
	 * @param values
	 * @return The unique key for the specified value set
	 * @throws ParseException
	 */
//TODO review and remove this function
	public String buildObjectKey( TreeMap<String, Boolean> keySet, Map<String, Object> values ) throws ParseException {
		StringBuilder keyBuilder = new StringBuilder();

		TreeSet<String> processedMembers = new TreeSet<String>();
		boolean allKeysEmpty = true, ignoreEmptyKeys = this.settings.getMainObjectConfig().ignoreEmptyKeys();
		try {
			String memberName = "";

			for( Entry<String, Boolean> keyEntry : keySet.entrySet() ) {
				String keyAlias = keyEntry.getKey();
				Boolean keyIsCaseSensitive = keyEntry.getValue();

				if ( this.settings.treatFieldAsReference(keyAlias) || this.settings.treatFieldAsReferenceSet(keyAlias) )
					memberName = this.settings.getAssociationColumnByAlias(keyAlias);
				else
					memberName = this.settings.getMemberNameByAlias(keyAlias);

				if ( !processedMembers.contains(memberName) ) {
					processedMembers.add(memberName);
					Object value = values.get(memberName);

					String keyValue = this.processKeyValue(keyIsCaseSensitive, this.getKeyValueByPrimitiveType(this.settings.getMemberType(keyAlias), keyAlias, value));

					if ( keyValue != null && !"".equals(keyValue) ) {
						keyBuilder.append(keyValue).append(keySeparator);
						allKeysEmpty = false;
					}
					else
						keyBuilder.append("").append(keySeparator);
				}
			}
		}
		catch( MendixReplicationException e ) {
			throw new ParseException(e);
		}

		if ( allKeysEmpty && ignoreEmptyKeys )
			return null;

		return keyBuilder.toString().trim();
	}

	protected String processKeyValue( boolean isCaseSensitive, String keyValue ) {
		if ( !isCaseSensitive && keyValue != null )
			keyValue = keyValue.toLowerCase();

		if ( this.settings.getMainObjectConfig().ignoreEmptyKeys() && keyValue == null )
			keyValue = "";

		return keyValue;
	}

	/**
	 * Prepare the value for an oql query, works basically the same as the function getKeyValueByPrimitiveType but with
	 * one difference, the dates will be returned as long values.
	 * 
	 * @param type
	 * @param value
	 * @return A string value ready to use in a query
	 * @throws ParseException
	 */
	public String getQueryKeyValue( PrimitiveType type, String alias, Object value ) throws ParseException {
		if ( type == PrimitiveType.DateTime ) {
			return getTrimmedValue(getLongValue(value), alias);
		}

		// Escape all quotes so the value won't break the oql query
		String keyValue = getKeyValueByPrimitiveType(type, alias, value);
		if ( keyValue != null ) {
			keyValue = keyValue.replace("'", "''");
		}

		return keyValue;
	}

	/**
	 * Process the value by the type,
	 * Validate the primitive type and parse the value ready as an value with the correct type
	 * When no primitive type is provided the value determines as which type it will be processed
	 * 
	 * @param PrimitiveType
	 * @param value
	 * @return A string value processed by the primitive type
	 * @throws ParseException
	 */
	protected String getKeyValueByPrimitiveType( PrimitiveType type, String alias, Object value ) throws ParseException {
		try {
			if ( (type == PrimitiveType.DateTime && value instanceof Date) ||
					(type == null && value instanceof Date) ) {
				return getDateKey(value);
			}
			else { 
				value = getValueByType(type, alias, value);
				return getTrimmedValue( value, alias );
			}
		}
		catch( Exception e ) {
			throw new ParseException(e);
		}
	}

	/**
	 * Parse the value in the same format as all other date keys
	 * 
	 * @param value
	 * @return
	 */
	private static String getDateKey( Object value ) {
		if ( value != null ) {
			return dateFormat.format((Date) value);
		}

		return null;
	}


	/**
	 * Get the value from the resultsSet, parse the value from the resultset depending on the type of the member
	 * 
	 * @param type
	 * @param columnAlias
	 * @param rs
	 * @return the value of the column parsed to the correct type
	 * @throws ParseException
	 */
	public Object getValue( PrimitiveType type, String columnAlias, Object value ) throws ParseException {
		try {
			if ( this.customValueParsers.containsKey(columnAlias) ) {
				IValueParser vp = this.customValueParsers.get(columnAlias);
				value = vp.parseValue(value);

				if ( validateParsedValue(type, value) )
					return value;

				throw new ParseException("An invalid value was returned by value parser: " + this.customValueParsers.get(columnAlias) + " for column: " + columnAlias + " expecting a " + type.name() + " but the value is of type: " + value
						.getClass().getSimpleName() + " \r\nParsedValue: " + value);
			}
		}
		catch( ParseException e ) {
			throw e;
		}
		catch( Exception e ) {
			throw new ParseException(e);
		}

		return getValueByType(type, columnAlias, value);
	}

	private Object getValueByType( PrimitiveType type, String columnAlias, Object value ) throws ParseException {
		if( this.settings != null ) //Having the settings is not mandatory
			return ValueParser.getValueByType(type, columnAlias, value, this.settings.getTimeZoneForMember(columnAlias), this.settings.getDisplayMask(columnAlias) );
		else 
			return ValueParser.getValueByType(type, columnAlias, value, null, null );
	}
	private static Object getValueByType( PrimitiveType type, String columnAlias, Object value, TimeZone dateTimezone, String dateMask ) throws ParseException {
		Object returnValue = null;
		switch (type) {
		case Boolean:
			returnValue = getBooleanValue(value);
			break;
		case DateTime:
			returnValue = getDateValue(dateTimezone, value, dateMask);
			break;
		case Decimal:
			returnValue = getBigDecimalValue(value);
			break;
		case Integer:
			returnValue = getIntegerValue(value);
			break;
		case AutoNumber:
		case Long:
			returnValue = getLongValue(value);
			break;
		case Enum:
		case String:
		case HashString:
			returnValue = getTrimmedValue(value, dateTimezone, dateMask);
			break;
		case Binary:
			throw new NotImplementedException("Binary members are currently not supported");
		default:
			//Compatibility fix since Currency is no longer part of the latest release 
			if( "Currency".equals( type.toString() ) || "Float".equals( type.toString() ) )
				returnValue = getDoubleValue(value);
			else 
				logNode.warn("Unknown attribute type: " + type + " for field: " +  columnAlias);
			
			break;
		}

		return returnValue;
	}


	/**
	 * Get the value of the column from the resultset,
	 * The value will be checked for a null value, if it isn't null the value will be trimmed and returned
	 * 
	 * When the value is a double or a float some extra actions will be done to format the double without some
	 * scientific notations
	 * 
	 * @param columnName
	 * @param values
	 * @return the trimmed value from the result OR null
	 */

	public String getTrimmedValue( Object value, String columnAlias ) {
		if( this.settings != null ) //Having the settings is not mandatory
			return getTrimmedValue(value, this.settings.getTimeZoneForMember(columnAlias), this.settings.getDisplayMask(columnAlias));
		else
			return getTrimmedValue(value, null, null);
	}

	/**
	 * Please use getTrimmedValue( Object value, TimeZone timeZone, String dateInputMask )
	 * @param value
	 * @return
	 */
	@Deprecated
	public static String getTrimmedValue( Object value ) {
		return getTrimmedValue(value, null, null);
	}
	public static String getTrimmedValue( Object value, TimeZone timeZone, String dateInputMask ) {
		String strValue = null;
		if ( value != null ) {
			if ( value instanceof String )
				strValue = ((String) value).trim();
			else if ( value instanceof Float || value instanceof Double || value instanceof BigDecimal ) {
				if ( value instanceof Float )
					value = ((Float) value).doubleValue();
				else if ( value instanceof BigDecimal )
					value = ((BigDecimal) value).doubleValue();

				strValue = getFormattedNumber((Double) value, 2, 20);
			}
			else if ( value instanceof Date ) {
				synchronized (dateFormat) {
					SimpleDateFormat format = dateFormat;
					if( dateInputMask != null ) {
						format = new SimpleDateFormat(dateInputMask);
					}
					
					if ( timeZone != null )
						format.setTimeZone(timeZone);
					else
						format.setTimeZone(TimeZone.getDefault());

					strValue = format.format((Date) value);
				}	
			}
			else if ( value != null )
				strValue = String.valueOf(value);
		}

		return strValue;
	}

	/**
	 * Get the value of the column from the resultset,
	 * if the value found is not an integer this function will return null
	 * 
	 * @param columnNumber
	 * @param values
	 * @return the integer value from the result OR null
	 * @throws ParseException
	 */
	public static Integer getIntegerValue( Object value ) throws ParseException {
		Integer intValue = null;

		if ( value instanceof Integer )
			intValue = (Integer) value;
		else if ( value instanceof Long )
			intValue = ((Long) value).intValue();
		if ( value instanceof Double )
			intValue = ((Double) value).intValue();
		else if ( value instanceof BigDecimal )
			intValue = ((BigDecimal) value).intValue();
		else if ( value != null && !"".equals(String.valueOf(value).trim()) ) {
			try {
				intValue = Integer.valueOf(String.valueOf(value).trim());
			}
			catch( Exception e ) {
				Double dblValue = getDoubleValue(value);
				if ( dblValue != null )
					intValue = dblValue.intValue();
			}
		}

		return intValue;
	}

	/**
	 * Get the value of the column from the resultset,
	 * if the value found is not an long this function will return null
	 * 
	 * @param columnNumber
	 * @param values
	 * @return the long value from the result OR null
	 * @throws ParseException
	 */
	public static Long getLongValue( Object value ) throws ParseException {
		Long longValue = null;

		if ( value instanceof Long )
			longValue = (Long) value;
		else if ( value instanceof Date )
			longValue = ((Date) value).getTime();
		else if ( value instanceof Double )
			longValue = ((Double) value).longValue();
		else if ( value instanceof BigDecimal )
			longValue = ((BigDecimal) value).longValue();
		else if ( value != null && !"".equals(String.valueOf(value).trim()) ) {
			try {
				longValue = Long.valueOf(String.valueOf(value).trim());
			}
			catch( Exception e ) {
				Double dblValue = getDoubleValue(value);
				if ( dblValue != null )
					longValue = dblValue.longValue();
			}
		}

		return longValue;
	}


	/**
	 * Get the value of the column from the Object[],
	 * if the value found is not an date this function will return null
	 * 
	 * @param columnNumber
	 * @param values
	 * @return the date value from the result OR null
	 * @throws ParseException
	 */
	public static Date getDateValue( TimeZone timeZone, Object value, String inputMask ) throws ParseException {
		Date dateValue = null;

		if ( value instanceof Integer ) {
			if ( ((Integer) value) != -3600000 && ((Integer) value) != 0L )
				value = new Long((Integer) value);
		}

		if ( value instanceof Long ) {
			if ( ((Long) value) != -3600000L && ((Long) value) != 0L ) {
				Calendar calendar = (timeZone != null ? new GregorianCalendar(timeZone) : new GregorianCalendar());
				calendar.setTimeInMillis((Long) value);
				dateValue = calendar.getTime();
			}
		}
		else if ( value instanceof String ) {
			String strValue = ((String) value).trim();
			if ( !"".equals(strValue) ) {
				try {
					synchronized (dateFormat) {
						SimpleDateFormat format = dateFormat;
						if( inputMask != null ) {
							format = new SimpleDateFormat(inputMask);
						}
						
						if ( timeZone != null )
							format.setTimeZone(timeZone);
						else
							format.setTimeZone(TimeZone.getDefault());

						dateValue = format.parse(strValue);
					}
				}
				catch( java.text.ParseException e ) {
					throw new ParseException(e);
				}
			}
		}
		else if ( value instanceof Date ) {
			dateValue = (Date) value;
		}
		else if ( value != null )
			throw new ParseException("The value isn't a valid date type, only long and date are supported, but the column is of type: " + value
					.getClass().getSimpleName());

		return dateValue;
	}

	/**
	 * Get the value of the column from the Object[],
	 * if the value found is not an boolean this function will return null
	 * 
	 * @param columnName
	 * @param rs
	 * @return the boolean value from the result OR null
	 * @throws ParseException
	 */
	public static Boolean getBooleanValue( Object value ) throws ParseException {
		Boolean boolValue = false;

		if ( value instanceof Boolean )
			boolValue = (Boolean) value;
		else if ( value instanceof Integer )
			boolValue = (((Integer) value) > 0);
		else if ( value instanceof Long )
			boolValue = (((Long) value) > 0);
		else if ( value instanceof Short )
			boolValue = (((Short) value) > 0);
		else if ( value instanceof Double )
			boolValue = (((Double) value) > 0);
		else if ( value instanceof BigDecimal )
			boolValue = (((BigDecimal) value).doubleValue() > 0);
		else if ( value instanceof String ) {
			String strValue = (String) value;
			if ( strValue.equalsIgnoreCase("true") )
				boolValue = true;
			else if ( strValue.equalsIgnoreCase("false") )
				boolValue = false;
			else if ( strValue.equalsIgnoreCase("1") )
				boolValue = true;
			else if ( strValue.equalsIgnoreCase("0") )
				boolValue = false;
			else if ( strValue.equalsIgnoreCase("yes") )
				boolValue = true;
			else if ( strValue.equalsIgnoreCase("no") )
				boolValue = false;
			else if ( strValue.equalsIgnoreCase("ja") )
				boolValue = true;
			else if ( strValue.equalsIgnoreCase("nee") )
				boolValue = false;
		}
		else if ( value != null )
			throw new ParseException("The value isn't a valid boolean type, the column is of type: " + value.getClass().getSimpleName());

		return boolValue;
	}


	/**
	 * Get the value of the column from the Object[],
	 * if the value found is not an double this function will return null
	 * 
	 * @param columnNumber
	 * @param values
	 * @return the double value from the result OR null
	 * @throws ParseException
	 */
	public static Double getDoubleValue( Object value ) throws ParseException {
		Double dblValue = null;

		if ( value instanceof Double )
			dblValue = (Double) value;
		else if ( value instanceof Float )
			dblValue = Double.valueOf((Float) value);
		else if ( value instanceof BigDecimal )
			dblValue = ((BigDecimal) value).doubleValue();
		else if ( value instanceof Integer )
			dblValue = Double.valueOf((Integer) value);
		else if ( value instanceof Long )
			dblValue = Double.valueOf((Long) value);
		else if ( value != null && !"".equals(String.valueOf(value).trim()) ) {
			String strValue = String.valueOf(value).trim();
			try {
				strValue = strValue.replaceAll("%", "").replaceAll("$", "").replaceAll("€", "");
				if ( !"".equals(strValue) ) {

					int nrOfDots = StringUtils.countMatches(strValue, ".");
					int nrOfComma = StringUtils.countMatches(strValue, ",");

					// In case of no comma's and 1 dot or no dots we can just parse
					if ( nrOfComma == 0 && nrOfDots <= 1 )
						dblValue = Double.valueOf(strValue);

					// More than one comma is only possible if it is a separator
					else if ( nrOfComma > 1 && (nrOfDots <= 1) || strValue.indexOf(".") > strValue.indexOf(",") )
						dblValue = Double.valueOf(strValue.replace(",", ""));
					else if ( nrOfDots > 1 && (nrOfComma <= 1) || strValue.indexOf(".") < strValue.indexOf(",") )
						dblValue = Double.valueOf(strValue.replace(".", "").replace(",", "."));

					else if ( nrOfComma == 1 && nrOfDots == 0 )
						dblValue = Double.valueOf(strValue.replace(",", "."));

					else
						throw new ParseException("Fall-through, decimal parse action " + value + " d" + nrOfDots + " c" + nrOfComma);
				}
			}
			catch( Exception e ) {

				throw new ParseException(e);
			}
		}

		return dblValue;
	}

	/**
	 * Get the value of the column from the Object[],
	 * if the value found is not an double this function will return null
	 * 
	 * @param columnNumber
	 * @param values
	 * @return the double value from the result OR null
	 * @throws ParseException
	 */
	public static BigDecimal getBigDecimalValue( Object value ) throws ParseException {
		BigDecimal dValue = null;

		if ( value instanceof BigDecimal )
			dValue = (BigDecimal) value;
		else if ( value instanceof Double )
			dValue = BigDecimal.valueOf((Double) value);
		else if ( value instanceof Float )
			dValue = BigDecimal.valueOf((Float) value);
		else if ( value instanceof Integer )
			dValue = BigDecimal.valueOf((Integer) value);
		else if ( value instanceof Long )
			dValue = BigDecimal.valueOf((Long) value);
		else if ( value != null && !"".equals(String.valueOf(value).trim()) ) {
			String strValue = String.valueOf(value).trim();
			try {
				Double dblValue = null;
				if ( !strValue.contains(".") && strValue.contains(",") )
					dblValue = Double.valueOf(strValue.replace(",", "."));
				else if ( strValue.contains(".") && strValue.contains(",") && strValue.indexOf(".") > strValue.indexOf(",") )
					dblValue = Double.valueOf(strValue.replaceAll(",", ""));
				else if ( strValue.contains(".") && strValue.contains(",") && strValue.indexOf(".") < strValue.indexOf(",") )
					dblValue = Double.valueOf(strValue.replaceAll(".", "").replaceAll(",", "."));
				else if ( !"".equals(strValue) )
					dblValue = Double.valueOf(strValue);

				if ( dblValue != null )
					dValue = BigDecimal.valueOf(dblValue);
			}
			catch( Exception e ) {
				throw new ParseException(e);
			}
		}

		return dValue;
	}


	// public static String getStringValueFromDate( Object value, String dateNotation ) throws ParseException {
	// Date dateValue = ValueParser.getDateValue(value);
	// String returnValue = null;
	//
	// if ( dateValue != null ) {
	// if ( dateNotation != null ) {
	// try {
	// SimpleDateFormat format = new SimpleDateFormat(dateNotation);
	//
	// returnValue = format.format(dateValue);
	// logNode.debug("Trying to format a date to the notation: " + dateNotation + ", the formatted value is: " +
	// returnValue);
	// }
	// catch( Exception e ) {
	// logNode.error("The folowing date could not be formated use notation: " + dateNotation + " the date was: " +
	// dateValue);
	// }
	// }
	//
	// if ( returnValue == null ) {
	// try {
	// returnValue = ValueParser.dateFormat.format(dateValue);
	// logNode.debug("Trying to format a date to the notation: " +
	// ValueParser.dateFormat.getDateFormatSymbols().toString() + ", the formatted value is: " + returnValue);
	// }
	// catch( Exception e ) {
	// logNode.error("The folowing date could not be formated with the default date notation: " + dateFormat.toPattern()
	// + " the date was: " + dateValue);
	// }
	// }
	// }
	//
	// return null;
	// }

	public static String getStringValueFromNumber( Object value, Integer precision ) throws ParseException {
		String strValue = null;
		Double dblValue = ValueParser.getDoubleValue(value);

		if ( dblValue != null )
			strValue = getFormattedNumber(dblValue, precision, precision);

		return strValue;
	}

	private static String getFormattedNumber( Double curValue, int minPrecision, int maxPrecision ) {
		NumberFormat numberFormat = NumberFormat.getInstance(Locale.ENGLISH);
		numberFormat.setMaximumFractionDigits(maxPrecision);
		numberFormat.setGroupingUsed(false);
		numberFormat.setMinimumFractionDigits(minPrecision);

		if ( !Double.isNaN(curValue) ) {
			String formattedNr = numberFormat.format(curValue);
			logNode.debug("Trying to format a double to a string, the formatted value is: " + formattedNr);

			return formattedNr;
		}
		logNode.debug("The current value is not a valid number returning: null");


		return null;
	}


	public static class ParseException extends MendixReplicationException {

		private static final long serialVersionUID = 1123455468734555L;

		public ParseException() {
			super();
		}

		public ParseException( Exception e ) {
			super(e);
		}

		public ParseException( String msg ) {
			super(msg);
		}

		public ParseException( String msg, Exception e ) {
			super(msg, e);
		}
	}


	public static boolean validateParsedValue( PrimitiveType type, Object value ) {
		if ( value == null )
			return true;

		boolean returnValue = false;
		switch (type) {
		case Boolean:
			if ( value instanceof Boolean )
				returnValue = true;
			break;
		case DateTime:
			if ( value instanceof Date || value instanceof Long )
				returnValue = true;
			break;
		case Decimal:
			if ( value instanceof Float || value instanceof Double || value instanceof Integer || value instanceof Long || value instanceof BigDecimal )
				returnValue = true;
			break;
		case Integer:
			if ( value instanceof Integer )
				returnValue = true;

			else if ( value instanceof Long ) {
				returnValue = true;

				if ( ((Long) value) > Integer.MAX_VALUE )
					returnValue = false;
			}
			else if ( value instanceof Float || value instanceof Double || value instanceof BigDecimal )
				returnValue = true;
			break;
		case AutoNumber:
		case Long:
			if ( value instanceof Long || value instanceof Integer || value instanceof Float || value instanceof Double || value instanceof BigDecimal )
				returnValue = true;
			break;
		case Enum:
		case String:
			returnValue = true;
			break;
		case Binary:
		case HashString:
			throw new NotImplementedException("Hashstring and binary are not supported to be mapped.");
			
		default:
			//Compatibility fix since Currency is no longer part of the latest release 
			if( "Currency".equals(type.toString()) || "Float".equals(type.toString()) ) {
				if ( value instanceof Float || value instanceof Double || value instanceof Integer || value instanceof Long || value instanceof BigDecimal )
					returnValue = true;	
			}
			else 
				logNode.warn("Unsupported primitive type: " + type );
			
			break;
		}

		return returnValue;
	}

	public static final String escapeHTML( String s ) {
		StringBuffer sb = new StringBuffer();
		int n = s.length();
		for( int i = 0; i < n; i++ ) {
			char c = s.charAt(i);
			switch (c) {
			case '<':
				sb.append("&lt;");
				break;
			case '>':
				sb.append("&gt;");
				break;
			case '&':
				sb.append("&amp;");
				break;
			case '"':
				sb.append("&quot;");
				break;
			case 'à':
				sb.append("&agrave;");
				break;
			case 'À':
				sb.append("&Agrave;");
				break;
			case 'â':
				sb.append("&acirc;");
				break;
			case 'Â':
				sb.append("&Acirc;");
				break;
			case 'ä':
				sb.append("&auml;");
				break;
			case 'Ä':
				sb.append("&Auml;");
				break;
			case 'å':
				sb.append("&aring;");
				break;
			case 'Å':
				sb.append("&Aring;");
				break;
			case 'æ':
				sb.append("&aelig;");
				break;
			case 'Æ':
				sb.append("&AElig;");
				break;
			case 'ç':
				sb.append("&ccedil;");
				break;
			case 'Ç':
				sb.append("&Ccedil;");
				break;
			case 'é':
				sb.append("&eacute;");
				break;
			case 'É':
				sb.append("&Eacute;");
				break;
			case 'è':
				sb.append("&egrave;");
				break;
			case 'È':
				sb.append("&Egrave;");
				break;
			case 'ê':
				sb.append("&ecirc;");
				break;
			case 'Ê':
				sb.append("&Ecirc;");
				break;
			case 'ë':
				sb.append("&euml;");
				break;
			case 'Ë':
				sb.append("&Euml;");
				break;
			case 'ï':
				sb.append("&iuml;");
				break;
			case 'Ï':
				sb.append("&Iuml;");
				break;
			case 'ô':
				sb.append("&ocirc;");
				break;
			case 'Ô':
				sb.append("&Ocirc;");
				break;
			case 'ö':
				sb.append("&ouml;");
				break;
			case 'Ö':
				sb.append("&Ouml;");
				break;
			case 'ø':
				sb.append("&oslash;");
				break;
			case 'Ø':
				sb.append("&Oslash;");
				break;
			case 'ß':
				sb.append("&szlig;");
				break;
			case 'ù':
				sb.append("&ugrave;");
				break;
			case 'Ù':
				sb.append("&Ugrave;");
				break;
			case 'û':
				sb.append("&ucirc;");
				break;
			case 'Û':
				sb.append("&Ucirc;");
				break;
			case 'ü':
				sb.append("&uuml;");
				break;
			case 'Ü':
				sb.append("&Uuml;");
				break;
			case '®':
				sb.append("&reg;");
				break;
			case '©':
				sb.append("&copy;");
				break;
			case '€':
				sb.append("&euro;");
				break;
			// be carefull with this one (non-breaking whitee space)
			// case ' ': sb.append("&nbsp;");break;

			default:
				sb.append(c);
				break;
			}
		}
		return sb.toString();
	}


	/**
	 * Validate if the value from the parameter is null, "", or "null"
	 * 
	 * @param value
	 * @return
	 */
	public static boolean isValueEmpty( String value ) {
		return value == null || "".equals(value) || "null".equals(value);
	}
}
