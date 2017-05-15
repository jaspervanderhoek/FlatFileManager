package flatfilemanager.implementation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.mendix.core.Core;
import com.mendix.core.CoreException;
import com.mendix.systemwideinterfaces.core.IContext;
import com.mendix.systemwideinterfaces.core.IMendixIdentifier;
import com.mendix.systemwideinterfaces.core.IMendixObject;

import flatfilemanager.proxies.DataSource;
import flatfilemanager.proxies.Field;
import flatfilemanager.proxies.FormatType;
import flatfilemanager.proxies.LineEndChar;
import flatfilemanager.proxies.SuffixOrPrefix;
import flatfilemanager.proxies.Template;
import mxmodelreflection.proxies.MxObjectType;

public class TemplateConfiguration {
	
	private IContext context;
	private Integer totalLength;
	// private Object number
	private String templateName;
	private FormatType formatType;
	private Long id;
	private String delimiter;
	private LineEndChar lineEnd;
	private String escapeChar;
	private String quoteChar;
	private boolean headerOnFirstLine;
	private String objectType;
	private Integer templateIdField = null;

	private Map<Integer, ColumnConfig> columns;

	public class ColumnConfig {

		private String microflow;
		private String mask = null;
		private String staticValue = null;

		private IMendixObject columnObj;
		private Integer nrOfDecimals;
		private int length;
		
		private DataSource datasource;
		private int colNumber;
		private String appendCharacter;
		private SuffixOrPrefix suffixOrPrefix;
		private String description;
		

		protected ColumnConfig( IMendixObject columnObj ) {
			this.columnObj = columnObj;
			
			this.datasource = DataSource.valueOf((String) columnObj.getValue(TemplateConfiguration.this.context, Field.MemberNames.DataSource.toString()));
			this.staticValue = columnObj.getValue(TemplateConfiguration.this.context, Field.MemberNames.StaticValue.toString());
			this.length = columnObj.getValue(TemplateConfiguration.this.context, Field.MemberNames.Length.toString());
			this.colNumber = columnObj.getValue(TemplateConfiguration.this.context, Field.MemberNames.ColNumber.toString());
			this.description = columnObj.getValue(TemplateConfiguration.this.context, Field.MemberNames.Description.toString());
			
			this.appendCharacter = columnObj.getValue(TemplateConfiguration.this.context, Field.MemberNames.AppendCharacter.toString());
			

			String sufOrPre = columnObj.getValue(TemplateConfiguration.this.context, Field.MemberNames.Append.toString());
			if( sufOrPre != null )
				this.suffixOrPrefix = SuffixOrPrefix.valueOf(sufOrPre);
			else 
				this.suffixOrPrefix = SuffixOrPrefix.Suffix;
		}

		protected void addMask( String mask ) {
			if ( mask == null || mask.isEmpty() )
				this.mask = null;
			else
				this.mask = mask;
		}

		protected void setNrOfDecimals( int nrOfDecimals ) {
			this.nrOfDecimals = nrOfDecimals;
		}

		public void addMicroflow( String microflow ) {
			this.microflow = microflow;
		}

		public IMendixObject getColumnObj() {
			return this.columnObj;
		}

		protected String getMicroflow() {
			return this.microflow;
		}

		public String getMask() {
			return this.mask;
		}

		public String getStaticValue() {
			return this.staticValue;
		}

		public Integer getNrOfDecimals() {
			return this.nrOfDecimals;
		}

		public DataSource getValueSource() {
			return datasource;
		}

		public int getLength() {
			return length;
		}

		public int getColNumber() {
			return colNumber;
		}

		public String getAppendCharacter() {
			if( this.appendCharacter != null )
				return this.appendCharacter;
			else 
				return " ";
		}

		public SuffixOrPrefix getSuffixOrPrefix() {
			return this.suffixOrPrefix;
		}

		public String getDescription() {
			return this.description;
		}
	}

	public TemplateConfiguration(IContext context, IMendixObject template ) throws CoreException {
		this.context = context;

		this.totalLength = (Integer) template.getValue(context, Template.MemberNames.TotalLength.toString());
		// this.number = template.getValue(context, Template.MemberNames.Nr.toString());
		this.templateName = template.getValue(context, Template.MemberNames.Title.toString());
		this.formatType = FormatType.valueOf((String) template.getValue(context, Template.MemberNames.FormatType.toString()));
		this.delimiter = (String) template.getValue(context, Template.MemberNames.Delimiter.toString());
		String lineEnd = (String) template.getValue(context, Template.MemberNames.LineEnd.toString());
		if ( lineEnd != null )
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
		List<IMendixObject> result = Core.retrieveXPathQuery(context,
				"//" + Field.getType() + "[" + Field.MemberNames.Field_Template + "='" + this.id + "']", Integer.MAX_VALUE, 0, sortMap);

		for( IMendixObject column : result ) {
			int colNr = column.getValue(context, Field.MemberNames.ColNumber.toString());

			ColumnConfig cc = new ColumnConfig(column);

			cc.addMask(column.getValue(context, Field.MemberNames.ValueMask.toString()));
			cc.setNrOfDecimals(column.getValue(context, Field.MemberNames.NrOfDecimals.toString()));

			IMendixIdentifier mfId = column.getValue(context, Field.MemberNames.Field_Microflows.toString());
			if ( mfId != null ) {
				IMendixObject mfObj = Core.retrieveId(context, mfId);
				cc.addMicroflow(mfObj.getValue(context, "CompleteName"));
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
		if ( this.delimiter == null || this.delimiter.length() == 0 )
			return ',';

		return this.delimiter.charAt(0);
	}

	public char getQuoteChar() {
		if ( this.quoteChar == null || this.quoteChar.length() == 0 )
			return '"';

		return this.quoteChar.charAt(0);
	}

	public char getEscapeChar() {
		if ( this.escapeChar == null || this.escapeChar.length() == 0 )
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
		if ( this.columns.containsKey(colNr) )
			return this.columns.get(colNr).getMicroflow();

		return null;
	}
}