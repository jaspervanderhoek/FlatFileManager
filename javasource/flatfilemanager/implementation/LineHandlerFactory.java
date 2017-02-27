package flatfilemanager.implementation;

import java.io.Writer;

import com.mendix.core.CoreException;
import com.mendix.systemwideinterfaces.core.IContext;

import flatfilemanager.implementation.FileHandler.TemplateConfiguration;

public class LineHandlerFactory {

	public static ILineHandler getLineHandler(IContext context, TemplateConfiguration config, Writer writer) throws CoreException {
		ILineHandler lh;

		switch (config.getFormatType()) {
		case DelimitedFile:
			lh = new DelimitedLineHandler();
			break;
		case FixedLength:
			lh = new FixedLengthLineHandler();
			break;
		default:
			throw new CoreException("Unkown configuration type");
		}
		if( writer != null )
			lh.initialize(context, writer, config);
		else 
			lh.initialize(context, config);

		return lh;
	}
	public static ILineHandler getLineHandler(IContext context, TemplateConfiguration config) throws CoreException {
		return getLineHandler(context, config, null);		
	}
}
