package flatfilemanager.implementation;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Writer;

import com.mendix.core.CoreException;
import com.mendix.systemwideinterfaces.core.IContext;
import com.mendix.systemwideinterfaces.core.IMendixObject;

import flatfilemanager.implementation.FileHandler.TemplateConfiguration;

public abstract class ILineHandler {

	public abstract void initialize( IContext context, Writer writer, TemplateConfiguration config );
	
	public abstract void initialize( IContext context, TemplateConfiguration config );

	public abstract void writeLine(IMendixObject object) throws CoreException;

	public abstract void importFromFile(BufferedReader reader, IMendixObject importFile) throws CoreException, IOException;
	public abstract void importFromFile(BufferedReader reader, IMendixObject importFile, IMendixObject parameterObject, String referenceName) throws CoreException, IOException;
}
