package trimmomatic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;

public class FastaParser
{
	private BufferedReader reader;

	private String currentLine;
	private FastaRecord current;

	public FastaParser()
	{

	}

	public void parseOne() throws IOException
	{
		current = null;

		if(currentLine==null)
			currentLine = reader.readLine();
		
		while(currentLine!=null && !currentLine.startsWith(">"))
			currentLine = reader.readLine();
		
		if (currentLine != null && currentLine.startsWith(">"))
			{
			String fullName = currentLine.substring(1).trim();
			String tokens[]=fullName.split("[\\| ]");
			String name=tokens[0];
			
			StringBuilder builder=new StringBuilder();
			
			currentLine=reader.readLine();
			while(currentLine!=null && !currentLine.startsWith(">"))
				{
				if(!currentLine.startsWith(";"))
					builder.append(currentLine.trim());
				currentLine=reader.readLine();
				}		
			current=new FastaRecord(name, builder.toString().trim(), fullName);
			}
	}

	public void parse(Path file) throws IOException
	{
		FileSystem fs = FileSystem.get(new Configuration());

		reader = new BufferedReader(new InputStreamReader(new BufferedInputStream(fs.open(file),1000000)));
		parseOne();
	}
	
	public void close() throws IOException
	{
		reader.close();
	}

	public boolean hasNext()
	{
		return current != null;
	}

	public FastaRecord next() throws IOException
	{
		FastaRecord current = this.current;
		parseOne();

		return current;
	}
}
