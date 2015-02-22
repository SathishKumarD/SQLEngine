
package edu.buffalo.cse562;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import net.sf.jsqlparser.schema.Table;

/**
 * @author Shiva
 *
 */ 
public class ScanOperator implements Operator {

	//private File tableSource;
	private BufferedReader buffer;
	private Path dataFile;
	private String tableName = "";
	private String tableAlias = "";
	private  HashMap<String, ColumnDetail> createTableSchemaMap = null;
	private HashMap<String,ColumnDetail> operatorTableSchema = null; 
	private HashMap<Integer, String> indexMaps = null;
	
	/* (non-Javadoc)
	 * @see edu.buffalo.cse562.Operator#readOneTuple()
	 */	
	ScanOperator(Table table){	
		this.tableName = table.getName();
		this.tableAlias = table.getAlias();		
		
		this.dataFile = FileSystems.getDefault().getPath(ConfigManager.getDataDir(), tableName.toLowerCase() +".dat");		
		this.operatorTableSchema = initialiseOperatorTableSchema(Main.tableMapping.get(this.tableName));	
		this.indexMaps = Main.indexTypeMaps.get(this.tableName);
		
		reset();
	}

	@Override
	public ArrayList<Tuple> readOneTuple() {
		if(buffer == null) return null;
		String line = null;

		try {
			line = buffer.readLine();
		} 
		catch (IOException e) {
			e.printStackTrace();
		}

		if(line == null || line.isEmpty()) return null;

		String col[] = line.split("\\|");	
		ArrayList<Tuple> tuples = new ArrayList<Tuple>();
		for(int counter = 0;counter < col.length;counter++) {
			if(indexMaps.containsKey(counter)){		
				String type = indexMaps.get(counter);			
				tuples.add(new Tuple(type, col[counter]));	
			}
		}
		return tuples;
	}

	/* (non-Javadoc)
	 * @see edu.buffalo.cse562.Operator#reset()
	 */
	@Override
	public void reset() {
		// TODO Auto-generated method stub
		try {
			Charset charset = Charset.forName("US-ASCII");
			this.buffer = Files.newBufferedReader(dataFile, charset);
		} 
		catch (IOException e) {
			e.printStackTrace();
		}			
	}

	public String toString(){
		return "SCAN TABLE " + dataFile.getFileName().toString();
	}

	@Override
	public Operator peekNextOp() {
		// TODO Auto-generated method stub
		return null;
	}

	// deep copies the map from static table schema object to operatorTableSchema
	//Replaces table aliases
	private HashMap<String,ColumnDetail> initialiseOperatorTableSchema(HashMap<String,ColumnDetail>  createTableSchemaMap)
	{
		HashMap<String,ColumnDetail> operatorTableSchema = new HashMap<String,ColumnDetail>();		
		for(Entry<String, ColumnDetail> es : createTableSchemaMap.entrySet())
		{
			String nameKey = es.getKey();
			
			if(tableAlias != null) 
			{
				if(nameKey.contains("."))
				{
					String[] columnWholeTableName = nameKey.split("\\.");				
					nameKey = tableAlias +"."+columnWholeTableName[1]; 
				}
			}
			operatorTableSchema.put(nameKey,es.getValue().clone());
		}
		
		return operatorTableSchema;
	}
	
	@Override
	public HashMap<String, ColumnDetail> getOutputTupleSchema() {
		return this.operatorTableSchema;
	}
}
