/**
 * 
 */
package edu.buffalo.cse562;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author Sathish
 *
 */
public class ScanOperator implements Operator {

	//private File tableSource;
	private BufferedReader buffer;
	private Path dataFile;
	private String tableName = "";
	
	/* (non-Javadoc)
	 * @see edu.buffalo.cse562.Operator#readOneTuple()
	 */	
	 ScanOperator(String tableName){
		//this.tableSource = new File (tableName);	
		 this.tableName = tableName;
		this.dataFile = FileSystems.getDefault().getPath("data\\"+tableName);
		reset();
	}
	
	@Override
	public Datum[] readOneTuple() {
		if(buffer == null) return null;
		int colLength = 0;	
		String line = null;
		
		try {
			line = buffer.readLine();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		if(line == null) return null;
		
		String col[] = line.split("\\|");	
		colLength =	col.length;
		Datum[] data = new Datum[colLength];//column length
		
		for(int i=0; i < colLength; i++){						
			//data[i] =   ;//new Datum   
			//TODO
			//ArrayList<HashMap<?,?>> 
			@SuppressWarnings("unchecked")
			HashMap<Integer, String> indexDataTypeMap = (HashMap<Integer, String>) (Main.tableMappings1.get(tableName)).get(0);	
			String type = indexDataTypeMap.get(i);
						
			data[i] = Datum.giveDatum(type, col[i]);			
		}
				
		return data;
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
		} catch (IOException e) {
			e.printStackTrace();
		}			
	}
	
	public String toString(){
		return "SCAN TABLE " + dataFile.getFileName().toString();
	}
	
	public Operator peekNextOp(){
		return null;
	}
}
