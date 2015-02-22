/**
 * 
 */
package edu.buffalo.cse562;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Sathish
 *
 */
public class Util {
	
	public static void printSchema(HashMap<String, ColumnDetail> inputSchema)
	{
		for(Map.Entry<String, ColumnDetail> colDetail: inputSchema.entrySet()){
			
			System.out.println(colDetail.getKey() + "   " + colDetail.getValue().getIndex() );
		
		}
		
	}

}
