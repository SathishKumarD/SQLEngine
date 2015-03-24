/**
 * 
 */
package edu.buffalo.cse562;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Sathish
 *
 */
public class Util {

	public static boolean DEBUG = false;

	public static void printSchema(HashMap<String, ColumnDetail> inputSchema)
	{
		if(DEBUG)
		{
			for(Map.Entry<String, ColumnDetail> colDetail: inputSchema.entrySet()){

				System.out.println(colDetail.getKey() + "   " + colDetail.getValue().getIndex() + "  " + colDetail.getValue().getColumnDefinition().getColDataType());		
			}
			System.out.println("________________________________");
		}

	}

	public static String getSchemaAsString(HashMap<String, ColumnDetail> inputSchema)
	{

		StringBuilder str = new StringBuilder();
		for(Map.Entry<String, ColumnDetail> colDetail: inputSchema.entrySet()){

			str.append(colDetail.getKey()) ;
			str.append("|");
		}

		return str.toString();
	}
	static void printTuple(ArrayList<Tuple> singleTuple) {
		if(DEBUG)
		{
			for(int i=0; i < singleTuple.size();i++){


				try
				{
					String str = (singleTuple.get(i)==null)?"":singleTuple.get(i).toString();
					System.out.print(str);
				}
				catch(Exception ex)
				{
					ex.printStackTrace();
					System.out.println(singleTuple.get(i));
				}

				if(i != singleTuple.size() - 1) System.out.print("|");
			}
			System.out.println();
		}
	}
	public static void printToStream(ArrayList<Tuple> singleTuple, PrintWriter printStream) {
		StringBuilder b = new StringBuilder();
		for(int i=0; i < singleTuple.size();i++){
			b.append(singleTuple.get(i));
			b.append("|");
		}
		printStream.println(b);
	}	

}
