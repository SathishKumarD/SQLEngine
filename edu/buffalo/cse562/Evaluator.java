/**
 * 
 */
package edu.buffalo.cse562;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.schema.Column;
import edu.buffalo.cse562.Eval;

public class Evaluator extends Eval {	
	ArrayList<Tuple> tuple;
	HashMap<String, ColumnDetail> tupleSchema;

	public Evaluator(ArrayList<Tuple> tuple, HashMap<String, ColumnDetail> tupleSchema)
	{
		this.tuple = tuple;
		this.tupleSchema = tupleSchema;
	}

	@Override
	public LeafValue eval(Expression e) throws SQLException{
		return super.eval(e);
	}

	@Override
	public LeafValue eval(Column column) {

		int colID = getIndex(tupleSchema,column);	
		
		return (colID==-1)?null:tuple.get(colID).val;
	}

	public static int getIndex(HashMap<String, ColumnDetail> tupleSchema, Column column)
	{
		ColumnDetail col = getColumnDetail(tupleSchema,column);
		return col.getIndex();
	}

	public static ColumnDetail getColumnDetail(HashMap<String, ColumnDetail> tupleSchema, Column column)
	{
		ColumnDetail col = tupleSchema.get(column.getWholeColumnName());
		if(col!=null)
		{
			return col;
		}
		else
		{
			for(Map.Entry<String, ColumnDetail> colDetail: tupleSchema.entrySet()){
				String key = colDetail.getKey();
				if(key.split("\\.").length>1)
				{
					// extract ColumnName from TableName.ColumnName
					String columnValue = key.split("\\.")[1];

					// validate it with the column passed
					if( columnValue.equalsIgnoreCase(column.getWholeColumnName()))
					{
						return 	  colDetail.getValue();

					}
				}
			}
		}
		return null;
	}
	
	

}
