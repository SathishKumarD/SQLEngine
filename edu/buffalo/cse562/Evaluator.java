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

		ColumnDetail col = tupleSchema.get(column.getWholeColumnName());

		if(col!=null)
		{
			int colID = (Integer) col.getIndex();		  
			return tuple.get(colID).val;
		}
		else
		{
			// column may present without table prefix
			for(Map.Entry<String, ColumnDetail> colDetail: this.tupleSchema.entrySet()){
				String key = colDetail.getKey();
				if(key.split("\\.").length>1)
				{
					// extract ColumnName from TableName.ColumnName
					String columnValue = key.split("\\.")[1];
					
					// validate it with the column passed
					if( columnValue.equalsIgnoreCase(column.getWholeColumnName()))
					{
						int colID = (Integer) colDetail.getValue().getIndex();		  
						return tuple.get(colID).val;
					}
				}
			}
			return null;
		}
	}

}
