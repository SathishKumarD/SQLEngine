/**
 * 
 */
package edu.buffalo.cse562;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

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
		 int colID = (Integer) tupleSchema.get(column.getWholeColumnName()).getIndex();		  
		  return tuple.get(colID).val;
	}

}
