/**
 * 
 */
package edu.buffalo.cse562;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.schema.Column;
import edu.buffalo.cse562.Eval;

/**
 * @author Sathish
 *
 */
public class Evaluator extends Eval {
	
	ArrayList<Tuple> tuple;
	HashMap<String, ColumnDetail> tupleSchema;
	
	public Evaluator(ArrayList<Tuple> tuple, HashMap<String, ColumnDetail> tupleSchema)
	{
		this.tuple = tuple;
		this.tupleSchema = tupleSchema;
	}

	

	@Override
	public LeafValue eval(Column column) throws SQLException {
		return null;
		// TODO Auto-generated method stub
		
		
		
	}

}
