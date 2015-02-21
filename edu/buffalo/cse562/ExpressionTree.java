package edu.buffalo.cse562;

import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
 
public class ExpressionTree {
	public Operator generateTree(SelectBody sel){
		Operator current = null;	
		PlainSelect select = (PlainSelect) sel;
		FromItem fi = select.getFromItem();		
		current = buildFroms(fi);
				
		List<Join> joins = (List<Join>) select.getJoins();
		if (joins != null){
			if (joins.size() > 0){
				for (Join j : joins){
					current = buildJoins(current, j);
				}
			}
		}		
		
		Expression exp = (Expression) select.getWhere();
		if (exp != null){
			current = new SelectionOperator(current, exp);
		}
		
		List<SelectItem> selItems = (List<SelectItem>) select.getSelectItems();
		if (selItems != null){
			if (selItems.size() > 0){				
				current = new ExtendedProjection(current, selItems);
			}
		}
		return current;
	}
	public Operator buildFroms(FromItem fi){
		Table table  = null;
		Operator current = null;	
		if (fi instanceof Table){
			table = (Table) fi;
			String tableName = (table).getWholeTableName();
			current = new ScanOperator(tableName);			
		}
		else if (fi instanceof SubSelect){
			current = generateTree(((SubSelect) fi).getSelectBody());
		}		
		else if (fi instanceof SubJoin){
			SubJoin sj = (SubJoin) fi;
			System.out.println(sj);	
//			current = buildJoins(buildFroms(sj.getLeft()), sj.getJoin());
		}
		return current;
	}
	public Operator buildJoins(Operator current, Join j){
		FromItem fr = j.getRightItem();		
		if (fr instanceof Table){
			current = new JoinOperator(current, new ScanOperator(((Table) fr).getName()), j.getOnExpression());
		}	
		return current;
	}
}
