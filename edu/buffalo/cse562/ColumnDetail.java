/**
 * 
 */
package edu.buffalo.cse562;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

/**
 * @author Sathish
 *
 */
public class ColumnDetail {
	
	
	private ColumnDefinition columnDefinition;
	private int index;
	private String columnAliasName;
	private String tableName;
	
	
	
	public int getIndex() {
		return index;
	}
	public void setIndex(int index) {
		this.index = index;
	}
	
	public String getColumnAliasName() {
		return columnAliasName;
	}
	public void setColumnAliasName(String columnAliasName) {
		this.columnAliasName = columnAliasName;
	}
	public ColumnDefinition getColumnDefinition() {
		return columnDefinition;
	}
	public void setColumnDefinition(ColumnDefinition columnDefinition) {
		this.columnDefinition = columnDefinition;
	}
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
}
