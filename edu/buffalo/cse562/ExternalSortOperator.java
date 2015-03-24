package edu.buffalo.cse562;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import net.sf.jsqlparser.statement.select.OrderByElement;

public class ExternalSortOperator implements Operator {
	Operator child;
	File swapDir;
	String sortField;
	Comparator<ArrayList<Tuple>> comp;
	int bufferLength;
	HashMap<String, ColumnDetail> outputSchema;
	private static final int BUFFER_SIZE = 3000;
	TreeMap<Integer, String> typeMap;
	List<ArrayList<Tuple>> workingSet;
	boolean sorted = false;
	MiniScan outputStream;

	
	public ExternalSortOperator(Operator child, List<OrderByElement> OrderByElements) {
		// TODO Auto-generated constructor stub
		swapDir = new File(ConfigManager.getSwapDir(), UUID.randomUUID().toString());
		if (!swapDir.exists()){
			swapDir.mkdir();
		}
		this.outputSchema = child.getOutputTupleSchema();
		
		String field = getFullField(OrderByElements.get(0).toString());
		this.child = child;
		this.sortField = field;
		
		this.comp = new TupleComparator(this.outputSchema.get(field).getIndex());
		this.bufferLength = Math.floorDiv(BUFFER_SIZE, this.getOutputTupleSchema().size());
		this.typeMap = new TreeMap<Integer, String>();
		this.workingSet = new ArrayList<ArrayList<Tuple>>(this.bufferLength);
		
		for (ColumnDetail c : outputSchema.values()){
			typeMap.put(c.getIndex(), c.getColumnDefinition().getColDataType().toString().toLowerCase());
		}
	}

	@Override
	public ArrayList<Tuple> readOneTuple() {
		// TODO Auto-generated method stub
		ArrayList<Tuple> currentTuple;
		
		// First run; sorts input tuples in batches, and writes to separate files on disk
		
		if (!sorted){
			int index = 0;
			int nPass = 0;
			File currentFileHandler = getFileHandle(index, nPass);
			while((currentTuple = child.readOneTuple())!= null){
				if (addToSet(currentTuple, true, currentFileHandler)){
					index = index + 1;
					currentFileHandler = getFileHandle(index, nPass);
				}
			}
			index = index + 1;
			currentFileHandler = getFileHandle(index, nPass);
			flushWorkingSet(currentFileHandler, true);
			System.out.println("Working set now " +workingSet.size());
			
			int size = (int) Math.pow(2, Math.ceil(Math.log(index)/Math.log(2)));
			
			// Merging
			File fName1;
			File fName2;
			for (int s = size; s > 0; s /= 2){
				int lcIndex = 0;
				for (int i = 0; i < s; i += 2){
					fName1 = getFileHandle(i, nPass);
					fName2 = getFileHandle(i+1, nPass);
					currentFileHandler = getFileHandle(lcIndex++, nPass + 1) ;
					mergeOnce(fName1, fName2, currentFileHandler);
				}
				nPass = nPass + 1;
			}
			try {
				outputStream = new MiniScan(currentFileHandler, typeMap);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			sorted = true;
		}
		
		
		//##########	
		//Finally, return tuples from sorted file
		currentTuple = outputStream.readTuple();	
		if (currentTuple != null){
			return currentTuple;
		}
		
		return null;
	}
	
	private void mergeOnce(File ifName1, File ifName2, File ofName){
		try {
			MiniScan left = new MiniScan(ifName1, typeMap);
			MiniScan right = new MiniScan(ifName2, typeMap);
			ArrayList<Tuple> leftTup = left.readTuple();
			ArrayList<Tuple> rightTup = right.readTuple();
			
			//Merge procedure
			while (!(leftTup == null) && !(rightTup == null)){
				if (this.comp.compare(leftTup, rightTup) < 0){
					addToSet(leftTup, false, ofName);
					leftTup = left.readTuple(); 
				}
				else{
					addToSet(rightTup, false, ofName);
					rightTup = right.readTuple();
				}
			}
			
			int lcnt = 0;
			while (leftTup != null){
				addToSet(leftTup, false, ofName);
				leftTup = left.readTuple();
				lcnt++;
			}
			int rcnt = 0;

			while (rightTup != null){
				addToSet(rightTup, false, ofName);
				rightTup = right.readTuple();
				rcnt++;
			}			
			flushWorkingSet(ofName, false);			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
	
	private boolean addToSet(ArrayList<Tuple> toAdd, boolean sort, File currentFileHandle){
		/* adds a tuple to the working set and also returns a flag indicating
		 * whether or not the working set was flushed (this is particularly useful for updating
		 * file indexes)
		 */
		if (workingSet.size() < this.bufferLength){
			workingSet.add(toAdd);
			return false;
		}
		else{
//			System.err.println(" Collecting garbage with " 
//					+(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory())/1000000 +"MB currently used");
			flushWorkingSet(currentFileHandle, sort);
			workingSet.add(toAdd);	
			return true;
		}
	}
	
	private boolean flushWorkingSet(File currFileHandle, boolean sorted){
		if (sorted){
			Collections.sort(workingSet, this.comp);
		}
		writeToDisk(workingSet, currFileHandle);
		System.gc();
		workingSet = new ArrayList<ArrayList<Tuple>>(this.bufferLength);
		return true;
	}
	
	@Override
	public void reset() {
		// TODO Auto-generated method stub

	}

	@Override
	public Operator peekNextOp() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public HashMap<String, ColumnDetail> getOutputTupleSchema() {
		// TODO Auto-generated method stub
		return child.getOutputTupleSchema();
	}
	
	private boolean writeToDisk(List<ArrayList<Tuple>> out, File writeDir){
		PrintWriter pw;	
		
		try {			
			//append to file; useful for merging, and ensures that there is never a filenotfound exception
			pw = new PrintWriter(new FileWriter(writeDir, true));
			for(ArrayList<Tuple> t : out){
				Util.printToStream(t, pw);
			}
			pw.close();
			return true;
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}
	
	private File getFileHandle(int index, int nPass){
		String fname = nPass+"-"+index;
		File writeDir = new File(this.swapDir, fname);
		
		if (!writeDir.exists()){
			try {
				writeDir.createNewFile();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return writeDir;
	}
	
	private String getFullField(String input){
		for (Map.Entry<String, ColumnDetail> entry : this.outputSchema.entrySet()){
			ColumnDetail col = entry.getValue();
			if (col.getColumnDefinition().getColumnName().equals(input)){
				return entry.getKey();
			}
		}
		return null;
	}
	
	private class TupleComparator implements Comparator<ArrayList<Tuple>>{
		int fieldIndex;
		public TupleComparator(int fieldIndex){
			this.fieldIndex = fieldIndex;
		}

		@Override
		public int compare(ArrayList<Tuple> o1,
				ArrayList<Tuple> o2) {
			// TODO Auto-generated method stub
			return o1.get(this.fieldIndex).compareTo(o2.get(this.fieldIndex));
		}
	}
	
	
	private class MiniScan{
		BufferedReader br;
		String line;
		TreeMap<Integer, String> typeMap;

		public MiniScan(File filename, TreeMap<Integer, String> typeMap) throws IOException{
			Charset charset = Charset.forName("US-ASCII");
			this.br = Files.newBufferedReader(filename.toPath(), charset);
			this.typeMap = typeMap;
		}
		
		private ArrayList<Tuple> parseLine(String raw){
			String col[] = line.split("\\|");	
			ArrayList<Tuple> tuples = new ArrayList<Tuple>();
			for(Map.Entry<Integer, String> entry : typeMap.entrySet()) {
				tuples.add(new Tuple(entry.getValue(), col[entry.getKey()]));	
			}
			return tuples;
		}
		
		public ArrayList<Tuple> readTuple(){
			try {
				if ((line = br.readLine())!= null){
					return parseLine(line);
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			try {
				this.br.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return null;			
		}
	}
	

}
