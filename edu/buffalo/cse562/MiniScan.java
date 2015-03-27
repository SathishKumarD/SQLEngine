package edu.buffalo.cse562;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

public class MiniScan{
	BufferedReader br;
	String line;
	TreeMap<Integer, String> typeMap;

	public MiniScan(File filename, TreeMap<Integer, String> typeMap) throws IOException{
		Charset charset = Charset.forName("US-ASCII");
		this.br = Files.newBufferedReader(filename.toPath(), charset);
		this.typeMap = typeMap;
		System.out.println(typeMap);
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
