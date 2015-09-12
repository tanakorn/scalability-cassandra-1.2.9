package edu.uchicago.cs.ucare.util;

import java.io.FileWriter;
import java.io.IOException;

public class SimpleCqlGenerator {

	public static void main(String[] args) {
		String fileName = "/tmp/data.cql";
		if (args.length >= 1) {
			fileName = args[0];
		}
		int numKeySpace = 50;
		if (args.length >= 2) {
			numKeySpace = Integer.parseInt(args[1]);
		}
		int numFamily = 50;
		if (args.length >= 3) {
			numFamily = Integer.parseInt(args[2]);
		}
		int numCol = 10;
		if (args.length >= 4) {
			numCol = Integer.parseInt(args[3]);
		}
		int numKey = 100;
		if (args.length >= 5) {
			numKey = Integer.parseInt(args[4]);
		}
		
		String keySpaceName = "KeySpace";
		String familyName = "Family";
		String colName = "Col";
		String keyName = "key";
		
		try {
			FileWriter writer = new FileWriter(fileName);
			String keySpace, family, col, key, value;
			for (int i = 0; i < numKeySpace; ++i) {
				keySpace = keySpaceName + i;
				writer.write("create keyspace " + keySpace + ";\n");
				writer.write("use " + keySpace + ";\n");
				for (int j = 0; j < numFamily; ++j) {
					family = familyName + j;
					writer.write("create column family " + family 
							+ " with comparator=UTF8Type and "
							+ "default_validation_class=UTF8Type and "
							+ "key_validation_class=UTF8Type;\n");
					for (int k = 0; k < numKey; ++k) {
                        key = keyName + k;
						for (int l = 0; l < numCol; ++l) {
							col = colName + l;
							value = keySpace + family + key + col;
							writer.write("set " + family + "[" + key + "][" + col + "] = '" + value + "';\n");
						}
					}
				}
			}
			writer.close();
		} catch (IOException e) {
		}
	}

}
