package edu.uchicago.cs.ucare.cassandra.util;

import java.math.BigInteger;

public class TokenGenerator {

    public static void main(String[] args) {
        if (args.length < 1 || args.length > 2) {
            System.err.println("usage: TokenGenerator num_nodes");
            System.err.println("usage: TokenGenerator num_nodes this_node");
            System.exit(1);
        }
        BigInteger max = new BigInteger("2");
        max = max.pow(127);
        max = max.add(BigInteger.ONE.negate());
        BigInteger onePartition = max.divide(new BigInteger(args[0]));
        if (args.length == 1) {
            for (int i = 0; i < Integer.parseInt(args[0]); ++i) {
                System.out.println(onePartition.multiply(new BigInteger(i + "")));
            }
        } else if (args.length == 2) {
            System.out.println(onePartition.multiply(new BigInteger(args[1])));
        }
    }

}
