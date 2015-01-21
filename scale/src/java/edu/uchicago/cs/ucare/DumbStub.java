package edu.uchicago.cs.ucare;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

public class DumbStub {

	public static void main(String[] args) throws IOException {
		for (int i = 0; i < 10000; ++i) {
            Socket connection = new Socket("127.0.0.1", 7000);
            OutputStream os = connection.getOutputStream();
            DataOutputStream dos = new DataOutputStream(os);
//    		os.write("helo".getBytes());
//    		os.write(0xCA552DFA);
            dos.writeInt(0xCA552DFA);
            dos.writeInt(6);
            System.out.println(connection.getLocalSocketAddress());
		}
		while (true);
	}

}
