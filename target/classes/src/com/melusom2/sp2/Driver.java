//***********************************************
// Michael Meluso
// CSC 470 - CLoud Computing
// Project 2: AWS S3 Client
// 
// Driver class
// Driver for the AWS S3 Client
//**********************************************

package src.com.melusom2.sp2;

import java.io.IOException;

public class Driver {
	public static void main(String argv[]) throws IOException {
		Client client = new Client();
		client.run();
	}
}
