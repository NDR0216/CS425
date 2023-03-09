import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Scanner;

public class node {
    public static void main(String[] args) throws IOException {
        String nodeName = args[0];
        String address = args[1];
        String port = args[2];

        Socket socket = new Socket(address, Integer.parseInt(port));
        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);

        out.println(nodeName);

        Scanner sc = new Scanner(System.in);
        while(sc.hasNextLine()){
            String timestamp = sc.next();
            String event = sc.next();
            out.println(timestamp +' '+ nodeName +' '+ event);
        }
    }
}
