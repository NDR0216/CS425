import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Objects;
import java.util.Scanner;

class ConnThread extends Thread {
    Socket socket;
    ConnThread(Socket socket) {
        this.socket = socket;
    }

    public void run() {
        String nodeName = "";

        Scanner sc = null;
        try {
            sc = new Scanner(socket.getInputStream());

            while(sc.hasNextLine() && Objects.equals(nodeName, "")){
                nodeName = sc.nextLine();
            }
            System.out.printf("%.3f - ", System.currentTimeMillis() / 1000.0);
            System.out.println(nodeName +' '+ "connected");

            PrintWriter out = new PrintWriter("log_" + nodeName + ".txt");
            out.printf("%.3f - ", System.currentTimeMillis() / 1000.0);
            out.println(nodeName);

            while(sc.ioException() == null && sc.hasNextLine()){
                String line = sc.nextLine();
                System.out.println(line);
                out.printf("%.3f - ", System.currentTimeMillis() / 1000.0);
                out.println(line);
            }

            System.out.printf("%.3f - ", System.currentTimeMillis() / 1000.0);
            System.out.println(nodeName +' '+ "disconnected");
            out.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

public class logger {
    public static void main(String[] args) throws IOException {
        ServerSocket serverSocket = new ServerSocket(Integer.parseInt(args[0]));

        while (true) {
            Socket socket = serverSocket.accept();
            ConnThread t = new ConnThread(socket);
            t.start();
        }
    }
}
