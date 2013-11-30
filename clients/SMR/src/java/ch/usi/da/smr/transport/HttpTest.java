package ch.usi.da.smr.transport;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

public class HttpTest {

	public static void main(String[] args) throws IOException, ClassNotFoundException{

		// server
        HttpServer server = HttpServer.create(new InetSocketAddress(8080), 0);
        server.createContext("/state", new SendFile("/tmp/snapshot.state"));        
        server.createContext("/snapshot", new SendFile("/tmp/snapshot.ser"));
        server.setExecutor(null); // creates a default executor
        server.start();

        // client
		URL url = new URL("http://127.0.0.1:8080/state");
		HttpURLConnection con = (HttpURLConnection)url.openConnection();
		InputStream ins = con.getInputStream();
		InputStreamReader isr = new InputStreamReader(ins);
		BufferedReader in = new BufferedReader(isr);
		String line;
		Map<Integer, Integer> state = new HashMap<Integer, Integer>();		
		while ((line = in.readLine()) != null){
			String[] s = line.split("=");
			state.put(Integer.parseInt(s[0]),Integer.parseInt(s[1]));
		}
		System.err.println(state);
		in.close();
		
		url = new URL("http://127.0.0.1:8080/snapshot");
		con = (HttpURLConnection)url.openConnection();
		ins = con.getInputStream();
		ObjectInputStream ois = new ObjectInputStream(ins);
		@SuppressWarnings("unchecked")
		Map<String,byte[]> snapshot = (Map<String,byte[]>) ois.readObject();
		for(Entry<String,byte[]> e : snapshot.entrySet()){
			System.out.println(e.getKey() + "->" + new String(e.getValue()));
		}
		ois.close();
	}
	
    static class SendFile implements HttpHandler {
    	
    	private final File file;
    	
    	public SendFile(String file){
    		this.file = new File(file);
    	}
    	
        public void handle(HttpExchange t) throws IOException {
            FileInputStream fis = new FileInputStream(file);
            BufferedInputStream bis = new BufferedInputStream(fis);

        	byte[] b = new byte[(int)file.length()];
            bis.read(b, 0, b.length); //FIXME: keeps file in mem!
            bis.close();
            fis.close();

            t.sendResponseHeaders(200, b.length);
            OutputStream os = t.getResponseBody();
            os.write(b);
            os.close();
        }
    }

}
