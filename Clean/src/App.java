import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

public class App {

    private static String username = "hqueinnec";
    private static String distantComputersList = "../Resources/machines.txt";
    private static String distantPath = "/tmp/hugo";

    private static int secondsTimeout = 10;

    public static void main(String[] args) throws Exception {

        FileReader fr = new FileReader(distantComputersList) ;
        BufferedReader bu = new BufferedReader(fr) ;
        Scanner sc = new Scanner(bu) ;

        while(sc.hasNextLine()){
            String distantComputersListLine = sc.nextLine();
            ProcessBuilder pb = new ProcessBuilder("ssh", username+"@"+distantComputersListLine, "hostname");
            Process p = pb.start();

            boolean timeoutStatus = p.waitFor(secondsTimeout, TimeUnit.SECONDS);
    
            if (timeoutStatus){
                InputStream is = p.getInputStream();
                BufferedReader br = new BufferedReader(new InputStreamReader(is));
                String line;

                boolean e = false; //error?
    
                while ((line = br.readLine()) != null){
                    System.out.println("  DONE: Connection   ("+ line +")");
                }
                InputStream es = p.getErrorStream();
                BufferedReader ber = new BufferedReader(new InputStreamReader(es));
                String eLine;
                
                while ((eLine = ber.readLine()) != null){
                    System.out.println("--ERROR: Connection - "+ eLine);
                    e = true;
                }
                br.close();
                is.close();
                ber.close();
                es.close();

                if (!e) {clean(distantComputersListLine);}

            } else {
                System.out.println("  TMO : Connection   ("+distantComputersListLine+")");
                p.destroy();
            }
            
        }
        sc.close();
        bu.close();
        fr.close();

    }

    private static void clean(String hostname) throws Exception {

        ProcessBuilder pb1 = new ProcessBuilder("ssh", username+"@"+hostname, "rm -rf " + distantPath);
        Process p1 = pb1.start();

        boolean timeoutStatus1 = p1.waitFor(secondsTimeout, TimeUnit.SECONDS);

        if (timeoutStatus1){  
                System.out.println("  DONE: Cleaning     ("+hostname+")");
        } else {
            System.out.println("  TMO : Cleaning     ("+hostname+")");
            p1.destroy();
        }




    }

}
