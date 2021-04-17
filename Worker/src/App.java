import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


public class App {
    private static int operatingMode;
    private static String splitFileName;
    private static String mapFileName;
    private static String username = "hqueinnec";
    private static String distantComputersList = "machines.txt";
    private static String distantPath = "/tmp/hugo";
    private static String outputAllReducedFileName = "all_reduced.txt";

    private static HashMap<String,Process> mapOfProcesses;
    private static int numberOfDistantComputers;
    private static List<Integer> hashCodeList = new ArrayList<Integer>();
    private static List<Integer> reduceHashCodeList = new ArrayList<Integer>();
    private static int mkdirCount = 0;

    private static int secondsTimeout = 120;
    
    public static void main(String[] args) throws Exception {

        operatingMode = Integer.valueOf(args[0]);

        if(operatingMode==0){ //map phase
            splitFileName = args[1];
            map();
        }
        else if(operatingMode==1){ //hash and shuffle phase
            mapFileName = args[1];
            hash();
            int status = shuffle();
            if (status==-1){
                System.err.println("TIMEOUT SHUFFLE");
            }
        }
        else if(operatingMode==2){ //reduce phase
            reduce();
            gatherReduce();
        }
        System.exit(0);
    }

    private static void map() throws Exception{
        System.out.println("STARTED");
        
        ProcessBuilder pb1 = new ProcessBuilder("mkdir","maps");
        Process p1 = pb1.start();

        boolean timeoutStatus1 = p1.waitFor(secondsTimeout, TimeUnit.SECONDS);

        if (timeoutStatus1){
            String outputFileName = "UM"+splitFileName.substring(1);
            //String outputFileName = "UM"+splitFileName.charAt(1)+".txt"; 
            File file = new File("maps/"+outputFileName);
            file.createNewFile();
            FileWriter writer = new FileWriter(file);

            FileReader reader = new FileReader("splits/"+splitFileName);
            Scanner scanner = new Scanner(reader);
            
            while(scanner.hasNext()){
                String element = scanner.next();
                writer.write(element + " 1\n");
            }

            writer.flush();
            writer.close();

            scanner.close();
            reader.close();

        } else {
            System.out.println("TIMEOUT 'mkdir'");
            p1.destroy();
        }

        System.out.println("MAP CREATED");

    }

    private static void hash() throws Exception{
        System.out.println("STARTED");
        
        ProcessBuilder pb1 = new ProcessBuilder("mkdir","shuffles");
        Process p1 = pb1.start();

        boolean timeoutStatus1 = p1.waitFor(secondsTimeout, TimeUnit.SECONDS);

        if (timeoutStatus1){
            FileReader reader = new FileReader("maps/"+mapFileName);
            Scanner scanner = new Scanner(reader);

            while(scanner.hasNextLine()){

                String line = scanner.nextLine();
                String element = line.substring(0, line.indexOf(' ')); 
                int hashCode = Math.abs(element.hashCode()); //positiv hash

                String outputFileName = hashCode+"-"+java.net.InetAddress.getLocalHost().getHostName()+".txt";
                File file = new File("shuffles/"+outputFileName);

                if(!hashCodeList.contains(hashCode)){
                    hashCodeList.add(hashCode);
                    file.createNewFile();
                }

                FileWriter writer = new FileWriter(file, true); // if the file already exists, appends text to the existing file
                writer.write(line+"\n");

                writer.flush();
                writer.close();
            }

            scanner.close();
            reader.close();

        } else {
            System.out.println("TIMEOUT 'mkdir'");
            p1.destroy();
        }

        System.out.println("HASH CREATED");

    }

    private static void prepareShuffle() throws Exception{ //create local folders to send to each distant computer with the corresponding hashes
        numberOfDistantComputers = countLines(distantComputersList);
        HashMap<Integer,Process> mapOfLocalProcesses = new HashMap<Integer,Process>();

        //mkdir
        System.out.println("START: Mkdir          (global)");

        int i;
        for (i = 0; i < numberOfDistantComputers; i++) {
            ProcessBuilder pb1 = new ProcessBuilder("mkdir","shuffles/shuffles_"+i);
            Process p1 = pb1.start();
            mapOfLocalProcesses.put(i, p1);
        }

        int localMkdirCount = 0;
        for(Integer id : mapOfLocalProcesses.keySet()){
            Process p = mapOfLocalProcesses.get(id);
            boolean timeoutStatus1 = p.waitFor(secondsTimeout, TimeUnit.SECONDS);

            if (timeoutStatus1){
                System.out.println("  DONE : Mkdir        ("+id+")");
                localMkdirCount++;
            } else {
                System.out.println("  TMOUT: Mkdir        ("+id+")");
                p.destroy();
            }
        }
        if(localMkdirCount!=numberOfDistantComputers){
            System.out.println("TMOUT: Mkdir          (global) "+numberOfDistantComputers+" vs "+mkdirCount);
            return;
        }
        System.out.println("DONE : Mkdir          (global)");

        //mv
        System.out.println("START: Mv             (global)");

        HashMap<String,Process> mapOfHashes = new HashMap<String,Process>();

        int localHashNumber = 0;
        for (int hashCode : hashCodeList){
            localHashNumber++;
            String hashFileName = hashCode+"-"+java.net.InetAddress.getLocalHost().getHostName()+".txt";
            int computerID = hashCode%numberOfDistantComputers;

            ProcessBuilder pb2 = new ProcessBuilder("mv","shuffles/"+hashFileName,"shuffles/shuffles_"+computerID);
            Process p2 = pb2.start();
            mapOfHashes.put(hashFileName, p2);
        }

        int localHashCount = 0;
        for(String hashFileName : mapOfHashes.keySet()){
            Process p = mapOfHashes.get(hashFileName);
            boolean timeoutStatus1 = p.waitFor(secondsTimeout, TimeUnit.SECONDS);

            if (timeoutStatus1){
                localHashCount++;
            } else {
                p.destroy();
            }
        }
        if(localHashCount!=localHashNumber){
            System.out.println("TMOUT: Mv             (global) ");
            return;
        }
        System.out.println("DONE : Mv             (global)");

    }

    private static int shuffle() throws Exception{

        System.out.println("STARTED");

        prepareShuffle();

        System.out.println("PREPARED");

        mapOfProcesses = new HashMap<String,Process>();

        //mkdir
        System.out.println("START: Mkdir          (global)");

        FileReader fr = new FileReader(distantComputersList) ;
        BufferedReader bu = new BufferedReader(fr) ;
        Scanner sc = new Scanner(bu) ;

        while(sc.hasNextLine()){
            String distantComputersListLine = sc.nextLine();
            mkdir(distantComputersListLine, distantPath+"/shufflesreceived");
        }

        sc.close();
        bu.close();
        fr.close();

        for(String hostname : mapOfProcesses.keySet()){
            Process p = mapOfProcesses.get(hostname);
            boolean timeoutStatus1 = p.waitFor(secondsTimeout, TimeUnit.SECONDS);

            if (timeoutStatus1){
                System.out.println("  DONE : Mkdir        ("+hostname+")");
                mkdirCount++;
            } else {
                System.out.println("  TMOUT: Mkdir        ("+hostname+")");
                p.destroy();
                return -1;
            }
        }
        if(mkdirCount!=numberOfDistantComputers){
            System.out.println("TMOUT: Mkdir          (global) "+numberOfDistantComputers+" vs "+mkdirCount);
            return -1;
        }
        System.out.println("DONE : Mkdir          (global)");


        // send hashes
        System.out.println("START: 1Hash Deploy   (global)");
        int hashesDeploymentCount = 0;

        List<Integer> range = IntStream.rangeClosed(0, numberOfDistantComputers-1).boxed().collect(Collectors.toList());
        Collections.shuffle(range); //shuffle indexes to reduce the number of simultaneous scp connections (that are limited to 10)
        System.err.println(java.net.InetAddress.getLocalHost().getHostName()+" : "+range);

        for (int folderID : range) {
            String hostname = Files.readAllLines(Paths.get(distantComputersList)).get(folderID);
            sendShuffles(hostname, "shuffles/shuffles_"+folderID);
        }

        for (int folderID = 0; folderID < numberOfDistantComputers; folderID++) {
            String hostname = Files.readAllLines(Paths.get(distantComputersList)).get(folderID);
            Process p = mapOfProcesses.get(hostname);
            boolean timeoutStatus1 = p.waitFor(secondsTimeout, TimeUnit.SECONDS);

            InputStream es = p.getErrorStream();
            BufferedReader ber = new BufferedReader(new InputStreamReader(es));
            String eLine;

            while ((eLine = ber.readLine()) != null){
                System.err.println("--ERROR: shuffle scp "+java.net.InetAddress.getLocalHost().getHostName()+" -> "+hostname+" - "+ eLine);
            }
            ber.close();
            es.close();

            if (timeoutStatus1){
                //System.out.println("  DONE: 1Hash Deploy ("+folderID+")");
                hashesDeploymentCount++;
            } else {
                System.out.println("  TMOUT: 1Hash Deploy ("+folderID+")");
                p.destroy();
                return -1;
            }
        }

        if(hashesDeploymentCount!=numberOfDistantComputers){
            System.err.println("TMOUT: 1Hash Deploy   (global)");
            return -1;
        }
        System.out.println("DONE : 1Hash Deploy   (global)");

        return 0;
    }


    private static void reduce() throws Exception{
        System.out.println("STARTED");

        int myIndex = getLineNumberOfString(distantComputersList, java.net.InetAddress.getLocalHost().getHostName())-1;
        String subFolder = "shuffles_"+myIndex+"/";
        
        ProcessBuilder pb1 = new ProcessBuilder("mkdir","reduces");
        Process p1 = pb1.start();

        boolean timeoutStatus1 = p1.waitFor(secondsTimeout, TimeUnit.SECONDS);

        if (timeoutStatus1){

            String[] pathnames;
            File f = new File("shufflesreceived/"+subFolder);
            pathnames = f.list();
   
            for (String pathname : pathnames) {

                int hash;
                if(pathname.indexOf("-")==0){
                    hash = Integer.parseInt(pathname.substring(0, pathname.substring(1).indexOf("-")+1));
                } else {
                    hash = Integer.parseInt(pathname.substring(0, pathname.indexOf("-")));
                }
                String reduceFileName = String.valueOf(hash)+".txt";
                File file = new File("reduces/"+reduceFileName);

                if(!reduceHashCodeList.contains(hash)){
                    reduceHashCodeList.add(hash);
                    file.createNewFile();
                }

                FileWriter writer = new FileWriter(file, true); // if the file already exists, appends text to the existing file
                
                FileReader reader = new FileReader("shufflesreceived/"+subFolder+pathname);
                Scanner scanner = new Scanner(reader);
    
                while(scanner.hasNextLine()){
                    String inputLine = scanner.nextLine();
                    writer.write(inputLine+"\n");
                }

                writer.flush();
                writer.close();

                scanner.close();
                reader.close();

            }

            for (int hash : reduceHashCodeList){
                String reduceFileName = String.valueOf(hash)+".txt";

                int occurences = countLines("reduces/"+reduceFileName);

                FileReader reader = new FileReader("reduces/"+reduceFileName);
                Scanner scanner = new Scanner(reader);
                String word = scanner.next();
                scanner.close();
                reader.close();

                File file = new File("reduces/"+reduceFileName);
                file.createNewFile();
                FileWriter writer = new FileWriter(file);
                writer.write(word+" "+String.valueOf(occurences)+"\n");
                writer.close();
            }

                
        } else {
            System.out.println("TIMEOUT 'mkdir'");
            p1.destroy();
        }

        System.out.println("REDUCE DONE");
        
    }

    private static boolean gatherReduce() throws Exception {
        System.out.println("STARTED");

        File file = new File(outputAllReducedFileName);
        file.createNewFile();
        FileWriter writer = new FileWriter(file);


        FileReader fr = new FileReader(distantComputersList) ;
        BufferedReader bu = new BufferedReader(fr) ;
        Scanner sc = new Scanner(bu) ;


        String[] pathnames;
        File f = new File("reduces/");
        pathnames = f.list();

        for (String pathname : pathnames) {

            ProcessBuilder pb3= new ProcessBuilder("cat","reduces/"+pathname); //-1 gives one output per line
            Process p3 = pb3.start();

            boolean timeoutStatus3 = p3.waitFor(secondsTimeout, TimeUnit.SECONDS);

            if(timeoutStatus3){
                InputStream is3 = p3.getInputStream();
                BufferedReader br3 = new BufferedReader(new InputStreamReader(is3));
                String lineCurrentReduce;

                while ((lineCurrentReduce = br3.readLine()) != null){
                    writer.write(lineCurrentReduce+"\n");
                }

                writer.flush();
                br3.close();
                is3.close();

            } else {
                System.out.println("  TMO : Gather Reduces");
                p3.destroy();
                sc.close();
                bu.close();
                fr.close();
                writer.close();
                return false;
            }
                
        }

        System.out.println("  DONE: Gather Reduces");

        sc.close();
        bu.close();
        fr.close();

        writer.close();
        return true;

    }

    private static void mkdir(String hostname, String directoryName) throws Exception{
        ProcessBuilder pb1 = new ProcessBuilder("ssh", username+"@"+hostname, "mkdir " + directoryName);
        Process p1 = pb1.start();
        mapOfProcesses.put(hostname, p1);
    }

    private static void sendShuffles(String hostname, String folder) throws Exception {
        ProcessBuilder pb2 = new ProcessBuilder("scp", "-r", folder,  username+"@"+hostname + ":"+distantPath+"/shufflesreceived/");
        Process p2 = pb2.start();
        mapOfProcesses.replace(hostname, p2);
    }

    private static int countLines(String fileName) throws Exception {
        FileReader fileReader = new FileReader(fileName);
        LineNumberReader reader = null;
        try {
            reader = new LineNumberReader(fileReader);
            while ((reader.readLine()) != null);
            return reader.getLineNumber();
        } catch (Exception ex) {
            return -1;
        } finally { 
            if(reader != null){
                reader.close();
            }
            fileReader.close();
        }
        
    }

    private static int getLineNumberOfString(String fileName, String string) throws Exception {
        FileReader fileReader = new FileReader(fileName);
        LineNumberReader reader = null;
        try {
            reader = new LineNumberReader(fileReader);
            String line;
            while ((line = reader.readLine()) != null){
                if(line.equals(string)){
                    return reader.getLineNumber();
                }
            }
            return -1;
        } catch (Exception ex) {
            return -1;
        } finally { 
            if(reader != null){
                reader.close();
            }
            fileReader.close();
        }
        
    }

}
