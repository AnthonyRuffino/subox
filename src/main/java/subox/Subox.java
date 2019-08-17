package subox;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Subox {


    private File todoFolder;
    private File zipMapFile;
    private List<String> csvPaths;
    private String rootPath;
    private Path processedPath;
    private Map<String, String> zipMap;

    public static final String ACCOUNT_NUMBER = "Account No.";
    public static final String ZIP = "\"Zip\"";
    public static final String INSURANCE = "\"Pri.Insurance_name\"";
    public static final String APPT_DATE = "\"Appt. Date\"";
    public static final String APPT_RESOURCE = "\"Appt. Resource\"";


    public static void main(String[] args) throws Exception {
        Subox subox = new Subox();
        subox.parseTodos();
        subox.createProcessedPath();
        subox.createZipMap();
        subox.processAllFiles();
    }

    public void processAllFiles() throws Exception {


        Set<String> uniqueAccounts = new HashSet<>();

        Map<String, Integer> zipCount = new HashMap<>();
        Map<String, Integer> cityCount = new HashMap<>();
        Map<String, Integer> insuranceCount = new HashMap<>();
        StringBuilder unknownZips = new StringBuilder();

        Map<String, Set<String>> resourceCount = new HashMap<>();

        csvPaths.forEach(f -> {
            System.out.println(f);

            File csvFile = new File(f);

            CSVFormat format = CSVFormat.EXCEL.withHeader(ACCOUNT_NUMBER, ZIP, INSURANCE, APPT_DATE, APPT_RESOURCE);

            CSVParser csvParser = null;
            try {
                csvParser = format.parse(new InputStreamReader(new FileInputStream(csvFile)));
            } catch (IOException e) {
                throw new RuntimeException("Error reading file: " + e);
            }

            boolean headerParsed = false;

            for (CSVRecord record : csvParser) {

                if(!headerParsed) {
                    headerParsed = true;
                    continue;
                }

                String accountNumber = record.get(ACCOUNT_NUMBER);
                String zip = record.get(ZIP);
                String insurance = record.get(INSURANCE);
                String date = record.get(APPT_DATE);
                String resource = record.get(APPT_RESOURCE);


                if(!uniqueAccounts.contains(accountNumber)) {
                    uniqueAccounts.add(accountNumber);

                    if(!zipCount.containsKey(zip)) {
                        zipCount.put(zip, 0);
                    }
                    zipCount.put(zip, zipCount.get(zip) + 1);

                    String city = zipMap.get(zip);
                    if(city == null) {
                        unknownZips.append("["+zip+"]");
                        city = "other";
                    }
                    if(!cityCount.containsKey(city)) {
                        cityCount.put(city, 0);
                    }
                    cityCount.put(city, cityCount.get(city) + 1);

                    if(!insuranceCount.containsKey(insurance)) {
                        insuranceCount.put(insurance, 0);
                    }
                    insuranceCount.put(insurance, insuranceCount.get(insurance) + 1);
                }

                if(!resourceCount.containsKey(resource)) {
                    resourceCount.put(resource, new HashSet<>());
                }
                String resourceKey = accountNumber + "-" + date;
                resourceCount.get(resource).add(resourceKey);

            }
        });

        final StringBuffer sb = new StringBuffer();

        sb.append("Files processed: ");
        csvPaths.forEach((file) -> sb.append("\n" + file));
        sb.append("\n\n");

        sb.append("\nUnique accounts: " + uniqueAccounts.size());

        sb.append("\n\n");
        sb.append("\nZip totals: ");
        zipCount.forEach((z,cnt) -> sb.append("\n" + z + ": " + cnt));

        sb.append("\n\n");
        String unknowZipsString = unknownZips.toString();
        if(!unknowZipsString.isEmpty()) {
            sb.append("\nUnknown zips: " + unknowZipsString);
        }

        sb.append("\n\n");
        sb.append("\nCity totals: ");
        cityCount.forEach((c,cnt) -> sb.append("\n" + c + ": " + cnt));

        sb.append("\n\n");
        sb.append("\nInsurance totals: ");
        insuranceCount.forEach((i,cnt) -> sb.append("\n" + i + ": " + cnt));

        sb.append("\n\n");
        sb.append("\nResource totals: ");
        resourceCount.forEach((i,cnt) -> sb.append("\n" + i + ": " + cnt.size()));

        Files.write(processedPath.resolve("results.csv"), sb.toString().getBytes());

        csvPaths.forEach((file) -> {
            Path temp = null;
            try {
                File oldFile = new File(file);
                temp = Files.move
                        (Paths.get(file),
                                processedPath.resolve(oldFile.getName()));
            } catch (IOException e) {
                System.out.println("Error moving source csv file" + e.getMessage());
            }

            if(temp == null)
            {
                System.out.println("Failed to move csv file: " + file);
            }
        });

    }


    public void createZipMap() throws Exception {
        zipMap = new HashMap<>();
        try (BufferedReader br = new BufferedReader(new FileReader(zipMapFile))) {
            String line;
            int i = 0;
            while ((line = br.readLine()) != null) {
                i++;
                if (!line.contains(",")) {
                    throw new RuntimeException("A non csv line was found in zipMap.csv. Line #" + i + ". " + line);
                }
                String[] parts = line.split(",");
                String zip = parts[0];
                String city = parts[1];

                if (zipMap.containsKey(zip)) {
                    throw new RuntimeException("Duplicate zip found in zipMap.csv. Line #" + i + ". " + line);
                }
                zipMap.put(zip, city);
            }
        }

        if (zipMap.isEmpty()) {
            throw new RuntimeException("Zips were not mapped!!!");
        }
    }


    public void createProcessedPath() {
        Calendar now = new GregorianCalendar();
        String pathStr = now.get(Calendar.MONTH) + "-"
                + now.get(Calendar.DAY_OF_MONTH) + "-"
                + now.get(Calendar.YEAR) + "_"
                + now.get(Calendar.HOUR_OF_DAY) + "_" + now.get(Calendar.MINUTE) + "_" + now.get(Calendar.SECOND);
        Path path = Paths.get(rootPath + "/processed/" + pathStr);
        try {
            processedPath = Files.createDirectories(path);
        } catch (IOException e) {
            System.err.println("Cannot create directories - " + e);
        }
    }

    public void parseTodos() {
        System.out.println("Working Directory = " +
                System.getProperty("user.dir"));

        rootPath = System.getProperty("user.dir");
        String zipMapPath = rootPath + "/zipMap.csv";
        String todoPath = rootPath + "/todo";

        todoFolder = new File(todoPath);
        if (!todoFolder.exists() || !todoFolder.isDirectory()) {
            throw new RuntimeException("todo directory not found!!!");
        }

        zipMapFile = new File(zipMapPath);
        if (!zipMapFile.exists() || zipMapFile.isDirectory()) {
            throw new RuntimeException("zipMap.csv not found!!!");
        }

        csvPaths = new ArrayList<>();


        try (Stream<Path> walk = Files.walk(Paths.get(todoPath))) {
            List<String> result = walk.map(x -> x.toString())
                    .filter(f -> f.endsWith(".csv")).collect(Collectors.toList());
            result.forEach(csvPaths::add);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Unexpected error searching for files in todo directory!!!", e);
        }


        if (csvPaths.isEmpty()) {
            throw new RuntimeException("No files to process in the todo directory!!!");
        } else {
            System.out.println("Files found: " + csvPaths.size());
        }
    }
}
