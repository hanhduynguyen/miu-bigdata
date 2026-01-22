package miu.edu;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import miu.edu.ConsoleColors;

public class Main {
    private static final String API_ENDPOINT="https://query1.finance.yahoo.com/v7/finance/quote?lang=en-US&region=US&corsDomain=finance.yahoo.com";
    private static HashMap<String, String> hashMap = new HashMap<>();
    private static String marketState = "";
    public static Producer<String, String> producer;

    public static void main(String[] args) throws IOException {

        // Load configuration
        BufferedReader bufferedReader = new BufferedReader(new FileReader(args[0]));
        bufferedReader.lines().forEach(line -> {
            String [] w = line.split("=");
            hashMap.put(w[0].trim(), w.length > 1 ? w[1].trim(): "");
        });
        bufferedReader.close();

        // Kafka Producer
        Properties props = new Properties();
        props.put("bootstrap.servers", hashMap.get("kafka_server"));
        props.put("acks", "all");
        props.put("retries", 100);
        props.put("linger.ms", 100);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<>(props);

        long interval = Long.parseLong(hashMap.get("interval"));
        String symbols = Arrays.stream(hashMap.get("tickers").split(",")).collect(Collectors.joining(","));

        try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()) {
            // Yahoo finance api
            HttpGet request = new HttpGet(API_ENDPOINT + "&symbols=" + URLEncoder.encode(symbols, String.valueOf(StandardCharsets.UTF_8)));

            while (true) {
                HttpResponse result = httpClient.execute(request);
                String json = EntityUtils.toString(result.getEntity());
                sendToKafka(new JSONObject(json));
                print(new JSONObject(json), hashMap.get("output"));
                try {
                    Thread.sleep(interval);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }

    private static void sendToKafka(JSONObject jsonObject) {

        JSONArray result = jsonObject.getJSONObject("quoteResponse").getJSONArray("result");

        // Check marketStatus
        if(result.length() > 0) {
            JSONObject firstRecord = result.getJSONObject(0);
            String currentMarketState = firstRecord.optString("marketState");
            if(currentMarketState.equals("CLOSED")) {
                if(!currentMarketState.equals(marketState)) {
                    sendToKafka(jsonObject, true);
                    sendToKafka(jsonObject, false);
                }
            } else {
                sendToKafka(jsonObject, false);
            }
            marketState = currentMarketState;
        }
    }

    private static void sendToKafka(JSONObject jsonObject, boolean EndOfDay) {

        JSONArray result = jsonObject.getJSONObject("quoteResponse").getJSONArray("result");
        String [] kafkaTopic = hashMap.get("kafka_live_topics").split(",");
        String [] fields = hashMap.get("fields").split(",");

        for (int i = 0; i < result.length(); i++) {
            JSONObject data = result.getJSONObject(i);
            String value = "";

            for(String field: fields) {
                value += String.format("\"%s\",", data.optString(field));
            }
            value = value.substring(0, value.length() - 1);

            for(String topic: kafkaTopic) {
                producer.send(new ProducerRecord<String, String>(topic, hashMap.get("fields"), value));
            }
            if(EndOfDay && hashMap.containsKey("kafka_history_topics") && !hashMap.get("kafka_history_topics").equals("")) {
                String [] kafkaEODTopic = hashMap.get("kafka_history_topics").split(",");
                for(String topic: kafkaEODTopic) {
                    producer.send(new ProducerRecord<String, String>(topic, hashMap.get("fields"), value));
                }
            }
        }
    }

    private static void print(JSONObject jsonObject, String output) throws IOException {
        JSONArray result = jsonObject.getJSONObject("quoteResponse").getJSONArray("result");
        result = sort(result);

        StringBuilder sb = new StringBuilder();
        sb.append("\033[H\033[2J");
        sb.append(String.format(ConsoleColors.CYAN_UNDERLINED+"%-15s%10s%10s%11s%10s%15s%12s %-20s\033[0m\n", "Name", "Symbol", "Price", "Diff", "Percent", "Delay", "MarketState", "Long Name"));

        long currentTimeMillis = System.currentTimeMillis();

        // Prepare for output file
//        StringBuilder sbFile = new StringBuilder();

        for (int i = 0; i < result.length(); i++) {
            JSONObject data = result.getJSONObject(i);

            String shortName = data.optString("shortName");
            shortName = shortName.length()>14?shortName.substring(0, 14):shortName;

            String longName = data.optString("longName", shortName);
            String symbol = data.optString("symbol");
            String marketState = data.optString("marketState");
            long regularMarketTime = data.optLong("regularMarketTime");

            double regularMarketPrice = data.optDouble("regularMarketPrice");
            double regularMarketDayHigh = data.optDouble("regularMarketDayHigh");
            double regularMarketDayLow = data.optDouble("regularMarketDayLow");
            double regularMarketChange = data.optDouble("regularMarketChange");
            double regularMarketChangePercent = data.optDouble("regularMarketChangePercent");

            String color = regularMarketChange==0?"":regularMarketChange>0?ConsoleColors.GREEN_BOLD_BRIGHT:ConsoleColors.RED_BOLD_BRIGHT;

            sb.append(String.format("%-15s", shortName));
            sb.append(String.format("%10s", symbol));

            if(regularMarketDayHigh == regularMarketPrice || regularMarketDayLow == regularMarketPrice) {
                sb.append(String.format(ConsoleColors.WHITE_BOLD+color+"%10.2f"+ConsoleColors.RESET, regularMarketPrice));
            } else {
                sb.append(String.format(ConsoleColors.WHITE_BOLD+"%10.2f"+ConsoleColors.RESET, regularMarketPrice));
            }

            sb.append(String.format(color+"%11s"+ConsoleColors.RESET, String.format("%.2f", regularMarketChange)+" "+(regularMarketChange>0?"▲":regularMarketChange<0?"▼":"-")));
            sb.append(String.format(color+"%10s"+ConsoleColors.RESET, String.format("(%.2f%%)", regularMarketChangePercent)));
            sb.append(String.format("%15s", prettyTime(currentTimeMillis-(regularMarketTime*1000))));
            sb.append(String.format("%12s", marketState));
            sb.append(String.format(" %-20s\n", longName));

//            double regularMarketOpen = data.optDouble("regularMarketOpen");//regularMarketVolume
//            double regularMarketVolume = data.optDouble("regularMarketVolume");
//            sbFile.append(String.format("%s,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%s\n",
//                 symbol,regularMarketOpen, regularMarketDayHigh, regularMarketDayLow, regularMarketPrice, regularMarketChange, regularMarketChangePercent
//                    , regularMarketVolume, regularMarketTime));
        }

        System.out.print(sb);
        // Save output to file
//        String fileName = String.format("%s%s.txt", output, currentTimeMillis);
//        BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));
//        writer.write(sbFile.toString());
//        writer.close();
    }

    private static String prettyTime(long millis) {
        return String.format("%dm, %ds",
                TimeUnit.MILLISECONDS.toMinutes(millis),
                TimeUnit.MILLISECONDS.toSeconds(millis) -
                        TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(millis)));
    }

    private static JSONArray sort(JSONArray result) {
        JSONArray sortedJsonArray = new JSONArray();

        List<JSONObject> jsonValues = new ArrayList<>();
        for (int i = 0; i < result.length(); i++) {
            jsonValues.add(result.getJSONObject(i));
        }

        jsonValues.sort((a, b) -> {
            double valA = a.optDouble("regularMarketChangePercent");
            double valB = b.optDouble("regularMarketChangePercent");
            return Double.compare(valB, valA);
        });

        for (JSONObject jsonValue : jsonValues) {
            sortedJsonArray.put(jsonValue);
        }

        return sortedJsonArray;
    }
}
