/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.fraunhofer.iosb.ilt.weatherforecast;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.fraunhofer.iosb.ilt.sta.ServiceFailureException;
import de.fraunhofer.iosb.ilt.sta.model.Datastream;
import de.fraunhofer.iosb.ilt.sta.model.Location;
import de.fraunhofer.iosb.ilt.sta.model.Observation;
import de.fraunhofer.iosb.ilt.sta.model.ObservedProperty;
import de.fraunhofer.iosb.ilt.sta.model.Sensor;
import de.fraunhofer.iosb.ilt.sta.model.Thing;
import de.fraunhofer.iosb.ilt.sta.model.ext.EntityList;
import de.fraunhofer.iosb.ilt.sta.model.ext.UnitOfMeasurement;
import de.fraunhofer.iosb.ilt.sta.service.SensorThingsService;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

/**
 *
 * @author hzg
 */
public class ReadOpenWeatherForecast {

    private static final String OPEN_WEATHER_API_KARLSRUHE_CITY_ID = "3214104";
    private static final String OPEN_WEATHER_API_PARIS_CITY_ID = "3214104";
    private static final String OPEN_WEATHER_API_HZG_APPID = "1e12aa36e7c7c42296496089072ef68a";
    private static final String OPEN_WEATHER_API_URL = "http://api.openweathermap.org/data/2.5/";

    private static final String BASE_URL = "http://akme-a3.iosb.fraunhofer.de:80/SensorThingsService/v1.0/";

    private static SensorThingsService service;

    private final List<Thing> things = new ArrayList<>();
    private final List<Location> locations = new ArrayList<>();
    private final List<Sensor> sensors = new ArrayList<>();
    private final List<ObservedProperty> oProps = new ArrayList<>();
    private final List<Datastream> datastreams = new ArrayList<>();
    private final List<Observation> observations = new ArrayList<>();

    public void createOpenWeatherSensor() throws ServiceFailureException, URISyntaxException {
        Sensor ows = new Sensor("OpenWeatherData", "OpenWeatherData Server free service", "text", "Some metadata.");
        service.create(ows);
        sensors.add(ows);

        ObservedProperty obsProp1 = new ObservedProperty("Temperature", new URI("http://ucom.org/temperature"), "forecast temperature");
        service.create(obsProp1);
        oProps.add(obsProp1);

        Thing iosb = new Thing("IOSB-KA", "IOSB Building, Fraunhoferstr. 1, 76131 Karlsruhe");
        service.create(iosb);
        things.add(iosb);

        Datastream datastream1 = new Datastream("forecast", "Temperature forecast", OPEN_WEATHER_API_KARLSRUHE_CITY_ID, new UnitOfMeasurement("degree kelvin", "°K", "ucum:T"));
        datastream1.setThing(iosb);
        datastream1.setSensor(ows);
        datastream1.setObservedProperty(obsProp1);
        service.create(datastream1);
        datastreams.add(datastream1);

    }
    
    
    public Observation getObservation (String cityCode) {
    
        Observation o = null;
        try {
            EntityList<Datastream> dataStreamList = service.datastreams().query().filter("observationType eq '" + OPEN_WEATHER_API_KARLSRUHE_CITY_ID + "'").list();
            String t = "";
        } catch (ServiceFailureException ex) {
            Logger.getLogger(ReadOpenWeatherForecast.class.getName()).log(Level.SEVERE, null, ex);
        }

        return o;
    }

    public void deleteOpenWeatherSensor() {

    }
    
    public void deleteForecastData() {
        
    }

    public void readForecastData() throws IOException {
        // read data from OpenWeatherData server
        CloseableHttpClient httpclient = HttpClients.createDefault();
        String result = "";
        try {
            String url = OPEN_WEATHER_API_URL + "forecast?id=" + OPEN_WEATHER_API_KARLSRUHE_CITY_ID + "&appid=" + OPEN_WEATHER_API_HZG_APPID;

            //HttpGet httpget = new HttpGet(OPEN_WEATHER_API_URL);
            HttpGet httpget = new HttpGet(url);

            HttpHost proxy = new HttpHost("proxy-ka.iosb.fraunhofer.de", 80, "http");
            RequestConfig config = RequestConfig.custom()
                    .setProxy(proxy)
                    .build();
            httpget.setConfig(config);

            System.out.println("Executing request " + httpget.getRequestLine());

            CloseableHttpResponse response = httpclient.execute(httpget);
            int status = response.getStatusLine().getStatusCode();
            if (status >= 200 && status < 300) {
                HttpEntity entity = response.getEntity();
                if (entity != null) {
                    result = EntityUtils.toString(entity);
                }
            } else {
                throw new ClientProtocolException("Unexpected response status: " + status);
            }

            Calendar zeit = Calendar.getInstance();
            DateTimeFormatter f = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSX")
                    .withZone(ZoneOffset.UTC);
            ObjectMapper mapper = new ObjectMapper();
            //Map<String, Object> forecastData = mapper.readValue(result, Map.class);
            JsonNode forecastData = mapper.readValue(result, JsonNode.class);
            JsonNode values = forecastData.path("list");
            for (int i = 0; i < values.size(); i++) {
                JsonNode time = values.path(i).path("dt");
                JsonNode temp = values.path(i).path("main").path("temp");
                zeit.setTimeInMillis(time.asLong());

                System.out.println(f.format(zeit.toInstant()) + ": " + temp.toString());
            }

        } finally {
            httpclient.close();
        }
        // get the datastream

        // add forecast data
    }

    public static void main(String[] args) throws ServiceFailureException, URISyntaxException, MalformedURLException, IOException {
        URI baseUri = URI.create(BASE_URL);
        service = new SensorThingsService(baseUri);

        // create OpenWeatherData Sensor for Karlsruhe
        ReadOpenWeatherForecast reader = new ReadOpenWeatherForecast();
        reader.createOpenWeatherSensor();

        reader.readForecastData();
        reader.getObservation(OPEN_WEATHER_API_KARLSRUHE_CITY_ID);
    }

}
