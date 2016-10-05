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
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Iterator;
import java.util.Properties;
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
import org.geojson.Point;

/**
 *
 * @author hzg
 */
public class ReadOpenWeatherForecast {

    private static Properties props;
    private static final String OPEN_WEATHER_API_KARLSRUHE_CITY_ID = "3214104";
    private static final String OPEN_WEATHER_API_PARIS_CITY_ID = "3214104";
    private static final String OPEN_WEATHER_API_HZG_APPID = "1e12aa36e7c7c42296496089072ef68a";
    private static final String OPEN_WEATHER_API_URL = "http://api.openweathermap.org/data/2.5/";

    private static final String BASE_URL = "http://akme-a3.iosb.fraunhofer.de:80/SensorThingsService/v1.0/";
    // to be moved to properties file
    private long SENSOR_ID = 332;
    private long THING_ID = 1;
    private long PROPERTY_ID = 1;
    private long LOCATION_ID = 1;
    private long DATASTREAM_ID = 358;

    private static SensorThingsService service;

    /**
     *
     * @return @throws ServiceFailureException
     * @throws URISyntaxException
     */
    public Datastream createOpenWeatherSensor() throws ServiceFailureException, URISyntaxException {
        Sensor ows = service.sensors().find(Long.parseLong(props.getProperty("SENSOR_ID")));
        if (ows == null) {
            ows = new Sensor("OpenWeatherData", "OpenWeatherData Server free service", "text", "Some metadata.");
            service.create(ows);
        }

        //Sensor ows = new Sensor("OpenWeatherData", "OpenWeatherData Server free service", "text", "Some metadata.");
        ObservedProperty obsProp1 = service.observedProperties().find(Long.parseLong(props.getProperty("TEMPERATURE_ID")));
        if (obsProp1 == null) {
            obsProp1 = new ObservedProperty("Temperature", new URI("http://ucom.org/temperature"), "forecast temperature");
            service.create(obsProp1);
        }

        Thing iosb = service.things().find(Long.parseLong(props.getProperty("THING_IOSB_KA_ID")));
        if (iosb == null) {
            iosb = new Thing("IOSB-KA", "IOSB Building, Fraunhoferstr. 1, 76131 Karlsruhe");
            service.create(iosb);
        }

        Location location = service.locations().find(Long.parseLong(props.getProperty("IOSB_LOCATION_ID")));
        if (location == null) {
            location = new Location("Location IOSB-KA", "Location of IOSB Building in Karlsruhe", "application/vnd.geo+json", new Point(8, 52));
            location.getThings().add(iosb);
            service.create(location);
        }

        Datastream datastream = service.datastreams().find(Long.parseLong(props.getProperty("DATASTREAM_ID")));
        if (datastream == null) {
            datastream = new Datastream("forecast", "Temperature forecast", OPEN_WEATHER_API_KARLSRUHE_CITY_ID, new UnitOfMeasurement("degree kelvin", "°K", "ucum:T"));
            datastream.setThing(iosb);
            datastream.setSensor(ows);
            datastream.setObservedProperty(obsProp1);
            service.create(datastream);
        }

        return datastream;
    }

    /**
     *
     * @param cityCode
     * @return
     * @throws ServiceFailureException
     * @throws URISyntaxException
     */
    public Datastream getDataStream(String cityCode) throws ServiceFailureException, URISyntaxException {

        Datastream ds = null;
        ds = service.datastreams().find(Long.parseLong(props.getProperty("DATASTREAM_ID")));
        if (ds == null) {
            ds = createOpenWeatherSensor();
            return ds;
        }
        return ds;
    }

    /**
     * Clean all OpenWeatherData related objects within Sensor Server
     */
    public void deleteOpenWeatherSensor() {

    }

    /**
     * Read the Forecast data from now and return the result
     *
     * @return
     * @throws IOException
     */
    public JsonNode readForecastData() throws IOException {
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

            ObjectMapper mapper = new ObjectMapper();
            JsonNode forecastData = mapper.readValue(result, JsonNode.class
            );
            return forecastData.path("list");
        } finally {
            httpclient.close();
        }
    }

    /**
     * Convert the forecast data into sensor server observations
     *
     * @param ds
     * @param forecast
     */
    public void storeForecastData(Datastream ds, JsonNode forecast) {
        Calendar zeit = Calendar.getInstance();
        DateTimeFormatter f = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSX")
                .withZone(ZoneOffset.UTC);
        for (int i = 0; i < forecast.size(); i++) {
            JsonNode time = forecast.path(i).path("dt");
            JsonNode temp = forecast.path(i).path("main").path("temp");
            zeit.setTimeInMillis(time.asLong() * 1000);
            System.out.println(f.format(zeit.toInstant()) + ": " + temp.toString());

            Observation o = new Observation(temp.toString(), ds);
            o.setPhenomenonTime(ZonedDateTime.parse(f.format(zeit.toInstant())));
            try {
                service.create(o);

            } catch (ServiceFailureException ex) {
                Logger.getLogger(ReadOpenWeatherForecast.class
                        .getName()).log(Level.SEVERE, null, ex);
            }
        }

    }

    /**
     * Remove all exisiting observations from datastream
     *
     * @param ds
     * @throws ServiceFailureException
     */
    public void cleanForcastData(Datastream ds) throws ServiceFailureException {
        boolean more = true;
        int count = 0;
        while (more) {
            EntityList<Observation> observations;
            observations = ds.observations().query().list();
            if (observations != null) {
                if (observations.getCount() > 0) {
                    System.out.println(observations.getCount() + " to go.");
                } else {
                    more = false;
                }
                for (Observation o : observations) {
                    service.delete(o);
                    count++;
                }
                System.out.println("Deleted " + count + " observations.");
            }
        }
    }

    /**
     *
     * @param args
     * @throws ServiceFailureException
     * @throws URISyntaxException
     * @throws MalformedURLException
     * @throws IOException
     */
    public static void main(String[] args) throws ServiceFailureException, URISyntaxException, MalformedURLException, IOException {

        props = new Properties();
        try {
            props.load(new FileInputStream("config.properties"));

        } catch (FileNotFoundException e) {
            System.out.println(e);
            System.out.println("properties file has been created with default values. Please check your correct settings");
            props.setProperty("KARLSRUHE_CITY_ID", "3214104");
            props.setProperty("PARIS_CITY_ID", "3214104");
            props.setProperty("OPEN_WEATHER_API_APPID", "1e12aa36e7c7c42296496089072ef68a");
            props.setProperty("OPEN_WEATHER_API_URL", "http://api.openweathermap.org/data/2.5/");

            props.setProperty("BASE_URL", "http://akme-a3.iosb.fraunhofer.de:80/SensorThingsService/v1.0/");
            props.setProperty("SENSOR_ID", "296");
            props.setProperty("THING_IOSB_KA_ID", "358");
            props.setProperty("TEMPERATURE_ID", "331");
            props.setProperty("IOSB_LOCATION_ID", "296");
            props.setProperty("DATASTREAM_ID", "359");

            props.store(
                    new FileOutputStream("config.properties"), "EBITA OpenWeatherData Scanner");
            return;
        }

        URL baseUri = new URL(BASE_URL);
        service = new SensorThingsService(baseUri);

        // create OpenWeatherData Sensor for Karlsruhe
        ReadOpenWeatherForecast reader = new ReadOpenWeatherForecast();

        // get DataStream for CityId = Karlsruhe 
        Datastream ds = reader.getDataStream(OPEN_WEATHER_API_KARLSRUHE_CITY_ID);

        // get forecast data from OpenWeatherMap server
        JsonNode forecast = reader.readForecastData();

        // remove existing forecast data
        reader.cleanForcastData(ds);

        // convert and store forecast data
        reader.storeForecastData(ds, forecast);

    }

}
