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
import java.net.URL;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Iterator;
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
import org.threeten.extra.Interval;

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

    public Datastream createOpenWeatherSensor() throws ServiceFailureException, URISyntaxException {
        Sensor ows = new Sensor("OpenWeatherData", "OpenWeatherData Server free service", "text", "Some metadata.");
        service.create(ows);

        ObservedProperty obsProp1 = new ObservedProperty("Temperature", new URI("http://ucom.org/temperature"), "forecast temperature");
        service.create(obsProp1);

        Thing iosb = new Thing("IOSB-KA", "IOSB Building, Fraunhoferstr. 1, 76131 Karlsruhe");
        service.create(iosb);

        Location location = new Location("Location IOSB-KA", "Location of IOSB Building in Karlsruhe", "application/vnd.geo+json", new Point(8, 52));
        location.getThings().add(iosb);
        service.create(location);

        Datastream datastream = new Datastream("forecast", "Temperature forecast", OPEN_WEATHER_API_KARLSRUHE_CITY_ID, new UnitOfMeasurement("degree kelvin", "°K", "ucum:T"));
        datastream.setThing(iosb);
        datastream.setSensor(ows);
        datastream.setObservedProperty(obsProp1);
        service.create(datastream);
        return datastream;
    }

    public Datastream getDataStream(String cityCode) throws ServiceFailureException, URISyntaxException {

        EntityList<Datastream> dataStreamList = service.datastreams().query().filter("observationType eq '" + OPEN_WEATHER_API_KARLSRUHE_CITY_ID + "'").list();
        Datastream ds = null;
        int size = dataStreamList.size();
        if (size == 0) {
            // no datastream found, initialize sensor
            ds = createOpenWeatherSensor();
            return ds;
        }
        if (size > 1) {
            System.out.println("found more than 1 datastream for " + cityCode + ", database needs cleanup");
        }
        Iterator i = dataStreamList.iterator();
        ds = (Datastream) i.next();
        return ds;
    }

    public void deleteOpenWeatherSensor() {

    }

    public void deleteForecastData() {

    }

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

    private void cleanForcastData(Datastream ds) throws ServiceFailureException {
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

    public static void main(String[] args) throws ServiceFailureException, URISyntaxException, MalformedURLException, IOException {
        URL baseUri = new URL(BASE_URL);
        service = new SensorThingsService(baseUri);

        // create OpenWeatherData Sensor for Karlsruhe
        ReadOpenWeatherForecast reader = new ReadOpenWeatherForecast();
        // get DataStream for CityId
        // create DataStream and ForeCastSensor if not existing
        Datastream ds = reader.getDataStream(OPEN_WEATHER_API_KARLSRUHE_CITY_ID);

        // get forecast data from OpenWeatherMap server
        JsonNode forecast = reader.readForecastData();

        // remove existing forecast data
        reader.cleanForcastData(ds);

        // convert and store forecast data
        reader.storeForecastData(ds, forecast);
    }

}
