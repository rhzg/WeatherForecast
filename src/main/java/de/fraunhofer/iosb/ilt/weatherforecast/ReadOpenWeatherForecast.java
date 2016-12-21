/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates and open the template
 * in the editor.
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
import org.slf4j.LoggerFactory;


/**
 * @author hzg
 */
public class ReadOpenWeatherForecast {

    private static final String           KARLSRUHE_CITY_ID      = "KARLSRUHE_CITY_ID";
    private static final String           PARIS_CITY_ID          = "PARIS_CITY_ID";
    private static final String           OPEN_WEATHER_API_APPID = "OPEN_WEATHER_API_APPID";
    private static final String           OPEN_WEATHER_API_URL   = "OPEN_WEATHER_API_URL";
    private static final String           BASE_URL               = "BASE_URL";
    private static final String           SENSOR_ID              = "SENSOR_ID";
    private static final String           THING_IOSB_KA_ID       = "THING_IOSB_KA_ID";
    private static final String           TEMPERATURE_ID         = "TEMPERATURE_ID";
    private static final String           IOSB_LOCATION_ID       = "IOSB_LOCATION_ID";
    private static final String           DATASTREAM_ID          = "DATASTREAM_ID";
    private static final String           PROXYHOST              = "proxyhost";
    private static final String           PROXYPORT              = "proxyport";
    private static final String           CLEAN_FORECAST_DATA     = "CLEAN_FORECAST_DATA";

    private static final int              MAX_ENTRIES            = 1000;

    private static Properties             props;
    private static SensorThingsService    service;

    private static final org.slf4j.Logger LOGGER                 = LoggerFactory.getLogger(ReadOpenWeatherForecast.class);

    /**
     *
     * @return
     * @throws ServiceFailureException
     * @throws URISyntaxException
     * @throws IOException
     */
    public Datastream createOpenWeatherSensor() throws ServiceFailureException, URISyntaxException, IOException {
        boolean saveProps = false;
        Sensor ows = service.sensors().find(Long.parseLong(props.getProperty(SENSOR_ID)));
        if (ows == null) {
            ows = new Sensor("OpenWeatherData", "OpenWeatherData Server free service", "text", "Some metadata.");
            service.create(ows);
            props.setProperty(SENSOR_ID, String.valueOf(ows.getId()));
            saveProps = true;
        }

        //Sensor ows = new Sensor("OpenWeatherData", "OpenWeatherData Server free service", "text", "Some metadata.");
        ObservedProperty obsProp1 = service.observedProperties().find(Long.parseLong(props.getProperty(TEMPERATURE_ID)));
        if (obsProp1 == null) {
            obsProp1 = new ObservedProperty("Temperature", new URI("http://ucom.org/temperature"), "forecast temperature");
            service.create(obsProp1);
            props.setProperty(TEMPERATURE_ID, String.valueOf(obsProp1.getId()));
            saveProps = true;
        }

        Thing iosb = service.things().find(Long.parseLong(props.getProperty("THING_IOSB_KA_ID")));
        if (iosb == null) {
            iosb = new Thing("IOSB-KA", "IOSB Building, Fraunhoferstr. 1, 76131 Karlsruhe");
            service.create(iosb);
            props.setProperty(THING_IOSB_KA_ID, String.valueOf(iosb.getId()));
            saveProps = true;
        }

        Location location = service.locations().find(Long.parseLong(props.getProperty(IOSB_LOCATION_ID)));
        if (location == null) {
            location = new Location("Location IOSB-KA", "Location of IOSB Building in Karlsruhe", "application/vnd.geo+json", new Point(8, 52));
            location.getThings().add(iosb);
            service.create(location);
            props.setProperty(IOSB_LOCATION_ID, String.valueOf(location.getId()));
            saveProps = true;
        }

        Datastream datastream = service.datastreams().find(Long.parseLong(props.getProperty(DATASTREAM_ID)));
        if (datastream == null) {
            // datastream for KARLSRUHE_CITY_ID, values are  given as UnitOfMeasurement("degree kelvin", "°K", "ucum:T") but will be mapped to degree celsius
            datastream = new Datastream("forecast", "Temperature forecast", props.getProperty(KARLSRUHE_CITY_ID), new UnitOfMeasurement("degree celsius", "°C", "ucum:T"));
            datastream.setThing(iosb);
            datastream.setSensor(ows);
            datastream.setObservedProperty(obsProp1);
            service.create(datastream);
            props.setProperty(DATASTREAM_ID, String.valueOf(datastream.getId()));
            saveProps = true;
        }

        if (saveProps) {
            props.store(new FileOutputStream("config.properties"), "EBITA OpenWeatherData Scanner");

        }
        return datastream;
    }


    /**
     * @param cityCode
     * @return datastream for cityCode
     * @throws ServiceFailureException
     * @throws URISyntaxException
     * @throws java.io.IOException
     */
    public Datastream getDataStream(final String cityCode) throws ServiceFailureException, URISyntaxException, IOException {
        // find datastream with given id
        Datastream ds;
        ds = service.datastreams().find(Long.parseLong(props.getProperty(DATASTREAM_ID)));
        if (ds == null) {
            // if no datastream is found, initialize sensor description
            ds = this.createOpenWeatherSensor();
            return ds;
        }
        return ds;
    }


    /**
     * Clean all OpenWeatherData related objects within Sensor Server
     * @throws de.fraunhofer.iosb.ilt.sta.ServiceFailureException
     */
    public void deleteOpenWeatherSensor() throws ServiceFailureException {
        LOGGER.info ("Removing forecast sensor.");
        Sensor ows = service.sensors().find(Long.parseLong(props.getProperty(SENSOR_ID)));
        if (ows == null) {
            service.delete(ows);
        }
    }


    /**
     * Read the Forecast data from now and return the result
     *
     * @return forecast data
     * @throws IOException
     */
    public JsonNode readForecastData() throws IOException {
        // read data from OpenWeatherData server
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        String result = "";
        try {
            final String url = props.getProperty(OPEN_WEATHER_API_URL) + "forecast?id=" + props.getProperty(KARLSRUHE_CITY_ID) + "&appid=" + props.getProperty(OPEN_WEATHER_API_APPID);
            final HttpGet httpget = new HttpGet(url);

            final String host = props.getProperty(PROXYHOST, "");
            if (!host.isEmpty()) {
                final int port = Integer.parseInt(props.getProperty(PROXYPORT, "80"));
                final HttpHost proxy = new HttpHost(host, port, "http");
                final RequestConfig config = RequestConfig.custom().setProxy(proxy).build();
                httpget.setConfig(config);
            }
            //System.out.println("Executing request " + httpget.getRequestLine());
            LOGGER.debug("Executing request " + httpget.getRequestLine());
            final CloseableHttpResponse response = httpclient.execute(httpget);
            final int status = response.getStatusLine().getStatusCode();
            if (status >= 200 && status < 300) {
                final HttpEntity entity = response.getEntity();
                if (entity != null) {
                    result = EntityUtils.toString(entity);
                }
            }
            else {
                throw new ClientProtocolException("Unexpected response status: " + status);
            }

            final ObjectMapper mapper = new ObjectMapper();
            final JsonNode forecastData = mapper.readValue(result, JsonNode.class);

            LOGGER.info("forecast data fetched");

            return forecastData.path("list");
        }
        finally {
            httpclient.close();
        }
    }


    /**
     * Convert the forecast data into sensor server observations
     *
     * @param ds
     * @param forecast
     */
    public void storeForecastData(final Datastream ds, final JsonNode forecast) {
        int count = 0;
        ZonedDateTime now = ZonedDateTime.now();
        final Calendar zeit = Calendar.getInstance();
        final DateTimeFormatter f = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSX").withZone(ZoneOffset.UTC);
        for (int i = 0; i < forecast.size(); i++) {
            final JsonNode time = forecast.path(i).path("dt");
            final JsonNode temp = forecast.path(i).path("main").path("temp");
            zeit.setTimeInMillis(time.asLong() * 1000);
            // observations are given in kelvin and are being converted to celsius
            int k = (int) (temp.asDouble() * 100.0);
            double c = (k - 27315) / 100.0;
            LOGGER.debug("forecast value: " + f.format(zeit.toInstant()) + " = " + temp.toString() + " Kelvin (" + c + " Celsius)");
            final Observation o = new Observation(c, ds);
            o.setPhenomenonTime(ZonedDateTime.parse(f.format(zeit.toInstant())));
            o.setResultTime(now);
            try {
                service.create(o);
                count++;
            }
            catch (final ServiceFailureException ex) {
                Logger.getLogger(ReadOpenWeatherForecast.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        LOGGER.info("new forcast data added: " + count + " new observations");
    }


    /**
     * Remove all exisiting observations from datastream
     *
     * @param ds
     * @throws de.fraunhofer.iosb.ilt.sta.ServiceFailureException
     */
    public void cleanForcastData(final Datastream ds) throws ServiceFailureException {
        
        int count = 0;
        boolean more = true;
        EntityList<Observation> observations;
        while (more) {
            observations = ds.observations().query().top(MAX_ENTRIES).list();
            if (observations != null) {
                final Iterator<Observation> it = observations.fullIterator();
                while (it.hasNext()) {
                    final Observation next = it.next();
                    service.delete(next);
                    count++;
                }
                if (observations.getCount() == 0) {
                    more = false;
                }
            } else {
                throw new ServiceFailureException();
            }
        }
        LOGGER.info("delete old forcast data: " + count + " observations deleted.");
    }


    /**
     * @param args
     * @throws ServiceFailureException
     * @throws URISyntaxException
     * @throws MalformedURLException
     * @throws IOException
     */
    public static void main(final String[] args) throws ServiceFailureException, URISyntaxException, MalformedURLException, IOException {

        props = new Properties();
        try {
            props.load(new FileInputStream("config.properties"));

        }
        catch (final FileNotFoundException e) {
            LOGGER.warn("No properties file found!");
            
            props.setProperty(KARLSRUHE_CITY_ID, "3214104");
            props.setProperty(PARIS_CITY_ID, "3214104");
            props.setProperty(OPEN_WEATHER_API_APPID, "1e12aa36e7c7c42296496089072ef68a");
            props.setProperty(OPEN_WEATHER_API_URL, "http://api.openweathermap.org/data/2.5/");
            props.setProperty(BASE_URL, "http://akme-a3.iosb.fraunhofer.de:80/SensorThingsService/v1.0/");
            props.setProperty(SENSOR_ID, "296");
            props.setProperty(THING_IOSB_KA_ID, "358");
            props.setProperty(TEMPERATURE_ID, "331");
            props.setProperty(IOSB_LOCATION_ID, "296");
            props.setProperty(DATASTREAM_ID, "359");
            props.setProperty(PROXYHOST, "proxy-ka.iosb.fraunhofer.de");
            props.setProperty(PROXYPORT, "80");
            props.setProperty(CLEAN_FORECAST_DATA,"true");


            props.store(new FileOutputStream("config.properties"), "EBITA OpenWeatherData Scanner");
            LOGGER.warn("New file has been created with default values. Please check your correct settings");
            LOGGER.warn(props.toString());
            return;
        }

        final URL baseUri = new URL(props.getProperty(BASE_URL));
        service = new SensorThingsService(baseUri);

        // create OpenWeatherData Sensor for Karlsruhe
        final ReadOpenWeatherForecast reader = new ReadOpenWeatherForecast();

        // get DataStream for CityId = Karlsruhe
        final Datastream ds = reader.getDataStream(props.getProperty(KARLSRUHE_CITY_ID));

        // get forecast data from OpenWeatherMap server
        final JsonNode forecast = reader.readForecastData();

        // remove existing forecast data
        if (props.getProperty(CLEAN_FORECAST_DATA).equalsIgnoreCase("TRUE"))
            reader.cleanForcastData(ds);

        // convert and store forecast data
        reader.storeForecastData(ds, forecast);
    }
}
