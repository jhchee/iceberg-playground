package github.jhchee.util;

import com.github.javafaker.Faker;
import org.apache.spark.sql.api.java.UDF0;

import java.util.UUID;

public class MockUtil {
    private static Faker faker = new Faker();

    public static class RandomUUID implements UDF0<String> {
        public RandomUUID() {
        }

        @Override
        public String call() throws Exception {
            return UUID.randomUUID().toString();
        }
    }

    public static class RandomName implements UDF0<String> {

        public RandomName() {
        }

        @Override
        public String call() throws Exception {
            return faker.name().name();
        }
    }

    public static class RandomNationality implements UDF0<String> {

        public RandomNationality() {
        }

        @Override
        public String call() throws Exception {
            return faker.nation().nationality();
        }
    }
}
