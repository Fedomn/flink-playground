import net.datafaker.Faker;

import java.util.concurrent.TimeUnit;

public class FakerTest {
  @org.junit.jupiter.api.Test
  void internet() {
    Faker faker = new Faker();
    System.out.println(faker.internet().userAgent());
    var date = faker.date().past(15, 5, TimeUnit.SECONDS);
    System.out.println(date);
  }
}
