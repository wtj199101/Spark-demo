package cn.spark;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Created by Administrator on 2017/06/23.
 */
@SpringBootApplication
public class MyBootApplication {
    public static void main(String[] args) {
      new SpringApplication(MyBootApplication.class).run(args);
    }
}
