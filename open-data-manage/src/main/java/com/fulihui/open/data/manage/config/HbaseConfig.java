package com.fulihui.open.data.manage.config;

import org.springframework.context.EnvironmentAware;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import org.springframework.core.env.Environment;

/**
 * Created by wtjun on 2017/10/31.
 */
@Configuration
@ImportResource(locations = {"classpath:application-context.xml"})
public class HbaseConfig  implements EnvironmentAware {
    @Override
    public void setEnvironment(Environment environment) {
        System.setProperty("hadoop.home.dir","D:\\linuxSoftgram\\hadoop-2.7.2\\hadoop-2.7.2");
    }
}
