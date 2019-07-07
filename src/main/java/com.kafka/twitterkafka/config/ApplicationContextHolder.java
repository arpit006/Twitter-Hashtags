package com.kafka.twitterkafka.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

/**
 * @author <a href = "mailto: iarpitsrivastava06@gmail.com"> Arpit Srivastava</a>
 */
@Component
public class ApplicationContextHolder {

    private static ApplicationContextHolder INSTANCE;

    @Autowired
    public ApplicationContext applicationContext;

    public void initialize() {
        INSTANCE = this;
    }

    public static<T> T getBean(Class<T> clazz)  {
        return INSTANCE.applicationContext.getBean(clazz);
    }

    public Object getBean(String inBeanName) {
        return INSTANCE.applicationContext.getBean(inBeanName);
    }

}
