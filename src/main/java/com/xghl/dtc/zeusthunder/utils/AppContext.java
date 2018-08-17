package com.xghl.dtc.zeusthunder.utils;


import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

/**
 * @author luodf
 * @description:bean工厂获取
 * @since 2017/08/01
 */
public class AppContext implements ApplicationContextAware {
    


    private static ApplicationContext applicationContext;
    
    private  static AnnotationConfigApplicationContext context=null;
    

    @Override
	public void setApplicationContext(ApplicationContext applicationContext)
            throws BeansException {
        AppContext.applicationContext = applicationContext;
    }


    public static Object getBean(String beanName) {
        if (null == beanName) {
            return null;
        }
        return applicationContext.getBean(beanName);
    }


    public void initialized() {

    }
    
    
   
	   public void init()
	   {
//		    context = new AnnotationConfigApplicationContext(EventConfig.class);
//		    context.setParent(applicationContext);
//		    eventProducer = context.getBean(EventProducer.class);
	   }
	   
//	   public static void  sendEvent(DtcMessageEvent event)
//	   {
//		   eventProducer.send(event);
//	   }
//	   
//	   public static void  sendEvent(String topic, Object content)
//	   {
//		   eventProducer.send( topic,  content);
//	   }
//	   
	   

}

