/**
 * 
 */
package fr.toutatice.ecm.elasticsearch.compat;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.List;

import javax.ws.rs.ext.MessageBodyReader;

import org.nuxeo.ecm.automation.jaxrs.io.operations.JsonRequestReader;

import fr.toutatice.ecm.platform.core.components.ToutaticeAbstractServiceHandler;


/**
 * @author david
 *
 */
public class CompatAutomationServer<T> extends ToutaticeAbstractServiceHandler<T> {

    @Override
    public T newProxy(T object, Class<T> itf) {
        setObject(object);
        return itf.cast(Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), new Class<?>[] { itf }, this));
    }
    
    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if("getReaders".equals(method.getName())){
            List<Class<? extends MessageBodyReader<?>>> readers = (List<Class<? extends MessageBodyReader<?>>>) method.invoke(super.object, args);
            
            if(readers != null){
                int jrrInd = readers.indexOf(JsonRequestReader.class);
                if(jrrInd != -1){
                    readers.remove(jrrInd);
                    return readers;
                }
            } 
            return readers;
        } else {
            return method.invoke(super.object, args);
        }
    }

}
