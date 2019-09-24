/**
 * 
 */
package org.opentoutatice.core.override.service;

import java.lang.reflect.Field;

/**
 * @author dchevrier <chevrier.david.pro@gmail.com>
 *
 */
// TODO: 
// * instead of Singleton, this life cycle class should be managed in OSGI way
// * use genericity
public class OverriddenTypeBuilder {
	
	private static OverriddenTypeBuilder instance;
	
	private OverriddenTypeBuilder() {
		super();
	}
	
	public static synchronized OverriddenTypeBuilder getInstance() {
		if(instance == null) {
			instance = new OverriddenTypeBuilder();
		}
		return instance;
	}
	
	/*
	 * Can only work on instances at Runtime
	 */
	public Class<?> makeAsInheritable(Class<?> overridenType){
		Class<?> newType = null;
		
		if(overridenType != null) {
			try {
				Field[] declaredFields = overridenType.getDeclaredFields();
				if(declaredFields != null) {
					Field[] privateFields = new Field[0];
					for (Field field : declaredFields) {
						field.getAnnotatedType();
					}
				}
				
			} finally {
				
			}
		}
		
		return newType;
	}

}
