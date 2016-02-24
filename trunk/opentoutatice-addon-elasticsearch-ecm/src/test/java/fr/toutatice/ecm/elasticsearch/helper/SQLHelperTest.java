/*
 * (C) Copyright 2014 Académie de Rennes (http://www.ac-rennes.fr/), OSIVIA (http://www.osivia.com) and others.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl-2.1.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 *
 * Contributors:
 *   mberhaut1
 *    
 */
package fr.toutatice.ecm.elasticsearch.helper;

import junit.framework.TestCase;

public class SQLHelperTest extends TestCase {
	
	// simple predicate, operator LIKE
	public void testEscape1() {
		assertEquals("SELECT * FROM Document WHERE dc:title LIKE 'l\\'arche'", 
				SQLHelper.getInstance().escape("SELECT * FROM Document WHERE dc:title LIKE 'l'arche'"));
	}

	// simple predicate, operator ILIKE
	public void testEscape2() {
		assertEquals("SELECT * FROM Document WHERE dc:title ILIKE 'l\\'arche'", 
				SQLHelper.getInstance().escape("SELECT * FROM Document WHERE dc:title ILIKE 'l'arche'"));
	}
	
	// simple predicate, operator =
	public void testEscape3() {
		assertEquals("SELECT * FROM Document WHERE dc:title = 'l\\'arche'", 
				SQLHelper.getInstance().escape("SELECT * FROM Document WHERE dc:title = 'l'arche'"));
	}

	// simple predicate, operator !=
	public void testEscape4() {
		assertEquals("SELECT * FROM Document WHERE dc:title != 'l\\'arche'", 
				SQLHelper.getInstance().escape("SELECT * FROM Document WHERE dc:title != 'l'arche'"));
	}

	// simple predicate, operator <>
	public void testEscape5() {
		assertEquals("SELECT * FROM Document WHERE dc:title <> 'l\\'arche'", 
				SQLHelper.getInstance().escape("SELECT * FROM Document WHERE dc:title <> 'l'arche'"));
	}

	// simple predicate, operator IN
	public void testEscape6() {
		assertEquals("SELECT * FROM Document WHERE dc:title IN ('l\\'arche','le porche','l\\'abris')", 
				SQLHelper.getInstance().escape("SELECT * FROM Document WHERE dc:title IN ('l'arche','le porche','l'abris')"));
	}

	// simple predicate, operator like (lowercase)
	public void testEscape7() {
		assertEquals("SELECT * FROM Document WHERE dc:title like 'l\\'arche'", 
				SQLHelper.getInstance().escape("SELECT * FROM Document WHERE dc:title like 'l'arche'"));
	}

	// simple predicate, operator ilike (lowercase)
	public void testEscape8() {
		assertEquals("SELECT * FROM Document WHERE dc:title ilike 'l\\'arche'", 
				SQLHelper.getInstance().escape("SELECT * FROM Document WHERE dc:title ilike 'l'arche'"));
	}

	// simple predicate, operator in (lowercase)
	public void testEscape9() {
		assertEquals("SELECT * FROM Document WHERE dc:title in ('l\\'arche','le porche','l\\'abris')", 
				SQLHelper.getInstance().escape("SELECT * FROM Document WHERE dc:title in ('l'arche','le porche','l'abris')"));
	}
	
	// multiple predicates (with AND key word used as literal)
	public void testEscape10() {
		assertEquals("SELECT * FROM Document WHERE dc:title LIKE 'l\\'arche' AND dc:subjects IN ('l\\'olivier','le poirier AND l\\'abricotier')", 
				SQLHelper.getInstance().escape("SELECT * FROM Document WHERE dc:title LIKE 'l'arche' AND dc:subjects IN ('l'olivier','le poirier AND l'abricotier')"));
	}

	// multiple predicates (global search like)
	public void testEscape11() {
		assertEquals("SELECT * FROM Document WHERE ecm:fulltext = 'Espace d\\'apprentissage en scolarité -noindex' AND (ecm:isProxy = 1 AND ecm:mixinType != 'HiddenInNavigation'  AND ecm:currentLifeCycleState <> 'deleted' )", 
				SQLHelper.getInstance().escape("SELECT * FROM Document WHERE ecm:fulltext = 'Espace d'apprentissage en scolarité -noindex' AND (ecm:isProxy = 1 AND ecm:mixinType != 'HiddenInNavigation'  AND ecm:currentLifeCycleState <> 'deleted' )"));
	}

	// multiple inner quotes
	public void testEscape12() {
		assertEquals("SELECT * FROM Document WHERE dc:title LIKE 'l\\'\\'arche'", 
				SQLHelper.getInstance().escape("SELECT * FROM Document WHERE dc:title LIKE 'l''arche'"));
	}

	// whitespaces
	public void testEscape13() {
		assertEquals("SELECT * FROM Document WHERE dc:title LIKE 'l \\' arc\\'he'", 
				SQLHelper.getInstance().escape("SELECT * FROM Document WHERE dc:title LIKE 'l ' arc'he'"));
	}

	// whitespaces combined with IN operator
	public void testEscape14() {
		assertEquals("SELECT * FROM Document WHERE dc:title in ('l \\' arche','le cerisier', 'l\\'abricotier')", 
				SQLHelper.getInstance().escape("SELECT * FROM Document WHERE dc:title in ('l \\' arche','le cerisier', 'l'abricotier')"));
	}

	// stupid monkey test
	public void testEscape15() {
		assertEquals("SELECT * FROM Document WHERE dc:title LIKE 'dc:title = 'l\\'arche''", 
				SQLHelper.getInstance().escape("SELECT * FROM Document WHERE dc:title LIKE 'dc:title = 'l'arche''"));
	}

}