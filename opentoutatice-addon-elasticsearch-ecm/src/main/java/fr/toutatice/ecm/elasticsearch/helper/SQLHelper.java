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
 * mberhaut1
 *
 */
package fr.toutatice.ecm.elasticsearch.helper;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

/**
 * A singleton Java pattern is used since this operation might be called very often. Hence, it is important to get
 * regex patterns initialized & compiled only once.
 *
 */
public class SQLHelper {

    private Pattern predicatesPattern;
    private Pattern expressionPattern;
    private Pattern escapePattern;

    private SQLHelper() {
        this.predicatesPattern = Pattern.compile(".+WHERE\\s(.+)");
        this.expressionPattern = Pattern.compile("^[^']+'(.+)'[^']*$");
        this.escapePattern = Pattern.compile("(\\w(?:\\w*\\s*'+\\s*\\w+)+\\w)");
    }

    private static SQLHelper INSTANCE = new SQLHelper();

    // unique access point of the singleton
    public static SQLHelper getInstance() {
        return INSTANCE;
    }

    public String escape(String query) {
        String escapedQuery = query;

        // extract the predicates from the query
        Matcher m = this.predicatesPattern.matcher(query);
        if (m.matches()) {
            String[] predicates = m.group(1).split("\\w+:\\w+");
            for (String predicate : predicates) {
                // Predicate <identifier> [NOT] BETWEEN <literal> AND <literal>
                // is the only one which must not be escaped
                if (StringUtils.isNotBlank(predicate) && !StringUtils.containsIgnoreCase(predicate, "BETWEEN")) {
                    // extract the expression (being compared to the metadata) from each predicate
                    Matcher mexp = this.expressionPattern.matcher(predicate);
                    if (mexp.matches()) {
                        String expression = mexp.group(1);
                        Matcher escpm = this.escapePattern.matcher(expression);
                        // escape any simple quoted word
                        while (escpm.find()) {
                            String item = escpm.group(1);
                            escapedQuery = escapedQuery.replace(item, item.replace("'", "\\'"));
                        }
                    }
                }
            }
        }

        return escapedQuery;
    }

}
