package org.opentoutatice.elasticsearch.config;

import java.io.Serializable;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.common.xmap.annotation.XObject;
import org.nuxeo.elasticsearch.config.ElasticSearchIndexConfig;

/**
 * From now on, Nx works (only) with aliases and configured elasticsearch.indexName points to elsaticsearch alias.
 * So, by convention, elasticsearch.indexName must ends with '-alias'.
 *
 * @author dchevrier <chevrier.david.pro@gmail.com>
 *
 */
@XObject(value = "elasticSearchIndex")
public class OttcElasticSearchIndexOrAliasConfig extends ElasticSearchIndexConfig implements Serializable, Cloneable {

    private static final long serialVersionUID = 4308134337132430655L;

    private static final Log log = LogFactory.getLog(OttcElasticSearchIndexOrAliasConfig.class);

    public static final String NX_ALIAS_SUFFIX = "-alias";

    final public static String DEFAULT_SETTING = "{\n" //
            + "   \"number_of_shards\" : 1,\n" //
            + "   \"number_of_replicas\" : 0,\n" //
            + "   \"analysis\" : {\n" //
            + "      \"filter\" : {\n" //
            + "         \"truncate_filter\" : {\n" //
            + "            \"length\" : 256,\n" //
            + "            \"type\" : \"truncate\"\n" //
            + "         },\n" //
            + "         \"en_stem_filter\" : {\n" //
            + "            \"name\" : \"minimal_english\",\n" //
            + "            \"type\" : \"stemmer\"\n" //
            + "         },\n" //
            + "         \"en_stop_filter\" : {\n" //
            + "            \"stopwords\" : [\n" //
            + "               \"_english_\"\n" //
            + "            ],\n" //
            + "            \"type\" : \"stop\"\n" //
            + "         }\n" //
            + "      },\n" //
            + "      \"tokenizer\" : {\n" //
            + "         \"path_tokenizer\" : {\n" //
            + "            \"delimiter\" : \"/\",\n" //
            + "            \"type\" : \"path_hierarchy\"\n" //
            + "         }\n" + "      },\n" //
            + "      \"analyzer\" : {\n" //
            + "         \"en_analyzer\" : {\n" //
            + "            \"alias\" : \"fulltext\",\n" //
            + "            \"filter\" : [\n" //
            + "               \"lowercase\",\n" //
            + "               \"en_stop_filter\",\n" //
            + "               \"en_stem_filter\",\n" //
            + "               \"asciifolding\"\n" //
            + "            ],\n" //
            + "            \"type\" : \"custom\",\n" //
            + "            \"tokenizer\" : \"standard\"\n" //
            + "         },\n" //
            + "         \"path_analyzer\" : {\n" //
            + "            \"type\" : \"custom\",\n" //
            + "            \"tokenizer\" : \"path_tokenizer\"\n" //
            + "         },\n" //
            + "         \"default\" : {\n" //
            + "            \"type\" : \"custom\",\n" //
            + "            \"tokenizer\" : \"keyword\",\n" //
            + "            \"filter\" : [\n" //
            + "               \"truncate_filter\"\n" //
            + "            ]\n" //
            + "         }\n" //
            + "      }\n" //
            + "   }\n" //
            + "}";

    final public static String DEFAULT_MAPPING = "{\n" //
            + "   \"_all\" : {\n" //
            + "      \"analyzer\" : \"fulltext\"\n" //
            + "   },\n" //
            + "   \"properties\" : {\n" //
            + "      \"dc:title\" : {\n" //
            + "         \"type\" : \"multi_field\",\n" //
            + "         \"fields\" : {\n" //
            + "           \"dc:title\" : {\n" //
            + "             \"type\" : \"string\"\n" //
            + "           },\n" //
            + "           \"fulltext\" : {\n" //
            + "             \"boost\": 2,\n" //
            + "             \"type\": \"string\",\n" //
            + "             \"analyzer\" : \"fulltext\"\n" //
            + "          }\n" //
            + "        }\n" //
            + "      },\n" //
            + "      \"dc:description\" : {\n" //
            + "         \"type\" : \"multi_field\",\n" //
            + "         \"fields\" : {\n" //
            + "           \"dc:description\" : {\n" //
            + "             \"type\" : \"string\"\n" //
            + "           },\n" //
            + "           \"fulltext\" : {\n" //
            + "             \"boost\": 1.5,\n" //
            + "             \"type\": \"string\",\n" //
            + "             \"analyzer\" : \"fulltext\"\n" //
            + "          }\n" //
            + "        }\n" //
            + "      },\n" //
            + "      \"ecm:binarytext\" : {\n" //
            + "         \"type\" : \"string\",\n" //
            + "         \"index\" : \"no\",\n" //
            + "         \"include_in_all\" : true\n" //
            + "      },\n" //
            + "      \"ecm:path\" : {\n" //
            + "         \"type\" : \"multi_field\",\n" //
            + "         \"fields\" : {\n" //
            + "            \"children\" : {\n" //
            + "               \"search_analyzer\" : \"keyword\",\n" //
            + "               \"index_analyzer\" : \"path_analyzer\",\n" //
            + "               \"type\" : \"string\"\n" //
            + "            },\n" //
            + "            \"ecm:path\" : {\n" //
            + "               \"index\" : \"not_analyzed\",\n" //
            + "               \"type\" : \"string\"\n" //
            + "            }\n" //
            + "         }\n" //
            + "      },\n" //
            + "      \"dc:created\": {\n" //
            + "         \"format\": \"dateOptionalTime\",\n" //
            + "        \"type\": \"date\"\n" //
            + "      },\n" //
            + "      \"dc:modified\": {\n" //
            + "         \"format\": \"dateOptionalTime\",\n" //
            + "        \"type\": \"date\"\n" //
            + "      },\n" //
            + "      \"ecm:pos*\" : {\n" //
            + "         \"type\" : \"integer\"\n" //
            + "      }\n" //
            + "   }\n" //
            + "}";

    /**
     * Alias name.
     * Replaces former indexName.
     *
     * @param name
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Explicit method for alias name which is technically same as name attribute.
     *
     * @return configured alias name.
     */
    public String getAliasName() {
        return super.getName();
    }
    
    public String getIndexName() {
        String idxName = null;
        if(this.aliasConfigured()) {
            idxName = StringUtils.split(super.getName(), NX_ALIAS_SUFFIX)[0];
        } else {
            idxName = super.getName();
        }
        return idxName;
    }

    /**
     * Do not create if alias is configured.
     */
    @Override
    public boolean mustCreate() {
        //return this.aliasConfigured() ? false : this.create;
        return this.create;
    }

    public boolean aliasConfigured() {
        return StringUtils.endsWith(this.getName(), NX_ALIAS_SUFFIX);
    }

    @Override
    public OttcElasticSearchIndexOrAliasConfig clone() {
        OttcElasticSearchIndexOrAliasConfig clone = null;
        try {
            // Shallow copy
            clone = (OttcElasticSearchIndexOrAliasConfig) super.clone();
        } catch (CloneNotSupportedException e) {
            clone = new OttcElasticSearchIndexOrAliasConfig(this.isEnabled(), this.getName(), this.getRepositoryName(), this.getType(), this.mustCreate(),
                    this.getSettings(), this.getMapping(), this.getExcludes(), this.getIncludes());
        }

        // Deep copy (mutable classes)
        clone.setExcludes(this.getExcludes().clone());
        clone.setIncludes(this.getIncludes().clone());

        return clone;
    }

    public OttcElasticSearchIndexOrAliasConfig() {
        super();
    }

    /**
     * @param isEnabled
     * @param name
     * @param repositoryName
     * @param type
     * @param create
     * @param settings
     * @param mapping
     * @param excludes
     * @param includes
     */
    public OttcElasticSearchIndexOrAliasConfig(boolean isEnabled, String name, String repositoryName, String type, boolean create, String settings,
            String mapping, String[] excludes, String[] includes) {
        super();
        this.isEnabled = isEnabled;
        this.name = name;
        this.repositoryName = repositoryName;
        this.type = type;
        this.create = create;
        this.settings = settings;
        this.mapping = mapping;
        this.excludes = excludes;
        this.includes = includes;
    }

    protected void setExcludes(String[] excludes) {
        this.excludes = excludes;
    }

    protected void setIncludes(String[] includes) {
        this.includes = includes;
    }

}